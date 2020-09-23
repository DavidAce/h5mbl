#include "general/class_tic_toc.h"
#include "io/nmspc_logger.h"
#include <getopt.h>
#include <h5pp/h5pp.h>
#include <iostream>
#include <string>


void print_usage() {
    std::cout <<
        R"(
==========  cpp_merger  ============
Usage                       : ./cpp_merger [-option <value>].
-h                          : Help. Shows this text.
-s <src_dir>                : Root directory for the output files
-t <tgt_dir>                : Target directory for the single output file
-f <tgt_fname>              : Target filename (default merged.h5)
-v <level>                  : Enables trace-level verbosity
)";
}

template<typename T>
void move_dset(h5pp::File &h5_tgt, const h5pp::File &h5_src, h5pp::DsetInfo &tgtInfo, h5pp::DsetInfo &srcInfo) {
    auto data = h5_src.readDataset<T>(srcInfo);
    h5_tgt.appendToDataset(data, tgtInfo, 1, {data.size(), 1});
}


double compute_renyi(const std::vector<std::complex<double>> &S, double q) {
    using Scalar               = std::complex<double>;
    auto                     L = Eigen::TensorMap<const Eigen::Tensor<const Scalar, 1>>(S.data(), S.size());
    Eigen::Tensor<Scalar, 0> renyi_q;
    if(q == 1.0) {
        renyi_q = -L.square().contract(L.square().log().eval(), h5pp::eigen::idx({0}, {0}));
    } else {
        renyi_q = (1.0 / 1.0 - q) * L.pow(2.0 * q).sum().log();
    }
    return std::real(renyi_q(0));
}

int main(int argc, char *argv[]) {
    // Here we use getopt to parse CLI input
    // Note that CLI input always override config-file values
    // wherever they are found (config file, h5 file)
    auto log = Logger::setLogger("cpp_merger", 2);
    log->info("Started cpp_merger from directory {}", h5pp::fs::current_path());
    std::vector<h5pp::fs::path> src_dirs;
    h5pp::fs::path tgt_dir;
    std::string    tgt_file  = "merged.h5";
    size_t         verbosity = 2;
    size_t         max_files = 2000000;
    while(true) {
        char opt = static_cast<char>(getopt(argc, argv, "hs:t:v"));
        if(opt == EOF) break;
        if(optarg == nullptr) log->info("Parsing input argument: -{}", opt);
        else
            log->info("Parsing input argument: -{} {}", opt, optarg);
        switch(opt) {
            case 'f': tgt_file = std::string(optarg); continue;
            case 's': src_dirs.push_back(h5pp::fs::canonical(optarg)); continue;
            case 't': tgt_dir = h5pp::fs::canonical(optarg); continue;
            case 'v': verbosity = std::strtoul(optarg, nullptr, 10); continue;
            case ':': log->error("Option -{} needs a value", opt); break;
            case 'h':
            case '?':
            default: print_usage(); exit(0);
            case -1: break;
        }
        break;
    }
    Logger::setLogLevel(log, verbosity);
    log->set_level(spdlog::level::info);

    for(auto & src_dir : src_dirs)
        if(not h5pp::fs::is_directory(src_dir)) throw std::runtime_error(fmt::format("Given source is not a directory: {}", src_dir.string()));

    h5pp::fs::path tgt_path = tgt_dir / tgt_file;
    h5pp::File     h5_tgt(tgt_path, h5pp::FilePermission::REPLACE, 2);
    h5_tgt.setDriver_core(true);
    h5_tgt.setCompressionLevel(4);

    // Define the allowed items
    enum class Type { INT, LONG, DOUBLE, COMPLEX };
    enum class Size { FIX, VAR };
    struct DsetMeta {
        std::string name;
        Type        type;
        Size        size = Size::FIX;
    };

    std::string              algo_key  = "DMRG";
    std::string              state_key = "state_";
    std::string              point_key = "finished";
    std::vector<std::string> tables    = {"measurements", "profiling", "status", "mem_usage"};
    //    std::vector<std::string> dsets = {"bond_dimensions", "entanglement_entropies", "truncation_errors"};
    std::vector<DsetMeta> dsets = {{"bond_dimensions", Type::LONG},
                                   {"entanglement_entropies", Type::DOUBLE},
                                   {"truncation_errors", Type::DOUBLE},
                                   {"schmidt_midchain", Type::COMPLEX, Size::VAR}};

    DsetMeta bonds{"L_", Type::COMPLEX, Size::VAR};

    class_tic_toc                                    t_tot(true, 5, "Total  time");
    class_tic_toc                                    t_pre(true, 5, "Pre    time");
    class_tic_toc                                    t_itr(true, 5, "Iter   time");
    class_tic_toc                                    t_tab(true, 5, "Table  time");
    class_tic_toc                                    t_grp(true, 5, "Group  time");
    class_tic_toc                                    t_get(true, 5, "Get    time");
    class_tic_toc                                    t_dst(true, 5, "Dset   time");
    class_tic_toc                                    t_ren(true, 5, "Renyi  time");
    class_tic_toc                                    t_ren_col(true, 5, "Renyi collect time");
    class_tic_toc                                    t_ren_app(true, 5, "Renyi append  time");
    class_tic_toc                                    t_crt(true, 5, "Create time");
    class_tic_toc                                    t_ham(true, 5, "Model  time");
    std::unordered_map<std::string, h5pp::TableInfo> tgtTableInfoMap;
    std::unordered_map<std::string, h5pp::DsetInfo>  tgtDsetInfoMap;
    t_tot.tic();
    size_t num_files = 0;
    using reciter    = h5pp::fs::recursive_directory_iterator;
    std::vector<h5pp::fs::path> recfiles;
    for(auto & src_dir : src_dirs){
        copy(reciter(src_dir), reciter(), back_inserter(recfiles));
    }
    std::sort(recfiles.begin(), recfiles.end());
    for(const auto &src_item : recfiles) {
        if(num_files > max_files) break;
        t_itr.tic();
        const auto &src_abs = src_item;
        if(not src_abs.has_filename()) {
            t_itr.toc();
            continue;
        }
        if(src_abs.extension() != ".h5") {
            t_itr.toc();
            continue;
        }
        t_itr.toc();
        t_pre.tic();

        // Check which source root this belongs to
        h5pp::fs::path src_dir;
        for(auto & src_can : src_dirs){
            auto[it1, it2] = std::mismatch(src_can.begin(), src_can.end(), src_abs.begin());
            if(it1 == src_can.end()) {src_dir = src_can;break;}
        }
        if(src_dir.empty()) throw std::runtime_error("Could not infer root src_dir from src_abs");


        auto src_rel  = h5pp::fs::relative(src_abs, src_dir);
        auto src_base = src_rel.parent_path();
        log->info("Found path: {}", src_abs.string(), src_rel.string());
        h5pp::File               h5_src;
        std::vector<std::string> groups;
        try {
            h5_src = h5pp::File(src_abs.string(), h5pp::FilePermission::READONLY);
            t_pre.toc();
            // Start finding the required components in the source
            t_grp.tic();
            groups = h5_src.findGroups(algo_key, "/", -1, 0);
            t_grp.toc();
        } catch(const std::exception &ex) {
            log->warn("Skipping broken file: {}\n", src_abs.string());
            continue;
        }
        num_files++;
        static std::unordered_map<std::string, h5pp::TableInfo> srcTableInfoMap;
        static std::unordered_map<std::string, h5pp::DsetInfo>  srcDsetInfoMap;

        for(const auto &algo : groups) {
            // Copy the hamiltonian
            {
                t_ham.tic();
                const std::string srcTablePath = fmt::format("{}/model/hamiltonian", algo);
                const std::string tgtTablePath = (src_base / srcTablePath).string();
                if(tgtDsetInfoMap.find(tgtTablePath) == tgtDsetInfoMap.end() and h5_src.linkExists(srcTablePath)) {
                    auto model_size   = h5_src.readAttribute<size_t>("model_size", srcTablePath);
                    auto model_type   = h5_src.readAttribute<std::string>("model_type", srcTablePath);
                    auto distribution = h5_src.readAttribute<std::string>("distribution", srcTablePath);
                    auto J_mean       = h5_src.readAttribute<double>("J_mean", srcTablePath);
                    auto J_stdv       = h5_src.readAttribute<double>("J_stdv", srcTablePath);
                    auto h_mean       = h5_src.readAttribute<double>("h_mean", srcTablePath);
                    auto h_stdv       = h5_src.readAttribute<double>("h_stdv", srcTablePath);
                    auto lambda       = h5_src.readAttribute<double>("lambda", srcTablePath);
                    auto delta        = h5_src.readAttribute<double>("delta", srcTablePath);
                    h5_tgt.writeDataset("Hamiltonian parameters", tgtTablePath);
                    h5_tgt.writeDataset(model_size, fmt::format("{}/{}/model/model_size", src_base.string(), algo));
                    h5_tgt.writeDataset(model_type, fmt::format("{}/{}/model/model_type", src_base.string(), algo));
                    h5_tgt.writeDataset(J_mean, fmt::format("{}/{}/model/J_mean", src_base.string(), algo));
                    h5_tgt.writeDataset(J_stdv, fmt::format("{}/{}/model/J_stdv", src_base.string(), algo));
                    h5_tgt.writeDataset(h_mean, fmt::format("{}/{}/model/h_mean", src_base.string(), algo));
                    h5_tgt.writeDataset(h_stdv, fmt::format("{}/{}/model/h_stdv", src_base.string(), algo));
                    h5_tgt.writeDataset(lambda, fmt::format("{}/{}/model/lambda", src_base.string(), algo));
                    h5_tgt.writeDataset(delta, fmt::format("{}/{}/model/delta", src_base.string(), algo));
                    h5_tgt.writeDataset(distribution, fmt::format("{}/{}/model/distribution", src_base.string(), algo));
                    tgtDsetInfoMap[tgtTablePath] = h5_tgt.getDatasetInfo(tgtTablePath);
                }
                t_ham.toc();
            }
            t_grp.tic();
            auto state_groups = h5_src.findGroups(state_key, algo, -1, 0);
            t_grp.toc();
            for(const auto &state : state_groups) {
                t_grp.tic();
                auto point_groups = h5_src.findGroups(point_key, fmt::format("{}/{}", algo, state), -1, 0);
                t_grp.toc();

                for(const auto &point : point_groups) {
                    // Copy the tables
                    for(const auto &table : tables) {
                        t_get.tic();
                        auto srcTablePath = fmt::format("{}/{}/{}/{}", algo, state, point, table);
                        auto srcTableKey  = fmt::format("{}|{}", src_base.string(), srcTablePath);
                        if(srcTableInfoMap.find(srcTableKey) == srcTableInfoMap.end()) {log->info("Adding key {}",srcTableKey); srcTableInfoMap[srcTableKey] = h5_src.getTableInfo(srcTablePath);}
                        auto &srcInfo = srcTableInfoMap[srcTableKey];
                        h5pp::Options options;
                        options.linkPath = srcTablePath;
                        srcInfo.tableDset = std::nullopt;
                        srcInfo.tableFile = std::nullopt;
                        srcInfo.numRecords = std::nullopt;
                        srcInfo.tableExists = std::nullopt;
                        h5pp::scan::fillTableInfo(srcInfo,h5_src.openFileHandle(),options);

                        t_get.toc();
                        if(not srcInfo.tableExists.value()) continue;
                        t_crt.tic();
                        auto tgtTablePath = (src_base / srcTablePath).string();
                        if(tgtTableInfoMap.find(tgtTablePath) == tgtTableInfoMap.end())
                            tgtTableInfoMap[tgtTablePath] = h5_tgt.createTable(srcInfo.tableType.value(), tgtTablePath, table);

                        auto &tgtInfo = tgtTableInfoMap[tgtTablePath];
                        t_crt.toc();

                        t_tab.tic();
                        h5_tgt.appendTableRecords(srcInfo, h5pp::TableSelection::LAST, tgtInfo);
                        t_tab.toc();
                    }
                    // Copy the datasets
                    for(const auto &dset : dsets) {
                        t_get.tic();
                        auto srcDsetPath = fmt::format("{}/{}/{}/{}", algo, state, point, dset.name);
                        auto srcDsetKey  = fmt::format("{}|{}", src_base.string(), srcDsetPath);
                        if(srcDsetInfoMap.find(srcDsetKey) == srcDsetInfoMap.end()){ log->info("Adding key {}",srcDsetKey);srcDsetInfoMap[srcDsetKey] = h5_src.getDatasetInfo(srcDsetPath);}
                        auto &srcInfo = srcDsetInfoMap[srcDsetKey];
                        h5pp::Options options;
                        options.linkPath = srcDsetPath;
                        srcInfo.h5Dset = std::nullopt;
                        srcInfo.h5File = std::nullopt;
                        srcInfo.h5Space = std::nullopt;
                        srcInfo.dsetExists = std::nullopt;
                        srcInfo.dsetSize = std::nullopt;
                        srcInfo.dsetDims = std::nullopt;
                        srcInfo.dsetByte = std::nullopt;
                        h5pp::scan::fillDsetInfo(srcInfo,h5_src.openFileHandle(),options);
                        t_get.toc();
                        if(not srcInfo.dsetExists.value()) continue;
                        t_crt.tic();
                        auto tgtDsetPath = (src_base / srcDsetPath).string();
                        if(tgtDsetInfoMap.find(tgtDsetPath) == tgtDsetInfoMap.end()) {
                            long rows = 0;
                            long cols = 0;
                            switch(dset.size) {
                                case Size::FIX: rows = static_cast<long>(srcInfo.dsetDims.value()[0]); break;
                                case Size::VAR: {
                                    std::string statusTablePath = fmt::format("{}/{}/{}/status", algo, state, point);
                                    rows                        = h5_src.readTableField<long>(statusTablePath, "cfg_chi_lim_max", h5pp::TableSelection::FIRST);
                                    break;
                                }
                            }
                            tgtDsetInfoMap[tgtDsetPath] = h5_tgt.createDataset(srcInfo.h5Type, tgtDsetPath, {rows, cols}, H5D_CHUNKED, {rows, 100l});
                        }

                        auto &tgtInfo = tgtDsetInfoMap[tgtDsetPath];
                        t_crt.toc();
                        t_dst.tic();

                        switch(dset.type) {
                            case Type::DOUBLE: move_dset<std::vector<double>>(h5_tgt, h5_src, tgtInfo, srcInfo); break;
                            case Type::LONG: move_dset<std::vector<long>>(h5_tgt, h5_src, tgtInfo, srcInfo); break;
                            case Type::INT: move_dset<std::vector<int>>(h5_tgt, h5_src, tgtInfo, srcInfo); break;
                            case Type::COMPLEX: move_dset<std::vector<std::complex<double>>>(h5_tgt, h5_src, tgtInfo, srcInfo); break;
                        }

                        // Append attributes
                        t_dst.toc();
                    }



                    // Compute renyi entropies from bond data
//                    t_ren.tic();
//
//                    auto                  bond_root  = fmt::format("{}/{}/{}/mps", algo, state, point);
//                    auto                  bond_paths = h5_src.findDatasets(bonds.name, bond_root, -1, 0);
//                    auto                  center_pos = h5_src.readAttribute<size_t>("position", bond_root);
//                    h5pp::Options         options;
//                    static h5pp::DsetInfo bondInfo;
//                    std::vector<double>   renyi_1(bond_paths.size());
//                    std::vector<double>   renyi_2(bond_paths.size());
//                    std::vector<double>   renyi_3(bond_paths.size());
//                    std::vector<double>   renyi_4(bond_paths.size());
//                    for(const auto &bond_name : bond_paths) {
//                        auto bond_path = bond_root + "/" + bond_name;
//
//                        // The srcInfo only changes its path, position and dimensions but we need to reset some extra meta data
//                        options.linkPath           = bond_path;
//                        bondInfo.dsetDims          = std::nullopt;
//                        bondInfo.dsetByte          = std::nullopt;
//                        bondInfo.dsetSize          = std::nullopt;
//                        bondInfo.h5Space           = std::nullopt;
//                        bondInfo.h5Dset            = std::nullopt;
//                        bondInfo.h5File            = std::nullopt;
//                        bondInfo.h5Group           = std::nullopt;
//                        bondInfo.h5ObjLoc          = std::nullopt;
//                        bondInfo.h5PlistDsetAccess = std::nullopt;
//                        bondInfo.h5PlistDsetCreate = std::nullopt;
//                        t_ren_app.tic();
//                        h5pp::scan::fillDsetInfo(bondInfo, h5_src.openFileHandle(), options);
//                        t_ren_app.toc();
//                        auto bond_dset = h5_src.readDataset<std::vector<std::complex<double>>>(bondInfo, options);
//                        t_ren_col.tic();
//                        auto bond_pos = h5_src.readAttribute<size_t>("position", bond_path);
//                        auto bond_idx = bond_pos > center_pos or bond_name == "L_C" ? bond_pos + 1 : bond_pos;
//                        t_ren_col.toc();
//
//                        renyi_1[bond_idx] = compute_renyi(bond_dset, 1);
//                        renyi_2[bond_idx] = compute_renyi(bond_dset, 2);
//                        renyi_3[bond_idx] = compute_renyi(bond_dset, 3);
//                        renyi_4[bond_idx] = compute_renyi(bond_dset, 4);
//                    }
//
//                    int q_count = 1;
//                    for(const auto &renyi : {renyi_1, renyi_2, renyi_3, renyi_4}) {
//                        auto tgtDsetPath = (src_base / fmt::format("{}/{}/{}/renyi/renyi_{}", algo, state, point, q_count++)).string();
//                        if(tgtDsetInfoMap.find(tgtDsetPath) == tgtDsetInfoMap.end()) {
//                            long           rows         = static_cast<long>(renyi.size());
//                            long           cols         = 0;
//                            h5pp::hid::h5t type         = H5Tcopy(H5T_NATIVE_DOUBLE);
//                            tgtDsetInfoMap[tgtDsetPath] = h5_tgt.createDataset(type, tgtDsetPath, {rows, cols}, H5D_CHUNKED, {rows, 100l});
//                        }
//
//                        auto &tgtInfo = tgtDsetInfoMap[tgtDsetPath];
//                        h5_tgt.appendToDataset(renyi, tgtInfo, 1, {renyi.size(), 1});
//                    }
//                    t_ren.toc();
                }
            }
        }
    }
    t_tot.toc();
    t_itr.print_measured_time();
    t_pre.print_measured_time();
    t_grp.print_measured_time();
    t_get.print_measured_time();
    t_tab.print_measured_time();
    t_dst.print_measured_time();
    t_ren.print_measured_time();
    t_ren_col.print_measured_time();
    t_ren_app.print_measured_time();
    t_crt.print_measured_time();
    t_ham.print_measured_time();
    fmt::print("Added  time: {:.5}\n", t_itr.get_measured_time() + t_pre.get_measured_time() + t_grp.get_measured_time() + t_get.get_measured_time() +
                                           t_tab.get_measured_time() + t_dst.get_measured_time() +  t_ren.get_measured_time() + t_ren_col.get_measured_time() +
                                           t_ren_app.get_measured_time() + t_crt.get_measured_time() + t_ham.get_measured_time());
    // Around 15 % overhead comes from closing the h5f, h5d and h5t members of DsetInfo and TableInfo objects
    t_tot.print_measured_time();
}