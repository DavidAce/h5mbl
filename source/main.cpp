#include <general/class_tic_toc.h>
#include <general/enums.h>
#include <general/prof.h>
#include <getopt.h>
#include <gitversion.h>
#include <h5pp/h5pp.h>
#include <io/find.h>
#include <io/h5db.h>
#include <io/h5dbg.h>
#include <io/h5io.h>
#include <io/hash.h>
#include <io/id.h>
#include <io/logger.h>
#include <io/meta.h>
#include <io/parse.h>
#include <iostream>
#include <string>
void print_usage() {
    std::cout <<
        R"(
==========  cpp_merger  ============
Usage                       : ./cpp_merger [-option <value>].
-h                          : Help. Shows this text.
-b <base_dir>               : Default base directory (default /mnt/Barracuda/Projects/mbl_transition)
-f                          : Require that src file has finished
-m <max files>              : Max number of files to merge (default = 10000)
-n <tgt_filename>           : Target filename (default merged.h5)
-o <src_out>                : Name of the output directory inside the root directory (default = "")
-s <src_dir>                : Root directory for the output files. Supports pattern matching for relative paths.
-t <tgt_dir>                : Target directory for the single output file
-v <level>                  : Enables verbosity. Default level 2 (0 is max verbosity)
-V <level>                  : Enables trace-level verbosity of h5pp. Default level 2 (0 is max verbosity)
)";
}

template<typename T>
void append_dset(h5pp::File &h5_tgt, const h5pp::File &h5_src, h5pp::DsetInfo &tgtInfo, h5pp::DsetInfo &srcInfo) {
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

    tools::logger::log = tools::logger::setLogger("h5mbl", 2);
    tools::prof::init();
    h5pp::fs::path              default_base = h5pp::fs::canonical("/mnt/Barracuda/Projects/mbl_transition");
    std::vector<h5pp::fs::path> src_dirs;
    std::string                 src_out  = "output";
    std::string                 tgt_file = "merged.h5";
    h5pp::fs::path              tgt_dir;
    bool                        finished       = false;
    size_t                      verbosity      = 2;
    size_t                      verbosity_h5pp = 2;
    size_t                      max_files      = 1000000;
    size_t                      max_dirs       = 10000;
    long                        seed_min       = 0;
    long                        seed_max       = 100000000;

    while(true) {
        char opt = static_cast<char>(getopt(argc, argv, "hb:fm:n:o:s:t:v:V:"));
        if(opt == EOF) break;
        if(optarg == nullptr)
            tools::logger::log->info("Parsing input argument: -{}", opt);
        else
            tools::logger::log->info("Parsing input argument: -{} {}", opt, optarg);
        switch(opt) {
            case 'b': default_base = h5pp::fs::canonical(optarg); continue;
            case 'f': finished = true; continue;
            case 'm': max_files = std::strtoul(optarg, nullptr, 10); continue;
            case 'n': tgt_file = std::string(optarg); continue;
            case 'o': src_out = std::string(optarg); continue;
            case 's': {
                h5pp::fs::path src_dir = optarg;
                if(src_dir.is_relative()) {
                    auto matching_dirs = tools::io::find_dir<false>(default_base, src_dir.string(), src_out);
                    if(matching_dirs.size() > 5) {
                        std::string error_msg = h5pp::format("Too many directories match the pattern {}:\n", (default_base / src_dir).string());
                        for(auto &dir : matching_dirs) error_msg.append(dir.string() + "\n");
                        throw std::runtime_error(error_msg);
                    }
                    if(matching_dirs.empty()) throw std::runtime_error(h5pp::format("No directories match the pattern: {}", (default_base / src_dir).string()));
                    for(auto &match : matching_dirs) src_dirs.emplace_back(match);
                } else
                    src_dirs.emplace_back(h5pp::fs::canonical(src_dir));
                continue;
            }
            case 't': tgt_dir = h5pp::fs::canonical(optarg); continue;
            case 'v': verbosity = std::strtoul(optarg, nullptr, 10); continue;
            case 'V': verbosity_h5pp = std::strtoul(optarg, nullptr, 10); continue;
            case ':': throw std::runtime_error(fmt::format("Option -{} needs a value", opt)); break;
            case 'h':
            case '?':
            default: print_usage(); exit(0);
            case -1: break;
        }
        break;
    }
    tools::logger::setLogLevel(tools::logger::log, verbosity);
    tools::logger::log->info("Started h5mbl from directory {}", h5pp::fs::current_path());
    if(src_dirs.empty()) throw std::runtime_error("Source directories are required. Pass -s <dirpath> (one or more times)");
    for(auto &src_dir : src_dirs) {
        if(not h5pp::fs::is_directory(src_dir)) throw std::runtime_error(fmt::format("Given source is not a directory: {}", src_dir.string()));
        tools::logger::log->info("Found source directory {}", src_dir.string());
    }
    if(tgt_dir.empty()) throw std::runtime_error("A target directory is required. Pass -t <dirpath>");
    h5pp::fs::path tgt_path = tgt_dir / tgt_file;
    tools::logger::log->info("Merge into target file {}", tgt_path.string());


//    using ModelT = sdual;
    using ModelT = lbit;
    std::vector<std::string> algo_keys, state_keys,point_keys, models,tables,cronos;
    std::vector<DsetKey> dsets;
    DsetKey bonds;
    if constexpr(std::is_same_v<ModelT,sdual>){
        algo_keys  = {"xDMRG"};
        state_keys = {"state_*"};
        point_keys = {"finished", "checkpoint/iter_*"};
        models = {"hamiltonian"};
        tables = {"measurements", "profiling", "status", "mem_usage"};
        cronos = {};
        //    std::vector<std::string> dsets = {"bond_dimensions", "entanglement_entropies", "truncation_errors"};

        dsets = {{Type::LONG, Size::FIX, "bond_dimensions", ""},
                 {Type::DOUBLE, Size::FIX, "entanglement_entropies", ""},
                 {Type::DOUBLE, Size::FIX, "truncation_errors", ""},
                 {Type::COMPLEX, Size::VAR, "schmidt_midchain", ""}};
        bonds = DsetKey{Type::COMPLEX, Size::VAR, "L_", ""};
    }else if constexpr(std::is_same_v<ModelT,lbit>){
        algo_keys  = {"fLBIT"};
        state_keys = {"state_*"};
        point_keys = {"finished", "checkpoint/iter_*"};
        models = {"hamiltonian"};
        tables = {"profiling", "status", "mem_usage"};
        cronos = {"measurements", "status"};
        dsets = {{Type::LONG, Size::FIX, "bond_dimensions", ""},
                 {Type::DOUBLE, Size::FIX, "entanglement_entropies", ""},
                 {Type::DOUBLE, Size::FIX, "number_entropies", ""},
                 {Type::DOUBLE, Size::FIX, "truncation_errors", ""},
                 {Type::COMPLEX, Size::VAR, "schmidt_midchain", ""}};
        bonds = DsetKey{Type::COMPLEX, Size::VAR, "L_", ""};
    }




    // Open the target file

    h5pp::File h5_tgt(tgt_path, h5pp::FilePermission::READWRITE, verbosity_h5pp);
    //    h5_tgt.setDriver_core();
    h5_tgt.setCompressionLevel(3);
    //    h5_tgt.setKeepFileOpened();
    if(not h5_tgt.linkExists("git/h5mbl")) {
        // Put git metadata in the target file
        h5_tgt.writeDataset(GIT::BRANCH, "git/h5mbl/branch");
        h5_tgt.writeDataset(GIT::COMMIT_HASH, "git/h5mbl/commit");
        h5_tgt.writeDataset(GIT::REVISION, "git/h5mbl/revision");
    }

    auto tgtFileDb  = tools::h5db::loadFileDatabase(h5_tgt); // This database maps  src_name <--> FileId
    auto tgtModelDb = tools::h5db::loadDatabase<h5pp::TableInfo>(h5_tgt, models);
    auto tgtTableDb = tools::h5db::loadDatabase<h5pp::TableInfo>(h5_tgt, tables);
    auto tgtDsetDb  = tools::h5db::loadDatabase<h5pp::DsetInfo>(h5_tgt, dsets);

    tools::prof::t_tot.tic();
    //    size_t                                  num_files = 0;
    std::unordered_map<std::string, size_t> file_counter;
    using reciter = h5pp::fs::recursive_directory_iterator;
    std::vector<h5pp::fs::path> recfiles;
    for(auto &src_dir : src_dirs) { copy(reciter(src_dir), reciter(), back_inserter(recfiles)); }
    std::sort(recfiles.begin(), recfiles.end());
    for(const auto &src_item : recfiles) {
        //        if(num_files > max_files) break;
        tools::prof::t_itr.tic();
        const auto &src_abs = src_item;
        if(not src_abs.has_filename()) {
            tools::prof::t_itr.toc();
            continue;
        }
        if(src_abs.extension() != ".h5") {
            tools::prof::t_itr.toc();
            continue;
        }
        if(not src_out.empty() and src_abs.string().find(src_out) == std::string::npos) {
            tools::prof::t_itr.toc();
            continue;
        }

        //        if(src_abs.string().find("L_20") != std::string::npos) {
        //            tools::prof::t_itr.toc();
        //            continue;
        //        }
        //        if(src_abs.string().find("l_0.0000") != std::string::npos) {
        //            tools::prof::t_itr.toc();
        //            continue;
        //        }
        //        if(src_abs.string().find("l_0.0050") != std::string::npos) {
        //            tools::prof::t_itr.toc();
        //            continue;
        //        }
        //        if(src_abs.string().find("l_0.0100") != std::string::npos) {
        //            tools::prof::t_itr.toc();
        //            continue;
        //        }
        //        if(src_abs.string().find("l_0.0150") != std::string::npos) {
        //            tools::prof::t_itr.toc();
        //            continue;
        //        }
        //        if(src_abs.string().find("d_0.0488") != std::string::npos) {
        //            tools::prof::t_itr.toc();
        //            continue;
        //        }
        //        if(src_abs.string().find("d_+0.0500") != std::string::npos) {
        //            tools::prof::t_itr.toc();
        //            continue;
        //        }

        tools::prof::t_itr.toc();
        tools::prof::t_pre.tic();

        // Check which source root this belongs to
        // TODO: What does this do? Expand comment
        h5pp::fs::path src_dir;
        for(auto &src_can : src_dirs) {
            auto [it1, it2] = std::mismatch(src_can.begin(), src_can.end(), src_abs.begin());
            if(it1 == src_can.end()) {
                src_dir = src_can;
                break;
            }
        }
        if(src_dir.empty()) throw std::runtime_error("Could not infer root src_dir from src_abs");

        auto src_rel  = h5pp::fs::relative(src_abs, src_dir);
        auto src_base = src_rel.parent_path();

        bool counter_exists = file_counter.find(src_base) != file_counter.end();
        if(not counter_exists) {
            if(file_counter.size() >= max_dirs) {
                if(tools::prof::t_pre.is_measuring) tools::prof::t_pre.toc();
                break;
            } else
                file_counter[src_base] = 0;
        } else if(file_counter[src_base] >= max_files) {
            if(tools::prof::t_pre.is_measuring) tools::prof::t_pre.toc();
            continue;
        }
        file_counter[src_base]++;

        // Append latest profiling information to table
        tools::prof::append();

        // We should now have enough to define a FileId
        tools::prof::t_hsh.tic();
        auto src_hash = tools::hash::md5_file_meta(src_abs);
        auto src_seed = tools::parse::extract_digits_from_h5_filename<long>(src_rel.filename());
        if(src_seed != std::clamp<long>(src_seed, seed_min, seed_max)) {
            if(tools::prof::t_pre.is_measuring) tools::prof::t_pre.toc();
            if(tools::prof::t_hsh.is_measuring) tools::prof::t_hsh.toc();
            tools::logger::log->warn("Skipping seed {}: Valid are [{}-{}]", src_seed, seed_min, seed_max);
            continue;
        }

        FileId fileId(src_seed, src_abs.string(), src_hash);

        // We check if it's in the file database
        auto status = tools::h5db::getFileIdStatus(tgtFileDb, fileId);
        tools::logger::log->info("Found file: {} | {} | {}", src_rel.string(), enum2str(status), src_hash);
        tgtFileDb[fileId.path] = fileId;
        tools::prof::t_hsh.toc();
        if(status == FileIdStatus::UPTODATE) {
            if(tools::prof::t_pre.is_measuring) tools::prof::t_pre.toc();
            continue;
        }

        // If we've reached this point we will start reading from h5_src many times.
        // It's best if we work from memory, so we load the whole file now
        // H5garbage_collect(); // Frees memory allocated in the previous iteration
        // h5_src.setDriver_sec2();
        // h5_src.setDriver_stdio();
        h5pp::File h5_src;
        try {
            h5_src = h5pp::File(src_abs.string(), h5pp::FilePermission::READONLY, verbosity_h5pp);
            //                        h5_src.setDriver_core(false, 10 * 1024 * 1024);
            h5_src.setDriver_sec2();
            //            h5_src.setDriver_core();
            if(tools::prof::t_pre.is_measuring) tools::prof::t_pre.toc();
        } catch(const std::exception &ex) {
            tools::logger::log->warn("Skipping broken file: {}\n\tReason: {}\n", src_abs.string(), ex.what());
            if(tools::prof::t_pre.is_measuring) tools::prof::t_pre.toc();
            continue;
        }
        try{
            if(not h5_src.linkExists("common/finished_all")) {
                tools::logger::log->warn("Skipping broken file: {}\n\tReason: Could not find dataset [common/finished_all]", src_abs.string());
                continue;
            }
            if(finished and not h5_src.readDataset<bool>("common/finished_all")) {
                tools::logger::log->warn("Skipping file: {}\n\tReason: Simulation has not finished", src_abs.string());
                continue;
            }
        }catch (const std::exception & ex){
            tools::logger::log->warn("Skipping file: {}\n\tReason: {}", src_abs.string(), ex.what());
            continue;
        }


        // Define reusable source Info
        static std::unordered_map<std::string, h5pp::TableInfo> srcTableDb;
        static std::unordered_map<std::string, h5pp::DsetInfo>  srcDsetDb;
        static std::unordered_map<std::string, ModelId<ModelT>> srcModelDb;

        // Start finding the required components in the source
        h5_src.setKeepFileOpened();
        tools::prof::t_gr1.tic();
        auto groups = tools::h5io::findKeys(h5_src, "/", algo_keys, -1, 0);
        //        groups = h5_src.findLinks(algo_key, "/", -1, 0);
        tools::prof::t_gr1.toc();

        for(const auto &algo : groups) {
            auto  modelKey = tools::h5io::loadModel(h5_src, srcModelDb, algo);
            auto &modelId  = srcModelDb.at(modelKey);
            auto  tgt_base = tools::h5io::get_standardized_base(modelId);

            // Start by storing the model if it hasn't already been
            tools::h5io::saveModel(h5_src, h5_tgt, tgtModelDb, modelId);

            // Next search for tables and datasets in the source file
            // and transfer them to the target file

            tools::prof::t_gr2.tic();
            auto state_groups = tools::h5io::findKeys(h5_src, algo, state_keys, -1, 0);
            tools::prof::t_gr2.toc();
            for(const auto &state : state_groups) {
                tools::prof::t_gr3.tic();
                auto point_groups = tools::h5io::findKeys(h5_src, fmt::format("{}/{}", algo, state), point_keys, -1, 1);
                tools::prof::t_gr3.toc();
                for(const auto &point : point_groups) {
                    auto srcGroupPath = fmt::format("{}/{}/{}", algo, state, point);
                    auto tgtGroupPath = fmt::format("{}/{}/{}/{}", tgt_base, algo, state, point);
                    // Try gathering all the tables
                    try {
                        auto dsetKeys = tools::h5io::gatherDsetKeys(h5_src, srcDsetDb, srcGroupPath, dsets);
                        tools::h5io::transferDatasets(h5_tgt, tgtDsetDb, h5_src, srcDsetDb, tgtGroupPath, dsetKeys, fileId);
                    } catch(const std::runtime_error &ex) { tools::logger::log->warn("Dset transfer failed in [{}]: {}", srcGroupPath, ex.what()); }

                    try {
                        auto tableKeys = tools::h5io::gatherTableKeys(h5_src, srcTableDb, srcGroupPath, tables);
                        tools::h5io::transferTables(h5_tgt, tgtTableDb, srcTableDb, tgtGroupPath, tableKeys, fileId);
                    } catch(const std::runtime_error &ex) { tools::logger::log->error("Table transfer failed in [{}]: {}", srcGroupPath, ex.what()); }

                    try {
                        auto cronoKeys = tools::h5io::gatherTableKeys(h5_src, srcTableDb, srcGroupPath, cronos);
                        tools::h5io::transferCronos(h5_tgt, tgtTableDb, srcTableDb, tgtGroupPath, cronoKeys, fileId);
                    } catch(const std::runtime_error &ex) { tools::logger::log->error("Crono transfer failed in[{}]: {}", srcGroupPath, ex.what()); }
                }
            }
        }

        auto ssize_objids = H5Fget_obj_count(h5_src.openFileHandle(), H5F_OBJ_DATASET | H5F_OBJ_GROUP | H5F_OBJ_ATTR);
        if(ssize_objids > 0) {
            auto               size_objids = static_cast<size_t>(ssize_objids);
            std::vector<hid_t> objids(size_objids);
            H5Fget_obj_ids(h5_src.openFileHandle(), H5F_OBJ_ALL, size_objids, objids.data());
            tools::logger::log->warn("File [{}] has {} open ids: {}", h5_src.getFilePath(), size_objids, objids);
            for(auto &id : objids) tools::logger::log->info("{}",tools::h5dbg::get_hid_string_details(id));
            throw std::logic_error(fmt::format("File [{}] has {} open ids: {}", h5_src.getFilePath(), size_objids, objids));
        } else if(ssize_objids < 0) {
            tools::logger::log->error("File [{}] failed to count ids: {}", h5_src.getFilePath(), ssize_objids);
        }

        h5_src.setKeepFileClosed(); // This closes the permanent file-handle

        // Check that there are no errors hiding in the HDF5 error-stack
        auto num_errors = H5Eget_num(H5E_DEFAULT);
        if(num_errors > 0) {
            H5Eprint(H5E_DEFAULT, stderr);
            throw std::runtime_error(fmt::format("Error when treating file [{}]", h5_src.getFilePath()));
        }
        H5Eprint(H5E_DEFAULT, stderr);
    }

    // TODO: Put the lines below in a "at quick exit" function

    tools::prof::append();
    tools::h5io::writeProfiling(h5_tgt);

    tools::h5db::saveDatabase(h5_tgt, tgtFileDb);
    tools::h5db::saveDatabase(h5_tgt, tgtModelDb);
    tools::h5db::saveDatabase(h5_tgt, tgtTableDb);
    tools::h5db::saveDatabase(h5_tgt, tgtDsetDb);
    tgtFileDb.clear();
    tgtModelDb.clear();
    tgtTableDb.clear();
    tgtDsetDb.clear();

    auto ssize_objids = H5Fget_obj_count(h5_tgt.openFileHandle(), H5F_OBJ_ALL);
    if(ssize_objids > 0) {
        auto               size_objids = static_cast<size_t>(ssize_objids);
        std::vector<hid_t> objids(size_objids);
        H5Fget_obj_ids(h5_tgt.openFileHandle(), H5F_OBJ_ALL, size_objids, objids.data());
        tools::logger::log->info("File [{}] has {} open ids: {}", h5_tgt.getFilePath(), size_objids, objids);
        for(auto &id : objids) tools::logger::log->info("{}",tools::h5dbg::get_hid_string_details(id));
    } else if(ssize_objids < 0) {
        tools::logger::log->info("File [{}] failed to count ids: {}", h5_tgt.getFilePath(), ssize_objids);
    }
    //    h5_tgt.setKeepFileClosed();
    // Check that there are no errors hiding in the HDF5 error-stack
    auto num_errors = H5Eget_num(H5E_DEFAULT);
    if(num_errors > 0) {
        H5Eprint(H5E_DEFAULT, stderr);
        throw std::runtime_error(fmt::format("Error when treating file [{}]", h5_tgt.getFilePath()));
    }

    tools::prof::t_tot.toc();
    tools::logger::log->info("{}: {:.5f}", tools::prof::t_itr.get_name(), tools::prof::t_itr.get_measured_time());
    tools::logger::log->info("{}: {:.5f}", tools::prof::t_pre.get_name(), tools::prof::t_pre.get_measured_time());
    tools::logger::log->info("{}: {:.5f}", tools::prof::t_gr1.get_name(), tools::prof::t_gr1.get_measured_time());
    tools::logger::log->info("{}: {:.5f}", tools::prof::t_gr2.get_name(), tools::prof::t_gr2.get_measured_time());
    tools::logger::log->info("{}: {:.5f}", tools::prof::t_gr3.get_name(), tools::prof::t_gr3.get_measured_time());
    tools::logger::log->info("{}: {:.5f}", tools::prof::t_get.get_name(), tools::prof::t_get.get_measured_time());
    tools::logger::log->info("{}: {:.5f}", tools::prof::t_tab.get_name(), tools::prof::t_tab.get_measured_time());
    tools::logger::log->info("{}: {:.5f}", tools::prof::t_dst.get_name(), tools::prof::t_dst.get_measured_time());
    tools::logger::log->info("{}: {:.5f}", tools::prof::t_crt.get_name(), tools::prof::t_crt.get_measured_time());
    tools::logger::log->info("{}: {:.5f}", tools::prof::t_ham.get_name(), tools::prof::t_ham.get_measured_time());
    tools::logger::log->info("{}: {:.5f}", tools::prof::t_dat.get_name(), tools::prof::t_dat.get_measured_time());
    tools::logger::log->info("{}: {:.5f}", tools::prof::t_hsh.get_name(), tools::prof::t_hsh.get_measured_time());
    fmt::print("Added time: {:.5}\n", tools::prof::t_itr.get_measured_time() + tools::prof::t_pre.get_measured_time() + tools::prof::t_gr1.get_measured_time() +
                                          tools::prof::t_gr2.get_measured_time() + tools::prof::t_gr3.get_measured_time() +
                                          tools::prof::t_get.get_measured_time() + tools::prof::t_tab.get_measured_time() +
                                          tools::prof::t_dst.get_measured_time() + tools::prof::t_ren.get_measured_time() +
                                          tools::prof::t_crt.get_measured_time() + tools::prof::t_ham.get_measured_time() +
                                          tools::prof::t_dat.get_measured_time() + tools::prof::t_hsh.get_measured_time());
    // Around 15 % overhead comes from closing the h5f, h5d and h5t members of DsetInfo and TableInfo objects
    tools::logger::log->info("{}: {:.5f}", tools::prof::t_tot.get_name(), tools::prof::t_tot.get_measured_time());
    tools::logger::log->info("Results written to file {}", tgt_path.string());
}