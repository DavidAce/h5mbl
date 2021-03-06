#include <general/class_tic_toc.h>
#include <general/enums.h>
#include <general/prof.h>
#include <getopt.h>
#include <gitversion.h>
#include <h5pp/h5pp.h>
#include <io/find.h>
#include <io/h5db.h>
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
-m <max files>              : Max number of files to merge (default = 10000)
-s <src_dir>                : Root directory for the output files. Supports pattern matching for relative paths.
-t <tgt_dir>                : Target directory for the single output file
-f <tgt_filename>           : Target filename (default merged.h5)
-v <level>                  : Enables trace-level verbosity
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
    h5pp::fs::path              tgt_dir;
    std::string                 tgt_file  = "merged.h5";
    size_t                      verbosity = 2;
    size_t                      max_files = 10000;
    while(true) {
        char opt = static_cast<char>(getopt(argc, argv, "hb:f:m:s:t:v:"));
        if(opt == EOF) break;
        if(optarg == nullptr) tools::logger::log->info("Parsing input argument: -{}", opt);
        else
            tools::logger::log->info("Parsing input argument: -{} {}", opt, optarg);
        switch(opt) {
            case 'b': default_base = h5pp::fs::canonical(optarg);
            case 'f': tgt_file = std::string(optarg); continue;
            case 'm': max_files = std::strtoul(optarg, nullptr, 10); continue;
            case 's': {
                h5pp::fs::path src_dir = optarg;
                if(src_dir.is_relative()) {
                    auto matching_dirs = tools::io::find_dir<false>(default_base, src_dir.string());
                    if(matching_dirs.size() > 1) {
                        std::string error_msg = h5pp::format("Too many directories match the pattern {}:\n", (default_base / src_dir).string());
                        for(auto &dir : matching_dirs) error_msg.append(dir.string() + "\n");
                        throw std::runtime_error(error_msg);
                    }
                    if(matching_dirs.empty()) throw std::runtime_error(h5pp::format("No directories match the pattern: {}", (default_base / src_dir).string()));
                    src_dirs.emplace_back(matching_dirs.back());
                } else
                    src_dirs.emplace_back(h5pp::fs::canonical(src_dir));
                continue;
            }
            case 't': tgt_dir = h5pp::fs::canonical(optarg); continue;
            case 'v': verbosity = std::strtoul(optarg, nullptr, 10); continue;
            case ':': tools::logger::log->error("Option -{} needs a value", opt); break;
            case 'h':
            case '?':
            default: print_usage(); exit(0);
            case -1: break;
        }
        break;
    }
    tools::logger::setLogLevel(tools::logger::log, verbosity);
    tools::logger::log->set_level(spdlog::level::info);
    tools::logger::log->info("Started h5mbl from directory {}", h5pp::fs::current_path());
    if(src_dirs.empty()) throw std::runtime_error("Source directories are required. Pass -s <dirpath> (one or more times)");
    for(auto &src_dir : src_dirs) {
        if(not h5pp::fs::is_directory(src_dir)) throw std::runtime_error(fmt::format("Given source is not a directory: {}", src_dir.string()));
        tools::logger::log->info("Found source directory {}", src_dir.string());
    }
    if(tgt_dir.empty()) throw std::runtime_error("A target directory is required. Pass -t <dirpath>");
    h5pp::fs::path tgt_path = tgt_dir / tgt_file;
    tools::logger::log->info("Merge into target file {}", tgt_path.string());

    std::string              algo_key  = "DMRG";
    std::string              state_key = "state_";
    std::string              point_key = "finished";
    std::vector<std::string> models = {"hamiltonian"};
    std::vector<std::string> tables    = {"measurements", "profiling", "status", "mem_usage"};
    //    std::vector<std::string> dsets = {"bond_dimensions", "entanglement_entropies", "truncation_errors"};
    std::vector<DsetMeta> dsets = {{Type::LONG, Size::FIX, "bond_dimensions", ""},     {Type::DOUBLE, Size::FIX, "entanglement_entropies", ""},
                                   {Type::DOUBLE, Size::FIX, "renyi_2", ""},           {Type::DOUBLE, Size::FIX, "renyi_3", ""},
                                   {Type::DOUBLE, Size::FIX, "renyi_4", ""},           {Type::DOUBLE, Size::FIX, "renyi_100", ""},
                                   {Type::DOUBLE, Size::FIX, "truncation_errors", ""}, {Type::COMPLEX, Size::VAR, "schmidt_midchain", ""}};

    DsetMeta bonds{Type::COMPLEX, Size::VAR, "L_", ""};

    // Open the target file

    h5pp::File h5_tgt(tgt_path, h5pp::FilePermission::READWRITE, 2);
    h5_tgt.setDriver_core(true);
    h5_tgt.setCompressionLevel(4);
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
    size_t num_files = 0;
    using reciter    = h5pp::fs::recursive_directory_iterator;
    std::vector<h5pp::fs::path> recfiles;
    for(auto &src_dir : src_dirs) { copy(reciter(src_dir), reciter(), back_inserter(recfiles)); }
    std::sort(recfiles.begin(), recfiles.end());
    for(const auto &src_item : recfiles) {
        if(num_files > max_files) break;
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
        tools::prof::t_itr.toc();
        tools::prof::t_pre.tic();

        // Check which source root this belongs to
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

        h5pp::File               h5_src;
        std::vector<std::string> groups;
        try {
            h5_src = h5pp::File(src_abs.string(), h5pp::FilePermission::READONLY);
            H5garbage_collect(); //Frees memory allocated in the previous iteration
            tools::prof::t_pre.toc();
        } catch(const std::exception &ex) {
            tools::logger::log->warn("Skipping broken file: {}\n\tReason: {}\n", src_abs.string(), ex.what());
            if(tools::prof::t_pre.is_measuring) tools::prof::t_pre.toc();
            continue;
        }

        // Append latest profiling information to table
        tools::prof::append();

        num_files++;

        // We should now have enough to define a FileId
        auto   src_hash = tools::hash::sha256_file(src_abs.string());
        auto   src_seed = tools::parse::extract_digits_from_h5_filename<long>(src_rel.filename());
        FileId fileId(src_seed, src_abs.string(), src_hash);

        // We check if it's in the file database
        auto status = tools::h5db::getFileIdStatus(tgtFileDb, fileId);
        tools::logger::log->info("Found path: {} | {}", src_rel.string(), enum2str(status));
        if(status == FileIdStatus::UPTODATE) continue;
        tgtFileDb[fileId.path] = fileId;

        // Define reusable source Info
        static std::unordered_map<std::string, h5pp::TableInfo> srcTableDb;
        static std::unordered_map<std::string, h5pp::DsetInfo>  srcDsetDb;
        static std::unordered_map<std::string, ModelId>         srcModelDb;

        // Start finding the required components in the source
        tools::prof::t_grp.tic();
        groups = h5_src.findGroups(algo_key, "/", -1, 0);
        tools::prof::t_grp.toc();

        for(const auto &algo : groups) {
            auto  modelKey = tools::h5io::loadModel(h5_src, srcModelDb, algo);
            auto &modelId  = srcModelDb.at(modelKey);
            auto  tgt_base = tools::h5io::get_standardized_base(modelId);

            // Start by storing the model if it hasn't already been
            tools::h5io::saveModel(h5_src, h5_tgt, tgtModelDb, modelId);

            // Next search for tables and datasets in the source file
            // and transfer them to the target file

            tools::prof::t_grp.tic();
            auto state_groups = h5_src.findGroups(state_key, algo, -1, 0);
            tools::prof::t_grp.toc();
            for(const auto &state : state_groups) {
                tools::prof::t_grp.tic();
                auto point_groups = h5_src.findGroups(point_key, fmt::format("{}/{}", algo, state), -1, 0);
                tools::prof::t_grp.toc();
                for(const auto &point : point_groups) {
                    auto srcGroupPath = fmt::format("{}/{}/{}", algo, state, point);
                    auto tgtGroupPath = fmt::format("{}/{}/{}/{}", tgt_base, algo, state, point);
                    // Try gathering all the tables
                    std::vector<std::string> tableKeys;
                    std::vector<DsetMeta>    dsetMetas;
                    try {
                        tableKeys = tools::h5io::gatherTableKeys(h5_src, srcTableDb, srcGroupPath, tables);
                        dsetMetas = tools::h5io::gatherDsetKeys(h5_src, srcDsetDb, srcGroupPath, dsets);
                    } catch(const std::exception &ex) {
                        tools::logger::log->warn("Skipping state [{}]: {}", srcGroupPath, ex.what());
                    }

                    tools::h5io::transferTables(h5_tgt, tgtTableDb, srcTableDb, tgtGroupPath, tableKeys, fileId);
                    tools::h5io::transferDatasets(h5_tgt, tgtDsetDb, h5_src, srcDsetDb, tgtGroupPath, dsetMetas, fileId);
                }
            }
        }
    }

    tools::prof::append();
    tools::h5io::writeProfiling(h5_tgt);


    tools::h5db::saveDatabase(h5_tgt, tgtFileDb);
    tools::h5db::saveDatabase(h5_tgt, tgtModelDb);
    tools::h5db::saveDatabase(h5_tgt, tgtTableDb);
    tools::h5db::saveDatabase(h5_tgt, tgtDsetDb);

    tools::prof::t_tot.toc();
    tools::prof::t_itr.print_measured_time();
    tools::prof::t_pre.print_measured_time();
    tools::prof::t_grp.print_measured_time();
    tools::prof::t_get.print_measured_time();
    tools::prof::t_tab.print_measured_time();
    tools::prof::t_dst.print_measured_time();
    tools::prof::t_crt.print_measured_time();
    tools::prof::t_ham.print_measured_time();
    tools::prof::t_dat.print_measured_time();
    fmt::print("Added  time: {:.5}\n", tools::prof::t_itr.get_measured_time() + tools::prof::t_pre.get_measured_time() +
                                           tools::prof::t_grp.get_measured_time() + tools::prof::t_get.get_measured_time() +
                                           tools::prof::t_tab.get_measured_time() + tools::prof::t_dst.get_measured_time() +
                                           tools::prof::t_ren.get_measured_time() + tools::prof::t_crt.get_measured_time() +
                                           tools::prof::t_ham.get_measured_time() + tools::prof::t_dat.get_measured_time());
    // Around 15 % overhead comes from closing the h5f, h5d and h5t members of DsetInfo and TableInfo objects
    tools::prof::t_tot.print_measured_time();

    tools::logger::log->info("Results written to file {}", tgt_path.string());
}