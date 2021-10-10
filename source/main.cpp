#include <CLI/CLI.hpp>
#include <cstdlib>
#include <debug/stacktrace.h>
#include <general/class_tic_toc.h>
#include <general/enums.h>
#include <general/human.h>
#include <general/prof.h>
#include <general/settings.h>
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
#include <mpi/mpi-tools.h>
#include <omp.h>
#include <string>
#include <tid/tid.h>
void print_usage() {
    std::printf(
        R"(
==========  h5mbl  ============
Usage                       : ./cpp_merger [-option <value>].
-h                          : Help. Shows this text.
-b <base_dir>               : Default base directory (default /mnt/WDB-AN1500/mbl_transition)
-d <max dirs>               : Max number of directories to merge (default = inf)
-f                          : Require that src file has finished
-l                          : Link only. Make the main file with external links to all the others.
-m <max files>              : Max number of files to merge (default = inf)
-M <model>                  : Choose [sdual|lbit] (default = sdual)
-n <tgt_filename>           : Target filename (default merged.h5)
-o <src_out>                : Name of the output directory inside the root directory (default = "")
-r                          : Replace target file if one exists (truncate)
-s <src_dir>                : Root directory for the output files. Supports pattern matching for relative paths.
-t <tgt_dir>                : Target directory for the single output file
-D                          : Write to destination directly, without using /tmp path
-v <level>                  : Enables verbosity. Default level 2 (0 is max verbosity)
-V <level>                  : Enables trace-level verbosity of h5pp. Default level 2 (0 is max verbosity)
)");
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

void clean_up() {
    if(not tools::h5io::tmp_path.empty()) {
        try {
            tools::logger::log->info("Cleaning up temporary file: [{}]", tools::h5io::tmp_path);
            h5pp::hdf5::moveFile(tools::h5io::tmp_path, tools::h5io::tgt_path, h5pp::FilePermission::REPLACE);
            tools::h5io::tmp_path.clear();
        } catch(const std::exception &err) { tools::logger::log->info("Cleaning not needed: {}", err.what()); }
    }
    H5garbage_collect();
    H5Eprint(H5E_DEFAULT, stderr);
}

int main(int argc, char *argv[]) {
    // Here we use getopt to parse CLI input
    // Note that CLI input always override config-file values
    // wherever they are found (config file, h5 file)
    tools::logger::log = tools::logger::setLogger("h5mbl", 2);

    h5pp::fs::path              base_dir = h5pp::fs::absolute("/mnt/WDB-AN1500/mbl_transition");
    std::vector<h5pp::fs::path> src_dirs;
    std::string                 src_out  = "output";
    std::string                 tgt_file = "merged.h5";
    h5pp::fs::path              tgt_dir;
    h5pp::fs::path              tmp_dir        = h5pp::fs::absolute(fmt::format("/tmp/{}", tools::h5io::get_tmp_dirname(argv[0])));
    bool                        finished       = false;
    bool                        link_only      = false;
    bool                        use_tmp        = false;
    size_t                      verbosity      = 2;
    size_t                      verbosity_h5pp = 2;
    size_t                      max_files      = 0ul;
    size_t                      max_dirs       = 0ul;
    long                        seed_min       = 0l;
    long                        seed_max       = std::numeric_limits<long>::max();
    Model                       model          = Model::SDUAL;
    bool                        replace        = false;
    std::vector<std::string>    incfilter      = {};
    std::vector<std::string>    excfilter      = {};
    {
        using namespace h5pp;
        using namespace spdlog;
        CLI::App app;
        app.description("h5mbl: Merges simulation data for fLBIT and xDMRG projects");
        app.get_formatter()->column_width(80);
        app.option_defaults()->always_capture_default();
        std::map<std::string, Model>                           ModelMap{{"sdual", Model::SDUAL}, {"lbit", Model::LBIT}};
        std::vector<std::pair<std::string, LogLevel>>          h5ppLevelMap{{"trace", LogLevel::trace}, {"debug", LogLevel::debug}, {"info", LogLevel::info}};
        std::vector<std::pair<std::string, level::level_enum>> SpdlogLevelMap{{"trace", level::trace}, {"debug", level::debug}, {"info", level::info}};

        /* clang-format off */
        app.add_option("-M,--model"       , model           , "Choose [sdual|lbit]")->required()->transform(CLI::CheckedTransformer(ModelMap, CLI::ignore_case))->always_capture_default(false);
        app.add_option("-b,--basedir"     , base_dir        , "The base directory for simulations of MBL transition");
        app.add_option("-n,--destname"    , tgt_file        , "The destination file name for the merge-file");
        app.add_option("-t,--destdir"     , tgt_dir         , "The destination directory for the merge-file")->required();
        app.add_option("-o,--srcout"      , src_out         , "The name of the source directory where simulation files are found");
        app.add_option("-s,--srcdirs"     , src_dirs        , "List of output directory patterns from where to collect simulation files");
        app.add_flag  ("-f,--finished"    , finished       , "Require that src file has finished");
        app.add_flag  ("-l,--linkonly"    , link_only       , "Link only. Make the main file with external links to all the others");
        app.add_flag  ("-r,--replace"     , replace         , "Replace existing files");
        app.add_flag  ("-T,--usetemp"     , use_tmp         , "Use temp directory");
        app.add_option("--maxfiles"       , max_files       , "Maximum number of .h5 files to collect in each set");
        app.add_option("--maxdirs"        , max_dirs        , "Maximum number of simulation sets");
        app.add_option("--maxseed"        , seed_max        , "Maximum seed number to collect");
        app.add_option("--minseed"        , seed_min        , "Minimum seed number to collect");
        app.add_option("--inc"            , incfilter       , "Include paths to .h5 matching any in this list");
        app.add_option("--exc"            , excfilter       , "Exclude paths to .h5 matching any in this list");
        app.add_option("-v,--log"         , verbosity       , "Log level")->transform(CLI::CheckedTransformer(h5ppLevelMap, CLI::ignore_case));
        app.add_option("-V,--logh5pp"     , verbosity_h5pp  , "Log level of h5pp")->transform(CLI::CheckedTransformer(h5ppLevelMap, CLI::ignore_case));

        CLI11_PARSE(app, argc, argv);
        /* clang-format on */
    }
    tgt_dir = h5pp::fs::absolute(tgt_dir);
    for(auto &src_dir : src_dirs) {
        if(src_dir.is_relative()) {
            auto matching_dirs = tools::io::find_dir<false>(base_dir, src_dir.string(), src_out);
            if(matching_dirs.size() > 5) {
                std::string error_msg = h5pp::format("Too many directories match the pattern {}:\n", (base_dir / src_dir).string());
                for(auto &dir : matching_dirs) error_msg.append(dir.string() + "\n");
                throw std::runtime_error(error_msg);
            }
            if(matching_dirs.empty()) throw std::runtime_error(h5pp::format("No directories match the pattern: {}", (base_dir / src_dir).string()));
            // We have multiple matches. Append them
            for(auto &match : matching_dirs) src_dirs.emplace_back(match);
        } else
            src_dir = h5pp::fs::canonical(src_dir);
    }
    // Now remove elements that do not exist
    src_dirs.erase(std::remove_if(src_dirs.begin(), src_dirs.end(),
                                  [](const h5pp::fs::path &p) {
                                      return not h5pp::fs::exists(p); // put your condition here
                                  }),
                   src_dirs.end());

    // Register termination codes and what to do in those cases
    debug::register_callbacks();
    if(mpi::world.id == 0) {
        std::atexit(tools::prof::print_profiling);
        std::at_quick_exit(tools::prof::print_profiling);
        std::atexit(tools::prof::print_mem_usage);
    }
    auto t_h5mbl = tid::tic_scope("h5mbl");
    mpi::init();

    //    while(true) {
    //        char opt = static_cast<char>(getopt(argc, argv, "hb:d:flm:M:n:o:rs:t:Tv:V:"));
    //        if(opt == EOF) break;
    //        if(optarg == nullptr)
    //            tools::logger::log->info("Parsing input argument: -{}", opt);
    //        else
    //            tools::logger::log->info("Parsing input argument: -{} {}", opt, optarg);
    //        switch(opt) {
    //            case 'b': base_dir = h5pp::fs::canonical(optarg); continue;
    //            case 'd': max_dirs = std::strtoul(optarg, nullptr, 10); continue;
    //            case 'f': finished = true; continue;
    //            case 'l': link_only = true; continue;
    //            case 'm': max_files = std::strtoul(optarg, nullptr, 10); continue;
    //            case 'M': model = str2enum<Model>(std::string_view(optarg)); continue;
    //            case 'n': tgt_file = std::string(optarg); continue;
    //            case 'o': src_out = std::string(optarg); continue;
    //            case 'r': replace = true; continue;
    //            case 's': {
    //                h5pp::fs::path src_dir = optarg;
    //                if(src_dir.is_relative()) {
    //                    auto matching_dirs = tools::io::find_dir<false>(base_dir, src_dir.string(), src_out);
    //                    if(matching_dirs.size() > 5) {
    //                        std::string error_msg = h5pp::format("Too many directories match the pattern {}:\n", (base_dir / src_dir).string());
    //                        for(auto &dir : matching_dirs) error_msg.append(dir.string() + "\n");
    //                        throw std::runtime_error(error_msg);
    //                    }
    //                    if(matching_dirs.empty()) throw std::runtime_error(h5pp::format("No directories match the pattern: {}", (base_dir /
    //                    src_dir).string())); for(auto &match : matching_dirs) src_dirs.emplace_back(match);
    //                } else
    //                    src_dirs.emplace_back(h5pp::fs::canonical(src_dir));
    //                continue;
    //            }
    //            case 't': tgt_dir = h5pp::fs::absolute(optarg); continue;
    //            case 'T': use_tmp = true; continue;
    //            case 'v': verbosity = std::strtoul(optarg, nullptr, 10); continue;
    //            case 'V': verbosity_h5pp = std::strtoul(optarg, nullptr, 10); continue;
    //            case ':': throw std::runtime_error(fmt::format("Option -{} needs a value", opt)); break;
    //            case 'h':
    //            case '?':
    //            default: print_usage(); exit(0);
    //            case -1: break;
    //        }
    //        break;
    //    }
    tools::logger::setLogLevel(tools::logger::log, verbosity);
    tools::logger::log->info("Started h5mbl from directory {}", h5pp::fs::current_path());
    if(src_dirs.empty()) { src_dirs.emplace_back(h5pp::fs::canonical(base_dir / src_out)); }
    if(src_dirs.empty()) throw std::runtime_error("Source directories are required. Pass -s <dirpath> (one or more times)");
    for(auto &src_dir : src_dirs) {
        if(not h5pp::fs::is_directory(src_dir)) throw std::runtime_error(fmt::format("Given source is not a directory: {}", src_dir.string()));
        tools::logger::log->info("Found source directory {}", src_dir.string());
    }
    if(tgt_dir.empty()) throw std::runtime_error("A target directory is required. Pass -t <dirpath>");

    // Set file permissions
    auto perm = h5pp::FilePermission::READWRITE;
    if(replace) perm = h5pp::FilePermission::REPLACE;

    h5pp::fs::path tgt_path = tgt_dir / tgt_file;
    tools::logger::log->info("Merge into target file {}", tgt_path.string());

    if(not link_only) {
        // Define which objects to consider for merging
        tools::h5db::Keys keys;
        switch(model) {
            case Model::SDUAL: {
                //            keys.algo  = {"xDMRG"};
                //            keys.state = {"state_*"};
                //            keys.point = {"finished", "checkpoint/iter_*"};
                //            keys.models = {"hamiltonian"};
                //            keys.tables = {"measurements", "profiling", "status", "mem_usage"};
                //            keys.cronos = {};
                //            //    std::vector<std::string> dsets = {"bond_dimensions", "entanglement_entropies", "truncation_errors"};
                //
                //            keys.dsets = {{Type::LONG, Size::FIX, "bond_dimensions", ""},
                //                     {Type::DOUBLE, Size::FIX, "entanglement_entropies", ""},
                //                     {Type::DOUBLE, Size::FIX, "truncation_errors", ""},
                //                     {Type::COMPLEX, Size::VAR, "schmidt_midchain", ""}};
                //            keys.bonds = DsetKey{Type::COMPLEX, Size::VAR, "L_", ""};

                keys.dsets.emplace_back(DsetKey("xDMRG", "state_*", "finished", "bond_dimensions", Size::FIX, Type::LONG));
                keys.dsets.emplace_back(DsetKey("xDMRG", "state_*", "finished", "entanglement_entropies", Size::FIX, Type::DOUBLE));
                keys.dsets.emplace_back(DsetKey("xDMRG", "state_*", "finished", "truncation_errors", Size::FIX, Type::DOUBLE));
                keys.dsets.emplace_back(DsetKey("xDMRG", "state_*", "finished", "schmidt_midchain", Size::VAR, Type::COMPLEX));
                keys.dsets.emplace_back(DsetKey("xDMRG", "state_*", "finished/profiling", "xDMRG.run", Size::FIX, Type::TID));

                keys.tables.emplace_back(TableKey("xDMRG", "state_*", "finished", "status"));
                keys.tables.emplace_back(TableKey("xDMRG", "state_*", "finished", "mem_usage"));
                keys.tables.emplace_back(TableKey("xDMRG", "state_*", "finished", "measurements"));

                keys.models.emplace_back(ModelKey("xDMRG", "model", "hamiltonian"));
                break;
            }
            case Model::LBIT: {
                keys.models.emplace_back(ModelKey("fLBIT", "model", "hamiltonian"));

                // A table records data from the last time step
                keys.tables.emplace_back(TableKey("fLBIT", "state_*", "tables", "status"));
                keys.tables.emplace_back(TableKey("fLBIT", "state_*", "tables", "mem_usage"));
                // A crono records data from each time step
                keys.cronos.emplace_back(CronoKey("fLBIT", "state_*", "tables", "measurements"));
                keys.cronos.emplace_back(CronoKey("fLBIT", "state_*", "tables", "bond_dimensions"));        // Available in v2
                keys.cronos.emplace_back(CronoKey("fLBIT", "state_*", "tables", "entanglement_entropies")); // Available in v2
                keys.cronos.emplace_back(CronoKey("fLBIT", "state_*", "tables", "number_entropies"));       // Available in v2
                keys.cronos.emplace_back(CronoKey("fLBIT", "state_*", "tables", "truncation_errors"));      // Available in v2

                //            keys.dsets.emplace_back(DsetKey("fLBIT", "state_*", "finished", "schmidt_midchain", Size::VAR, Type::COMPLEX));
                //            keys.dsets.emplace_back(DsetKey("fLBIT", "state_*", "finished/profiling", "fLBIT.run", Size::FIX, Type::TID));
                break;
            }
            default: throw std::runtime_error("Invalid model");
        }

        // Open the target file
        auto h5dirs = tools::io::find_h5_dirs(src_dirs, max_dirs, incfilter, excfilter);
        tools::logger::log->info("num h5dirs: {}", h5dirs.size());
        std::unordered_map<std::string, FileStats> file_stats;
        uintmax_t                                  srcBytes = 0; // Count the total size of scanned files
        for(const auto &h5dir : h5dirs) {
            // Define a new target h5file for the files in this h5dir
            auto h5dir_hash  = tools::hash::std_hash(h5dir.string());
            auto h5_tgt_path = h5pp::format("{}/{}.{}{}", tgt_dir.string(), h5pp::fs::path(tgt_path).stem().string(), tools::hash::std_hash(h5dir.string()),
                                            h5pp::fs::path(tgt_path).extension().string());

            // Initialize file
            //        auto plists = h5pp::defaultPlists;
            //        plists.fileCreate = H5Pcreate(H5P_FILE_CREATE);
            //        plists.fileAccess = H5Pcreate(H5P_FILE_ACCESS);

            //        herr_t ers    = H5Pset_file_space_strategy(plists.fileCreate, H5F_FSPACE_STRATEGY_PAGE, true, 1);
            //        herr_t erp =    H5Pset_file_space_page_size(plists.fileCreate,100 * 1024);
            //        herr_t erb    = H5Pset_page_buffer_size(plists.fileAccess, 10 * 1024 * 1024, 1, 1);
            auto h5_tgt = h5pp::File();

            try{
                h5_tgt = h5pp::File(h5_tgt_path, perm, verbosity_h5pp);

            }catch (const std::exception & ex){
                H5Eprint(H5E_DEFAULT, stderr);
                tools::logger::log->error("Error opening target file: {}", ex.what());
                tools::logger::log->error("Replacing broken file: [{}]: {}", h5_tgt_path);
                h5_tgt = h5pp::File(h5_tgt_path, h5pp::FilePermission::REPLACE, verbosity_h5pp);
            }

//            auto h5_tgt = h5pp::File(h5_tgt_path, perm, verbosity_h5pp);
            tools::h5dbg::assert_no_dangling_ids(h5_tgt, __FUNCTION__, __LINE__); // Check that there are no open HDF5 handles
            //        hsize_t fsp_size; size_t pbs_size;
            //        H5Pget_file_space_page_size(H5Fget_create_plist(h5_tgt.openFileHandle()), &fsp_size);
            //        H5Pget_page_buffer_size(H5Fget_access_plist(h5_tgt.openFileHandle()),&pbs_size,nullptr,nullptr);
            //        tools::logger::log->info("fsp_size: {} | pbs_size {}", fsp_size, pbs_size);

            //            h5_tgt.setDriver_core();
            //    h5_tgt.setKeepFileOpened();
            h5_tgt.setCompressionLevel(2);
            //        size_t rdcc_nbytes = 4*1024*1024;
            //        size_t rdcc_nslots = rdcc_nbytes * 100 / (320 * 64);
            //        H5Pset_cache(h5_tgt.plists.fileAccess, 1000, rdcc_nslots,rdcc_nbytes, 0.20 );
            //
            //        h5pp::hdf5::setTableSize(tableInfo, 2500);
            //        h5pp::hid::h5p dapl = H5Dget_access_plist(tableInfo.h5Dset.value());
            //        size_t rdcc_nbytes = 4*1024*1024;
            //        size_t rdcc_nslots = rdcc_nbytes * 100 / (tableInfo.recordBytes.value() * tableInfo.chunkSize.value());
            //        H5Pset_chunk_cache(dapl, rdcc_nslots, rdcc_nbytes, 0.5);
            //        tableInfo.h5Dset = h5pp::hdf5::openLink<h5pp::hid::h5d>(tableInfo.getLocId(), tableInfo.tablePath.value(), true, dapl);

            if(use_tmp) {
                tools::h5io::tmp_path = (tmp_dir / tgt_file).string();
                tools::h5io::tgt_path = tgt_path.string();
                tools::logger::log->info("Moving to {} -> {}", h5_tgt.getFilePath(), tools::h5io::tmp_path);
                h5_tgt.moveFileTo(tools::h5io::tmp_path, h5pp::FilePermission::REPLACE);
                std::atexit(clean_up);
                std::at_quick_exit(clean_up);
            }

            // Load database
            tools::h5db::TgtDb tgtdb;
            //    h5_tgt.setDriver_core();
            {
                auto keepOpen = h5_tgt.getFileHandleToken();
                tgtdb.file    = tools::h5db::loadFileDatabase(h5_tgt); // This database maps  src_name <--> FileId
                tgtdb.dset    = tools::h5db::loadDatabase<h5pp::DsetInfo>(h5_tgt, keys.dsets);
                tgtdb.table   = tools::h5db::loadDatabase<h5pp::TableInfo>(h5_tgt, keys.tables);
                tgtdb.crono   = tools::h5db::loadDatabase<BufferedTableInfo>(h5_tgt, keys.cronos);
                tgtdb.model   = tools::h5db::loadDatabase<h5pp::TableInfo>(h5_tgt, keys.models);
            }
            {
                for(const auto &[infoKey, infoId] : tgtdb.table) infoId.info.assertReadReady();
                for(const auto &[infoKey, infoId] : tgtdb.dset) infoId.info.assertReadReady();
                //                for(const auto &[infoKey, infoId] : tgtdb.model) infoId.info.assertReadReady() ;
                for(const auto &[infoKey, infoId] : tgtdb.table) infoId.info.assertReadReady();
                for(const auto &[infoKey, infoId] : tgtdb.crono) infoId.info.assertReadReady();
            }
            uintmax_t tgtBytes = h5pp::fs::file_size(h5_tgt.getFilePath());

            // Collect and sort all the files in h5dir
            using h5iter = h5pp::fs::directory_iterator;
            std::vector<h5pp::fs::path> h5files;
            copy(h5iter(h5dir), h5iter(), back_inserter(h5files));
            std::sort(h5files.begin(), h5files.end());
            tools::logger::log->info("num h5files: {}", h5files.size());
            // No barriers from now on: There can be a different number of files in h5files!
            for(const auto &src_item : h5files) {
                auto        t_src_item = tid::tic_scope("src_item");
                const auto &src_abs    = src_item;
                if(not h5pp::fs::is_regular_file(src_abs)) continue;
                if(src_abs.extension() != ".h5") continue;

                auto t_pre = tid::tic_scope("preamble");

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

                bool stats_exists = file_stats.find(src_base) != file_stats.end();
                if(not stats_exists) {
                    // Creeate new entry
                    file_stats[src_base].count = 0;
                    file_stats[src_base].files = h5files.size();
                } else if(max_files > 0 and file_stats[src_base].count >= max_files) {
                    tools::logger::log->debug("Max files reached in {}: {}", src_base, file_stats[src_base].count);
                    break;
                }

                // Append latest profiling information to table
                t_pre.toc();

                // We should now have enough to define a FileId
                auto src_hash = tools::hash::hash_file_meta(src_abs);
                auto src_seed = tools::parse::extract_digits_from_h5_filename<long>(src_rel.filename());
                if(src_seed != std::clamp<long>(src_seed, seed_min, seed_max)) {
                    tools::logger::log->warn("Skipping seed {}: Valid are [{}-{}]", src_seed, seed_min, seed_max);
                    continue;
                }

                FileId fileId(src_seed, src_abs.string(), src_hash);
                // We check if it's in the file database
                auto status             = tools::h5db::getFileIdStatus(tgtdb.file, fileId);
                tgtdb.file[fileId.path] = fileId;

                // Update file stats
                file_stats[src_base].elaps = file_stats[src_base].count == 0 ? t_src_item->restart_lap() : t_src_item->get_lap();
                file_stats[src_base].count++;
                file_stats[src_base].bytes += h5pp::fs::file_size(src_abs.string());

                // Print file status
                srcBytes += h5pp::fs::file_size(src_abs.string());
                tgtBytes = h5pp::fs::file_size(h5_tgt.getFilePath());
                tgtBytes = h5pp::fs::file_size(h5_tgt.getFilePath());

                auto fmt_grp_bytes = tools::fmtBytes(true, file_stats[src_base].bytes, 1024, 1);
                auto fmt_src_bytes = tools::fmtBytes(true, srcBytes, 1024, 1);
                auto fmt_tgt_bytes = tools::fmtBytes(true, tgtBytes, 1024, 1);
                auto fmt_spd_bytes = fmt::format("{:.1f} files", file_stats[src_base].get_speed());
                if(tools::logger::log->level() <= 1) {
                    tools::logger::log->info(FMT_STRING("Found file: {} | {} | {} | count {} | src {} ({}) | tgt {} | {}/s"), src_rel.string(),
                                             enum2str(status), src_hash, file_stats[src_base].count, fmt_grp_bytes, fmt_src_bytes, fmt_tgt_bytes,
                                             fmt_spd_bytes);
                } else {
                    static size_t lastcount = 0;
                    if(file_stats[src_base].count < lastcount) lastcount = 0;
                    size_t filecounter = file_stats[src_base].count - lastcount;
                    if(t_h5mbl->get_lap() > 1.0 or file_stats[src_base].count % 1000 == 0 or file_stats[src_base].count == 1 or
                       file_stats[src_base].count == file_stats[src_base].files) {
                        tools::logger::log->info(FMT_STRING("Directory {} ({}) | count {} | src {} ({}) | tgt {} | {:.2f}/s"), h5dir.string(),
                                                 file_stats[src_base].files, file_stats[src_base].count, fmt_grp_bytes, fmt_src_bytes, fmt_tgt_bytes,
                                                 static_cast<double>(filecounter) / t_h5mbl->restart_lap());
                        lastcount = file_stats[src_base].count;
                    }
                }

                if(status == FileIdStatus::UPTODATE) continue;

                // If we've reached this point we will start reading from h5_src many times.
                auto       t_open = tid::tic_scope("open");
                h5pp::File h5_src;
                try {
                    h5_src = h5pp::File(src_abs.string(), h5pp::FilePermission::READONLY, verbosity_h5pp);
                    // h5_src.setDriver_core(false, 10 * 1024 * 1024);
                    // h5_src.setDriver_sec2();
                    //                 h5_src.setDriver_core();
                    // H5Pset_cache(h5_src.plists.fileAccess, 1000, 7919,rdcc_nbytes, 0.0 );
                    h5_src.setCloseDegree(H5F_close_degree_t::H5F_CLOSE_WEAK); // Delay closing ids related to this file.

                } catch(const std::exception &ex) {
                    tools::logger::log->warn("Skipping broken file: {}\n\tReason: {}\n", src_abs.string(), ex.what());
                    continue;
                }
                try {
                    if(not h5_src.linkExists("common/finished_all")) {
                        tools::logger::log->warn("Skipping broken file: {}\n\tReason: Could not find dataset [common/finished_all]", src_abs.string());
                        continue;
                    }
                    if(finished and not h5_src.readDataset<bool>("common/finished_all")) {
                        tools::logger::log->warn("Skipping file: {}\n\tReason: Simulation has not finished", src_abs.string());
                        continue;
                    }
                } catch(const std::exception &ex) {
                    tools::logger::log->warn("Skipping file: {}\n\tReason: {}", src_abs.string(), ex.what());
                    continue;
                }

                t_open.toc();

                {
                    auto tgtKeepOpen = h5_tgt.getFileHandleToken();
                    auto srcKeepOpen = h5_src.getFileHandleToken();
                    switch(model) {
                        case Model::SDUAL: {
                            tools::h5io::merge<sdual>(h5_tgt, h5_src, fileId, file_stats[src_base], keys, tgtdb);
                            break;
                        }
                        case Model::LBIT: {
                            tools::h5io::merge<lbit>(h5_tgt, h5_src, fileId, file_stats[src_base], keys, tgtdb);
                            break;
                        }
                    }
                }
                tools::logger::log->debug("mem[rss {:<.2f}|peak {:<.2f}|vm {:<.2f}]MB | file db size {}", tools::prof::mem_rss_in_mb(),
                                          tools::prof::mem_hwm_in_mb(), tools::prof::mem_vm_in_mb(), tgtdb.file.size());
            }

            tools::h5db::saveDatabase(h5_tgt, tgtdb.file);
            tools::h5db::saveDatabase(h5_tgt, tgtdb.model);
            tools::h5db::saveDatabase(h5_tgt, tgtdb.table);
            tools::h5db::saveDatabase(h5_tgt, tgtdb.crono);
            tools::h5db::saveDatabase(h5_tgt, tgtdb.dset);

            tgtdb.file.clear();
            tgtdb.model.clear();
            tgtdb.table.clear();
            tgtdb.crono.clear();
            tgtdb.dset.clear();

            // TODO: Put the lines below in a "at quick exit" function
            tools::h5io::writeProfiling(h5_tgt);

            h5_tgt.flush();

            if(use_tmp) {
                tools::logger::log->info("Moving to {} -> {}", h5_tgt.getFilePath(), tools::h5io::tgt_path);
                h5_tgt.moveFileTo(tools::h5io::tgt_path, h5pp::FilePermission::REPLACE);
            }

            tools::logger::log->info("Results written to file {}", h5_tgt_path);
            tools::h5dbg::assert_no_dangling_ids(h5_tgt, __FUNCTION__, __LINE__); // Check that there are no open HDF5 handles
        }
    }

    mpi::barrier();

    // Now id 1 can merge the files into one final file
    if(mpi::world.id == 0) {
        tools::logger::log->info("Creating main file for external links: {}", tgt_path);
        auto h5_tgt   = h5pp::File(tgt_path, h5pp::FilePermission::REPLACE, verbosity_h5pp);
        auto tgt_stem = h5pp::fs::path(tgt_file).stem().string();
        auto tgt_algo = model == Model::LBIT ? "fLBIT" : "xDMRG";
        for(const auto &obj : h5pp::fs::directory_iterator(tgt_dir, h5pp::fs::directory_options::follow_directory_symlink)) {
            if(obj.is_regular_file() and obj.path().extension() == ".h5") {
                if(obj.path().filename() != tgt_file and obj.path().stem().string().find(tgt_stem) != std::string::npos) {
                    // Found a file that we can link!
                    auto h5_ext = h5pp::File(obj.path().string(), h5pp::FilePermission::READONLY, verbosity_h5pp);
                    // Find the path to the algorithm in this external file
                    auto algo_group = h5_ext.findGroups(tgt_algo, "/", 1);
                    if(algo_group.empty())
                        throw std::runtime_error(
                            h5pp::format("Could not find algo group {} in external file {}: [{}]", tgt_algo, obj.path().string(), algo_group));
                    auto tgt_link = h5pp::fs::proximate(obj.path(), tgt_dir);
                    tools::logger::log->info("Creating external link: {} -> {}", algo_group[0], tgt_link.string());
                    h5_tgt.createExternalLink(tgt_link.string(), algo_group[0], algo_group[0]);
                }
            }
        }
    }

    mpi::barrier();
    mpi::finalize();
    return 0;
}
