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
#include <string>

void print_usage() {
    std::printf(
        R"(
==========  h5mbl  ============
Usage                       : ./cpp_merger [-option <value>].
-h                          : Help. Shows this text.
-b <base_dir>               : Default base directory (default /mnt/Barracuda/Projects/mbl_transition)
-f                          : Require that src file has finished
-m <max files>              : Max number of files to merge (default = 10000)
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
    // Register termination codes and what to do in those cases
    debug::register_callbacks();

    tools::prof::init();
    auto                        t_tot_token  = tools::prof::t_tot.tic_token();
    auto                        t_spd_token  = tools::prof::t_spd.tic_token();
    h5pp::fs::path              default_base = h5pp::fs::absolute("/mnt/Barracuda/Projects/mbl_transition");
    std::vector<h5pp::fs::path> src_dirs;
    std::string                 src_out  = "output";
    std::string                 tgt_file = "merged.h5";
    h5pp::fs::path              tgt_dir;
    h5pp::fs::path              tmp_dir        = h5pp::fs::absolute(fmt::format("/tmp/{}", tools::h5io::get_tmp_dirname(argv[0])));
    bool                        finished       = false;
    bool                        skip_tmp       = false;
    size_t                      verbosity      = 2;
    size_t                      verbosity_h5pp = 2;
    size_t                      max_files      = std::numeric_limits<size_t>::max();
    size_t                      max_dirs       = std::numeric_limits<size_t>::max();
    long                        seed_min       = 0l;
    long                        seed_max       = std::numeric_limits<long>::max();
    Model                       model          = Model::SDUAL;
    bool                        replace        = false;

    while(true) {
        char opt = static_cast<char>(getopt(argc, argv, "hb:Dfm:M:n:o:rs:t:v:V:"));
        if(opt == EOF) break;
        if(optarg == nullptr)
            tools::logger::log->info("Parsing input argument: -{}", opt);
        else
            tools::logger::log->info("Parsing input argument: -{} {}", opt, optarg);
        switch(opt) {
            case 'b': default_base = h5pp::fs::canonical(optarg); continue;
            case 'f': finished = true; continue;
            case 'D': skip_tmp = true; continue;
            case 'm': max_files = std::strtoul(optarg, nullptr, 10); continue;
            case 'M': model = str2enum<Model>(std::string_view(optarg)); continue;
            case 'n': tgt_file = std::string(optarg); continue;
            case 'o': src_out = std::string(optarg); continue;
            case 'r': replace = true; continue;
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
            case 't': tgt_dir = h5pp::fs::absolute(optarg); continue;
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
    if(src_dirs.empty()) { src_dirs.emplace_back(h5pp::fs::canonical(default_base / src_out)); }
    if(src_dirs.empty()) throw std::runtime_error("Source directories are required. Pass -s <dirpath> (one or more times)");
    for(auto &src_dir : src_dirs) {
        if(not h5pp::fs::is_directory(src_dir)) throw std::runtime_error(fmt::format("Given source is not a directory: {}", src_dir.string()));
        tools::logger::log->info("Found source directory {}", src_dir.string());
    }
    if(tgt_dir.empty()) throw std::runtime_error("A target directory is required. Pass -t <dirpath>");
    h5pp::fs::path tgt_path = tgt_dir / tgt_file;
    tools::logger::log->info("Merge into target file {}", tgt_path.string());

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
    auto perm = h5pp::FilePermission::READWRITE;
    if(replace) perm = h5pp::FilePermission::REPLACE;
    h5pp::File h5_tgt(tgt_path, perm, verbosity_h5pp);
    if(not skip_tmp) {
        tools::h5io::tmp_path = (tmp_dir / tgt_file).string();
        tools::h5io::tgt_path = tgt_path.string();
        tools::logger::log->info("Moving to {} -> {}", h5_tgt.getFilePath(), tools::h5io::tmp_path);
        h5_tgt.moveFileTo(tools::h5io::tmp_path, h5pp::FilePermission::REPLACE);
        std::atexit(clean_up);
        std::at_quick_exit(clean_up);
    }

    //    h5_tgt.setDriver_core();
    //    h5_tgt.setKeepFileOpened();
    h5_tgt.setCompressionLevel(3);
    //    size_t rdcc_nbytes = 1024 * 1024 * 1024;
    //    H5Pset_cache(h5_tgt.plists.fileAccess, 1000, 7919,rdcc_nbytes, 0.0 );

    if(not h5_tgt.linkExists("git/h5mbl")) {
        // Put git metadata in the target file
        h5_tgt.writeDataset(GIT::BRANCH, "git/h5mbl/branch");
        h5_tgt.writeDataset(GIT::COMMIT_HASH, "git/h5mbl/commit");
        h5_tgt.writeDataset(GIT::REVISION, "git/h5mbl/revision");
    }

    tools::h5db::TgtDb tgtdb;
    //    h5_tgt.setDriver_core();
    h5_tgt.setKeepFileOpened();
    tgtdb.file  = tools::h5db::loadFileDatabase(h5_tgt); // This database maps  src_name <--> FileId
    tgtdb.dset  = tools::h5db::loadDatabase<h5pp::DsetInfo>(h5_tgt, keys.dsets);
    tgtdb.table = tools::h5db::loadDatabase<h5pp::TableInfo>(h5_tgt, keys.tables);
    tgtdb.crono = tools::h5db::loadDatabase<h5pp::TableInfo>(h5_tgt, keys.cronos);
    tgtdb.model = tools::h5db::loadDatabase<h5pp::TableInfo>(h5_tgt, keys.models);
    h5_tgt.setKeepFileClosed();
    //    h5_tgt.setDriver_sec2();

    //    size_t                                  num_files = 0;
    uintmax_t                                  srcBytes = 0;
    uintmax_t                                  tgtBytes = h5pp::fs::file_size(h5_tgt.getFilePath());
    std::unordered_map<std::string, FileStats> file_stats;
    using reciter = h5pp::fs::recursive_directory_iterator;
    std::vector<h5pp::fs::path> recfiles;
    for(auto &src_dir : src_dirs) { copy(reciter(src_dir), reciter(), back_inserter(recfiles)); }
    std::sort(recfiles.begin(), recfiles.end());
    for(const auto &src_item : recfiles) {
        auto        t_itr_token = tools::prof::t_itr.tic_token();
        const auto &src_abs     = src_item;
        if(not src_abs.has_filename()) continue;
        if(src_abs.extension() != ".h5") continue;
        if(not src_out.empty() and src_abs.string().find(src_out) == std::string::npos) continue;

        t_itr_token.toc();

        auto t_pre_token = tools::prof::t_pre.tic_token();

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
            if(file_stats.size() >= max_dirs)
                break;
            else
                file_stats[src_base].count = 0;
        } else if(file_stats[src_base].count >= max_files) {
            tools::logger::log->debug("Max files reached in {}: {}", src_base, file_stats[src_base].count);
            continue;
        }

        // Append latest profiling information to table
        tools::prof::append();
        t_pre_token.toc();

        // We should now have enough to define a FileId
        auto t_hsh_token = tools::prof::t_hsh.tic_token();
        auto src_hash    = tools::hash::md5_file_meta(src_abs);
        auto src_seed    = tools::parse::extract_digits_from_h5_filename<long>(src_rel.filename());
        if(src_seed != std::clamp<long>(src_seed, seed_min, seed_max)) {
            tools::logger::log->warn("Skipping seed {}: Valid are [{}-{}]", src_seed, seed_min, seed_max);
            continue;
        }

        FileId fileId(src_seed, src_abs.string(), src_hash);

        // We check if it's in the file database
        auto status          = tools::h5db::getFileIdStatus(tgtdb.file, fileId);
        auto fmt_grp_bytes   = tools::fmtBytes(true, file_stats[src_base].bytes, 1024, 1);
        auto fmt_src_bytes   = tools::fmtBytes(true, srcBytes, 1024, 1);
        auto fmt_tgt_bytes = tools::fmtBytes(true, tgtBytes, 1024, 1);
        auto fmt_spd_bytes   = tools::fmtBytes(true, file_stats[src_base].get_speed(), 1024, 1);
        tools::logger::log->info(FMT_STRING("Found file: {} | {} | {} | count {} | src {} ({}) | tgt {} | {}/s"), src_rel.string(), enum2str(status), src_hash,
                                 file_stats[src_base].count, fmt_grp_bytes, fmt_src_bytes, fmt_tgt_bytes, fmt_spd_bytes);
        tgtdb.file[fileId.path] = fileId;
        if(status == FileIdStatus::UPTODATE) continue;

        // If we've reached this point we will start reading from h5_src many times.
        t_hsh_token.toc();
        auto t_opn_token = tools::prof::t_opn.tic_token();

        h5pp::File h5_src;
        try {
            h5_src = h5pp::File(src_abs.string(), h5pp::FilePermission::READONLY, verbosity_h5pp);
            //                        h5_src.setDriver_core(false, 10 * 1024 * 1024);
            //            h5_src.setDriver_sec2();
            //            h5_src.setDriver_core();
            //            H5Pset_cache(h5_src.plists.fileAccess, 1000, 7919,rdcc_nbytes, 0.0 );
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
        if(file_stats[src_base].count == 0) tools::prof::t_spd.restart_lap();
        file_stats[src_base].count++;
        file_stats[src_base].bytes += h5pp::fs::file_size(h5_src.getFilePath());
        file_stats[src_base].elaps = tools::prof::t_spd.get_lap();
        srcBytes += h5pp::fs::file_size(h5_src.getFilePath());
        tgtBytes = h5pp::fs::file_size(h5_tgt.getFilePath());

        t_opn_token.toc();

        {
            auto t_mrg_set_token = tools::prof::t_mrg_set.tic_token();
            h5_src.setKeepFileOpened();
            h5_tgt.setKeepFileOpened();
            switch(model) {
                case Model::SDUAL: {
                    tools::h5io::merge<sdual>(h5_tgt, h5_src, fileId, keys, tgtdb);
                    break;
                }
                case Model::LBIT: {
                    tools::h5io::merge<lbit>(h5_tgt, h5_src, fileId, keys, tgtdb);
                    break;
                }
            }
            h5_src.setKeepFileClosed();
            h5_tgt.setKeepFileClosed();
        }
        auto t_cnt_token = tools::prof::t_cnt.tic_token();

        tools::logger::log->debug("mem[rss {:<.2f}|peak {:<.2f}|vm {:<.2f}]MB | file db size {}", tools::prof::mem_rss_in_mb(), tools::prof::mem_hwm_in_mb(),
                                  tools::prof::mem_vm_in_mb(), tgtdb.file.size());
        if constexpr(settings::debug) {
            auto src_count_open_id = H5Fget_obj_count(h5_src.openFileHandle(), H5F_OBJ_ALL);
            auto src_count_file_id = H5Fget_obj_count(h5_src.openFileHandle(), H5F_OBJ_FILE);
            auto tgt_count_open_id = H5Fget_obj_count(h5_tgt.openFileHandle(), H5F_OBJ_ALL);
            auto tgt_count_file_id = H5Fget_obj_count(h5_tgt.openFileHandle(), H5F_OBJ_FILE);
            tools::logger::log->debug("open ids: tgt [objs {} file {}] src [objs {} file {}]", tgt_count_open_id, tgt_count_file_id, src_count_open_id,
                                      src_count_file_id);
        }

        t_cnt_token.toc();
        /* clang-format off */
        tools::logger::log->debug("-- Open  file: {:>10.3f} ms | {:>8.3f} s", 1000 * tools::prof::t_opn.get_last_interval(), tools::prof::t_opn.get_measured_time());
        tools::logger::log->debug("-- Close file: {:>10.3f} ms | {:>8.3f} s", 1000 * tools::prof::t_clo.get_last_interval(), tools::prof::t_clo.get_measured_time());
        tools::logger::log->debug("-- Prep  file: {:>10.3f} ms | {:>8.3f} s", 1000 * tools::prof::t_pre.get_last_interval(), tools::prof::t_pre.get_measured_time());
        tools::logger::log->debug("-- Hash  file: {:>10.3f} ms | {:>8.3f} s", 1000 * tools::prof::t_hsh.get_last_interval(), tools::prof::t_hsh.get_measured_time());
        tools::logger::log->debug("-- Count ids : {:>10.3f} ms | {:>8.3f} s", 1000 * tools::prof::t_cnt.get_last_interval(), tools::prof::t_cnt.get_measured_time());
        tools::logger::log->debug("-- Iterate   : {:>10.3f} ms | {:>8.3f} s", 1000 * tools::prof::t_itr.get_last_interval(), tools::prof::t_itr.get_measured_time());
        tools::logger::log->debug("-- Find Keys : {:>10.3f} ms | {:>8.3f} s", 1000 * tools::prof::t_fnd.restart_lap(), tools::prof::t_fnd.get_measured_time());
        tools::logger::log->debug("-- Merge file: {:>10.3f} ms | {:>8.3f} s", 1000 * tools::prof::t_mrg.get_last_interval(), tools::prof::t_mrg.get_measured_time());
        tools::logger::log->debug("-- -- set    : {:>10.3f} ms | {:>8.3f} s", 1000 * tools::prof::t_mrg_set.get_last_interval(), tools::prof::t_mrg_set.get_measured_time());
        tools::logger::log->debug("-- -- dsets  : {:>10.3f} ms | {:>8.3f} s", 1000 * tools::prof::t_mrg_dst.get_lap(), tools::prof::t_mrg_dst.get_measured_time());
        tools::logger::log->debug("-- -- tables : {:>10.3f} ms | {:>8.3f} s", 1000 * tools::prof::t_mrg_tab.get_lap(), tools::prof::t_mrg_tab.get_measured_time());
        tools::logger::log->debug("-- -- cronos : {:>10.3f} ms | {:>8.3f} s", 1000 * tools::prof::t_mrg_cro.get_lap(), tools::prof::t_mrg_cro.get_measured_time());
        tools::logger::log->debug("-- Dsets     : {:>10.3f} ms | {:>8.3f} s", 1000 * tools::prof::t_mrg_dst.restart_lap(), tools::prof::t_mrg_dst.get_measured_time());
        tools::logger::log->debug("-- -- gather : {:>10.3f} ms | {:>8.3f} s", 1000 * tools::prof::t_dst_get.restart_lap(), tools::prof::t_dst_get.get_measured_time());
        tools::logger::log->debug("-- -- transf : {:>10.3f} ms | {:>8.3f} s", 1000 * tools::prof::t_dst_trn.restart_lap(), tools::prof::t_dst_trn.get_measured_time());
        tools::logger::log->debug("-- -- copy   : {:>10.3f} ms | {:>8.3f} s", 1000 * tools::prof::t_dst_cpy.restart_lap(), tools::prof::t_dst_cpy.get_measured_time());
        tools::logger::log->debug("-- -- create : {:>10.3f} ms | {:>8.3f} s", 1000 * tools::prof::t_dst_crt.restart_lap(), tools::prof::t_dst_crt.get_measured_time());
        tools::logger::log->debug("-- Tables    : {:>10.3f} ms | {:>8.3f} s", 1000 * tools::prof::t_mrg_tab.restart_lap(), tools::prof::t_mrg_tab.get_measured_time());
        tools::logger::log->debug("-- -- gather : {:>10.3f} ms | {:>8.3f} s", 1000 * tools::prof::t_tab_get.restart_lap(), tools::prof::t_tab_get.get_measured_time());
        tools::logger::log->debug("-- -- transf : {:>10.3f} ms | {:>8.3f} s", 1000 * tools::prof::t_tab_trn.restart_lap(), tools::prof::t_tab_trn.get_measured_time());
        tools::logger::log->debug("-- -- copy   : {:>10.3f} ms | {:>8.3f} s", 1000 * tools::prof::t_tab_cpy.restart_lap(), tools::prof::t_tab_cpy.get_measured_time());
        tools::logger::log->debug("-- -- create : {:>10.3f} ms | {:>8.3f} s", 1000 * tools::prof::t_tab_crt.restart_lap(), tools::prof::t_tab_crt.get_measured_time());
        tools::logger::log->debug("-- Cronos    : {:>10.3f} ms | {:>8.3f} s", 1000 * tools::prof::t_mrg_cro.restart_lap(), tools::prof::t_mrg_cro.get_measured_time());
        tools::logger::log->debug("-- -- gather : {:>10.3f} ms | {:>8.3f} s", 1000 * tools::prof::t_cro_get.restart_lap(), tools::prof::t_cro_get.get_measured_time());
        tools::logger::log->debug("-- -- transf : {:>10.3f} ms | {:>8.3f} s", 1000 * tools::prof::t_cro_trn.restart_lap(), tools::prof::t_cro_trn.get_measured_time());
        tools::logger::log->debug("-- -- copy   : {:>10.3f} ms | {:>8.3f} s", 1000 * tools::prof::t_cro_cpy.restart_lap(), tools::prof::t_cro_cpy.get_measured_time());
        tools::logger::log->debug("-- -- create : {:>10.3f} ms | {:>8.3f} s", 1000 * tools::prof::t_cro_crt.restart_lap(), tools::prof::t_cro_crt.get_measured_time());
        tools::logger::log->debug("-- model     : {:>10.3f} ms | {:>8.3f} s", 1000 * tools::prof::t_ham.get_last_interval(), tools::prof::t_ham.get_measured_time());
        tools::logger::log->debug("-- gr1       : {:>10.3f} ms | {:>8.3f} s", 1000 * tools::prof::t_gr1.get_last_interval(), tools::prof::t_gr1.get_measured_time());
        tools::logger::log->debug("-- gr2       : {:>10.3f} ms | {:>8.3f} s", 1000 * tools::prof::t_gr2.get_last_interval(), tools::prof::t_gr2.get_measured_time());
        tools::logger::log->debug("-- gr3       : {:>10.3f} ms | {:>8.3f} s", 1000 * tools::prof::t_gr3.get_last_interval(), tools::prof::t_gr3.get_measured_time());
        tools::logger::log->debug("-- ch1       : {:>10.3f} ms | {:>8.3f} s", 1000 * tools::prof::t_ch1.get_last_interval(), tools::prof::t_ch1.get_measured_time());
        tools::logger::log->debug("-- ch2       : {:>10.3f} ms | {:>8.3f} s", 1000 * tools::prof::t_ch2.get_last_interval(), tools::prof::t_ch2.get_measured_time());
        tools::logger::log->debug("-- ch3       : {:>10.3f} ms | {:>8.3f} s", 1000 * tools::prof::t_ch3.get_last_interval(), tools::prof::t_ch3.get_measured_time());
        tools::logger::log->debug("-- ch4       : {:>10.3f} ms | {:>8.3f} s", 1000 * tools::prof::t_ch4.get_last_interval(), tools::prof::t_ch4.get_measured_time());
        tools::logger::log->debug("-- Total     : {:>10.3f} ms | {:>8.3f} s", 1000 * tools::prof::t_tot.restart_lap(), tools::prof::t_tot.get_measured_time());
        /* clang-format on */
    }

    // TODO: Put the lines below in a "at quick exit" function

    tools::prof::append();
    tools::h5io::writeProfiling(h5_tgt);

    tools::h5db::saveDatabase(h5_tgt, tgtdb.file);
    tools::h5db::saveDatabase(h5_tgt, tgtdb.model);
    tools::h5db::saveDatabase(h5_tgt, tgtdb.table);
    tools::h5db::saveDatabase(h5_tgt, tgtdb.dset);
    tgtdb.file.clear();
    tgtdb.model.clear();
    tgtdb.table.clear();
    tgtdb.dset.clear();

    auto ssize_objids = H5Fget_obj_count(h5_tgt.openFileHandle(), H5F_OBJ_ALL);
    if(ssize_objids > 0) {
        auto               size_objids = static_cast<size_t>(ssize_objids);
        std::vector<hid_t> objids(size_objids);
        H5Fget_obj_ids(h5_tgt.openFileHandle(), H5F_OBJ_ALL, size_objids, objids.data());
        tools::logger::log->info("File [{}] has {} open ids: {}", h5_tgt.getFilePath(), size_objids, objids);
        for(auto &id : objids) tools::logger::log->info("{}", tools::h5dbg::get_hid_string_details(id));
    } else if(ssize_objids < 0) {
        tools::logger::log->info("File [{}] failed to count ids: {}", h5_tgt.getFilePath(), ssize_objids);
    }
    // Check that there are no errors hiding in the HDF5 error-stack
    auto num_errors = H5Eget_num(H5E_DEFAULT);
    if(num_errors > 0) {
        H5Eprint(H5E_DEFAULT, stderr);
        throw std::runtime_error(fmt::format("Error when treating file [{}]", h5_tgt.getFilePath()));
    }

    tools::logger::log->info("-- Open  file: {:>8.3f} s", tools::prof::t_opn.get_measured_time());
    tools::logger::log->info("-- Close file: {:>8.3f} s", tools::prof::t_clo.get_measured_time());
    tools::logger::log->info("-- Prep  file: {:>8.3f} s", tools::prof::t_pre.get_measured_time());
    tools::logger::log->info("-- Hash  file: {:>8.3f} s", tools::prof::t_hsh.get_measured_time());
    tools::logger::log->info("-- Count ids : {:>8.3f} s", tools::prof::t_cnt.get_measured_time());
    tools::logger::log->info("-- Iterate   : {:>8.3f} s", tools::prof::t_itr.get_measured_time());
    tools::logger::log->info("-- Find Keys : {:>8.3f} s", tools::prof::t_fnd.get_measured_time());
    tools::logger::log->info("-- Merge file: {:>8.3f} s", tools::prof::t_mrg.get_measured_time());
    tools::logger::log->info("-- -- set    : {:>8.3f} s", tools::prof::t_mrg_set.get_measured_time());
    tools::logger::log->info("-- -- dsets  : {:>8.3f} s", tools::prof::t_mrg_dst.get_measured_time());
    tools::logger::log->info("-- -- tables : {:>8.3f} s", tools::prof::t_mrg_tab.get_measured_time());
    tools::logger::log->info("-- -- cronos : {:>8.3f} s", tools::prof::t_mrg_cro.get_measured_time());
    tools::logger::log->info("-- Dsets     : {:>8.3f} s", tools::prof::t_mrg_dst.get_measured_time());
    tools::logger::log->info("-- -- gather : {:>8.3f} s", tools::prof::t_dst_get.get_measured_time());
    tools::logger::log->info("-- -- transf : {:>8.3f} s", tools::prof::t_dst_trn.get_measured_time());
    tools::logger::log->info("-- -- copy   : {:>8.3f} s", tools::prof::t_dst_cpy.get_measured_time());
    tools::logger::log->info("-- -- create : {:>8.3f} s", tools::prof::t_dst_crt.get_measured_time());
    tools::logger::log->info("-- Tables    : {:>8.3f} s", tools::prof::t_mrg_tab.get_measured_time());
    tools::logger::log->info("-- -- gather : {:>8.3f} s", tools::prof::t_tab_get.get_measured_time());
    tools::logger::log->info("-- -- transf : {:>8.3f} s", tools::prof::t_tab_trn.get_measured_time());
    tools::logger::log->info("-- -- copy   : {:>8.3f} s", tools::prof::t_tab_cpy.get_measured_time());
    tools::logger::log->info("-- -- create : {:>8.3f} s", tools::prof::t_tab_crt.get_measured_time());
    tools::logger::log->info("-- Cronos    : {:>8.3f} s", tools::prof::t_mrg_cro.get_measured_time());
    tools::logger::log->info("-- -- gather : {:>8.3f} s", tools::prof::t_cro_get.get_measured_time());
    tools::logger::log->info("-- -- transf : {:>8.3f} s", tools::prof::t_cro_trn.get_measured_time());
    tools::logger::log->info("-- -- copy   : {:>8.3f} s", tools::prof::t_cro_cpy.get_measured_time());
    tools::logger::log->info("-- -- create : {:>8.3f} s", tools::prof::t_cro_crt.get_measured_time());
    tools::logger::log->info("-- model     : {:>8.3f} s", tools::prof::t_ham.get_measured_time());
    tools::logger::log->info("-- gr1       : {:>8.3f} s", tools::prof::t_gr1.get_measured_time());
    tools::logger::log->info("-- gr2       : {:>8.3f} s", tools::prof::t_gr2.get_measured_time());
    tools::logger::log->info("-- gr3       : {:>8.3f} s", tools::prof::t_gr3.get_measured_time());
    tools::logger::log->info("-- ch1       : {:>8.3f} s", tools::prof::t_ch1.get_measured_time());
    tools::logger::log->info("-- ch2       : {:>8.3f} s", tools::prof::t_ch2.get_measured_time());
    tools::logger::log->info("-- ch3       : {:>8.3f} s", tools::prof::t_ch3.get_measured_time());
    tools::logger::log->info("-- ch4       : {:>8.3f} s", tools::prof::t_ch4.get_measured_time());
    tools::logger::log->info("-- Total     : {:>8.3f} s", tools::prof::t_tot.get_measured_time());
    tools::logger::log->info("{}: {:.5f}", tools::prof::t_tot.get_name(), tools::prof::t_tot.get_measured_time());

    if(not skip_tmp) {
        tools::logger::log->info("Moving to {} -> {}", h5_tgt.getFilePath(), tools::h5io::tgt_path);
        h5_tgt.moveFileTo(tools::h5io::tgt_path, h5pp::FilePermission::REPLACE);
    }

    tools::logger::log->info("Results written to file {}", tgt_path.string());
}
