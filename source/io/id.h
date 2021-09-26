#pragma once
#include <general/text.h>
#include <h5pp/details/h5ppFormat.h>
#include <h5pp/details/h5ppHid.h>
#include <string>
#include <string_view>
#include <unordered_map>

struct FileId {
    long seed      = -1;
    char path[256] = {};
    char hash[32]  = {};
    FileId()       = default;
    FileId(long seed_, std::string_view path_, std::string_view hash_);
    [[nodiscard]] std::string string() const;
};

struct lbit {
    double J1_mean, J2_mean, J3_mean;
    double J1_wdth, J2_wdth, J3_wdth;
    double J2_base;
    size_t J2_span;
    double f_mixer;
    size_t u_layer;
};

struct sdual {
    double J_mean;
    double J_stdv;
    double h_mean;
    double h_stdv;
    double lambda;
    double delta;
};

template<typename Param>
struct ModelId {
    Param       p;
    size_t      model_size;
    std::string model_type;
    std::string distribution;
    std::string algorithm;
    std::string key;
    std::string path;
    std::string basepath;
};

struct PathId {
    public:
    std::string src_path, tgt_path;
    std::string base, algo, state, point;
    PathId(std::string_view base_, std::string_view algo_, std::string_view state_, std::string_view point_);
    [[nodiscard]] bool match(std::string_view algo_pattern, std::string_view state_pattern, std::string_view point_pattern) const;
    [[nodiscard]] std::string dset_path(std::string_view dsetname) const;
    [[nodiscard]] std::string table_path(std::string_view tablename) const;
    [[nodiscard]] std::string crono_path(std::string_view tablename, size_t iter) const;
    private:
    static bool match(std::string_view comp, std::string_view pattern);
};

template<typename InfoType>
struct InfoId {
    std::unordered_map<long, long> db;
    InfoType                       info = InfoType();
    InfoId()                            = default;
    InfoId(long seed_, long index_);
    InfoId(const InfoType &info_);
};

struct SeedId {
    long seed  = -1;
    long index = -1;
    SeedId()   = default;
    SeedId(long seed_, long index_) : seed(seed_), index(index_) {}
    [[nodiscard]] std::string string() const { return h5pp::format("seed {} | index {}", index, seed); }
};

class H5T_FileId {
    public:
    static inline h5pp::hid::h5t h5_type;
    H5T_FileId();
    static void register_table_type();
};

class H5T_SeedId {
    public:
    static inline h5pp::hid::h5t h5_type;
    H5T_SeedId();
    static void register_table_type();
};

struct H5T_profiling {
    public:
    static inline h5pp::hid::h5t h5_type;
    struct table {
        double t_tot = 0;
        double t_pre = 0;
        double t_itr = 0;
        double t_tab = 0;
        double t_grp = 0;
        double t_get = 0;
        double t_dst = 0;
        double t_ren = 0;
        double t_crt = 0;
        double t_ham = 0;
        double t_dat = 0;
    };
    H5T_profiling();
    static void register_table_type();
};
