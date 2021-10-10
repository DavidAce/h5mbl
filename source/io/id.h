#pragma once
#include <deque>
#include <general/text.h>
#include <h5pp/details/h5ppFormat.h>
#include <h5pp/details/h5ppHid.h>
#include <h5pp/details/h5ppInfo.h>
#include <set>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

struct BufferedTableInfo {
    private:
    struct ContiguousBuffer {
        hsize_t                offset = 0; // In table units
        hsize_t                extent = 0; // In table units
        std::vector<std::byte> rawdata;
    };

    public:
    h5pp::TableInfo              *info = nullptr;
    std::vector<ContiguousBuffer> recordBuffer;
    size_t                        maxRecords = 1000;

    BufferedTableInfo();
    BufferedTableInfo(h5pp::TableInfo *info_);
    BufferedTableInfo &operator=(h5pp::TableInfo *info_);
    ~BufferedTableInfo();

    void insert(const std::vector<std::byte> &entry, hsize_t index /* In units of table entries */);
    void flush();
};

struct FileStats {
    size_t               files = 0;
    size_t               count = 0;
    uintmax_t            bytes = 0;
    double               elaps = 0.0;
    [[nodiscard]] double get_speed() const {
        if(elaps == 0)
            return 0.;
        else
            return static_cast<double>(count) / elaps;
    }
};

struct FileId {
    long seed      = -1;
    char path[256] = {};
    char hash[32]  = {};
    FileId()       = default;
    FileId(long seed_, std::string_view path_, std::string_view hash_);
    [[nodiscard]] std::string string() const;
};

struct lbit {
    double                   J1_mean, J2_mean, J3_mean;
    double                   J1_wdth, J2_wdth, J3_wdth;
    double                   J2_base;
    size_t                   J2_span;
    double                   f_mixer;
    size_t                   u_layer;
    std::vector<std::string> fields = {"J1_mean", "J2_mean", "J3_mean", "J1_wdth", "J2_wdth", "J3_wdth", "J2_base", "J2_span", "f_mixer", "u_layer"};
};

struct sdual {
    double                   J_mean;
    double                   J_stdv;
    double                   h_mean;
    double                   h_stdv;
    double                   lambda;
    double                   delta;
    std::vector<std::string> fields = {"J_mean", "J_stdv", "h_mean", "h_stdv", "lambda", "delta"};
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
    [[nodiscard]] bool        match(std::string_view algo_pattern, std::string_view state_pattern, std::string_view point_pattern) const;
    [[nodiscard]] std::string dset_path(std::string_view dsetname) const;
    [[nodiscard]] std::string table_path(std::string_view tablename) const;
    [[nodiscard]] std::string crono_path(std::string_view tablename, size_t iter) const;

    private:
    static bool match(std::string_view comp, std::string_view pattern);
};

template<typename InfoType>
struct InfoId {
    private:
    bool                              modified = false;
    std::unordered_map<long, hsize_t> db;

    public:
    InfoType info = InfoType();
    InfoId()      = default;
    InfoId(long seed_, hsize_t index_);
    InfoId(const InfoType &info_);
    bool    db_modified() const { return modified; }
    bool    has_index(long seed) const { return db.find(seed) != db.end(); }
    hsize_t get_index(long seed) const {
        auto res = db.find(seed);
        if(res != db.end())
            return res->second;
        else
            return std::numeric_limits<hsize_t>::max();
    }
    void insert(long seed, hsize_t index) {
        auto res = db.insert({seed, index});
        if(res.second) modified = true;
    }
    [[nodiscard]] const std::unordered_map<long, hsize_t> &get_db() const { return db; }
};

template<>
struct InfoId<BufferedTableInfo> {
    private:
    bool                              modified = false;
    std::unordered_map<long, hsize_t> db;

    public:
    h5pp::TableInfo   info = h5pp::TableInfo();
    BufferedTableInfo buff = BufferedTableInfo();
    InfoId()               = default;
    InfoId(long seed_, hsize_t index_);
    InfoId(const h5pp::TableInfo &info_);
    InfoId &operator=(const h5pp::TableInfo &info_);
    bool    db_modified() const { return modified; }
    bool    has_index(long seed) const { return db.find(seed) != db.end(); }
    hsize_t get_index(long seed) const {
        auto res = db.find(seed);
        if(res != db.end())
            return res->second;
        else
            return std::numeric_limits<hsize_t>::max();
    }
    void insert(long seed, hsize_t index) {
        auto res = db.insert({seed, index});
        if(res.second) modified = true;
    }

    [[nodiscard]] const std::unordered_map<long, hsize_t> &get_db() const { return db; }
};

struct SeedId {
    long seed  = -1;
    hsize_t index = std::numeric_limits<hsize_t>::max();
    SeedId()   = default;
    SeedId(long seed_, hsize_t index_) : seed(seed_), index(index_) {}
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


class H5T_profiling {
    public:
    static inline h5pp::hid::h5t h5_type;

    struct item {
        double time;
        double avg;
        size_t count;
    };

    H5T_profiling();
    static void register_table_type();
};