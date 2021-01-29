#include "h5io.h"
#include <functional>
#include <general/prof.h>
#include <general/text.h>
#include <h5pp/h5pp.h>
#include <io/id.h>
#include <io/logger.h>
#include <io/meta.h>
#include <io/parse.h>
#include <regex>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace tools::h5io {
    namespace internal {
        template<typename T>
        void append_dset(h5pp::File &h5_tgt, const h5pp::File &h5_src, h5pp::DsetInfo &tgtInfo, h5pp::DsetInfo &srcInfo) {
            auto   data    = h5_src.readDataset<T>(srcInfo);
            size_t maxrows = data.size();
            if(tgtInfo.dsetDims) maxrows = std::min<size_t>(maxrows, tgtInfo.dsetDims.value()[0]);
            h5_tgt.appendToDataset(data, tgtInfo, 1, {maxrows, 1});
        }
        template<typename T>
        void copy_dset(h5pp::File &h5_tgt, const h5pp::File &h5_src, h5pp::DsetInfo &tgtInfo, h5pp::DsetInfo &srcInfo, long index) {
            auto uindex = static_cast<size_t>(index);
            if(uindex >= tgtInfo.dsetDims.value().at(1)) return append_dset<T>(h5_tgt, h5_src, tgtInfo, srcInfo);

            auto data = h5_src.readDataset<T>(srcInfo);

            size_t maxrows = data.size();
            if(tgtInfo.dsetDims) maxrows = std::min<size_t>(maxrows, tgtInfo.dsetDims.value()[0]);

            h5pp::Options options;
            tgtInfo.resizePolicy     = h5pp::ResizePolicy::INCREASE_ONLY;
            tgtInfo.dsetSlab         = h5pp::Hyperslab();
            tgtInfo.dsetSlab->extent = {maxrows, 1};
            tgtInfo.dsetSlab->offset = {0, uindex};
            options.dataDims         = {maxrows, 1};
            h5_tgt.writeDataset(data, tgtInfo, options);

            // Restore previous settings
            tgtInfo.resizePolicy = h5pp::ResizePolicy::INCREASE_ONLY;
            tgtInfo.dsetSlab     = std::nullopt;
        }

        struct SearchResult {
            std::string                      root;
            std::string                      key;
            long                             hits;
            long                             depth;
            mutable std::vector<std::string> result;
            bool                             operator==(const SearchResult &rhs) const {
                return this->root == rhs.root and this->key == rhs.key and this->hits == rhs.hits and this->depth == rhs.depth;
            }
        };

        struct SearchHasher {
            auto operator()(const SearchResult &r) const { return std::hash<std::string>{}(fmt::format("{}|{}|{}|{}", r.root, r.key, r.hits, r.depth)); }
        };
    }

    template<typename T>
    std::string get_standardized_base(const ModelId<T> &H, int decimals) {
        if constexpr(std::is_same_v<T, sdual>) return h5pp::format("L_{1}/l_{2:.{0}f}/d_{3:+.{0}f}", decimals, H.model_size, H.p.lambda, H.p.delta);
        if constexpr(std::is_same_v<T, lbit>) {
            decimals = 2;
            std::string J_mean_str = fmt::format("J[{1:+.{0}f}_{2:+.{0}f}_{3:+.{0}f}]", decimals, H.p.J1_mean, H.p.J2_mean, H.p.J3_mean);
            std::string J_wdth_str = fmt::format("w[{1:+.{0}f}_{2:+.{0}f}_{3:+.{0}f}]", decimals, H.p.J1_wdth, H.p.J2_wdth, H.p.J3_wdth);
            std::string f_str = fmt::format("f_{1:.{0}f}",decimals, H.p.f_mixer);
            std::string u_str = fmt::format("u_{}", H.p.u_layer );
            return h5pp::format("L_{}/{}/{}/{}/{}", H.model_size, J_mean_str, J_wdth_str, f_str, u_str);
        }
    }
    template std::string get_standardized_base(const ModelId<sdual> &H, int decimals);
    template std::string get_standardized_base(const ModelId<lbit> &H, int decimals);

    std::vector<std::string> findKeys(const h5pp::File &h5_src, const std::string &root, const std::vector<std::string> &expectedKeys, long hits, long depth) {
        static std::unordered_set<internal::SearchResult, internal::SearchHasher> cache;
        std::vector<std::string>                                                  result;
        for(const auto &key : expectedKeys) {
            internal::SearchResult searchQuery = {root, key, hits, depth, {}};
            auto                   cachedIt    = cache.find(searchQuery);
            bool                   cacheHit    = cachedIt != cache.end() and                      // The search query has been processed earlier
                            ((hits > 0 and static_cast<long>(cachedIt->result.size()) >= hits) or // Asked for a up to "hits" reslts
                             (hits <= 0 and static_cast<long>(cachedIt->result.size()) >= 1)      // Asked for any number of results
                            );
            if(cacheHit) {
                tools::logger::log->trace("Cache hit: key {} | result {} | cache {}", key, result, cachedIt->result);
                for(auto &item : cachedIt->result) {
                    if(result.size() > 1 and std::find(result.begin(), result.end(), item) != result.end()) continue;
                    result.emplace_back(item);
                }
            } else {
                tools::logger::log->trace("Searching: key {} | result {}", key, result);
                std::vector<std::string> found;
                if(key.empty())
                    found.emplace_back(key);
                else if(key.back() == '*') {
                    std::string_view key_match = std::string_view(key).substr(0, key.size() - 2); // .substr(0,key.size()-2);
                    for(auto &item : h5_src.findGroups(key_match, root, hits, depth)) {
                        if(not text::startsWith(item, key_match)) continue;
                        if(found.size() > 1 and std::find(found.begin(), found.end(), item) != found.end()) continue;
                        found.emplace_back(item);
                    }
                } else {
                    for(auto &item : h5_src.findGroups(key, root, hits, depth)) {
                        if(not text::endsWith(item, key)) continue;
                        if(found.size() > 1 and std::find(found.begin(), found.end(), item) != found.end()) continue;
                        found.emplace_back(item);
                    }
                }
                auto [it, ok] = cache.insert(searchQuery);
                it->result    = found;
                // Now we have built a cache hit. Add it to results
                for(auto &item : it->result) {
                    if(result.size() > 1 and std::find(result.begin(), result.end(), item) != result.end()) continue;
                    result.emplace_back(item);
                }
            }
        }
        return result;
    }
    template<typename T>
    std::string loadModel(const h5pp::File &h5_src, std::unordered_map<std::string, ModelId<T>> &srcModelDb, const std::string &algo) {
        tools::prof::t_ham.tic();
        auto modelPath   = fmt::format("{}/model/hamiltonian", algo);
        auto modelKey    = fmt::format("{}|{}", h5pp::fs::path(h5_src.getFilePath()).parent_path(), algo);
        auto modelExists = srcModelDb.find(modelKey) != srcModelDb.end();
        if(not modelExists and h5_src.linkExists(modelPath)) {
            // Copy the model
            srcModelDb[modelKey] = ModelId<T>();
            auto &H              = srcModelDb[modelKey];
            H.model_size         = h5_src.readAttribute<size_t>("model_size", modelPath);
            H.model_type         = h5_src.readAttribute<std::string>("model_type", modelPath);
            H.distribution       = h5_src.readAttribute<std::string>("distribution", modelPath);
            H.algorithm          = algo;
            H.key                = modelKey;
            H.path               = modelPath;
            if constexpr(std::is_same_v<T, sdual>) {
                H.p.J_mean = h5_src.readAttribute<double>("J_mean", modelPath);
                H.p.J_stdv = h5_src.readAttribute<double>("J_stdv", modelPath);
                H.p.h_mean = h5_src.readAttribute<double>("h_mean", modelPath);
                H.p.h_stdv = h5_src.readAttribute<double>("h_stdv", modelPath);
                H.p.lambda = h5_src.readAttribute<double>("lambda", modelPath);
                H.p.delta  = h5_src.readAttribute<double>("delta", modelPath);
            }
            if constexpr(std::is_same_v<T, lbit>) {
                H.p.J1_mean = h5_src.readAttribute<double>("J1_mean", modelPath);
                H.p.J2_mean = h5_src.readAttribute<double>("J2_mean", modelPath);
                H.p.J3_mean = h5_src.readAttribute<double>("J3_mean", modelPath);
                H.p.J1_wdth = h5_src.readAttribute<double>("J1_wdth", modelPath);
                H.p.J2_wdth = h5_src.readAttribute<double>("J2_wdth", modelPath);
                H.p.J3_wdth = h5_src.readAttribute<double>("J3_wdth", modelPath);
                try{
                    H.p.f_mixer = h5_src.readAttribute<double>("f_mixer", modelPath);
                    H.p.u_layer = h5_src.readAttribute<size_t>("u_layer", modelPath);
                }catch (const std::exception & ex){
                    H.p.f_mixer = tools::parse::extract_paramter_from_path<double>(h5_src.getFilePath(), "f+");
                    H.p.u_layer = 6;
                    tools::logger::log->debug("Could not find model parameter: {} | Replaced with f=[{:.2f}] u=[{}]", ex.what(), H.p.f_mixer, H.p.u_layer);
                }
            }
        }
        tools::prof::t_ham.toc();
        return modelKey;
    }
    template std::string loadModel(const h5pp::File &h5_src, std::unordered_map<std::string, ModelId<sdual>> &srcModelDb, const std::string &algo);
    template std::string loadModel(const h5pp::File &h5_src, std::unordered_map<std::string, ModelId<lbit>> &srcModelDb, const std::string &algo);

    template<typename T>
    void saveModel(const h5pp::File &h5_src, h5pp::File &h5_tgt, std::unordered_map<std::string, InfoId<h5pp::TableInfo>> &tgtModelDb,
                   const ModelId<T> &modelId) {
        tools::prof::t_ham.tic();
        const auto &      algo         = modelId.algorithm;
        const std::string tgt_base     = get_standardized_base(modelId);
        const std::string tgtModelPath = fmt::format("{}/{}", tgt_base, modelId.path);
        bool              tgtExists    = tgtModelDb.find(tgtModelPath) != tgtModelDb.end();
        if(not tgtExists) {
            // Copy the whole Hamiltonian table (with site information)
//            h5_tgt.copyLinkFromLocation(tgtModelPath, h5_src.openFileHandle(), modelId.path);
            tgtModelDb[tgtModelPath] = h5_tgt.getTableInfo(tgtModelPath);
            // Now copy some helpful scalar datasets. This data is available in the attributes of the table
            // above but this is also handy
            h5_tgt.writeDataset(modelId.model_size, fmt::format("{}/{}/model/model_size", tgt_base, algo));
            h5_tgt.writeDataset(modelId.model_type, fmt::format("{}/{}/model/model_type", tgt_base, algo));
            h5_tgt.writeDataset(modelId.distribution, fmt::format("{}/{}/model/distribution", tgt_base, algo));
            if constexpr(std::is_same_v<T, sdual>) {
                h5_tgt.writeDataset(modelId.p.J_mean, fmt::format("{}/{}/model/J_mean", tgt_base, algo));
                h5_tgt.writeDataset(modelId.p.J_stdv, fmt::format("{}/{}/model/J_stdv", tgt_base, algo));
                h5_tgt.writeDataset(modelId.p.h_mean, fmt::format("{}/{}/model/h_mean", tgt_base, algo));
                h5_tgt.writeDataset(modelId.p.h_stdv, fmt::format("{}/{}/model/h_stdv", tgt_base, algo));
                h5_tgt.writeDataset(modelId.p.lambda, fmt::format("{}/{}/model/lambda", tgt_base, algo));
                h5_tgt.writeDataset(modelId.p.delta, fmt::format("{}/{}/model/delta", tgt_base, algo));
            }
            if constexpr(std::is_same_v<T, lbit>) {
                h5_tgt.writeDataset(modelId.p.J1_mean, fmt::format("{}/{}/model/J1_mean", tgt_base, algo));
                h5_tgt.writeDataset(modelId.p.J2_mean, fmt::format("{}/{}/model/J2_mean", tgt_base, algo));
                h5_tgt.writeDataset(modelId.p.J3_mean, fmt::format("{}/{}/model/J3_mean", tgt_base, algo));
                h5_tgt.writeDataset(modelId.p.J1_wdth, fmt::format("{}/{}/model/J1_wdth", tgt_base, algo));
                h5_tgt.writeDataset(modelId.p.J2_wdth, fmt::format("{}/{}/model/J2_wdth", tgt_base, algo));
                h5_tgt.writeDataset(modelId.p.J3_wdth, fmt::format("{}/{}/model/J3_wdth", tgt_base, algo));
                h5_tgt.writeDataset(modelId.p.f_mixer, fmt::format("{}/{}/model/f_mixer", tgt_base, algo));
                h5_tgt.writeDataset(modelId.p.u_layer, fmt::format("{}/{}/model/u_layer", tgt_base, algo));
            }
        }
        tools::prof::t_ham.toc();
    }
    template void saveModel(const h5pp::File &h5_src, h5pp::File &h5_tgt, std::unordered_map<std::string, InfoId<h5pp::TableInfo>> &tgtModelDb,
                            const ModelId<sdual> &modelId);
    template void saveModel(const h5pp::File &h5_src, h5pp::File &h5_tgt, std::unordered_map<std::string, InfoId<h5pp::TableInfo>> &tgtModelDb,
                            const ModelId<lbit> &modelId);

    std::vector<std::string> gatherTableKeys(const h5pp::File &h5_src, std::unordered_map<std::string, h5pp::TableInfo> &srcTableDb,
                                             const std::string &groupPath, const std::vector<std::string> &tables) {
        prof::t_get.tic();
        std::vector<std::string> keys;
        try {
            h5pp::Options options;
            std::string   srcParentPath = h5pp::fs::path(h5_src.getFilePath()).parent_path();
            for(auto &table : tables) {
                auto tablePath = fmt::format("{}/{}", groupPath, table);
                auto tableKey  = fmt::format("{}|{}", srcParentPath, tablePath);
                if(srcTableDb.find(tableKey) == srcTableDb.end()) {
                    tools::logger::log->info("Detected new source table {}", tableKey);
                    srcTableDb[tableKey] = h5_src.getTableInfo(tablePath);
                }
                auto &info = srcTableDb[tableKey];
                // We reuse the struct srcDsetDb[dsetKey] for every source file,
                // but each time have to renew the following fields
                info.h5Dset      = std::nullopt;
                info.h5File      = h5_src.openFileHandle();
                info.numRecords  = std::nullopt;
                info.tableExists = std::nullopt;
                options.linkPath = tablePath;
                h5pp::scan::readTableInfo(info, info.h5File.value(), options);
                if(info.tableExists and info.tableExists.value())
                    keys.emplace_back(tableKey);
                else
                    tools::logger::log->debug("Missing table [{}] in file [{}]", tablePath, h5_src.getFilePath());

                //                if(not info.tableExists or not info.tableExists.value())
                //                    throw std::runtime_error(h5pp::format("Missing table [{}] in file [{}]", tablePath, h5_src.getFilePath()));
            }
        } catch(const std::exception &ex) {
            prof::t_get.toc();
            throw;
        }
        prof::t_get.toc();
        return keys;
    }

    std::vector<DsetKey> gatherDsetKeys(const h5pp::File &h5_src, std::unordered_map<std::string, h5pp::DsetInfo> &srcDsetDb, const std::string &groupPath,
                                        const std::vector<DsetKey> &srcDsetMetas) {
        prof::t_get.tic();
        std::vector<DsetKey> keys;
        try {
            std::string   srcParentPath = h5pp::fs::path(h5_src.getFilePath()).parent_path();
            h5pp::Options options;
            for(auto &meta : srcDsetMetas) {
                // A "meta" is a dataset metadata struct, with name, type, size, etc
                // Here we look for a matching dataset in the given group path, and generate a key for it.
                // The metas that are found are saved in keys.
                // The database srcDsetDb  is used to keep track the DsetInfo structs for found datasets.
                auto dsetPath = fmt::format("{}/{}", groupPath, meta.name);
                auto dsetKey  = fmt::format("{}|{}", srcParentPath, dsetPath);
                if(srcDsetDb.find(dsetKey) == srcDsetDb.end()) {
                    tools::logger::log->info("Detected new source dataset {}", dsetPath);
                    srcDsetDb[dsetKey] = h5_src.getDatasetInfo(dsetPath);
                }
                auto &info = srcDsetDb[dsetKey];
                // We reuse the struct srcDsetDb[dsetKey] for every source file,
                // but each time have to renew the following fields
                info.h5Dset      = std::nullopt;
                info.h5File      = h5_src.openFileHandle();
                info.h5Space     = std::nullopt;
                info.dsetExists  = std::nullopt;
                info.dsetSize    = std::nullopt;
                info.dsetDims    = std::nullopt;
                info.dsetByte    = std::nullopt;
                options.linkPath = dsetPath;
                h5pp::scan::readDsetInfo(info, info.h5File.value(), options, h5_src.plists);
                if(info.dsetExists and info.dsetExists.value()) {
                    keys.emplace_back(meta);
                    keys.back().key = dsetKey;
                } else {
                    tools::logger::log->debug("Missing dataset [{}] in file [{}]", dsetPath, h5_src.getFilePath());
                }
                //                if(not info.dsetExists or not info.dsetExists.value())
                //                    throw std::runtime_error(h5pp::format("Missing dataset [{}] in file [{}]", dsetPath, h5_src.getFilePath()));
            }
        } catch(const std::exception &ex) {
            prof::t_get.toc();
            throw ex;
        }
        prof::t_get.toc();
        return keys;
    }

    void transferDatasets(h5pp::File &h5_tgt, std::unordered_map<std::string, InfoId<h5pp::DsetInfo>> &tgtDsetDb, const h5pp::File &h5_src,
                          std::unordered_map<std::string, h5pp::DsetInfo> &srcDsetDb, const std::string &groupPath, const std::vector<DsetKey> &srcDsetMetas,
                          const FileId &fileId) {
        for(const auto &meta : srcDsetMetas) {
            auto &srcKey = meta.key;
            if(srcDsetDb.find(srcKey) == srcDsetDb.end()) throw std::runtime_error(h5pp::format("Key [{}] was not found in source map", srcKey));
            auto &srcInfo = srcDsetDb[srcKey];
            auto  tgtName = h5pp::fs::path(srcInfo.dsetPath.value()).filename().string();
            auto  tgtPath = h5pp::format("{}/{}", groupPath, tgtName);
            tools::prof::t_crt.tic();
            try {
                if(tgtDsetDb.find(tgtPath) == tgtDsetDb.end()) {
                    long rows = 0;
                    long cols = 0;
                    switch(meta.size) {
                        case Size::FIX: rows = static_cast<long>(srcInfo.dsetDims.value()[0]); break;
                        case Size::VAR: {
                            auto        srcGroupPath    = h5pp::fs::path(srcInfo.dsetPath.value()).parent_path().string();
                            std::string statusTablePath = fmt::format("{}/status", srcGroupPath);
                            rows                        = h5_src.readTableField<long>(statusTablePath, "cfg_chi_lim_max", h5pp::TableSelection::FIRST);
                            break;
                        }
                    }
                    tools::logger::log->info("Adding target dset {}", tgtPath);
                    tgtDsetDb[tgtPath] = h5_tgt.createDataset(srcInfo.h5Type.value(), tgtPath, {rows, cols}, H5D_CHUNKED, {rows, 100l});
                }
            } catch(const std::exception &ex) {
                tools::prof::t_crt.toc();
                throw ex;
            }
            tools::prof::t_crt.toc();
            tools::prof::t_dst.tic();
            try {
                auto &tgtInfo = tgtDsetDb[tgtPath].info;
                auto &tgtDb   = tgtDsetDb[tgtPath].db;
                // Determine the target index where to copy this record
                long index = static_cast<long>(tgtInfo.dsetDims.value().at(1));
                if(tgtDb.find(fileId.seed) != tgtDb.end()) {
                    index = tgtDb[fileId.seed];
                    //                    tools::logger::log->info("Found seed {} at index {}: dset {}", fileId.seed,index, srcInfo.dsetPath.value());
                }

                switch(meta.type) {
                    case Type::DOUBLE: internal::copy_dset<std::vector<double>>(h5_tgt, h5_src, tgtInfo, srcInfo, index); break;
                    case Type::LONG: internal::copy_dset<std::vector<long>>(h5_tgt, h5_src, tgtInfo, srcInfo, index); break;
                    case Type::INT: internal::copy_dset<std::vector<int>>(h5_tgt, h5_src, tgtInfo, srcInfo, index); break;
                    case Type::COMPLEX: internal::copy_dset<std::vector<std::complex<double>>>(h5_tgt, h5_src, tgtInfo, srcInfo, index); break;
                }

                // Update the database
                tgtDb[fileId.seed] = index;

            } catch(const std::exception &ex) {
                tools::prof::t_dst.toc();
                throw ex;
            }
            tools::prof::t_dst.toc();
        }
    }

    void transferTables(h5pp::File &h5_tgt, std::unordered_map<std::string, InfoId<h5pp::TableInfo>> &tgtTableDb,
                        const std::unordered_map<std::string, h5pp::TableInfo> &srcTableDb, const std::string &groupPath,
                        const std::vector<std::string> &srcTableKeys, const FileId &fileId) {
        for(const auto &srcKey : srcTableKeys) {
            if(srcTableDb.find(srcKey) == srcTableDb.end()) throw std::runtime_error(h5pp::format("Key [{}] was not found in source map", srcKey));
            auto &srcInfo = srcTableDb.at(srcKey);
            auto  tgtName = h5pp::fs::path(srcInfo.tablePath.value()).filename().string();
            auto  tgtPath = h5pp::format("{}/{}", groupPath, tgtName);
            tools::prof::t_crt.tic();
            try {
                if(tgtTableDb.find(tgtPath) == tgtTableDb.end()) {
                    tools::logger::log->info("Adding target table {}", tgtPath);
                    h5pp::TableInfo tableInfo = h5_tgt.getTableInfo(tgtPath);
                    if(not tableInfo.tableExists.value()) tableInfo = h5_tgt.createTable(srcInfo.h5Type.value(), tgtPath, srcInfo.tableTitle.value());
                    tgtTableDb[tgtPath] = tableInfo;
                }
            } catch(const std::exception &ex) {
                tools::prof::t_crt.toc();
                throw ex;
            }
            tools::prof::t_crt.toc();

            auto &tgtInfo = tgtTableDb[tgtPath].info;
            auto &tgtDb   = tgtTableDb[tgtPath].db;

            // Determine the target index where to copy this record
            tools::prof::t_tab.tic();
            try {
                long index = static_cast<long>(tgtInfo.numRecords.value());
                if(tgtDb.find(fileId.seed) != tgtDb.end()) {
                    index = tgtDb[fileId.seed];
                    //                    tools::logger::log->info("Found seed {} at index {}: table {}", fileId.seed,index, srcInfo.tablePath.value());
                }
                h5_tgt.copyTableRecords(srcInfo, h5pp::TableSelection::LAST, tgtInfo, static_cast<hsize_t>(index));
                // Update the database
                tgtDb[fileId.seed] = index;
            } catch(const std::exception &ex) {
                tools::prof::t_tab.toc();
                throw ex;
            }
            tools::prof::t_tab.toc();
        }
    }

    void transferCronos(h5pp::File &h5_tgt, std::unordered_map<std::string, InfoId<h5pp::TableInfo>> &tgtTableDb,
                        const std::unordered_map<std::string, h5pp::TableInfo> &srcTableDb, const std::string &groupPath,
                        const std::vector<std::string> &srcTableKeys, const FileId &fileId) {
        // In this function we take time series data from each srcTable and create multiple tables tgtTable, one for each
        // time point (iteration). Each entry in tgtTable corresponds to the same time point on different realizations.

        for(const auto &srcKey : srcTableKeys) {
            if(srcTableDb.find(srcKey) == srcTableDb.end()) throw std::runtime_error(h5pp::format("Key [{}] was not found in source map", srcKey));
            auto &srcInfo    = srcTableDb.at(srcKey);
            auto  srcRecords = srcInfo.numRecords.value();

            // Iterate over all table elements. These should be a time series measured at every iteration
            // Note that there could in principle exist duplicate entries, which is why we can't trust the
            // "rec" iterator but have to get the iteration number from the table directly.
            // Try getting the iteration number, which is more accurate.
            tools::prof::t_cro.tic();
            std::vector<size_t> iters;

            try {
                h5pp::hdf5::readTableField(iters, srcInfo, {"iter"});
            } catch(const std::exception &ex) {
                tools::logger::log->warn("Failed to get iteration numbers: {}", ex.what());
            }
            tools::prof::t_cro.toc();

            for(size_t rec = 0; rec < srcRecords; rec++) {
                size_t iter = rec;
                if(not iters.empty()) iter = iters.at(rec); // Get the actual iteration number
                auto tgtName = h5pp::format("{}", iter);
                auto tgtPath = h5pp::format("{}/iter/{}/{}", groupPath,h5pp::fs::path(srcInfo.tablePath.value()).filename().string(), tgtName);
                tools::prof::t_crt.tic();
                try {
                    if(tgtTableDb.find(tgtPath) == tgtTableDb.end()) {
                        tools::logger::log->info("Adding target table {}", tgtPath);
                        h5pp::TableInfo tableInfo = h5_tgt.getTableInfo(tgtPath);
                        if(not tableInfo.tableExists.value()) tableInfo = h5_tgt.createTable(srcInfo.h5Type.value(), tgtPath, srcInfo.tableTitle.value());
                        tgtTableDb[tgtPath] = tableInfo;
                    }
                } catch(const std::exception &ex) {
                    tools::prof::t_crt.toc();
                    throw ex;
                }
                tools::prof::t_crt.toc();

                auto &tgtInfo = tgtTableDb[tgtPath].info;
                auto &tgtDb   = tgtTableDb[tgtPath].db;

                // Determine the target index where to copy this record
                // Under normal circumstances, the "index" counts the number of realizations, or simulation seeds.
                tools::prof::t_cro.tic();
                try {
                    // tgtInfo.numRecords is the total number of realizations registered until now
                    long index = static_cast<long>(tgtInfo.numRecords.value());
                    if(tgtDb.find(fileId.seed) != tgtDb.end()) {
                        index = tgtDb[fileId.seed];
                        //                    tools::logger::log->info("Found seed {} at index {}: table {}", fileId.seed,index, srcInfo.tablePath.value());
                    }
                    // copy/append a source record at "iter" into the "index" position on the table.
                    h5_tgt.copyTableRecords(srcInfo, rec, 1, tgtInfo, static_cast<hsize_t>(index));
                    // Update the database
                    tgtDb[fileId.seed] = index;
                } catch(const std::exception &ex) {
                    tools::prof::t_cro.toc();
                    throw ex;
                }
                tools::prof::t_cro.toc();
            }
        }
    }

    void writeProfiling(h5pp::File &h5_tgt) {
        H5T_profiling::register_table_type();
        if(not h5_tgt.linkExists(".db/prof")) { h5_tgt.createTable(H5T_profiling::h5_type, ".db/prof", "H5MBL Profiling", {100}, 4); }
        h5_tgt.appendTableRecords(tools::prof::buffer, ".db/prof");
    }
}