#include "h5io.h"
#include <functional>
#include <general/prof.h>
#include <general/text.h>
#include <h5pp/h5pp.h>
#include <io/h5dbg.h>
#include <io/id.h>
#include <io/logger.h>
#include <io/meta.h>
#include <io/parse.h>
#include <io/type.h>
#include <regex>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <cstdlib>

namespace tools::h5io {
    std::string get_tmp_dirname(std::string_view exename) {
        return fmt::format("{}.{}", h5pp::fs::path(exename).filename().string(),getenv("USER")); }

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
            auto   data    = h5_src.readDataset<T>(srcInfo);
            size_t maxrows = data.size();
            if(tgtInfo.dsetDims) maxrows = std::min<size_t>(maxrows, tgtInfo.dsetDims.value()[0]);

            h5pp::Options options;
            tgtInfo.resizePolicy     = h5pp::ResizePolicy::GROW;
            tgtInfo.dsetSlab         = h5pp::Hyperslab();
            tgtInfo.dsetSlab->extent = {maxrows, 1};
            tgtInfo.dsetSlab->offset = {0, uindex};
            options.dataDims         = {maxrows, 1};
            h5_tgt.writeDataset(data, tgtInfo, options);
            // Restore previous settings
            tgtInfo.resizePolicy = h5pp::ResizePolicy::GROW;
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
            decimals               = 2;
            std::string J_mean_str = fmt::format("J[{1:+.{0}f}_{2:+.{0}f}_{3:+.{0}f}]", decimals, H.p.J1_mean, H.p.J2_mean, H.p.J3_mean);
            std::string J_wdth_str = fmt::format("w[{1:+.{0}f}_{2:+.{0}f}_{3:+.{0}f}]", decimals, H.p.J1_wdth, H.p.J2_wdth, H.p.J3_wdth);
            std::string b_str      = fmt::format("b_{1:.{0}f}", decimals, H.p.J2_base);
            std::string f_str      = fmt::format("f_{1:.{0}f}", decimals, H.p.f_mixer);
            std::string u_str      = fmt::format("u_{}", H.p.u_layer);
            std::string r_str      = fmt::format("r_{}", H.p.J2_span);
            return h5pp::format("L_{}/{}/{}/{}/{}/{}/{}", H.model_size, J_mean_str, J_wdth_str, b_str, f_str, u_str, r_str);
        }
    }
    template std::string get_standardized_base(const ModelId<sdual> &H, int decimals);
    template std::string get_standardized_base(const ModelId<lbit> &H, int decimals);

    std::vector<std::string> findKeys(const h5pp::File &h5_src, const std::string &root, const std::vector<std::string> &expectedKeys, long hits, long depth) {
        auto                                                                      t_fnd_token = tools::prof::t_fnd.tic_token();
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
    std::vector<ModelKey> loadModel(const h5pp::File &h5_src, std::unordered_map<std::string, ModelId<T>> &srcModelDb, const std::vector<ModelKey> &srcKeys) {
        auto                  t_ham_token = tools::prof::t_ham.tic_token();
        std::vector<ModelKey> keys;
        for(const auto &srcKey : srcKeys) {
            auto path = fmt::format("{}/{}/{}", srcKey.algo, srcKey.model, srcKey.name);
            auto key  = fmt::format("{}|{}", h5pp::fs::path(h5_src.getFilePath()).parent_path(), path);
            if(srcModelDb.find(key) == srcModelDb.end() and h5_src.linkExists(path)) {
                // Copy the model from the attributes in h5_src to a struct ModelId
                srcModelDb[key]   = ModelId<T>();
                auto &srcModelId  = srcModelDb[key];
                auto &hamiltonian = srcModelId.p;
                if constexpr(std::is_same_v<T, sdual>) {
                    hamiltonian.J_mean = h5_src.readAttribute<double>("J_mean", path);
                    hamiltonian.J_stdv = h5_src.readAttribute<double>("J_stdv", path);
                    hamiltonian.h_mean = h5_src.readAttribute<double>("h_mean", path);
                    hamiltonian.h_stdv = h5_src.readAttribute<double>("h_stdv", path);
                    hamiltonian.lambda = h5_src.readAttribute<double>("lambda", path);
                    hamiltonian.delta  = h5_src.readAttribute<double>("delta", path);
                }
                if constexpr(std::is_same_v<T, lbit>) {
                    hamiltonian.J1_mean = h5_src.readAttribute<double>("J1_mean", path);
                    hamiltonian.J2_mean = h5_src.readAttribute<double>("J2_mean", path);
                    hamiltonian.J3_mean = h5_src.readAttribute<double>("J3_mean", path);
                    hamiltonian.J1_wdth = h5_src.readAttribute<double>("J1_wdth", path);
                    hamiltonian.J2_wdth = h5_src.readAttribute<double>("J2_wdth", path);
                    hamiltonian.J3_wdth = h5_src.readAttribute<double>("J3_wdth", path);
                    try {
                        hamiltonian.J2_base = h5_src.readAttribute<double>("J2_base", path);
                    } catch(const std::exception &ex) {
                        hamiltonian.J2_base = tools::parse::extract_paramter_from_path<double>(h5_src.getFilePath(), "b_");
                        tools::logger::log->debug("Could not find model parameter: {} | Replaced with b=[{:.2f}]", ex.what(), hamiltonian.J2_base);
                    }
                    try {
                        hamiltonian.J2_span = h5_src.readAttribute<size_t>("J2_span", path);
                    } catch(const std::exception &ex) {
                        hamiltonian.J2_span = tools::parse::extract_paramter_from_path<size_t>(h5_src.getFilePath(), "r_");
                        tools::logger::log->debug("Could not find model parameter: {} | Replaced with r=[{}]", ex.what(), hamiltonian.J2_span);
                    }
                    try {
                        hamiltonian.f_mixer = h5_src.readAttribute<double>("f_mixer", path);
                        hamiltonian.u_layer = h5_src.readAttribute<size_t>("u_layer", path);
                    } catch(const std::exception &ex) {
                        hamiltonian.f_mixer = tools::parse::extract_paramter_from_path<double>(h5_src.getFilePath(), "f+");
                        hamiltonian.u_layer = 6;
                        tools::logger::log->debug("Could not find model parameter: {} | Replaced with f=[{:.2f}] u=[{}]", ex.what(), hamiltonian.f_mixer,
                                                  hamiltonian.u_layer);
                    }
                }
                srcModelId.model_size   = h5_src.readAttribute<size_t>("model_size", path);
                srcModelId.model_type   = h5_src.readAttribute<std::string>("model_type", path);
                srcModelId.distribution = h5_src.readAttribute<std::string>("distribution", path);
                srcModelId.algorithm    = srcKey.algo;
                srcModelId.key          = key;
                srcModelId.path         = path;
                srcModelId.basepath     = get_standardized_base(srcModelId);
            }
            keys.emplace_back(srcKey);
            keys.back().key = key;
        }
        return keys;
    }
    template std::vector<ModelKey> loadModel(const h5pp::File &h5_src, std::unordered_map<std::string, ModelId<sdual>> &srcModelDb,
                                             const std::vector<ModelKey> &srcKeys);
    template std::vector<ModelKey> loadModel(const h5pp::File &h5_src, std::unordered_map<std::string, ModelId<lbit>> &srcModelDb,
                                             const std::vector<ModelKey> &srcKeys);

    template<typename T>
    void saveModel([[maybe_unused]] const h5pp::File &h5_src, h5pp::File &h5_tgt, std::unordered_map<std::string, InfoId<h5pp::TableInfo>> &tgtModelDb,
                   const ModelId<T> &modelId) {
        auto              t_ham_token  = tools::prof::t_ham.tic_token();
        const std::string tgtModelPath = fmt::format("{}/{}", modelId.basepath, modelId.path);
        if(tgtModelDb.find(tgtModelPath) == tgtModelDb.end()) {
            tgtModelDb[tgtModelPath] = h5_tgt.getTableInfo(tgtModelPath);
            // It doesn't make sense to copy the whole hamiltonian table here:
            // It is specific to a single realization, but here we collect the fields common to all realizations.
            const auto      &algorithm   = modelId.algorithm;
            const auto      &hamiltonian = modelId.p;
            std::string_view basepath    = modelId.basepath;

            // Now copy some helpful scalar datasets. This data is available in the attributes of the table above but this is also handy
            h5_tgt.writeDataset(modelId.model_size, fmt::format("{}/{}/model/model_size", basepath, algorithm));
            h5_tgt.writeDataset(modelId.model_type, fmt::format("{}/{}/model/model_type", basepath, algorithm));
            h5_tgt.writeDataset(modelId.distribution, fmt::format("{}/{}/model/distribution", basepath, algorithm));
            if constexpr(std::is_same_v<T, sdual>) {
                h5_tgt.writeDataset(hamiltonian.J_mean, fmt::format("{}/{}/model/J_mean", basepath, algorithm));
                h5_tgt.writeDataset(hamiltonian.J_stdv, fmt::format("{}/{}/model/J_stdv", basepath, algorithm));
                h5_tgt.writeDataset(hamiltonian.h_mean, fmt::format("{}/{}/model/h_mean", basepath, algorithm));
                h5_tgt.writeDataset(hamiltonian.h_stdv, fmt::format("{}/{}/model/h_stdv", basepath, algorithm));
                h5_tgt.writeDataset(hamiltonian.lambda, fmt::format("{}/{}/model/lambda", basepath, algorithm));
                h5_tgt.writeDataset(hamiltonian.delta, fmt::format("{}/{}/model/delta", basepath, algorithm));
            }
            if constexpr(std::is_same_v<T, lbit>) {
                h5_tgt.writeDataset(hamiltonian.J1_mean, fmt::format("{}/{}/model/J1_mean", basepath, algorithm));
                h5_tgt.writeDataset(hamiltonian.J2_mean, fmt::format("{}/{}/model/J2_mean", basepath, algorithm));
                h5_tgt.writeDataset(hamiltonian.J3_mean, fmt::format("{}/{}/model/J3_mean", basepath, algorithm));
                h5_tgt.writeDataset(hamiltonian.J1_wdth, fmt::format("{}/{}/model/J1_wdth", basepath, algorithm));
                h5_tgt.writeDataset(hamiltonian.J2_wdth, fmt::format("{}/{}/model/J2_wdth", basepath, algorithm));
                h5_tgt.writeDataset(hamiltonian.J3_wdth, fmt::format("{}/{}/model/J3_wdth", basepath, algorithm));
                h5_tgt.writeDataset(hamiltonian.J2_base, fmt::format("{}/{}/model/J2_base", basepath, algorithm));
                h5_tgt.writeDataset(hamiltonian.J2_span, fmt::format("{}/{}/model/J2_span", basepath, algorithm));
                h5_tgt.writeDataset(hamiltonian.f_mixer, fmt::format("{}/{}/model/f_mixer", basepath, algorithm));
                h5_tgt.writeDataset(hamiltonian.u_layer, fmt::format("{}/{}/model/u_layer", basepath, algorithm));
            }
        }
    }
    template void saveModel(const h5pp::File &h5_src, h5pp::File &h5_tgt, std::unordered_map<std::string, InfoId<h5pp::TableInfo>> &tgtModelDb,
                            const ModelId<sdual> &modelId);
    template void saveModel(const h5pp::File &h5_src, h5pp::File &h5_tgt, std::unordered_map<std::string, InfoId<h5pp::TableInfo>> &tgtModelDb,
                            const ModelId<lbit> &modelId);

    std::vector<DsetKey> gatherDsetKeys(const h5pp::File &h5_src, std::unordered_map<std::string, h5pp::DsetInfo> &srcDsetDb, const PathId &pathid,
                                        const std::vector<DsetKey> &srcKeys) {
        auto                 t_get_token     = prof::t_get.tic_token();
        auto                 t_dst_get_token = prof::t_dst_get.tic_token();
        std::vector<DsetKey> keys;

        std::string   srcParentPath = h5pp::fs::path(h5_src.getFilePath()).parent_path();
        h5pp::Options options;
        for(auto &srcKey : srcKeys) {
            // Make sure to only collect keys for objects that we have asked for.
            if(not pathid.match(srcKey.algo, srcKey.state, srcKey.point)) continue;

            // A "srcKey" is a struct describing a dataset that we want, with name, type, size, etc
            // Here we look for a matching dataset in the given group path, and generate a key for it.
            // The srcKeys that match an existing dataset are returned in "keys", with an additional parameter ".key" which
            // is an unique identifier to find the DsetInfo object in the map srcDsetDb
            // The database srcDsetDb  is used to keep track the DsetInfo structs for found datasets.
            auto path = fmt::format("{}/{}", pathid.src_path, srcKey.name);
            auto key  = fmt::format("{}|{}", srcParentPath, path);
            if(srcDsetDb.find(key) == srcDsetDb.end()) {
                srcDsetDb[key] = h5_src.getDatasetInfo(path);
                if(srcDsetDb[key].dsetExists.value()) tools::logger::log->debug("Detected new source dataset {}", key);
            } else {
                auto &srcInfo = srcDsetDb[key];
                // We reuse the struct srcDsetDb[dsetKey] for every source file,
                // but each time have to renew the following fields
                srcInfo.h5File     = h5_src.openFileHandle();
                srcInfo.h5Dset     = std::nullopt;
                srcInfo.h5Space    = std::nullopt;
                srcInfo.dsetExists = std::nullopt;
                srcInfo.dsetSize   = std::nullopt;
                srcInfo.dsetDims   = std::nullopt;
                srcInfo.dsetByte   = std::nullopt;
                srcInfo.dsetPath   = path;
                h5pp::scan::readDsetInfo(srcInfo, srcInfo.h5File.value(), options, h5_src.plists);
            }
            auto &srcInfo = srcDsetDb[key];
            if(srcInfo.dsetExists and srcInfo.dsetExists.value()) {
                keys.emplace_back(srcKey);
                keys.back().key = key;
            } else {
                tools::logger::log->debug("Missing dataset [{}] in file [{}]", path, h5_src.getFilePath());
            }
        }
        return keys;
    }

    std::vector<TableKey> gatherTableKeys(const h5pp::File &h5_src, std::unordered_map<std::string, h5pp::TableInfo> &srcTableDb, const PathId &pathid,
                                          const std::vector<TableKey> &srcKeys) {
        auto t_get_token     = prof::t_get.tic_token();
        auto t_tab_get_token = prof::t_tab_get.tic_token();

        std::vector<TableKey> keys;
        h5pp::Options         options;
        std::string           srcParentPath = h5pp::fs::path(h5_src.getFilePath()).parent_path();
        for(auto &srcKey : srcKeys) {
            // Make sure to only collect keys for objects that we have asked for.
            if(not pathid.match(srcKey.algo, srcKey.state, srcKey.point)) continue;

            // A "srcKey" is a struct describing a table that we want, with name, type, size, etc
            // Here we look for a matching table in the given group path, and generate a key for it.
            // The srcKeys that match an existing table are returned in "keys", with an additional parameter ".key" which
            // is an unique identifier to find the TableInfo object in the map srcTableDb
            // The database srcTableDb  is used to keep track the TableInfo structs for found datasets.
            auto path = fmt::format("{}/{}", pathid.src_path, srcKey.name);
            auto key  = fmt::format("{}|{}", srcParentPath, path);

            if(srcTableDb.find(key) == srcTableDb.end()) {
                srcTableDb[key] = h5_src.getTableInfo(path);
                if(srcTableDb[key].tableExists.value()) tools::logger::log->debug("Detected new source table {}", key);
            } else {
                auto &srcInfo = srcTableDb[key];
                // We reuse the struct srcDsetDb[dsetKey] for every source file,
                // but each time have to renew the following fields
                srcInfo.h5File      = h5_src.openFileHandle();
                srcInfo.h5Dset      = std::nullopt;
                srcInfo.numRecords  = std::nullopt;
                srcInfo.tableExists = std::nullopt;
                srcInfo.tablePath   = path;
                h5pp::scan::readTableInfo(srcInfo, srcInfo.h5File.value(), options, h5_src.plists);
            }

            auto &srcInfo = srcTableDb[key];
            if(srcInfo.tableExists and srcInfo.tableExists.value()) {
                keys.emplace_back(srcKey);
                keys.back().key = key;
            } else {
                srcInfo.h5File = std::nullopt;
                tools::logger::log->debug("Missing table [{}] in file [{}]", path, h5_src.getFilePath());
            }
        }
        return keys;
    }

    std::vector<CronoKey> gatherCronoKeys(const h5pp::File &h5_src, std::unordered_map<std::string, h5pp::TableInfo> &srcTableDb, const PathId &pathid,
                                          const std::vector<CronoKey> &srcKeys) {
        auto                  t_get_token     = prof::t_get.tic_token();
        auto                  t_cro_get_token = prof::t_cro_get.tic_token();
        std::vector<CronoKey> keys;
        h5pp::Options         options;
        std::string           srcParentPath = h5pp::fs::path(h5_src.getFilePath()).parent_path();
        for(auto &srcKey : srcKeys) {
            // Make sure to only collect keys for objects that we have asked for.
            if(not pathid.match(srcKey.algo, srcKey.state, srcKey.point)) continue;

            // A "srcKey" is a struct describing a table that we want, with name, type, size, etc
            // Here we look for a matching table in the given group path, and generate a key for it.
            // The srcKeys that match an existing table are returned in "keys", with an additional parameter ".key" which
            // is an unique identifier to find the TableInfo object in the map srcTableDb
            // The database srcTableDb  is used to keep track the TableInfo structs for found datasets.
            auto path = fmt::format("{}/{}", pathid.src_path, srcKey.name);
            auto key  = fmt::format("{}|{}", srcParentPath, path);

            if(srcTableDb.find(key) == srcTableDb.end()) {
                srcTableDb[key] = h5_src.getTableInfo(path);
                if(srcTableDb[key].tableExists.value()) tools::logger::log->debug("Detected new source crono {}", key);
            } else {
                auto &srcInfo = srcTableDb[key];
                // We reuse the struct srcDsetDb[dsetKey] for every source file,
                // but each time have to renew the following fields

                srcInfo.h5File      = h5_src.openFileHandle();
                srcInfo.h5Dset      = std::nullopt;
                srcInfo.numRecords  = std::nullopt;
                srcInfo.tableExists = std::nullopt;
                srcInfo.tablePath   = path;
                h5pp::scan::readTableInfo(srcInfo, srcInfo.h5File.value(), options, h5_src.plists);
            }

            auto &srcInfo = srcTableDb[key];
            if(srcInfo.tableExists and srcInfo.tableExists.value()) {
                keys.emplace_back(srcKey);
                keys.back().key = key;
            } else {
                tools::logger::log->debug("Missing crono [{}] in file [{}]", path, h5_src.getFilePath());
            }
        }
        return keys;
    }

    void transferDatasets(h5pp::File &h5_tgt, std::unordered_map<std::string, InfoId<h5pp::DsetInfo>> &tgtDsetDb, const h5pp::File &h5_src,
                          std::unordered_map<std::string, h5pp::DsetInfo> &srcDsetDb, const PathId &pathid, const std::vector<DsetKey> &srcDsetKeys,
                          const FileId &fileId) {
        auto t_dst           = tools::prof::t_dst.tic_token();
        auto t_dst_trn_token = tools::prof::t_dst_trn.tic_token();
        for(const auto &srcKey : srcDsetKeys) {
            if(srcDsetDb.find(srcKey.key) == srcDsetDb.end()) throw std::logic_error(h5pp::format("Key [{}] was not found in source map", srcKey.key));
            auto &srcInfo = srcDsetDb[srcKey.key];
            if(not srcInfo.dsetExists or not srcInfo.dsetExists.value()) continue;
            auto tgtName = h5pp::fs::path(srcInfo.dsetPath.value()).filename().string();
            auto tgtPath = h5pp::format("{}/{}", pathid.tgt_path, tgtName);
            if(tgtDsetDb.find(tgtPath) == tgtDsetDb.end()) {
                auto t_dst_crt = tools::prof::t_dst_crt.tic_token();
                long rows      = 0;
                long cols      = 0;
                switch(srcKey.size) {
                    case Size::FIX: {
                        if(srcInfo.dsetDims.has_value() and not srcInfo.dsetDims->empty())
                            rows = static_cast<long>(srcInfo.dsetDims.value()[0]);
                        else
                            rows = 1; // It may be a H5S_SCALAR
                        break;
                    }
                    case Size::VAR: {
                        auto        srcGroupPath    = h5pp::fs::path(srcInfo.dsetPath.value()).parent_path().string();
                        std::string statusTablePath = fmt::format("{}/status", srcGroupPath);
                        rows                        = h5_src.readTableField<long>(statusTablePath, "chi_lim_max", h5pp::TableSelection::FIRST);
                        break;
                    }
                }
                long chunk_rows = std::clamp(rows, 1l, 100l);
                tools::logger::log->debug("Adding target dset {} | dims ({},{}) | chnk ({},{})", tgtPath, rows, cols, chunk_rows, 10l);
                tgtDsetDb[tgtPath] = h5_tgt.createDataset(srcInfo.h5Type.value(), tgtPath, {rows, cols}, H5D_CHUNKED, {chunk_rows, 10l});
            }
            auto &tgtInfo = tgtDsetDb[tgtPath].info;
            auto &tgtDb   = tgtDsetDb[tgtPath].db;
            // Determine the target index where to copy this record
            long index = static_cast<long>(tgtInfo.dsetDims.value().at(1));
            if(tgtDb.find(fileId.seed) != tgtDb.end()) {
                index = tgtDb[fileId.seed];
                //                    tools::logger::log->info("Found seed {} at index {}: dset {}", fileId.seed,index, srcInfo.dsetPath.value());
            }
            auto t_dst_cpy = tools::prof::t_dst_cpy.tic_token();
            switch(srcKey.type) {
                case Type::DOUBLE: internal::copy_dset<std::vector<double>>(h5_tgt, h5_src, tgtInfo, srcInfo, index); break;
                case Type::LONG: internal::copy_dset<std::vector<long>>(h5_tgt, h5_src, tgtInfo, srcInfo, index); break;
                case Type::INT: internal::copy_dset<std::vector<int>>(h5_tgt, h5_src, tgtInfo, srcInfo, index); break;
                case Type::COMPLEX: internal::copy_dset<std::vector<std::complex<double>>>(h5_tgt, h5_src, tgtInfo, srcInfo, index); break;
                case Type::TID: {
                    internal::copy_dset<std::vector<tid_t>>(h5_tgt, h5_src, tgtInfo, srcInfo, index);
                    break;
                }
            }

            // Update the database
            tgtDb[fileId.seed] = index;
        }
    }

    void transferTables(h5pp::File &h5_tgt, std::unordered_map<std::string, InfoId<h5pp::TableInfo>> &tgtTableDb,
                        std::unordered_map<std::string, h5pp::TableInfo> &srcTableDb, const PathId &pathid, const std::vector<TableKey> &srcTableKeys,
                        const FileId &fileId) {
        auto t_tab           = tools::prof::t_tab.tic_token();
        auto t_tab_trn_token = tools::prof::t_tab_trn.tic_token();
        for(const auto &srcKey : srcTableKeys) {
            if(srcTableDb.find(srcKey.key) == srcTableDb.end()) throw std::runtime_error(h5pp::format("Key [{}] was not found in source map", srcKey.key));
            auto &srcInfo = srcTableDb[srcKey.key];
            if(not srcInfo.tableExists or not srcInfo.tableExists.value()) continue;
            auto tgtName = h5pp::fs::path(srcInfo.tablePath.value()).filename().string();
            auto tgtPath = pathid.table_path(tgtName);
            if(tgtTableDb.find(tgtPath) == tgtTableDb.end()) {
                auto t_tab_crt = tools::prof::t_tab_crt.tic_token();
                tools::logger::log->debug("Adding target table {}", tgtPath);
                h5pp::TableInfo tableInfo = h5_tgt.getTableInfo(tgtPath);
                if(not tableInfo.tableExists.value()) tableInfo = h5_tgt.createTable(srcInfo.h5Type.value(), tgtPath, srcInfo.tableTitle.value());
                tgtTableDb[tgtPath] = tableInfo;
            }

            auto &tgtInfo = tgtTableDb[tgtPath].info;
            auto &tgtDb   = tgtTableDb[tgtPath].db;
            long  index   = static_cast<long>(tgtInfo.numRecords.value());
            if(tgtDb.find(fileId.seed) != tgtDb.end()) index = tgtDb[fileId.seed];
            tools::logger::log->trace("Copying table index {} -> {}: {}",srcInfo.numRecords.value(), index, tgtPath);
            auto t_tab_cpy = tools::prof::t_tab_cpy.tic_token();
            h5_tgt.copyTableRecords(srcInfo, h5pp::TableSelection::LAST, tgtInfo, static_cast<hsize_t>(index));
            // Update the database
            tgtDb[fileId.seed] = index;
        }
    }

    void transferCronos(h5pp::File &h5_tgt, std::unordered_map<std::string, InfoId<h5pp::TableInfo>> &tgtTableDb,
                        std::unordered_map<std::string, h5pp::TableInfo> &srcTableDb, const PathId &pathid, const std::vector<CronoKey> &srcCronoKeys,
                        const FileId &fileId) {
        // In this function we take time series data from each srcTable and create multiple tables tgtTable, one for each
        // time point (iteration). Each entry in tgtTable corresponds to the same time point on different realizations.
        auto t_cro           = tools::prof::t_cro.tic_token();
        auto t_cro_trn_token = tools::prof::t_cro_trn.tic_token();
        for(const auto &srcKey : srcCronoKeys) {
            if(srcTableDb.find(srcKey.key) == srcTableDb.end()) throw std::logic_error(h5pp::format("Key [{}] was not found in source map", srcKey.key));
            auto &srcInfo    = srcTableDb[srcKey.key];
            auto  srcRecords = srcInfo.numRecords.value();

            // Iterate over all table elements. These should be a time series measured at every iteration
            // Note that there could in principle exist duplicate entries, which is why we can't trust the
            // "rec" iterator but have to get the iteration number from the table directly.
            // Try getting the iteration number, which is more accurate.

            std::vector<size_t> iters;

            try {
                h5pp::hdf5::readTableField(iters, srcInfo, {"iter"});
            } catch(const std::exception &ex) { throw std::logic_error(fmt::format("Failed to get iteration numbers: {}", ex.what())); }
            for(size_t rec = 0; rec < srcRecords; rec++) {
                size_t iter = rec;
                if(not iters.empty()) iter = iters[rec]; // Get the actual iteration number
                auto tgtName = h5pp::fs::path(srcInfo.tablePath.value()).filename().string();
                auto tgtPath = pathid.crono_path(tgtName, iter);

                if(tgtTableDb.find(tgtPath) == tgtTableDb.end()) {
                    auto t_cro_crt = tools::prof::t_cro_crt.tic_token();
                    tools::logger::log->debug("Adding target crono {}", tgtPath);
                    h5pp::TableInfo tableInfo = h5_tgt.getTableInfo(tgtPath);
                    if(not tableInfo.tableExists.value()) tableInfo = h5_tgt.createTable(srcInfo.h5Type.value(), tgtPath, srcInfo.tableTitle.value());
                    tgtTableDb[tgtPath] = tableInfo;
                }

                auto &tgtInfo = tgtTableDb[tgtPath].info;
                auto &tgtDb   = tgtTableDb[tgtPath].db;

                // The table entry for the current realization may already have been added
                // This happens for instance if we make extra entries in "finished" after adding them to "checkpoint" and/or "savepoint"
                // However, these entries should be identical, so no need to copy them again.
                if(tgtDb.find(fileId.seed) != tgtDb.end()) {
#pragma message "Skipping here may not be wise?"
                    tools::logger::log->info("Skip copying existing crono entry: {} | index {}", tgtPath, tgtDb[fileId.seed]);
                    continue;
                }

                // Determine the target index where to copy this record
                // Under normal circumstances, the "index" counts the number of realizations, or simulation seeds.
                // tgtInfo.numRecords is the total number of realizations registered until now
                long index = static_cast<long>(tgtInfo.numRecords.value());

                // Find the previous table index in case it has already been registered
                if(tgtDb.find(fileId.seed) != tgtDb.end()) index = tgtDb[fileId.seed];

                // copy/append a source record at "iter" into the "index" position on the table.
                tools::logger::log->trace("Copying crono index {} -> {}: {}",rec, index, tgtPath);
                                auto t_cro_cpy_token = tools::prof::t_cro_cpy.tic_token();
                h5_tgt.copyTableRecords(srcInfo, rec, 1, tgtInfo, static_cast<hsize_t>(index));
                // Update the database
                tgtDb[fileId.seed] = index;
            }
        }
    }

    template<typename ModelType>
    void merge(h5pp::File &h5_tgt, const h5pp::File &h5_src, const FileId &fileId, const tools::h5db::Keys &keys, tools::h5db::TgtDb &tgtdb) {
        auto t_mrg_token = tools::prof::t_mrg.tic_token();

        // Define reusable source Info
        static tools::h5db::SrcDb<ModelId<ModelType>> srcdb;
        h5pp::fs::path                                parent_path = h5pp::fs::path(h5_src.getFilePath()).parent_path();
        if(srcdb.parent_path != parent_path) {
            // Clear when moving to another set of seeds (new point on the phase diagram)
            srcdb.clear();
            srcdb.parent_path = parent_path;
            tools::h5db::saveDatabase(h5_tgt, tgtdb.file);
            tools::h5db::saveDatabase(h5_tgt, tgtdb.model);
            tools::h5db::saveDatabase(h5_tgt, tgtdb.table);
            tools::h5db::saveDatabase(h5_tgt, tgtdb.dset);
            tools::h5db::saveDatabase(h5_tgt, tgtdb.crono);
            tgtdb.table.clear();
            tgtdb.model.clear();
            tgtdb.dset.clear();
            tgtdb.crono.clear();
            h5_tgt.flush();
        }

        // Start finding the required components in the source
        tools::prof::t_gr1.tic();
        auto groups = tools::h5io::findKeys(h5_src, "/", keys.get_algos(), -1, 0);
        tools::prof::t_gr1.toc();

        for(const auto &algo : groups) {
            // Start by extracting the model
            auto modelKeys = tools::h5io::loadModel(h5_src, srcdb.model, keys.models);
            if(modelKeys.size() != 1) throw std::runtime_error("Exactly 1 model has to be loaded into keys");
            auto &modelId = srcdb.model[modelKeys.back().key];
            // Save the model to file if it hasn't
            tools::h5io::saveModel(h5_src, h5_tgt, tgtdb.model, modelId);
            auto tgt_base = modelId.basepath;
            // Next search for tables and datasets in the source file
            // and transfer them to the target file
            tools::prof::t_gr2.tic();
            auto state_groups = tools::h5io::findKeys(h5_src, algo, keys.get_states(), -1, 0);
            tools::prof::t_gr2.toc();

            for(const auto &state : state_groups) {
                tools::prof::t_gr3.tic();
                auto point_groups = tools::h5io::findKeys(h5_src, fmt::format("{}/{}", algo, state), keys.get_points(), -1, 1);
                tools::prof::t_gr3.toc();
                for(const auto &point : point_groups) {
                    auto pathid = PathId(tgt_base, algo, state, point);
                    // Try gathering all the tables
                    try {
                        auto t_mrg_dst_token = tools::prof::t_mrg_dst.tic_token();
                        auto dsetKeys        = tools::h5io::gatherDsetKeys(h5_src, srcdb.dset, pathid, keys.dsets);
                        //                        tools::logger::log->info("Gathered dset keys");
                        tools::h5io::transferDatasets(h5_tgt, tgtdb.dset, h5_src, srcdb.dset, pathid, dsetKeys, fileId);
                    } catch(const std::runtime_error &ex) { tools::logger::log->warn("Dset transfer failed in [{}]: {}", pathid.src_path, ex.what()); }

                    try {
                        auto t_mrg_tab_token = tools::prof::t_mrg_tab.tic_token();
                        auto tableKeys       = tools::h5io::gatherTableKeys(h5_src, srcdb.table, pathid, keys.tables);
                        //                        tools::logger::log->info("Gathered table keys {}", tableKeys);
                        tools::h5io::transferTables(h5_tgt, tgtdb.table, srcdb.table, pathid, tableKeys, fileId);
                    } catch(const std::runtime_error &ex) { tools::logger::log->error("Table transfer failed in [{}]: {}", pathid.src_path, ex.what()); }

                    try {
                        auto t_mrg_cro_token = tools::prof::t_mrg_cro.tic_token();
                        auto cronoKeys       = tools::h5io::gatherCronoKeys(h5_src, srcdb.crono, pathid, keys.cronos);
                        //                        tools::logger::log->info("Gathered crono keys {}", cronoKeys);
                        tools::h5io::transferCronos(h5_tgt, tgtdb.table, srcdb.crono, pathid, cronoKeys, fileId);
                    } catch(const std::runtime_error &ex) { tools::logger::log->error("Crono transfer failed in[{}]: {}", pathid.src_path, ex.what()); }
                }
            }
        }

        auto t_clo = tools::prof::t_clo.tic_token();

        // Check that there are no errors hiding in the HDF5 error-stack
        auto num_errors = H5Eget_num(H5E_DEFAULT);
        if(num_errors > 0) {
            H5Eprint(H5E_DEFAULT, stderr);
            throw std::runtime_error(fmt::format("Error when treating file [{}]", h5_src.getFilePath()));
        }
    }
    template void merge<sdual>(h5pp::File &h5_tgt, const h5pp::File &h5_src, const FileId &fileId, const tools::h5db::Keys &keys, tools::h5db::TgtDb &tgtdb);
    template void merge<lbit>(h5pp::File &h5_tgt, const h5pp::File &h5_src, const FileId &fileId, const tools::h5db::Keys &keys, tools::h5db::TgtDb &tgtdb);

    void writeProfiling(h5pp::File &h5_tgt) {
        H5T_profiling::register_table_type();
        if(not h5_tgt.linkExists(".db/prof")) { h5_tgt.createTable(H5T_profiling::h5_type, ".db/prof", "H5MBL Profiling", {100}, 4); }
        h5_tgt.appendTableRecords(tools::prof::buffer, ".db/prof");
    }
}