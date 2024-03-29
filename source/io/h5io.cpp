#include "h5io.h"
#include <cstdlib>
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
#include <mpi/mpi-tools.h>
#include <regex>
#include <set>
#include <string>
#include <thread>
#include <tid/tid.h>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace tools::h5io {
    std::string get_tmp_dirname(std::string_view exename) { return fmt::format("{}.{}", h5pp::fs::path(exename).filename().string(), getenv("USER")); }

    namespace internal {
        void copy_dset(h5pp::File &h5_tgt, const h5pp::File &h5_src, h5pp::DsetInfo &tgtInfo, h5pp::DsetInfo &srcInfo, hsize_t index, size_t axis) {
            auto t_scope = tid::tic_scope(__FUNCTION__);
            auto data = h5_src.readDataset<std::vector<std::byte>>(srcInfo); // Read the data into a generic buffer.
            h5pp::DataInfo dataInfo;

            // The axis parameter must be  < srcInfo.rank+1
            if(axis > srcInfo.dsetDims->size()){
                dataInfo.dataByte = srcInfo.dsetByte;
                dataInfo.dataSize = srcInfo.dsetSize;
                dataInfo.dataRank = tgtInfo.dsetRank; // E.g. if stacking rank 2 matrices then axis == 2 and so data rank must be 3.
                dataInfo.dataDims = std::vector<hsize_t>(axis+1, 1ull);
                for(size_t i = 0; i < srcInfo.dsetDims->size(); i++) dataInfo.dataDims.value()[i] = srcInfo.dsetDims.value()[i];
                dataInfo.h5Space = h5pp::util::getMemSpace(dataInfo.dataSize.value(), dataInfo.dataDims.value());
            }else{
                dataInfo.dataDims = srcInfo.dsetDims;
                dataInfo.dataSize = srcInfo.dsetSize;
                dataInfo.dataByte = srcInfo.dsetByte;
                dataInfo.dataRank = srcInfo.dsetRank;
                dataInfo.h5Space = srcInfo.h5Space;
            }
            tgtInfo.resizePolicy     = h5pp::ResizePolicy::GROW;
            tgtInfo.dsetSlab         = h5pp::Hyperslab();
            tgtInfo.dsetSlab->extent = dataInfo.dataDims;
            tgtInfo.dsetSlab->offset = std::vector<hsize_t>(tgtInfo.dsetDims->size(), 0);
            tgtInfo.dsetSlab->offset.value()[axis] = index;

            h5_tgt.appendToDataset(data, dataInfo, tgtInfo, static_cast<size_t>(axis));
            // Restore previous settings
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
        auto t_scope = tid::tic_scope(__FUNCTION__);
        if constexpr(std::is_same_v<T, sdual>) return h5pp::format("L_{1}/l_{2:.{0}f}/d_{3:+.{0}f}", decimals, H.model_size, H.p.lambda, H.p.delta);
        if constexpr(std::is_same_v<T, lbit>) {
            std::string J_mean_str = fmt::format("J[{1:+.{0}f}_{2:+.{0}f}_{3:+.{0}f}]", decimals, H.p.J1_mean, H.p.J2_mean, H.p.J3_mean);
            std::string J_wdth_str = fmt::format("w[{1:+.{0}f}_{2:+.{0}f}_{3:+.{0}f}]", decimals, H.p.J1_wdth, H.p.J2_wdth, H.p.J3_wdth);
            std::string x_str      = fmt::format("x_{1:.{0}f}", decimals, H.p.J2_xcls);
            std::string f_str      = fmt::format("f_{1:.{0}f}", decimals, H.p.f_mixer);
            std::string u_str      = fmt::format("u_{}", H.p.u_layer);
            std::string r_str;
            // J2_span is special since it can be -1ul. We prefer putting -1 in the path rather than 18446744073709551615
            if(H.p.J2_span == -1ul)
                r_str = fmt::format("r_L");
            else
                r_str = fmt::format("r_{}", H.p.J2_span);
            auto base = h5pp::format("L_{}/{}/{}/{}/{}/{}/{}", H.model_size, J_mean_str, J_wdth_str, x_str, f_str, u_str, r_str);
            tools::logger::log->info("creating base with {} decimals: {}", decimals, base);
            return base;
        }
    }
    template std::string get_standardized_base(const ModelId<sdual> &H, int decimals);
    template std::string get_standardized_base(const ModelId<lbit> &H, int decimals);

    std::vector<std::string> findKeys(const h5pp::File &h5_src, const std::string &root, const std::vector<std::string> &expectedKeys, long hits, long depth) {
        auto                                                                      t_scope = tid::tic_scope(__FUNCTION__);
        static std::unordered_set<internal::SearchResult, internal::SearchHasher> cache;
        std::vector<std::string>                                                  result;
        for(const auto &key : expectedKeys) {
            internal::SearchResult searchQuery = {root, key, hits, depth, {}};
            std::string            cacheMsg;
            auto                   cachedIt = cache.find(searchQuery);
            bool                   cacheHit = cachedIt != cache.end() and                         // The search query has been processed earlier
                            ((hits > 0 and static_cast<long>(cachedIt->result.size()) >= hits) or // Asked for a up to "hits" reslts
                             (hits <= 0 and static_cast<long>(cachedIt->result.size()) >= 1)      // Asked for any number of results
                            );
            if(cacheHit) {
                for(auto &item : cachedIt->result) {
                    if(result.size() > 1 and std::find(result.begin(), result.end(), item) != result.end()) continue;
                    result.emplace_back(item);
                }
                cacheMsg = " | cache hit";
            } else {
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
            tools::logger::log->trace("Search: key [{}] | result [{}]{}", key, result, cacheMsg);
        }
        return result;
    }
    template<typename T>
    std::vector<ModelKey> loadModel(const h5pp::File &h5_src, std::unordered_map<std::string, ModelId<T>> &srcModelDb, const std::vector<ModelKey> &srcKeys) {
        auto                  t_scope = tid::tic_scope(__FUNCTION__);
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
                        hamiltonian.J2_xcls = h5_src.readAttribute<double>("J2_xcls", path);
                    } catch(const std::exception &ex) {
                        hamiltonian.J2_xcls = tools::parse::extract_parameter_from_path<double>(h5_src.getFilePath(), "x_");
                        tools::logger::log->debug("Could not find model parameter: {} | Replaced with b=[{:.2f}]", ex.what(), hamiltonian.J2_xcls);
                    }
                    try {
                        hamiltonian.J2_span = h5_src.readAttribute<size_t>("J2_span", path);
                    } catch(const std::exception &ex) {
                        hamiltonian.J2_span = tools::parse::extract_parameter_from_path<size_t>(h5_src.getFilePath(), "r_");
                        tools::logger::log->debug("Could not find model parameter: {} | Replaced with r=[{}]", ex.what(), hamiltonian.J2_span);
                    }
                    try {
                        hamiltonian.f_mixer = h5_src.readAttribute<double>("f_mixer", path);
                        hamiltonian.u_layer = h5_src.readAttribute<size_t>("u_layer", path);
                    } catch(const std::exception &ex) {
                        hamiltonian.f_mixer = tools::parse::extract_parameter_from_path<double>(h5_src.getFilePath(), "f+");
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
                   const ModelId<T> &modelId, const FileId &fileId) {
        auto              t_scope      = tid::tic_scope(__FUNCTION__);
        const std::string tgtModelPath = fmt::format("{}/{}", modelId.basepath, modelId.path);

        tools::logger::log->debug("Attempting to copy model to tgtPath {}", tgtModelPath);
        tools::logger::log->debug("modelId key    {}", modelId.key);
        tools::logger::log->debug("modelId path   {}", modelId.path);
        tools::logger::log->debug("modelId bpath  {}", modelId.basepath);
        tools::logger::log->debug("modelId fields {}", modelId.p.fields);

        if(tgtModelDb.find(tgtModelPath) == tgtModelDb.end()) {
            tgtModelDb[tgtModelPath] = h5_tgt.getTableInfo(tgtModelPath);

            auto &tgtId   = tgtModelDb[tgtModelPath];
            auto &tgtInfo = tgtModelDb[tgtModelPath].info;

            // It doesn't make sense to copy the whole hamiltonian table here:
            // It is specific to a single realization, but here we collect the fields common to all realizations.
            const auto &srcModelInfo = h5_src.getTableInfo(modelId.path); // This will return a TableInfo to a an existing table in the source file
            auto        modelPath    = fmt::format("{}/{}/model", modelId.basepath, modelId.algorithm); // Generate a new path for the target group
            auto        tablePath    = fmt::format("{}/hamiltonian", modelPath);                        // Generate a new path for the target table

            // Update an entry of the hamiltonian table with the relevant fields
            tools::logger::log->trace("Copying model {}", modelId.basepath);
            auto h5t_model = h5pp::util::getFieldTypeId(srcModelInfo, modelId.p.fields); // Generate a h5t with the relevant fields
            auto modelData =
                h5_src.readTableField<std::vector<std::byte>>(srcModelInfo, h5t_model, h5pp::TableSelection::LAST); // Read those fields into a buffer
            tgtInfo = h5_tgt.createTable(h5t_model, tablePath, h5pp::format("{} Hamiltonian", modelId.algorithm));
            h5_tgt.writeTableRecords(modelData, tablePath);
            // Update the database
            tgtId.insert(fileId.seed, 0);

            // Now copy some helpful scalar datasets. This data is available in the attributes of the table above but this is also handy
            h5_tgt.writeDataset(modelId.model_size, fmt::format("{}/{}/model/model_size", modelId.basepath, modelId.algorithm));
            h5_tgt.writeDataset(modelId.model_type, fmt::format("{}/{}/model/model_type", modelId.basepath, modelId.algorithm));
            h5_tgt.writeDataset(modelId.distribution, fmt::format("{}/{}/model/distribution", modelId.basepath, modelId.algorithm));
            if constexpr(std::is_same_v<T, sdual>) {
                h5_tgt.writeDataset(modelId.p.J_mean, fmt::format("{}/{}/model/J_mean", modelId.basepath, modelId.algorithm));
                h5_tgt.writeDataset(modelId.p.J_stdv, fmt::format("{}/{}/model/J_stdv", modelId.basepath, modelId.algorithm));
                h5_tgt.writeDataset(modelId.p.h_mean, fmt::format("{}/{}/model/h_mean", modelId.basepath, modelId.algorithm));
                h5_tgt.writeDataset(modelId.p.h_stdv, fmt::format("{}/{}/model/h_stdv", modelId.basepath, modelId.algorithm));
                h5_tgt.writeDataset(modelId.p.lambda, fmt::format("{}/{}/model/lambda", modelId.basepath, modelId.algorithm));
                h5_tgt.writeDataset(modelId.p.delta, fmt::format("{}/{}/model/delta", modelId.basepath, modelId.algorithm));
            }
            if constexpr(std::is_same_v<T, lbit>) {
                h5_tgt.writeDataset(modelId.p.J1_mean, fmt::format("{}/J1_mean", modelPath));
                h5_tgt.writeDataset(modelId.p.J2_mean, fmt::format("{}/J2_mean", modelPath));
                h5_tgt.writeDataset(modelId.p.J3_mean, fmt::format("{}/J3_mean", modelPath));
                h5_tgt.writeDataset(modelId.p.J1_wdth, fmt::format("{}/J1_wdth", modelPath));
                h5_tgt.writeDataset(modelId.p.J2_wdth, fmt::format("{}/J2_wdth", modelPath));
                h5_tgt.writeDataset(modelId.p.J3_wdth, fmt::format("{}/J3_wdth", modelPath));
                h5_tgt.writeDataset(modelId.p.J2_xcls, fmt::format("{}/J2_xcls", modelPath));
                h5_tgt.writeDataset(modelId.p.J2_span, fmt::format("{}/J2_span", modelPath));
                h5_tgt.writeDataset(modelId.p.f_mixer, fmt::format("{}/f_mixer", modelPath));
                h5_tgt.writeDataset(modelId.p.u_layer, fmt::format("{}/u_layer", modelPath));
            }
        }
    }
    template void saveModel(const h5pp::File &h5_src, h5pp::File &h5_tgt, std::unordered_map<std::string, InfoId<h5pp::TableInfo>> &tgtModelDb,
                            const ModelId<sdual> &modelId, const FileId &fileId);
    template void saveModel(const h5pp::File &h5_src, h5pp::File &h5_tgt, std::unordered_map<std::string, InfoId<h5pp::TableInfo>> &tgtModelDb,
                            const ModelId<lbit> &modelId, const FileId &fileId);

    std::vector<DsetKey> gatherDsetKeys(const h5pp::File &h5_src, std::unordered_map<std::string, h5pp::DsetInfo> &srcDsetDb, const PathId &pathid,
                                        const std::vector<DsetKey> &srcKeys) {
        auto                 t_scope = tid::tic_scope(__FUNCTION__);
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
                auto t_read = tid::tic_scope("readDsetInfo");

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
        auto                  t_scope = tid::tic_scope(__FUNCTION__);
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
                auto t_read = tid::tic_scope("readTableInfo");

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
        auto                  t_scope = tid::tic_scope(__FUNCTION__);
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
                auto t_read = tid::tic_scope("readTableInfo");

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
    std::vector<ScaleKey> gatherScaleKeys(const h5pp::File &h5_src, std::unordered_map<std::string, h5pp::TableInfo> &srcTableDb, const PathId &pathid,
                                          const std::vector<ScaleKey> &srcKeys) {
        auto                  t_scope = tid::tic_scope(__FUNCTION__);
        std::vector<ScaleKey> keys;
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

            // scaleKeys should be a vector [ "chi_8", "chi_16", "chi_24" ...] with tables for each scaling measurement
            auto scaleKeys = findKeys(h5_src, pathid.src_path, {srcKey.scale}, -1, 1);
            for(const auto &scaleKey : scaleKeys) {
                auto path = fmt::format("{}/{}/{}", pathid.src_path, scaleKey, srcKey.name);
                auto key  = fmt::format("{}|{}", srcParentPath, path);
                auto chi  = tools::parse::extract_parameter_from_path<size_t>(path, "chi_");
                if(srcTableDb.find(key) == srcTableDb.end()) {
                    srcTableDb[key] = h5_src.getTableInfo(path);
                    if(srcTableDb[key].tableExists.value()) tools::logger::log->debug("Detected new source scale {}", key);
                } else {
                    auto t_read = tid::tic_scope("readTableInfo");

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
                    keys.back().chi = chi; // Here we save the chi value to use later for generating a target path
                } else {
                    tools::logger::log->debug("Missing scale [{}] in file [{}]", path, h5_src.getFilePath());
                }
            }
        }
        return keys;
    }

    void transferDatasets(h5pp::File &h5_tgt, std::unordered_map<std::string, InfoId<h5pp::DsetInfo>> &tgtDsetDb, const h5pp::File &h5_src,
                          std::unordered_map<std::string, h5pp::DsetInfo> &srcDsetDb, const PathId &pathid, const std::vector<DsetKey> &srcDsetKeys,
                          const FileId &fileId) {
        auto t_scope = tid::tic_scope(__FUNCTION__);
        for(const auto &srcKey : srcDsetKeys) {
            if(srcDsetDb.find(srcKey.key) == srcDsetDb.end()) throw std::logic_error(h5pp::format("Key [{}] was not found in source map", srcKey.key));
            auto &srcInfo = srcDsetDb[srcKey.key];
            if(not srcInfo.dsetExists or not srcInfo.dsetExists.value()) continue;
            auto tgtName = h5pp::fs::path(srcInfo.dsetPath.value()).filename().string();
            auto tgtPath = h5pp::format("{}/{}", pathid.tgt_path, tgtName);
            if(tgtDsetDb.find(tgtPath) == tgtDsetDb.end()) {
                auto t_create = tid::tic_scope("createDataset");
                auto tgtDims = srcInfo.dsetDims.value();
                if (tgtDims.empty()) tgtDims = {0}; // In case src is a scalar.
                while(tgtDims.size() < srcKey.axis + 1) tgtDims.push_back(1);
                tgtDims[srcKey.axis] = 0;// Create with 0 extent in the new axis direction, so that the dataset starts empty (zero volume)

                // Determine a good chunksize between 10 and 500 elements
                auto tgtChunk = tgtDims;
                auto chunkSize = std::clamp(5e5 / static_cast<double>(srcInfo.dsetByte.value()), 10. , 1000. );
                tgtChunk[srcKey.axis] = static_cast<hsize_t>(chunkSize); // number of elements in the axis that we append into.

                if(srcKey.size == Size::VAR){
                    auto        srcGroupPath    = h5pp::fs::path(srcInfo.dsetPath.value()).parent_path().string();
                    std::string statusTablePath = fmt::format("{}/status", srcGroupPath);
                    long chi_max = h5_src.readTableField<long>(statusTablePath, "chi_lim_max", h5pp::TableSelection::FIRST);
                    tgtDims[0] = static_cast<hsize_t>(chi_max);
                }

                tools::logger::log->debug("Adding target dset {} | dims {} | chnk {}", tgtPath, tgtDims, tgtChunk);
                tgtDsetDb[tgtPath] = h5_tgt.createDataset(tgtPath, srcInfo.h5Type.value(), H5D_CHUNKED, tgtDims, tgtChunk);
            }
            auto &tgtId   = tgtDsetDb[tgtPath];
            auto &tgtInfo = tgtId.info;
            // Determine the target index where to copy this record
            hsize_t index = tgtId.get_index(fileId.seed); // Positive if it has been read already
            index         = index != std::numeric_limits<hsize_t>::max() ? index : tgtInfo.dsetDims.value().at(srcKey.axis);
            internal::copy_dset(h5_tgt, h5_src, tgtInfo, srcInfo, index, srcKey.axis);

            // Update the database
            tgtId.insert(fileId.seed, index);
        }
    }

    void transferTables(h5pp::File &h5_tgt, std::unordered_map<std::string, InfoId<h5pp::TableInfo>> &tgtTableDb,
                        std::unordered_map<std::string, h5pp::TableInfo> &srcTableDb, const PathId &pathid, const std::vector<TableKey> &srcTableKeys,
                        const FileId &fileId) {
        auto t_scope = tid::tic_scope(__FUNCTION__);
        for(const auto &srcKey : srcTableKeys) {
            if(srcTableDb.find(srcKey.key) == srcTableDb.end()) throw std::runtime_error(h5pp::format("Key [{}] was not found in source map", srcKey.key));
            auto &srcInfo = srcTableDb[srcKey.key];
            if(not srcInfo.tableExists or not srcInfo.tableExists.value()) continue;
            auto tgtName = h5pp::fs::path(srcInfo.tablePath.value()).filename().string();
            auto tgtPath = pathid.table_path(tgtName);
            if(tgtTableDb.find(tgtPath) == tgtTableDb.end()) {
                auto t_create = tid::tic_scope("createTable");
                tools::logger::log->debug("Adding target table {}", tgtPath);
                h5pp::TableInfo tableInfo = h5_tgt.getTableInfo(tgtPath);
                if(not tableInfo.tableExists.value())
                    tableInfo = h5_tgt.createTable(srcInfo.h5Type.value(), tgtPath, srcInfo.tableTitle.value(), std::nullopt, true);
                tgtTableDb[tgtPath] = tableInfo;
            }
            auto &tgtId   = tgtTableDb[tgtPath];
            auto &tgtInfo = tgtId.info;

            // Determine the target index where to copy this record
            hsize_t index = tgtId.get_index(fileId.seed); // Positive if it has been read already, numeric_limits::max if it's new
            index         = index != std::numeric_limits<hsize_t>::max() ? index : tgtInfo.numRecords.value();

            tools::logger::log->trace("Transferring table {} -> {}", srcInfo.numRecords.value(), tgtPath);
            auto t_copy = tid::tic_scope("copyTableRecords");
            h5_tgt.copyTableRecords(srcInfo, tgtInfo, h5pp::TableSelection::LAST, index);
            // Update the database
            tgtId.insert(fileId.seed, index);
        }
    }

    void transferCronos(h5pp::File &h5_tgt, std::unordered_map<std::string, InfoId<BufferedTableInfo>> &tgtTableDb,
                        std::unordered_map<std::string, h5pp::TableInfo> &srcTableDb, const PathId &pathid, const std::vector<CronoKey> &srcCronoKeys,
                        const FileId &fileId, const FileStats &fileStats) {
        // In this function we take time series data from each srcTable and create multiple tables tgtTable, one for each
        // time point (iteration). Each entry in tgtTable corresponds to the same time point on different realizations.
        auto                   t_scope = tid::tic_scope(__FUNCTION__);
        std::vector<std::byte> srcReadBuffer;
        std::vector<size_t>    iters; // We can assume all tables have the same iteration numbers. Only update on mismatch
        for(const auto &srcKey : srcCronoKeys) {
            if(srcTableDb.find(srcKey.key) == srcTableDb.end()) throw std::logic_error(h5pp::format("Key [{}] was not found in source map", srcKey.key));
            auto &srcInfo    = srcTableDb[srcKey.key];
            auto  srcRecords = srcInfo.numRecords.value();
            tools::logger::log->trace("Transferring crono table {} record {}", srcRecords, srcInfo.tablePath.value());

            // Iterate over all table elements. These should be a time series measured at every iteration
            // Note that there could in principle exist duplicate entries, which is why we can't trust the
            // "rec" iterator but have to get the iteration number from the table directly.
            // Try getting the iteration number, which is more accurate.

            try {
                if(iters.size() != srcRecords) { // Update iteration numbers if it's not the same that we have already.
                    auto t_read = tid::tic_scope("readTableField");
                    h5pp::hdf5::readTableField(iters, srcInfo, {"iter"});
                    if(iters.empty()) tools::logger::log->warn("column [iter] does not exist in table [{}]", srcInfo.tablePath.value());
                }
            } catch(const std::exception &ex) { throw std::logic_error(fmt::format("Failed to get iteration numbers: {}", ex.what())); }
            for(size_t rec = 0; rec < srcRecords; rec++) {
                size_t iter = rec;
                if(not iters.empty()) iter = iters[rec]; // Get the actual iteration number
                auto tgtName = h5pp::fs::path(srcInfo.tablePath.value()).filename().string();
                auto tgtPath = pathid.crono_path(tgtName, iter);

                if(tgtTableDb.find(tgtPath) == tgtTableDb.end()) {
                    auto t_create = tid::tic_scope("createTable");
                    tools::logger::log->debug("Adding target crono {}", tgtPath);
                    h5pp::TableInfo tableInfo = h5_tgt.getTableInfo(tgtPath);
                    // Disabling compression is supposed to give a nice speedup. Read here:
                    // https://support.hdfgroup.org/HDF5/doc1.8/Advanced/DirectChunkWrite/UsingDirectChunkWrite.pdf
                    if(not tableInfo.tableExists.value())
                        tableInfo = h5_tgt.createTable(srcInfo.h5Type.value(), tgtPath, srcInfo.tableTitle.value(), std::nullopt, true);
                    //                    h5pp::hdf5::setTableSize(tableInfo, fileStats.files);
                    //        h5pp::hid::h5p dapl = H5Dget_access_plist(tableInfo.h5Dset.value());
                    //        size_t rdcc_nbytes = 4*1024*1024;
                    //        size_t rdcc_nslots = rdcc_nbytes * 100 / (tableInfo.recordBytes.value() * tableInfo.chunkSize.value());
                    //        H5Pset_chunk_cache(dapl, rdcc_nslots, rdcc_nbytes, 0.5);
                    //        tableInfo.h5Dset = h5pp::hdf5::openLink<h5pp::hid::h5d>(tableInfo.getLocId(), tableInfo.tablePath.value(), true, dapl);

                    tgtTableDb[tgtPath] = tableInfo;
                    //                    tgtTableDb[tgtPath].info.assertWriteReady();
                }
                auto &tgtId   = tgtTableDb[tgtPath];
                auto &tgtBuff = tgtId.buff;
                //                auto &tgtInfo = tgtTableDb[tgtPath].info;

                // Determine the target index where to copy this record
                // Under normal circumstances, the "index" counts the number of realizations, or simulation seeds.
                // tgtInfo.numRecords is the total number of realizations registered until now

                hsize_t index = tgtId.get_index(fileId.seed); // Positive if it has been read already
                if(index != std::numeric_limits<hsize_t>::max()) {
                    // The table entry for the current realization may already have been added
                    // This happens for instance if we make extra entries in "finished" after adding them to "checkpoint" and/or "savepoint"
                    // However, these entries should be identical, so no need to copy them again.
#pragma message "Skipping here may not be wise?"
                    tools::logger::log->info("Skip copying existing crono entry: {} | index {}", tgtPath, index);
                    continue;
                }

                index = index != std::numeric_limits<hsize_t>::max() ? index : static_cast<hsize_t>(fileStats.count - 1);

                // read a source record at "iter" into the "index" position in the buffer
                auto t_buffer = tid::tic_scope("bufferCronoRecords");
                srcReadBuffer.resize(srcInfo.recordBytes.value());
                h5pp::hdf5::readTableRecords(srcReadBuffer, srcInfo, rec, 1);
                tgtBuff.insert(srcReadBuffer, index);

                // copy/append a source record at "iter" into the "index" position on the table.
                //                tools::logger::log->trace("Copying crono index {} -> {}: {}", rec, index, tgtPath);
                //                auto t_copy = tid::tic_scope("copyTableRecords");
                //                h5_tgt.copyTableRecords(srcInfo, rec, 1, tgtInfo, static_cast<hsize_t>(index));
                // Update the database
                tgtId.insert(fileId.seed, index);
            }
        }
    }

    void transferScales(h5pp::File &h5_tgt, std::unordered_map<std::string, InfoId<BufferedTableInfo>> &tgtTableDb,
                        std::unordered_map<std::string, h5pp::TableInfo> &srcTableDb, const PathId &pathid, const std::vector<ScaleKey> &srcScaleKeys,
                        const FileId &fileId, const FileStats &fileStats) {
        // In this function we take time series data from each srcTable and create multiple tables tgtTable, one for each
        // time point (iteration). Each entry in tgtTable corresponds to the same time point on different realizations.
        auto                   t_scope = tid::tic_scope(__FUNCTION__);
        std::vector<std::byte> srcReadBuffer;
        for(const auto &srcKey : srcScaleKeys) {
            if(srcTableDb.find(srcKey.key) == srcTableDb.end()) throw std::logic_error(h5pp::format("Key [{}] was not found in source map", srcKey.key));
            auto &srcInfo    = srcTableDb[srcKey.key];
            auto  srcRecords = srcInfo.numRecords.value();
            tools::logger::log->trace("Transferring scale table {}", srcInfo.tablePath.value());
            auto tgtName = h5pp::fs::path(srcInfo.tablePath.value()).filename().string();
            auto tgtPath = pathid.scale_path(tgtName, srcKey.chi);

            if(tgtTableDb.find(tgtPath) == tgtTableDb.end()) {
                auto t_create = tid::tic_scope("createTable");
                tools::logger::log->debug("Adding target scale {}", tgtPath);
                h5pp::TableInfo tableInfo = h5_tgt.getTableInfo(tgtPath);
                // Disabling compression is supposed to give a nice speedup. Read here:
                // https://support.hdfgroup.org/HDF5/doc1.8/Advanced/DirectChunkWrite/UsingDirectChunkWrite.pdf
                if(not tableInfo.tableExists.value())
                    tableInfo = h5_tgt.createTable(srcInfo.h5Type.value(), tgtPath, srcInfo.tableTitle.value(), std::nullopt, true);

                tgtTableDb[tgtPath] = tableInfo;
                //                    tgtTableDb[tgtPath].info.assertWriteReady();
            }
            auto &tgtId   = tgtTableDb[tgtPath];
            auto &tgtBuff = tgtId.buff;
            //                auto &tgtInfo = tgtTableDb[tgtPath].info;

            // Determine the target index where to copy this record
            // Under normal circumstances, the "index" counts the number of realizations, or simulation seeds.
            // tgtInfo.numRecords is the total number of realizations registered until now

            hsize_t index = tgtId.get_index(fileId.seed); // Positive if it has been read already
            if(index != std::numeric_limits<hsize_t>::max()) {
                // The table entry for the current realization may already have been added
                // This happens for instance if we make extra entries in "finished" after adding them to "checkpoint" and/or "savepoint"
                // However, these entries should be identical, so no need to copy them again.
#pragma message "Skipping here may not be wise?"
                tools::logger::log->info("Skip copying existing scale entry: {} | index {}", tgtPath, index);
                continue;
            }

            index = index != std::numeric_limits<hsize_t>::max() ? index : static_cast<hsize_t>(fileStats.count - 1);
            // read a source record at "iter" into the "index" position in the buffer
            auto t_buffer = tid::tic_scope("bufferScaleRecords");
            srcReadBuffer.resize(srcInfo.recordBytes.value());
            h5pp::hdf5::readTableRecords(srcReadBuffer, srcInfo, srcRecords - 1, 1); // Read the last entry
            tgtBuff.insert(srcReadBuffer, index);

            // copy/append a source record at "iter" into the "index" position on the table.
            //                tools::logger::log->trace("Copying scale index {} -> {}: {}", rec, index, tgtPath);
            //                auto t_copy = tid::tic_scope("copyTableRecords");
            //                h5_tgt.copyTableRecords(srcInfo, rec, 1, tgtInfo, static_cast<hsize_t>(index));
            // Update the database
            tgtId.insert(fileId.seed, index);
        }
    }

    template<typename ModelType>
    void merge(h5pp::File &h5_tgt, const h5pp::File &h5_src, const FileId &fileId, const FileStats &fileStats, const tools::h5db::Keys &keys,
               tools::h5db::TgtDb &tgtdb) {
        auto t_scope = tid::tic_scope(__FUNCTION__);
        // Define reusable source Info
        static tools::h5db::SrcDb<ModelId<ModelType>> srcdb;
        h5pp::fs::path                                parent_path = h5pp::fs::path(h5_src.getFilePath()).parent_path();
        if(srcdb.parent_path != parent_path) {
            // Clear when moving to another set of seeds (new point on the phase diagram)
            srcdb.clear();
            srcdb.parent_path = parent_path;
        }

        // Start finding the required components in the source
        auto groups = tools::h5io::findKeys(h5_src, "/", keys.get_algos(), -1, 0);
        for(const auto &algo : groups) {
            // Start by extracting the model
            auto modelKeys = tools::h5io::loadModel(h5_src, srcdb.model, keys.models);
            if(modelKeys.size() != 1) throw std::runtime_error("Exactly 1 model has to be loaded into keys");
            auto &modelId = srcdb.model[modelKeys.back().key];
            // Save the model to file if it hasn't
            tools::h5io::saveModel(h5_src, h5_tgt, tgtdb.model, modelId, fileId);
            auto tgt_base = modelId.basepath;
            // Next search for tables and datasets in the source file
            // and transfer them to the target file
            auto state_groups = tools::h5io::findKeys(h5_src, algo, keys.get_states(), -1, 0);
            for(const auto &state : state_groups) {
                auto point_groups = tools::h5io::findKeys(h5_src, fmt::format("{}/{}", algo, state), keys.get_points(), -1, 1);
                for(const auto &point : point_groups) {
                    auto pathid = PathId(tgt_base, algo, state, point);
                    // Try gathering all the tables
                    try {
                        auto t_dset   = tid::tic_scope("dset");
                        auto dsetKeys = tools::h5io::gatherDsetKeys(h5_src, srcdb.dset, pathid, keys.dsets);
                        tools::h5io::transferDatasets(h5_tgt, tgtdb.dset, h5_src, srcdb.dset, pathid, dsetKeys, fileId);
                    } catch(const std::runtime_error &ex) { tools::logger::log->warn("Dset transfer failed in [{}]: {}", pathid.src_path, ex.what()); }

                    try {
                        auto t_table   = tid::tic_scope("table");
                        auto tableKeys = tools::h5io::gatherTableKeys(h5_src, srcdb.table, pathid, keys.tables);
                        tools::h5io::transferTables(h5_tgt, tgtdb.table, srcdb.table, pathid, tableKeys, fileId);
                    } catch(const std::runtime_error &ex) { tools::logger::log->error("Table transfer failed in [{}]: {}", pathid.src_path, ex.what()); }

                    try {
                        auto t_crono   = tid::tic_scope("crono");
                        auto cronoKeys = tools::h5io::gatherCronoKeys(h5_src, srcdb.crono, pathid, keys.cronos);
                        tools::h5io::transferCronos(h5_tgt, tgtdb.crono, srcdb.crono, pathid, cronoKeys, fileId, fileStats);
                    } catch(const std::runtime_error &ex) { tools::logger::log->error("Crono transfer failed in[{}]: {}", pathid.src_path, ex.what()); }
                    try {
                        auto t_scale   = tid::tic_scope("scale");
                        auto scaleKeys = tools::h5io::gatherScaleKeys(h5_src, srcdb.scale, pathid, keys.scales);
                        tools::h5io::transferScales(h5_tgt, tgtdb.scale, srcdb.scale, pathid, scaleKeys, fileId, fileStats);
                    } catch(const std::runtime_error &ex) { tools::logger::log->error("Scale transfer failed in[{}]: {}", pathid.src_path, ex.what()); }
                }
            }
        }

        auto t_close = tid::tic_scope("close");

        // Check that there are no errors hiding in the HDF5 error-stack
        auto num_errors = H5Eget_num(H5E_DEFAULT);
        if(num_errors > 0) {
            H5Eprint(H5E_DEFAULT, stderr);
            throw std::runtime_error(fmt::format("Error when treating file [{}]", h5_src.getFilePath()));
        }
    }
    template void merge<sdual>(h5pp::File &h5_tgt, const h5pp::File &h5_src, const FileId &fileId, const FileStats &fileStats, const tools::h5db::Keys &keys,
                               tools::h5db::TgtDb &tgtdb);
    template void merge<lbit>(h5pp::File &h5_tgt, const h5pp::File &h5_src, const FileId &fileId, const FileStats &fileStats, const tools::h5db::Keys &keys,
                              tools::h5db::TgtDb &tgtdb);

    void writeProfiling(h5pp::File &h5_tgt) {
        auto t_scope = tid::tic_scope(__FUNCTION__);
        H5T_profiling::register_table_type();
        for(const auto &t : tid::get_tree("", tid::level::normal)) {
            auto tablepath = h5pp::format(".db/prof_{}/{}", mpi::world.id, t->get_label());
            if(not h5_tgt.linkExists(tablepath)) h5_tgt.createTable(H5T_profiling::h5_type, tablepath, "H5MBL Profiling", {100});
            H5T_profiling::item entry{t->get_time(), t->get_time_avg(), t->get_tic_count()};
            h5_tgt.writeTableRecords(entry, tablepath, 0);
        }
    }
}