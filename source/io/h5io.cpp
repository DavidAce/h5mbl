#include "h5io.h"

#include <general/prof.h>
#include <h5pp/h5pp.h>
#include <io/id.h>
#include <io/logger.h>
#include <io/meta.h>
#include <string>
#include <unordered_map>
#include <vector>

namespace tools::h5io {
    namespace internal {
        template<typename T>
        void append_dset(h5pp::File &h5_tgt, const h5pp::File &h5_src, h5pp::DsetInfo &tgtInfo, h5pp::DsetInfo &srcInfo) {
            auto data = h5_src.readDataset<T>(srcInfo);
            h5_tgt.appendToDataset(data, tgtInfo, 1, {data.size(), 1});
        }
    }

    std::string get_standardized_base(const ModelId &H, int decimals) {
        return h5pp::format("L_{1}/l_{2:.{0}f}/J_{3:.{0}f}/h_{4:.{0}f}", decimals, H.model_size, H.lambda, H.J_mean, H.h_mean);
    }

    std::string loadModel(const h5pp::File &h5_src, std::unordered_map<std::string, ModelId> &srcModelDb, const std::string &algo) {
        tools::prof::t_ham.tic();
        auto modelPath   = fmt::format("{}/model/hamiltonian", algo);
        auto modelKey    = fmt::format("{}|{}", h5pp::fs::path(h5_src.getFilePath()).parent_path(), algo);
        auto modelExists = srcModelDb.find(modelKey) != srcModelDb.end();
        if(not modelExists and h5_src.linkExists(modelPath)) {
            // Copy the model
            srcModelDb[modelKey] = ModelId();
            auto &H              = srcModelDb[modelKey];
            H.model_size         = h5_src.readAttribute<size_t>("model_size", modelPath);
            H.model_type         = h5_src.readAttribute<std::string>("model_type", modelPath);
            H.distribution       = h5_src.readAttribute<std::string>("distribution", modelPath);
            H.J_mean             = h5_src.readAttribute<double>("J_mean", modelPath);
            H.J_stdv             = h5_src.readAttribute<double>("J_stdv", modelPath);
            H.h_mean             = h5_src.readAttribute<double>("h_mean", modelPath);
            H.h_stdv             = h5_src.readAttribute<double>("h_stdv", modelPath);
            H.lambda             = h5_src.readAttribute<double>("lambda", modelPath);
            H.delta              = h5_src.readAttribute<double>("delta", modelPath);
            H.algorithm          = algo;
            H.key                = modelKey;
            H.path               = modelPath;
        }
        tools::prof::t_ham.toc();
        return modelKey;
    }

    void saveModel(const h5pp::File &h5_src, h5pp::File &h5_tgt, std::unordered_map<std::string, InfoId<h5pp::TableInfo>> &tgtModelDb, const ModelId &modelId) {
        tools::prof::t_ham.tic();
        const auto &      algo         = modelId.algorithm;
        const std::string tgt_base     = get_standardized_base(modelId);
        const std::string tgtTablePath = fmt::format("{}/{}", tgt_base, modelId.path);
        bool              tgtExists    = tgtModelDb.find(tgtTablePath) != tgtModelDb.end();
        if(not tgtExists) {
            for(auto & [key,val] : tgtModelDb) h5pp::print("tgtTableDb: match {}: [{}] == [{}]\n",tgtTablePath == key, tgtTablePath,key);
            // Copy the whole Hamiltonian table (with site information)
            h5_tgt.copyLinkFromLocation(tgtTablePath, h5_src.openFileHandle(), modelId.path);
            tgtModelDb[tgtTablePath] = h5_tgt.getTableInfo(tgtTablePath);
            // Now copy some helpful scalar datasets. This data is available in the attributes of the table
            // above but this is also handy
            h5_tgt.writeDataset(modelId.model_size, fmt::format("{}/{}/model/model_size", tgt_base, algo));
            h5_tgt.writeDataset(modelId.model_type, fmt::format("{}/{}/model/model_type", tgt_base, algo));
            h5_tgt.writeDataset(modelId.J_mean, fmt::format("{}/{}/model/J_mean", tgt_base, algo));
            h5_tgt.writeDataset(modelId.J_stdv, fmt::format("{}/{}/model/J_stdv", tgt_base, algo));
            h5_tgt.writeDataset(modelId.h_mean, fmt::format("{}/{}/model/h_mean", tgt_base, algo));
            h5_tgt.writeDataset(modelId.h_stdv, fmt::format("{}/{}/model/h_stdv", tgt_base, algo));
            h5_tgt.writeDataset(modelId.lambda, fmt::format("{}/{}/model/lambda", tgt_base, algo));
            h5_tgt.writeDataset(modelId.delta, fmt::format("{}/{}/model/delta", tgt_base, algo));
            h5_tgt.writeDataset(modelId.distribution, fmt::format("{}/{}/model/distribution", tgt_base, algo));
        }
        tools::prof::t_ham.toc();
    }

    std::vector<std::string> gatherTableKeys(const h5pp::File &h5_src, std::unordered_map<std::string, h5pp::TableInfo> &srcTableDb, const std::string &groupPath,
                                             const std::vector<std::string> &tables) {
        prof::t_get.tic();
        std::vector<std::string> keys;
        std::string              srcParentPath = h5pp::fs::path(h5_src.getFilePath()).parent_path();
        for(auto &table : tables) {
            auto tablePath = fmt::format("{}/{}", groupPath, table);
            auto tableKey  = fmt::format("{}|{}", srcParentPath, tablePath);
            if(srcTableDb.find(tableKey) == srcTableDb.end()) {
                tools::logger::log->info("Detected new source table {}", tableKey);
                srcTableDb[tableKey] = h5_src.getTableInfo(tablePath);
            }
            auto &        info = srcTableDb[tableKey];
            h5pp::Options options;
            options.linkPath = tablePath;
            info.tableDset   = std::nullopt;
            info.tableFile   = std::nullopt;
            info.numRecords  = std::nullopt;
            info.tableExists = std::nullopt;
            h5pp::scan::fillTableInfo(info, h5_src.openFileHandle(), options);
            if(not info.tableExists or not info.tableExists.value()){
                prof::t_get.toc();
                throw std::runtime_error(h5pp::format("Missing table [{}] in file [{}]", tablePath, h5_src.getFilePath()));
            }
            keys.emplace_back(tableKey);
        }
        prof::t_get.toc();
        return keys;
    }

    std::vector<DsetMeta> gatherDsetKeys(const h5pp::File &h5_src, std::unordered_map<std::string, h5pp::DsetInfo> &srcDsetDb, const std::string &groupPath,
                                         const std::vector<DsetMeta> &srcDsetMetas) {
        prof::t_get.tic();
        std::vector<DsetMeta> keys;
        std::string           srcParentPath = h5pp::fs::path(h5_src.getFilePath()).parent_path();
        for(auto &meta : srcDsetMetas) {
            auto dsetPath = fmt::format("{}/{}", groupPath, meta.name);
            auto dsetKey  = fmt::format("{}|{}", srcParentPath, dsetPath);
            if(srcDsetDb.find(dsetKey) == srcDsetDb.end()) {
                tools::logger::log->info("Detected new source dataset {}", dsetKey);
                srcDsetDb[dsetKey] = h5_src.getDatasetInfo(dsetPath);
            }
            auto &        info = srcDsetDb[dsetKey];
            h5pp::Options options;
            options.linkPath = dsetPath;
            info.h5Dset      = std::nullopt;
            info.h5File      = std::nullopt;
            info.h5Space     = std::nullopt;
            info.dsetExists  = std::nullopt;
            info.dsetSize    = std::nullopt;
            info.dsetDims    = std::nullopt;
            info.dsetByte    = std::nullopt;
            h5pp::scan::fillDsetInfo(info, h5_src.openFileHandle(), options);
            if(not info.dsetExists or not info.dsetExists.value()){
                prof::t_get.toc();
                throw std::runtime_error(h5pp::format("Missing dataset [{}] in file [{}]", dsetPath, h5_src.getFilePath()));
            }
            keys.emplace_back(meta);
            keys.back().key = dsetKey;
        }
        prof::t_get.toc();
        return keys;
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
            if(tgtTableDb.find(tgtPath) == tgtTableDb.end()) {
                tools::logger::log->info("Adding target table {}", tgtPath);
                h5pp::TableInfo tableInfo = h5_tgt.getTableInfo(tgtPath);
                if(not tableInfo.tableExists.value()) tableInfo = h5_tgt.createTable(srcInfo.tableType.value(), tgtPath, srcInfo.tableTitle.value());
                tgtTableDb[tgtPath] = tableInfo;
            }
            tools::prof::t_crt.toc();
            auto &tgtInfo = tgtTableDb[tgtPath].info;
            auto &tgtDb   = tgtTableDb[tgtPath].db;

            // Determine the target index where to copy this record
            long index = 0;
            if(tgtDb.find(fileId.seed) == tgtDb.end()) index = static_cast<long>(tgtInfo.numRecords.value());
            else
                index = tgtDb[fileId.seed];
            tools::prof::t_tab.tic();
            h5_tgt.copyTableRecords(srcInfo, h5pp::TableSelection::LAST, tgtInfo, static_cast<hsize_t>(index));
            tools::prof::t_tab.toc();

            // Update the database
            tgtDb[fileId.seed] = index;
        }
    }

    void transferDatasets(h5pp::File &h5_tgt, std::unordered_map<std::string, InfoId<h5pp::DsetInfo>> &tgtDsetDb, const h5pp::File &h5_src,
                          std::unordered_map<std::string, h5pp::DsetInfo> &srcDsetDb, const std::string &groupPath, const std::vector<DsetMeta> &srcDsetMetas,
                          const FileId &fileId) {
        for(const auto &meta : srcDsetMetas) {
            auto &srcKey = meta.key;
            if(srcDsetDb.find(srcKey) == srcDsetDb.end()) throw std::runtime_error(h5pp::format("Key [{}] was not found in source map", srcKey));
            auto &srcInfo = srcDsetDb[srcKey];
            auto  tgtName = h5pp::fs::path(srcInfo.dsetPath.value()).filename().string();
            auto  tgtPath = h5pp::format("{}/{}", groupPath, tgtName);
            tools::prof::t_crt.tic();
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

            auto &tgtInfo = tgtDsetDb[tgtPath].info;
            auto &tgtDb   = tgtDsetDb[tgtPath].db;
            // Determine the target index where to copy this record
            long index = 0;
            if(tgtDb.find(fileId.seed) == tgtDb.end()) index = static_cast<long>(tgtInfo.dsetDims.value()[1]);
            else
                index = tgtDb[fileId.seed];

            tools::prof::t_crt.toc();
            tools::prof::t_dst.tic();

            switch(meta.type) {
                case Type::DOUBLE: internal::append_dset<std::vector<double>>(h5_tgt, h5_src, tgtInfo, srcInfo); break;
                case Type::LONG: internal::append_dset<std::vector<long>>(h5_tgt, h5_src, tgtInfo, srcInfo); break;
                case Type::INT: internal::append_dset<std::vector<int>>(h5_tgt, h5_src, tgtInfo, srcInfo); break;
                case Type::COMPLEX: internal::append_dset<std::vector<std::complex<double>>>(h5_tgt, h5_src, tgtInfo, srcInfo); break;
            }
            tools::prof::t_dst.toc();

            // Update the database
            tgtDb[fileId.seed] = index;
        }
    }

    void writeProfiling(h5pp::File &h5_tgt){
        H5T_profiling::register_table_type();
        if(not h5_tgt.linkExists(".db/prof")){
            h5_tgt.createTable(H5T_profiling::h5_type,".db/prof", "H5MBL Profiling", {100},4);
        }
        h5_tgt.appendTableRecords(tools::prof::buffer, ".db/prof");
    }
}