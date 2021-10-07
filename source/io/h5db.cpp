#include "h5db.h"
#include <general/enums.h>
#include <general/prof.h>
#include <h5pp/h5pp.h>
#include <io/id.h>
#include <io/logger.h>
#include <io/meta.h>
#include <string>
#include <tid/tid.h>
#include <unordered_map>
#include <vector>

namespace tools::h5db {

    std::unordered_map<std::string, FileId> loadFileDatabase(const h5pp::File &h5_tgt) {
        auto                                    t_scope = tid::tic_scope(__FUNCTION__);
        std::unordered_map<std::string, FileId> fileDatabase;
        if(h5_tgt.linkExists(".db/files")) {
            tools::logger::log->info("Loading database files");
            auto database = h5_tgt.readTableRecords<std::vector<FileId>>(".db/files");
            for(auto &item : database) fileDatabase[item.path] = item;
        }
        return fileDatabase;
    }

    template<typename InfoType, typename KeyType>
    std::unordered_map<std::string, InfoId<InfoType>> loadDatabase(const h5pp::File &h5_tgt, const std::vector<KeyType> &keys) {
        auto                                              t_scope = tid::tic_scope(__FUNCTION__);
        std::unordered_map<std::string, InfoId<InfoType>> infoDataBase;
        auto                                              dbGroups = h5_tgt.findGroups(".db");
        tools::logger::log->info("Found {} groups matching [.db]", dbGroups.size());

        for(auto &dbGroup : dbGroups) {
            // Find table databases
            tools::logger::log->info("Loading databases in {}", dbGroup);
            for(auto &key : keys) {
                std::vector<std::string> dbNames;
                tools::logger::log->trace("-- Searching for database");
                dbNames = h5_tgt.findDatasets(key.name, dbGroup, -1, 0);
                tools::logger::log->trace("-- Found database names {}", dbNames);
                if(dbNames.empty()) continue;
                if(dbNames.size() > 1) throw std::logic_error(h5pp::format("Found multiple seed databases: {}", dbNames));
                // Now we have to load our database, which itself is a table with fields [seed,index].
                // It also has a [key] attribute so that we can place it in our map, as well as a [path]
                // attribute to find the actual table
                auto dbPath = h5pp::format("{}/{}", dbGroup, dbNames.front());
                tools::logger::log->trace("-- Loading database {}", dbPath);
                auto seedIdDb = h5_tgt.readTableRecords<std::vector<SeedId>>(dbPath);
                auto infoKey  = h5_tgt.readAttribute<std::string>("key", dbPath);
                auto infoPath = h5_tgt.readAttribute<std::string>("path", dbPath);
                if constexpr(std::is_same_v<InfoType, h5pp::DsetInfo>)
                    infoDataBase[infoKey] = h5_tgt.getDatasetInfo(infoPath);
                else if constexpr(std::is_same_v<InfoType, h5pp::TableInfo>)
                    infoDataBase[infoKey] = h5_tgt.getTableInfo(infoPath);
                auto &infoId = infoDataBase[infoKey];
                // We can now load the seed/index database it into the map
                for(auto &seedId : seedIdDb) infoId.db[seedId.seed] = seedId.index;
            }
        }

        return infoDataBase;
    }

    template std::unordered_map<std::string, InfoId<h5pp::DsetInfo>>    loadDatabase(const h5pp::File &h5_tgt, const std::vector<DsetKey> &keys);
    template std::unordered_map<std::string, InfoId<h5pp::TableInfo>>   loadDatabase(const h5pp::File &h5_tgt, const std::vector<TableKey> &keys);
    template std::unordered_map<std::string, InfoId<h5pp::TableInfo>>   loadDatabase(const h5pp::File &h5_tgt, const std::vector<CronoKey> &keys);
    template std::unordered_map<std::string, InfoId<BufferedTableInfo>> loadDatabase(const h5pp::File &h5_tgt, const std::vector<CronoKey> &keys);
    template std::unordered_map<std::string, InfoId<h5pp::TableInfo>>   loadDatabase(const h5pp::File &h5_tgt, const std::vector<ModelKey> &keys);

    void saveDatabase(h5pp::File &h5_tgt, const std::unordered_map<std::string, FileId> &fileIdDb) {
        auto t_scope = tid::tic_scope(__FUNCTION__);
        tools::logger::log->debug("Writing database: .db/files");
        if(not h5_tgt.linkExists(".db/files")) {
            H5T_FileId::register_table_type();
            h5_tgt.createTable(H5T_FileId::h5_type, ".db/files", "File database", {1000}, 3);
        }
        std::vector<FileId> fileIdVec;
        for(auto &fileId : fileIdDb) { fileIdVec.emplace_back(fileId.second); }
        auto sorter = [](auto &lhs, auto &rhs) {
            return lhs.seed < rhs.seed;
        };
        std::sort(fileIdVec.begin(), fileIdVec.end(), sorter);
        h5_tgt.writeTableRecords(fileIdVec, ".db/files");
    }

    void clearInfo(std::optional<h5pp::DataInfo> &info) {
        if(info) {
            info->dataSize = std::nullopt;
            info->dataByte = std::nullopt;
            info->dataDims = std::nullopt;
            info->h5Space  = std::nullopt;
        }
    }
    void clearInfo(std::optional<h5pp::TableInfo> &info) {
        if(info) {
            info->h5Dset         = std::nullopt;
            info->numRecords     = std::nullopt;
            info->tableGroupName = std::nullopt;
            info->tablePath      = std::nullopt;
            info->tableExists    = std::nullopt;
        }
    }
    void clearInfo(std::optional<h5pp::AttrInfo> &info) {
        if(info) {
            info->h5Attr   = std::nullopt;
            info->h5Space  = std::nullopt;
            info->linkPath = std::nullopt;
            info->attrSize = std::nullopt;
            info->attrByte = std::nullopt;
            info->attrDims = std::nullopt;
        }
    }

    template<typename InfoType>
    void saveDatabase(h5pp::File &h5_tgt, std::unordered_map<std::string, InfoId<InfoType>> &infoDb) {
        auto t_scope = tid::tic_scope(__FUNCTION__);
        std::optional<h5pp::DataInfo>  dataInfoKey;
        std::optional<h5pp::DataInfo>  dataInfoPath;
        std::optional<h5pp::AttrInfo>  attrInfoKey;
        std::optional<h5pp::AttrInfo>  attrInfoPath;
        std::optional<h5pp::TableInfo> tableInfo;


        h5_tgt.setKeepFileOpened();
        for(const auto &[infoKey, infoId] : infoDb) {
            tools::logger::log->info("Saving database infoKey {} | InfoType {} | infoId {} | infoDb.size() {}", infoKey, type::sfinae::type_name<InfoType>(), type::sfinae::type_name<decltype(infoId)>(), infoDb.size());

            std::vector<SeedId> seedIdxVec;
            for(const auto &[seed, index] : infoId.db) { seedIdxVec.emplace_back(SeedId{seed, index}); }
            auto sorter = [](auto &lhs, auto &rhs) {
                return lhs.seed < rhs.seed;
            };
            std::sort(seedIdxVec.begin(), seedIdxVec.end(), sorter);
            h5pp::fs::path tgtPath;
            if constexpr(std::is_same_v<InfoType, h5pp::DsetInfo>)
                tgtPath = infoId.info.dsetPath.value();
            else if constexpr(std::is_same_v<InfoType, h5pp::TableInfo>)
                tgtPath = infoId.info.tablePath.value();
            else if constexpr(std::is_same_v<InfoType, BufferedTableInfo>)
                tgtPath = infoId.info.tablePath.value();
            else
                throw std::runtime_error(h5pp::format("Failed to identify InfoType: {}", type::sfinae::type_name<InfoType>()));
            std::string tgtName   = tgtPath.filename();
            std::string tgtGroup  = tgtPath.parent_path();
            std::string tgtDbPath = h5pp::format("{}/.db/{}", tgtGroup, tgtName);
            tools::logger::log->debug("Writing database: {}", tgtDbPath);
            if(not h5_tgt.linkExists(tgtDbPath)) {
                H5T_SeedId::register_table_type();
                if(tableInfo) {
                    clearInfo(tableInfo);
                    tableInfo->tablePath = tgtDbPath;
                    h5_tgt.createTable(tableInfo.value());
                } else {
                    tableInfo = h5_tgt.createTable(H5T_SeedId::h5_type, tgtDbPath, "Seed index database", {1000}, 4);
                }

                if(attrInfoKey and dataInfoKey and attrInfoPath and dataInfoPath) {
                    // Renew some of the metdata
                    clearInfo(attrInfoKey);
                    clearInfo(attrInfoPath);
                    clearInfo(dataInfoKey);
                    clearInfo(dataInfoPath);
                    attrInfoKey->linkPath  = tgtDbPath;
                    attrInfoPath->linkPath = tgtDbPath;
                    h5_tgt.writeAttribute(infoKey, dataInfoKey.value(), attrInfoKey.value());
                    h5_tgt.writeAttribute(tgtPath.string(), dataInfoPath.value(), attrInfoPath.value());
                } else {
                    attrInfoKey  = h5_tgt.writeAttribute(infoKey, "key", tgtDbPath);
                    attrInfoPath = h5_tgt.writeAttribute(tgtPath.string(), "path", tgtDbPath);
                }
            }
            if(seedIdxVec.empty()) continue;
            h5_tgt.writeTableRecords(seedIdxVec, tgtDbPath);
        }
        h5_tgt.setKeepFileClosed();
    }
    template void saveDatabase(h5pp::File &h5_tgt, std::unordered_map<std::string, InfoId<h5pp::DsetInfo>> &infoDb);
    template void saveDatabase(h5pp::File &h5_tgt, std::unordered_map<std::string, InfoId<h5pp::TableInfo>> &infoDb);
    template void saveDatabase(h5pp::File &h5_tgt, std::unordered_map<std::string, InfoId<BufferedTableInfo>> &infoDb);

    FileIdStatus getFileIdStatus(const std::unordered_map<std::string, FileId> &fileIdDb, const FileId &newFileId) {
        auto t_scope = tid::tic_scope(__FUNCTION__);

        // There can be a number of scenarios:
        // a) the entry does not exist in the database --> MISSING
        // b) the entry exists in the database and both seed and hash match --> copy index --> return UPTODATE
        // c) the entry exists in the database and the name matches but not the hash  --> copy index --> STALE
        const std::string filePath(newFileId.path);

        bool exists = fileIdDb.find(filePath) != fileIdDb.end();
        if(not exists) return FileIdStatus::MISSING;

        auto &oldFileId = fileIdDb.at(filePath);

        bool seedMatch = oldFileId.seed == newFileId.seed;
        bool hashMatch = std::string_view(oldFileId.hash, 32) == std::string_view(newFileId.hash, 32);

        if(seedMatch and hashMatch)
            return FileIdStatus::UPTODATE;
        else if(seedMatch and not hashMatch)
            return FileIdStatus::STALE;
        else if(not seedMatch and hashMatch)
            throw std::logic_error(
                h5pp::format("Hash matches but not seeds! This should never happen\n Old entry {}\n New entry {}", oldFileId.string(), newFileId.string()));
        else
            throw std::runtime_error(
                h5pp::format("Hashes and seeds do not match. Something is wrong! \n Old entry {}\n New entry {}", oldFileId.string(), newFileId.string()));
    }
}
