#pragma once
#include <io/meta.h>
#include <general/enums.h>
#include <h5pp/details/h5ppInfo.h>
#include <io/id.h>
#include <string>
#include <unordered_map>
#include <vector>

namespace h5pp {
    class File;
}




namespace tools::h5db {


    struct Keys{
        std::vector<std::string> algo, state,point, models,tables,cronos;
        std::vector<DsetKey> dsets;
        DsetKey bonds;
    };

    template<typename ModelType>
    struct SrcDb{
        std::unordered_map<std::string, h5pp::TableInfo> table;
        std::unordered_map<std::string, h5pp::DsetInfo>  dset;
        std::unordered_map<std::string, ModelType>       model;
    };

    struct TgtDb{
        std::unordered_map<std::string, FileId>                  file;
        std::unordered_map<std::string, InfoId<h5pp::TableInfo>> model;
        std::unordered_map<std::string, InfoId<h5pp::TableInfo>> table;
        std::unordered_map<std::string, InfoId<h5pp::DsetInfo>>  dset;
    };



    std::unordered_map<std::string, FileId> loadFileDatabase(const h5pp::File &h5_tgt);

    template<typename InfoType, typename MetaType>
    std::unordered_map<std::string, InfoId<InfoType>> loadDatabase(const h5pp::File &h5_tgt, const std::vector<MetaType> &metas);

    void saveDatabase(h5pp::File &h5_tgt, const std::unordered_map<std::string, FileId> &fileIdDb);

    template<typename InfoType>
    void saveDatabase(h5pp::File &h5_tgt, std::unordered_map<std::string, InfoId<InfoType>> &infoDb);

    FileIdStatus getFileIdStatus(const std::unordered_map<std::string, FileId> &fileIdDb, const FileId &newFileId);

}
