#pragma once
#include <general/enums.h>
#include <io/id.h>
#include <string>
#include <unordered_map>
#include <vector>

namespace h5pp {
    class File;
}

namespace tools::h5db {

    std::unordered_map<std::string, FileId> loadFileDatabase(const h5pp::File &h5_tgt);

    template<typename InfoType, typename MetaType>
    std::unordered_map<std::string, InfoId<InfoType>> loadDatabase(const h5pp::File &h5_tgt, const std::vector<MetaType> &metas);

    void saveDatabase(h5pp::File &h5_tgt, const std::unordered_map<std::string, FileId> &fileIdDb);

    template<typename InfoType>
    void saveDatabase(h5pp::File &h5_tgt, std::unordered_map<std::string, InfoId<InfoType>> &infoDb);

    FileIdStatus getFileIdStatus(const std::unordered_map<std::string, FileId> &fileIdDb, const FileId &newFileId);
}
