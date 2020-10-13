#pragma once
#include <io/id.h>
#include <io/meta.h>
#include <string>
#include <unordered_map>
#include <vector>
#include <h5pp/details/h5ppInfo.h>

namespace h5pp {
    class File;
}

namespace tools::h5io {

    std::string get_standardized_base(const ModelId &H, int decimals = 3);

    std::string loadModel(const h5pp::File &h5_src, std::unordered_map<std::string, ModelId> &srcModelDb, const std::string &algo);

    void saveModel(const h5pp::File &h5_src, h5pp::File &h5_tgt, std::unordered_map<std::string, InfoId<h5pp::TableInfo>> &tgtTableDb, const ModelId &modelId);

    std::vector<std::string> gatherTableKeys(const h5pp::File &h5_src, std::unordered_map<std::string, h5pp::TableInfo> &srcTableDb, const std::string &groupPath,
                                             const std::vector<std::string> &tables);

    std::vector<DsetMeta> gatherDsetKeys(const h5pp::File &h5_src, std::unordered_map<std::string, h5pp::DsetInfo> &srcDsetDb, const std::string &groupPath,
                                         const std::vector<DsetMeta> &srcDsetMetas);

    void transferTables(h5pp::File &h5_tgt, std::unordered_map<std::string, InfoId<h5pp::TableInfo>> &tgtTableDb,
                        const std::unordered_map<std::string, h5pp::TableInfo> &srcTableDb, const std::string &groupPath,
                        const std::vector<std::string> &srcTableKeys, const FileId &fileId);



    void transferDatasets(h5pp::File &h5_tgt, std::unordered_map<std::string, InfoId<h5pp::DsetInfo>> &tgtDsetDb, const h5pp::File &h5_src,
                          std::unordered_map<std::string, h5pp::DsetInfo> &srcDsetDb, const std::string &groupPath, const std::vector<DsetMeta> &srcDsetMetas,
                          const FileId &fileId);

    void writeProfiling(h5pp::File &h5_tgt);

}