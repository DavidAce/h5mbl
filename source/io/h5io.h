#pragma once
#include <h5pp/details/h5ppInfo.h>
#include <io/id.h>
#include <io/meta.h>
#include <string>
#include <unordered_map>
#include <vector>

namespace h5pp {
    class File;
}

namespace tools::h5io {
    template<typename T>
    std::string get_standardized_base(const ModelId<T> &H, int decimals = 4);

    std::vector<std::string> findKeys(const h5pp::File &h5_src, const std::string &root, const std::vector<std::string> &expectedKeys, long hits = -1,
                                      long depth = 0);

    template<typename T>
    std::string loadModel(const h5pp::File &h5_src, std::unordered_map<std::string, ModelId<T>> &srcModelDb, const std::string &algo);

    template<typename T>
    void saveModel(const h5pp::File &h5_src, h5pp::File &h5_tgt, std::unordered_map<std::string, InfoId<h5pp::TableInfo>> &tgtTableDb,
                   const ModelId<T> &modelId);

    std::vector<std::string> gatherTableKeys(const h5pp::File &h5_src, std::unordered_map<std::string, h5pp::TableInfo> &srcTableDb,
                                             const std::string &groupPath, const std::vector<std::string> &tables);

    std::vector<DsetKey> gatherDsetKeys(const h5pp::File &h5_src, std::unordered_map<std::string, h5pp::DsetInfo> &srcDsetDb, const std::string &groupPath,
                                        const std::vector<DsetKey> &srcDsetMetas);

    void transferDatasets(h5pp::File &h5_tgt, std::unordered_map<std::string, InfoId<h5pp::DsetInfo>> &tgtDsetDb, const h5pp::File &h5_src,
                          std::unordered_map<std::string, h5pp::DsetInfo> &srcDsetDb, const std::string &groupPath, const std::vector<DsetKey> &srcDsetKeys,
                          const FileId &fileId);

    void transferTables(h5pp::File &h5_tgt, std::unordered_map<std::string, InfoId<h5pp::TableInfo>> &tgtTableDb,
                        std::unordered_map<std::string, h5pp::TableInfo> &srcTableDb, const std::string &groupPath,
                        const std::vector<std::string> &srcTableKeys, const FileId &fileId);

    void transferCronos(h5pp::File &h5_tgt, std::unordered_map<std::string, InfoId<h5pp::TableInfo>> &tgtTableDb,
                        std::unordered_map<std::string, h5pp::TableInfo> &srcTableDb, const std::string &groupPath,
                        const std::vector<std::string> &srcTableKeys, const FileId &fileId);

    void writeProfiling(h5pp::File &h5_tgt);

}