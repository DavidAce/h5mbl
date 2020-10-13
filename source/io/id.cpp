#include "id.h"
#include <h5pp/details/h5ppInfo.h>

FileId::FileId(long seed_, std::string_view path_, std::string_view hash_) : seed(seed_) {
    strncpy(path, path_.data(), 256);
    strncpy(hash, hash_.data(), 32);
}
std::string FileId::string() const { return h5pp::format("path [{}] | seed {} | hash {}", path, seed, hash); }


template<typename InfoType> InfoId<InfoType>::InfoId(long seed_, long index_){ db[seed_] = index_;}
template<typename InfoType> InfoId<InfoType>::InfoId(const InfoType & info_) : info(info_){}
template struct InfoId<h5pp::DsetInfo>;
template struct InfoId<h5pp::TableInfo>;


H5T_FileId::H5T_FileId() { register_table_type(); }
void H5T_FileId::register_table_type() {
    if(h5_type.valid()) return;
    h5pp::hid::h5t H5T_HASH = H5Tcopy(H5T_C_S1);
    h5pp::hid::h5t H5T_PATH = H5Tcopy(H5T_C_S1);
    H5Tset_size(H5T_PATH, 256);
    H5Tset_size(H5T_HASH, 32);
    // Optionally set the null terminator '\0'
    H5Tset_strpad(H5T_HASH, H5T_STR_NULLTERM);
    h5_type = H5Tcreate(H5T_COMPOUND, sizeof(FileId));
    H5Tinsert(h5_type, "seed", HOFFSET(FileId, seed), H5T_NATIVE_LONG);
    H5Tinsert(h5_type, "path", HOFFSET(FileId, path), H5T_PATH);
    H5Tinsert(h5_type, "hash", HOFFSET(FileId, hash), H5T_HASH);
}


H5T_SeedId::H5T_SeedId() { register_table_type(); }
void H5T_SeedId::register_table_type() {
    if(h5_type.valid()) return;
    h5_type = H5Tcreate(H5T_COMPOUND, sizeof(SeedId));
    H5Tinsert(h5_type, "seed", HOFFSET(SeedId, seed), H5T_NATIVE_LONG);
    H5Tinsert(h5_type, "index", HOFFSET(SeedId, index), H5T_NATIVE_LONG);
}

H5T_profiling::H5T_profiling() { register_table_type(); }
void H5T_profiling::register_table_type() {
    if(h5_type.valid()) return;
    h5_type = H5Tcreate(H5T_COMPOUND, sizeof(table));
    H5Tinsert(h5_type, "t_tot", HOFFSET(table, t_tot), H5T_NATIVE_DOUBLE);
    H5Tinsert(h5_type, "t_pre", HOFFSET(table, t_pre), H5T_NATIVE_DOUBLE);
    H5Tinsert(h5_type, "t_itr", HOFFSET(table, t_itr), H5T_NATIVE_DOUBLE);
    H5Tinsert(h5_type, "t_tab", HOFFSET(table, t_tab), H5T_NATIVE_DOUBLE);
    H5Tinsert(h5_type, "t_grp", HOFFSET(table, t_grp), H5T_NATIVE_DOUBLE);
    H5Tinsert(h5_type, "t_get", HOFFSET(table, t_get), H5T_NATIVE_DOUBLE);
    H5Tinsert(h5_type, "t_dst", HOFFSET(table, t_dst), H5T_NATIVE_DOUBLE);
    H5Tinsert(h5_type, "t_ren", HOFFSET(table, t_ren), H5T_NATIVE_DOUBLE);
    H5Tinsert(h5_type, "t_crt", HOFFSET(table, t_crt), H5T_NATIVE_DOUBLE);
    H5Tinsert(h5_type, "t_ham", HOFFSET(table, t_ham), H5T_NATIVE_DOUBLE);
    H5Tinsert(h5_type, "t_dat", HOFFSET(table, t_dat), H5T_NATIVE_DOUBLE);
}