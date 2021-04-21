#pragma once
#include <stdexcept>
#include <string_view>
enum class FileIdStatus { UPTODATE, STALE, MISSING };
enum class Model {SDUAL, LBIT};
template<typename T>
constexpr std::string_view enum2str(const T &item) {
    if constexpr(std::is_same_v<T, FileIdStatus>) {
        if(item == FileIdStatus::UPTODATE) return "UPTODATE";
        if(item == FileIdStatus::STALE) return "STALE";
        if(item == FileIdStatus::MISSING) return "MISSING";
    }
    if constexpr(std::is_same_v<T, Model>) {
        if(item == Model::SDUAL) return "SDUAL";
        if(item == Model::LBIT)  return "LBIT";
    }
    throw std::runtime_error("Given invalid enum item");
}

template<typename T>
constexpr auto str2enum(std::string_view item) {
    if constexpr(std::is_same_v<T, FileIdStatus>) {
        if(item == "UPTODATE") return FileIdStatus::UPTODATE;
        if(item == "STALE") return FileIdStatus::STALE;
        if(item == "MISSING") return FileIdStatus::MISSING;
    }
    if constexpr(std::is_same_v<T, Model>) {
        if(item == "sdual") return Model::SDUAL;
        if(item == "SDUAL") return Model::SDUAL;
        if(item == "xdmrg") return Model::SDUAL;
        if(item == "xdmrg") return Model::SDUAL;
        if(item == "xDMRG") return Model::SDUAL;
        if(item == "XDMRG") return Model::SDUAL;
        if(item == "lbit")  return Model::LBIT;
        if(item == "LBIT")  return Model::LBIT;
        if(item == "flbit")  return Model::LBIT;
        if(item == "f-lbit")  return Model::LBIT;
        if(item == "FLBIT")  return Model::LBIT;
    }
    throw std::runtime_error("str2enum given invalid string item: " + std::string(item));
}