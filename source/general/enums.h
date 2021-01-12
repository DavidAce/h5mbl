#pragma once
#include <stdexcept>
#include <string_view>
enum class FileIdStatus { UPTODATE, STALE, MISSING };

template<typename T>
constexpr std::string_view enum2str(const T &item) {
    if constexpr(std::is_same_v<T, FileIdStatus>) {
        if(item == FileIdStatus::UPTODATE) return "UPTODATE";
        if(item == FileIdStatus::STALE) return "STALE";
        if(item == FileIdStatus::MISSING) return "MISSING";
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
    throw std::runtime_error("str2enum given invalid string item: " + std::string(item));
}