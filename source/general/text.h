#pragma once

#include <string_view>
#include <string>
namespace text {
    extern bool endsWith(std::string_view str, std::string_view suffix);
    extern bool startsWith(std::string_view str, std::string_view prefix);
    extern std::string replace(std::string_view str, std::string_view from, std::string_view to);
}
