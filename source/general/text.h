#pragma once

#include <string_view>

namespace text{
    extern bool endsWith(std::string_view str, std::string_view suffix);
    extern bool startsWith(std::string_view str, std::string_view prefix);
}

