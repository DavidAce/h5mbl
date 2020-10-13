#pragma once

#include <string>

namespace tools::hash{
    std::string sha256_file(const std::string &fn);
    constexpr long hash_length();
}
