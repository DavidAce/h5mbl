#pragma once
#include <string>

// Define the allowed items
enum class Type { INT, LONG, DOUBLE, COMPLEX };
enum class Size { FIX, VAR };
struct DsetKey {
    Type        type;
    Size        size = Size::FIX;
    std::string name;
    std::string key;
};
