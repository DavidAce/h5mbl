#include "text.h"

bool text::endsWith(std::string_view str, std::string_view suffix) {
    return str.size() >= suffix.size() && 0 == str.compare(str.size() - suffix.size(), suffix.size(), suffix);
}

bool text::startsWith(std::string_view str, std::string_view prefix) { return str.size() >= prefix.size() && 0 == str.compare(0, prefix.size(), prefix); }