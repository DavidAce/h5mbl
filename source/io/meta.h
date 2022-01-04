#pragma once
#include <string>

// Define the allowed items
enum class Type { INT, LONG, DOUBLE, COMPLEX, TID };
enum class Size { FIX, VAR };
// struct DsetKey {
//     Type        type;
//     Size        size = Size::FIX;
//     std::string name;
//     std::string key;
// };

struct Key {
    std::string algo, state, point;
    std::string name;
    std::string key;
    Key() = default;
    Key(std::string_view algo_, std::string_view state_, std::string_view point_, std::string_view name_)
        : algo(algo_), state(state_), point(point_), name(name_) {}
};

struct DsetKey : public Key {
    Size size = Size::FIX;
    size_t axis;
    DsetKey(std::string_view algo_, std::string_view state_, std::string_view point_, std::string_view name_, Size size_, size_t axis_)
        : Key(algo_, state_, point_, name_), size(size_), axis(axis_) {}
};

struct TableKey : public Key {
    using Key::Key;
};

struct CronoKey : public Key {
    using Key::Key;
};

struct ScaleKey : public Key {
    std::string scale; // The group pattern, usually something like "chi_*"
    size_t chi; // The actual value of chi found
    ScaleKey(std::string_view algo_, std::string_view state_, std::string_view point_, std::string_view scale_, std::string_view name_)
        : Key(algo_, state_, point_, name_), scale(scale_), chi(-1ul){}
};

struct ModelKey {
    std::string algo, model;
    std::string name;
    std::string key;
    ModelKey(std::string_view algo_, std::string_view model_, std::string_view name_) : algo(algo_), model(model_), name(name_) {}
};
