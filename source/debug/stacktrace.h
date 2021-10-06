#pragma once
#include <string>

namespace debug {
    void register_callbacks();
    void signal_callback_handler(int status);
    void print_stack_trace();
    void throw_stack_trace(const std::string &msg);
}