#include <fstream>
#include <general/prof.h>
#include <io/logger.h>
#include <sstream>
#include <tid/tid.h>
namespace tools::prof {

    void print_profiling() {
        auto lvl = tid::level::normal;
        for(const auto &t : tid::get_tree("", lvl)) tools::logger::log->info("{}", t.str());
    }


    double mem_usage_in_mb(std::string_view name) {
        std::ifstream filestream("/proc/self/status");
        std::string   line;
        while(std::getline(filestream, line)) {
            std::istringstream is_line(line);
            std::string        key;
            if(std::getline(is_line, key, ':')) {
                if(key == name) {
                    std::string value_str;
                    if(std::getline(is_line, value_str)) {
                        // Filter non-digit characters
                        value_str.erase(std::remove_if(value_str.begin(), value_str.end(), [](auto const &c) -> bool { return not std::isdigit(c); }),
                                        value_str.end());
                        // Extract the number
                        long long value = 0;
                        try {
                            std::string::size_type sz; // alias of size_t
                            value = std::stoll(value_str, &sz);
                        } catch(const std::exception &ex) {
                            std::printf("Could not read mem usage from /proc/self/status: Failed to parse string %s: %s", value_str.data(), ex.what());
                        }
                        // Now we have the value in kb
                        return static_cast<double>(value) / 1024.0;
                    }
                }
            }
        }
        return -1.0;
    }

    std::string get_mem_usage() {
        std::string msg;
        msg.append(fmt::format("{:<30}{:>10.2f} MB", "Memory RSS\n", mem_rss_in_mb()));
        msg.append(fmt::format("{:<30}{:>10.2f} MB", "Memory Peak\n", mem_hwm_in_mb()));
        msg.append(fmt::format("{:<30}{:>10.2f} MB", "Memory Vm\n", mem_vm_in_mb()));
        return msg;
    }
    void print_mem_usage() {
        tools::logger::log->info("{:<30}{:>10.2f} MB", "Memory RSS", mem_rss_in_mb());
        tools::logger::log->info("{:<30}{:>10.2f} MB", "Memory Peak", mem_hwm_in_mb());
        tools::logger::log->info("{:<30}{:>10.2f} MB", "Memory Vm", mem_vm_in_mb());
    }
    void print_mem_usage_oneliner() {
        tools::logger::log->debug("mem[rss {:<.2f}|peak {:<.2f}|vm {:<.2f}]MB ", mem_rss_in_mb(), mem_hwm_in_mb(), mem_vm_in_mb());
    }

    double mem_rss_in_mb() { return mem_usage_in_mb("VmRSS"); }
    double mem_hwm_in_mb() { return mem_usage_in_mb("VmHWM"); }
    double mem_vm_in_mb() { return mem_usage_in_mb("VmPeak"); }

}
