#include <general/prof.h>
#include <fstream>
#include <sstream>
#include <io/logger.h>
namespace tools::prof {

    void init() {
        tools::prof::t_tot = class_tic_toc(true, 5, "Total    time");
        tools::prof::t_pre = class_tic_toc(true, 5, "Pre      time");
        tools::prof::t_itr = class_tic_toc(true, 5, "Iter     time");
        tools::prof::t_tab = class_tic_toc(true, 5, "Table    time");
        tools::prof::t_gr1 = class_tic_toc(true, 5, "Group 1  time");
        tools::prof::t_gr2 = class_tic_toc(true, 5, "Group 2  time");
        tools::prof::t_gr3 = class_tic_toc(true, 5, "Group 3  time");
        tools::prof::t_get = class_tic_toc(true, 5, "Get      time");
        tools::prof::t_dst = class_tic_toc(true, 5, "Dset     time");
        tools::prof::t_ren = class_tic_toc(true, 5, "Renyi    time");
        tools::prof::t_crt = class_tic_toc(true, 5, "Create   time");
        tools::prof::t_ham = class_tic_toc(true, 5, "Model    time");
        tools::prof::t_dat = class_tic_toc(true, 5, "Database time");
        tools::prof::t_spd = class_tic_toc(true, 5, "Speed    time");
    }

    void append() {
        buffer.emplace_back(H5T_profiling::table{t_tot.get_measured_time(), t_pre.get_last_interval(), t_itr.get_last_interval(), t_tab.get_last_interval(),
                                                 t_gr1.get_last_interval(), t_get.get_last_interval(), t_dst.get_last_interval(), t_ren.get_last_interval(),
                                                 t_crt.get_last_interval(), t_ham.get_last_interval(), t_dat.get_last_interval()

        });
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

    std::string get_mem_usage(){
        std::string msg;
        msg.append(fmt::format("{:<30}{:>10.2f} MB", "Memory RSS\n", mem_rss_in_mb()));
        msg.append(fmt::format("{:<30}{:>10.2f} MB", "Memory Peak\n", mem_hwm_in_mb()));
        msg.append(fmt::format("{:<30}{:>10.2f} MB", "Memory Vm\n", mem_vm_in_mb()));
        return msg;
    }
    void print_mem_usage(){
        tools::logger::log->debug("{:<30}{:>10.2f} MB", "Memory RSS", mem_rss_in_mb());
        tools::logger::log->debug("{:<30}{:>10.2f} MB", "Memory Peak", mem_hwm_in_mb());
        tools::logger::log->debug("{:<30}{:>10.2f} MB", "Memory Vm", mem_vm_in_mb());
    }
    void print_mem_usage_oneliner(){
        tools::logger::log->debug("mem[rss {:<.2f}|peak {:<.2f}|vm {:<.2f}]MB ",
                                  mem_rss_in_mb(),
                                  mem_hwm_in_mb(),
                                  mem_vm_in_mb());
    }


    double mem_rss_in_mb() { return mem_usage_in_mb("VmRSS"); }
    double mem_hwm_in_mb() { return mem_usage_in_mb("VmHWM"); }
    double mem_vm_in_mb() { return mem_usage_in_mb("VmPeak"); }



}
