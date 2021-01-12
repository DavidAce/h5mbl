#include <general/prof.h>

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
    }

    void append() {
        buffer.emplace_back(H5T_profiling::table{t_tot.get_measured_time(), t_pre.get_last_interval(), t_itr.get_last_interval(), t_tab.get_last_interval(),
                                                 t_gr1.get_last_interval(), t_get.get_last_interval(), t_dst.get_last_interval(), t_ren.get_last_interval(),
                                                 t_crt.get_last_interval(), t_ham.get_last_interval(), t_dat.get_last_interval()

        });
    }
}