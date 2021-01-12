#pragma once
#include <memory>
#include <vector>
#include <general/class_tic_toc.h>
#include <h5pp/details/h5ppHid.h>
#include <io/id.h>
namespace tools::prof{
    // Define profiling timers
    inline class_tic_toc t_tot = class_tic_toc(true, 5, "Total    time");
    inline class_tic_toc t_pre = class_tic_toc(true, 5, "Pre      time");
    inline class_tic_toc t_itr = class_tic_toc(true, 5, "Iter     time");
    inline class_tic_toc t_tab = class_tic_toc(true, 5, "Table    time");
    inline class_tic_toc t_gr1 = class_tic_toc(true, 5, "Group 1  time");
    inline class_tic_toc t_gr2 = class_tic_toc(true, 5, "Group 2  time");
    inline class_tic_toc t_gr3 = class_tic_toc(true, 5, "Group 3  time");
    inline class_tic_toc t_get = class_tic_toc(true, 5, "Get      time");
    inline class_tic_toc t_dst = class_tic_toc(true, 5, "Dset     time");
    inline class_tic_toc t_ren = class_tic_toc(true, 5, "Renyi    time");
    inline class_tic_toc t_crt = class_tic_toc(true, 5, "Create   time");
    inline class_tic_toc t_ham = class_tic_toc(true, 5, "Model    time");
    inline class_tic_toc t_dat = class_tic_toc(true, 5, "Database time");
    inline class_tic_toc t_hsh = class_tic_toc(true, 5, "Hash     time");
    inline std::vector<H5T_profiling::table> buffer;
    void init();
    void append();

};
