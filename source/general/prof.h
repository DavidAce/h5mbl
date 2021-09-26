#pragma once
#include <general/class_tic_toc.h>
#include <h5pp/details/h5ppHid.h>
#include <io/id.h>
#include <memory>
#include <vector>
namespace tools::prof {
    // Define profiling timers
    inline class_tic_toc                     t_tot = class_tic_toc(true, 5, "Total    time");
    inline class_tic_toc                     t_pre = class_tic_toc(true, 5, "Pre      time");
    inline class_tic_toc                     t_itr = class_tic_toc(true, 5, "Iter     time");
    inline class_tic_toc                     t_tab = class_tic_toc(true, 5, "Table    time");
    inline class_tic_toc                     t_cro = class_tic_toc(true, 5, "Crono    time");
    inline class_tic_toc                     t_gr1 = class_tic_toc(true, 5, "Group 1  time");
    inline class_tic_toc                     t_gr2 = class_tic_toc(true, 5, "Group 2  time");
    inline class_tic_toc                     t_gr3 = class_tic_toc(true, 5, "Group 3  time");
    inline class_tic_toc                     t_get = class_tic_toc(true, 5, "Get      time");
    inline class_tic_toc                     t_dst = class_tic_toc(true, 5, "Dset     time");
    inline class_tic_toc                     t_ren = class_tic_toc(true, 5, "Renyi    time");
    inline class_tic_toc                     t_crt = class_tic_toc(true, 5, "Create   time");
    inline class_tic_toc                     t_ham = class_tic_toc(true, 5, "Model    time");
    inline class_tic_toc                     t_dat = class_tic_toc(true, 5, "Database time");
    inline class_tic_toc                     t_hsh = class_tic_toc(true, 5, "Hash     time");
    inline class_tic_toc                     t_fnd = class_tic_toc(true, 5, "Find     time");
    inline class_tic_toc                     t_spd = class_tic_toc(true, 5, "Speed    time");

    inline class_tic_toc                     t_opn = class_tic_toc(true, 5, "Open file");
    inline class_tic_toc                     t_mrg = class_tic_toc(true, 5, "Merge file");
    inline class_tic_toc                     t_cnt = class_tic_toc(true, 5, "Count ids");
    inline class_tic_toc                     t_trn = class_tic_toc(true, 5, "Transfer");
    inline class_tic_toc                     t_clo = class_tic_toc(true, 5, "Close file");
    inline class_tic_toc                     t_cpy = class_tic_toc(true, 5, "Copy dset");
    inline class_tic_toc                     t_app = class_tic_toc(true, 5, "Append dset");
    inline class_tic_toc                     t_ch1 = class_tic_toc(true, 5, "Check 1");
    inline class_tic_toc                     t_ch2 = class_tic_toc(true, 5, "Check 2");
    inline class_tic_toc                     t_ch3 = class_tic_toc(true, 5, "Check 3");
    inline class_tic_toc                     t_ch4 = class_tic_toc(true, 5, "Check 4");
    inline class_tic_toc                     t_mrg_dst = class_tic_toc(true, 5, "Merge");
    inline class_tic_toc                     t_mrg_tab = class_tic_toc(true, 5, "Merge");
    inline class_tic_toc                     t_mrg_cro = class_tic_toc(true, 5, "Merge");
    inline class_tic_toc                     t_mrg_set = class_tic_toc(true, 5, "Merge");
    inline class_tic_toc                     t_dst_get  = class_tic_toc(true, 5, "Dset");
    inline class_tic_toc                     t_dst_trn  = class_tic_toc(true, 5, "Dset");
    inline class_tic_toc                     t_dst_crt  = class_tic_toc(true, 5, "Dset");
    inline class_tic_toc                     t_dst_cpy  = class_tic_toc(true, 5, "Dset");
    inline class_tic_toc                     t_tab_get  = class_tic_toc(true, 5, "Table");
    inline class_tic_toc                     t_tab_trn  = class_tic_toc(true, 5, "Table");
    inline class_tic_toc                     t_tab_crt  = class_tic_toc(true, 5, "Table");
    inline class_tic_toc                     t_tab_cpy  = class_tic_toc(true, 5, "Table");
    inline class_tic_toc                     t_cro_get  = class_tic_toc(true, 5, "Crono");
    inline class_tic_toc                     t_cro_trn  = class_tic_toc(true, 5, "Crono");
    inline class_tic_toc                     t_cro_crt  = class_tic_toc(true, 5, "Crono");
    inline class_tic_toc                     t_cro_cpy  = class_tic_toc(true, 5, "Crono");

    inline std::vector<H5T_profiling::table> buffer;
    void                                     init();
    void                                     append();

    extern std::string get_mem_usage();
    extern void print_mem_usage();
    extern void print_mem_usage_oneliner();
    extern double mem_usage_in_mb(std::string_view name);
    extern double mem_rss_in_mb();
    extern double mem_hwm_in_mb();
    extern double mem_vm_in_mb();

}
