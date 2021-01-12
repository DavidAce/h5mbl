//
// Created by david on 2019-03-27.
//

#pragma once
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>
#if defined(SPDLOG_FMT_EXTERNAL)
    #include <fmt/ostream.h>
    #include <fmt/ranges.h>
#else
    #include <spdlog/fmt/bundled/ostream.h>
    #include <spdlog/fmt/bundled/ranges.h>
#endif

namespace tools::logger {
    inline std::shared_ptr<spdlog::logger> log;
    void                                   enableTimeStamp(std::shared_ptr<spdlog::logger> &log);
    void                                   disableTimeStamp(std::shared_ptr<spdlog::logger> &log);
    void                                   setLogLevel(std::shared_ptr<spdlog::logger> &log, size_t levelZeroToSix);
    size_t                                 getLogLevel(std::shared_ptr<spdlog::logger> &log);
    void                            setLogger(std::shared_ptr<spdlog::logger> &log, const std::string &name, size_t levelZeroToSix = 2, bool timestamp = true);
    std::shared_ptr<spdlog::logger> setLogger(const std::string &name, size_t levelZeroToSix = 2, bool timestamp = true);
    std::shared_ptr<spdlog::logger> getLogger(const std::string &name);
}
