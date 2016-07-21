#include "logger.h"

#include <stdarg.h>
#include <stdio.h>


void daemon_log(log_level level, const char *fmt, ...) {
    va_list args;
    va_start(args, fmt);
    vdaemon_log(level, fmt, args);
    va_end(args);
}

void vdaemon_log(log_level level, const char *fmt, va_list args) {
    const char *level_str;
    switch (level) {
        case LOG_LEVEL_DEBUG: level_str = "DEBUG"; break;
        case LOG_LEVEL_INFO: level_str = "INFO"; break;
        case LOG_LEVEL_WARN: level_str = "WARN"; break;
        case LOG_LEVEL_ERROR: level_str = "ERROR"; break;
        case LOG_LEVEL_FATAL: level_str = "FATAL"; break;
    }

    fprintf(stderr, "[%s] ", level_str);
    vfprintf(stderr, fmt, args);
    fprintf(stderr, "\n");
}
