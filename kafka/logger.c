#include "logger.h"

#include <stdarg.h>
#include <stdio.h>


void daemon_log(log_level level, const char *fmt, ...) {
    const char *level_str;
    switch (level) {
        case LOG_LEVEL_DEBUG: level_str = "DEBUG"; break;
        case LOG_LEVEL_INFO: level_str = "INFO"; break;
        case LOG_LEVEL_WARN: level_str = "WARN"; break;
        case LOG_LEVEL_ERROR: level_str = "ERROR"; break;
    }

    fprintf(stderr, "[%s] ", level_str);

    va_list args;
    va_start(args, fmt);
    vfprintf(stderr, fmt, args);
    va_end(args);

    fprintf(stderr, "\n");
}
