#ifndef LOGGER_H
#define LOGGER_H

#include <stdarg.h>


typedef enum log_level {
  LOG_LEVEL_DEBUG,
  LOG_LEVEL_INFO,
  LOG_LEVEL_WARN,
  LOG_LEVEL_ERROR,
  LOG_LEVEL_FATAL
} log_level;

void daemon_log(log_level level, const char *fmt, ...) __attribute__ ((format (printf, 2, 3)));
void vdaemon_log(log_level level, const char *fmt, va_list args) __attribute__ ((format (printf, 2, 0)));

#ifdef DEBUG
#define log_debug(...) daemon_log(LOG_LEVEL_DEBUG, __VA_ARGS__)
#define vlog_debug(args) vdaemon_log(LOG_LEVEL_DEBUG, (args))
#else
#define log_debug(...)
#define vlog_debug(args)
#endif

#define log_info(...) daemon_log(LOG_LEVEL_INFO, __VA_ARGS__)
#define vlog_info(args) vdaemon_log(LOG_LEVEL_INFO, (args))
#define log_warn(...) daemon_log(LOG_LEVEL_WARN, __VA_ARGS__)
#define vlog_warn(args) vdaemon_log(LOG_LEVEL_WARN, (args))
#define log_error(...) daemon_log(LOG_LEVEL_ERROR, __VA_ARGS__)
#define vlog_error(...) vdaemon_log(LOG_LEVEL_ERROR, __VA_ARGS__)
#define log_fatal(...) daemon_log(LOG_LEVEL_FATAL, __VA_ARGS__)
#define vlog_fatal(...) vdaemon_log(LOG_LEVEL_FATAL, __VA_ARGS__)


#endif /* LOGGER_H */
