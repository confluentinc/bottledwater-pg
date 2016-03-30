#ifndef LOGGER_H
#define LOGGER_H


typedef enum log_level {
  LOG_LEVEL_DEBUG,
  LOG_LEVEL_INFO,
  LOG_LEVEL_WARN,
  LOG_LEVEL_ERROR
} log_level;

void daemon_log(log_level level, const char *fmt, ...) __attribute__ ((format (printf, 2, 3)));

#ifdef DEBUG
#define log_debug(...) daemon_log(LOG_LEVEL_DEBUG, __VA_ARGS__)
#else
#define log_debug(...)
#endif

#define log_info(...) daemon_log(LOG_LEVEL_INFO, __VA_ARGS__)
#define log_warn(...) daemon_log(LOG_LEVEL_WARN, __VA_ARGS__)
#define log_error(...) daemon_log(LOG_LEVEL_ERROR, __VA_ARGS__)


#endif /* LOGGER_H */
