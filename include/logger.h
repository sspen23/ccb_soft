#ifndef LOGGER_H
#define LOGGER_H

#include <stdint.h>
#include <time.h>

/* Log level values. */
typedef enum {
    LOG_DEBUG = 0,
    LOG_INFO,
    LOG_WARN,
    LOG_ERROR
} LogLevel;

/* One row from the system_logs table. */
typedef struct {
    int id;                    /* Log row id. */
    time_t timestamp;          /* Log timestamp. */
    LogLevel level;            /* Log level. */
    char module[32];           /* Module name. */
    char message[256];         /* Message text. */
} LogEntry;

/*
 * Initialize the logging database.
 *
 * @param db_path Database file path.
 * @return 0 on success, -1 on failure.
 */
int logger_init(const char *db_path);

/* Close the logging database. */
void logger_close(void);

/*
 * Write one log entry.
 *
 * @param level Log level.
 * @param module Module name.
 * @param format printf-style format string.
 * @param ... Format arguments.
 * @return 0 on success, -1 on failure.
 */
int logger_write(LogLevel level, const char *module, const char *format, ...);

/* Convenience logging macros. */
#if CURRENT_LOG_LEVEL <= LOG_LEVEL_DEBUG
#define LOG_DEBUG(module, fmt, ...) \
    logger_write(LOG_DEBUG, module, fmt, ##__VA_ARGS__)
#else
#define LOG_DEBUG(module, fmt, ...) ((void)0)
#endif

#if CURRENT_LOG_LEVEL <= LOG_LEVEL_INFO
#define LOG_INFO(module, fmt, ...) \
    logger_write(LOG_INFO, module, fmt, ##__VA_ARGS__)
#else
#define LOG_INFO(module, fmt, ...) ((void)0)
#endif

#if CURRENT_LOG_LEVEL <= LOG_LEVEL_WARN
#define LOG_WARN(module, fmt, ...) \
    logger_write(LOG_WARN, module, fmt, ##__VA_ARGS__)
#else
#define LOG_WARN(module, fmt, ...) ((void)0)
#endif

#if CURRENT_LOG_LEVEL <= LOG_LEVEL_ERROR
#define LOG_ERROR(module, fmt, ...) \
    logger_write(LOG_ERROR, module, fmt, ##__VA_ARGS__)
#else
#define LOG_ERROR(module, fmt, ...) ((void)0)
#endif

/*
 * Query recent logs.
 *
 * @param limit Maximum row count.
 * @param entries Output array allocated by caller.
 * @return Number of rows returned, or -1 on failure.
 */
int logger_query(int limit, LogEntry *entries);

/*
 * Delete old logs.
 *
 * @param days Keep the most recent N days; 0 deletes all logs.
 * @return Number of rows deleted, or -1 on failure.
 */
int logger_delete_old(int days);

#endif
