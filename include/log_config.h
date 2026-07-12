#ifndef LOG_CONFIG_H
#define LOG_CONFIG_H

#include "db_config.h"

/* Log level configuration. */
#define LOG_LEVEL_DEBUG   0
#define LOG_LEVEL_INFO    1
#define LOG_LEVEL_WARN    2
#define LOG_LEVEL_ERROR   3

/* Current log level. Set to LOG_LEVEL_DEBUG/INFO/WARN/ERROR. */
#define CURRENT_LOG_LEVEL LOG_LEVEL_DEBUG

/* Database path configuration is centralized in db_config.h. */
/* #define LOG_DB_PATH "logs.db" */

/* Buffer sizes. */
#define LOG_BUFFER_SIZE 512
#define LOG_MODULE_MAX_LEN 32
#define LOG_MESSAGE_MAX_LEN 256

/* Async logging configuration. */
#define ASYNC_LOG_ENABLED 0

/* Maximum async queue length. Unused when ASYNC_LOG_ENABLED is 0. */
#define ASYNC_QUEUE_MAX_LEN 20

#endif
