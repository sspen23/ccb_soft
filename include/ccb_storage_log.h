#ifndef CCB_STORAGE_LOG_H
#define CCB_STORAGE_LOG_H

#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include "storage_config.h"

/*
 * Storage text has a small, explicit severity vocabulary.  A legacy text
 * request is only a compatibility default when SRC_REAL_LOG_LEVEL is unset;
 * it must not override an explicitly selected quiet/summary level.
 */
typedef enum {
    STORAGE_LOG_ALWAYS_CRITICAL = 0,
    STORAGE_LOG_SUMMARY = 1,
    STORAGE_LOG_DEBUG = 2,
    STORAGE_LOG_TRACE = 3
} StorageLogSeverity;

static inline bool storage_log_env_flag_enabled(const char *name)
{
    const char *value = storage_config_compat_getenv(name);

    return value && value[0] != '\0' && strcmp(value, "0") != 0 &&
           strcmp(value, "false") != 0 && strcmp(value, "FALSE") != 0 &&
           strcmp(value, "off") != 0 && strcmp(value, "OFF") != 0 &&
           strcmp(value, "no") != 0 && strcmp(value, "NO") != 0;
}

static inline StorageLogSeverity storage_log_effective_level(void)
{
    const char *value = storage_config_compat_getenv("SRC_REAL_LOG_LEVEL");

    if (!value || value[0] == '\0') {
        return storage_log_env_flag_enabled("SRC_REAL_LEGACY_STORAGE_TEXT")
                   ? STORAGE_LOG_DEBUG
                   : STORAGE_LOG_ALWAYS_CRITICAL;
    }
    if (strcmp(value, "summary") == 0) return STORAGE_LOG_SUMMARY;
    if (strcmp(value, "debug") == 0) return STORAGE_LOG_DEBUG;
    if (strcmp(value, "trace") == 0) return STORAGE_LOG_TRACE;
    return STORAGE_LOG_ALWAYS_CRITICAL;
}

static inline bool storage_log_severity_enabled(StorageLogSeverity severity)
{
    return severity == STORAGE_LOG_ALWAYS_CRITICAL ||
           storage_log_effective_level() >= severity;
}

#endif
