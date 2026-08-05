#ifndef CCB_STORAGE_LOG_H
#define CCB_STORAGE_LOG_H

#include <stdbool.h>
#include "storage_config.h"

typedef enum {
    STORAGE_LOG_ALWAYS_CRITICAL = 0,
    STORAGE_LOG_SUMMARY = 1,
    STORAGE_LOG_DEBUG = 2,
    STORAGE_LOG_TRACE = 3
} StorageLogSeverity;

static inline StorageLogSeverity storage_log_level_from_config(CcbLogLevel level)
{
    if (level == CCB_LOG_DEBUG) return STORAGE_LOG_DEBUG;
    if (level == CCB_LOG_INFO || level == CCB_LOG_PERF)
        return STORAGE_LOG_SUMMARY;
    return STORAGE_LOG_ALWAYS_CRITICAL;
}

static inline StorageLogSeverity storage_log_effective_level(void)
{
    const AppConfig *config = storage_config_get();

    return config ? storage_log_level_from_config(config->log_level)
                  : STORAGE_LOG_ALWAYS_CRITICAL;
}

static inline bool storage_log_severity_enabled(StorageLogSeverity severity)
{
    const AppConfig *config = storage_config_get();

    if (severity == STORAGE_LOG_ALWAYS_CRITICAL) return true;
    return config && config->log_enabled &&
           storage_log_effective_level() >= severity;
}

#endif
