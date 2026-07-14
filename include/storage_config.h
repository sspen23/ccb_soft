#ifndef STORAGE_CONFIG_H
#define STORAGE_CONFIG_H

#include "ccb_types.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define STORAGE_CONFIG_UART_PATH_MAX 256u

typedef enum {
    CCB_LOG_ERROR = 0,
    CCB_LOG_INFO,
    CCB_LOG_PERF,
    CCB_LOG_DEBUG
} CcbLogLevel;

typedef enum {
    STORAGE_PROFILE_SAFE_QD1 = 0,
    STORAGE_PROFILE_PERF_QD8
} StorageProfile;

typedef enum {
    STORAGE_WRITER_LEGACY = 0,
    STORAGE_WRITER_CROSS_SLOT
} StorageWriterMode;

typedef struct {
    uint32_t channel;
    StorageWriterMode writer_mode;
    uint64_t ring_bytes;
    uint32_t descriptor_bytes;
    uint32_t command_bytes;
    uint32_t nvme_qd;
    uint32_t max_active_slots;
    uint32_t cq_batch;
} ChannelStorageConfig;

typedef struct {
    char uart_device[STORAGE_CONFIG_UART_PATH_MAX];
    CcbLogLevel log_level;
    bool perf_enabled;
    uint32_t perf_interval_ms;
    uint32_t status_timeout_ms;
    uint32_t first_data_timeout_ms;
    StorageProfile storage_profile;
    bool dump_diag_on_error;
    ChannelStorageConfig channels[NUM_CHANNELS];
} AppConfig;

/* Load one immutable configuration snapshot. */
int storage_config_load(AppConfig *out, char *error, size_t error_size);
int storage_config_load_global(void);
const AppConfig *storage_config_get(void);
const ChannelStorageConfig *storage_config_channel(const AppConfig *config,
                                                   uint32_t channel);
const char *storage_config_profile_name(StorageProfile profile);
const char *storage_config_log_level_name(CcbLogLevel level);

/* Transitional compatibility entry point.  Process-environment access is
 * confined to storage_config.c while old overrides move to the snapshot. */
const char *storage_config_compat_getenv(const char *name);

#endif
