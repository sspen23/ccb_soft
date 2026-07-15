#include "storage_config.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MIB (1024u * 1024u)
#define DEFAULT_UART_DEVICE "/dev/ttyUL1"
#define DEFAULT_STATUS_TIMEOUT_MS 100u
#define DEFAULT_FIRST_DATA_TIMEOUT_MS 5000u
#define DEFAULT_PERF_INTERVAL_MS 1000u

static AppConfig g_config;
static bool g_config_loaded;
static char g_deprecated_names[128][96];
static size_t g_deprecated_name_count;

static bool text_is_false(const char *value)
{
    return !value || value[0] == '\0' || strcmp(value, "0") == 0 ||
           strcmp(value, "false") == 0 || strcmp(value, "FALSE") == 0 ||
           strcmp(value, "off") == 0 || strcmp(value, "OFF") == 0 ||
           strcmp(value, "no") == 0 || strcmp(value, "NO") == 0;
}

bool storage_config_legacy_compat_enabled(void)
{
    return !text_is_false(getenv("CCB_STORAGE_COMPAT_MODE"));
}

static bool is_profile_override_name(const char *name)
{
    return name && strncmp(name, "SRC_REAL_", 9u) == 0 &&
           (strstr(name, "NVME_CMD_KIB") != NULL ||
            strstr(name, "NVME_QD") != NULL ||
            strstr(name, "CROSS_SLOT_QD") != NULL ||
            strstr(name, "MAX_ACTIVE") != NULL ||
            strstr(name, "CROSS_SLOT_BATCH") != NULL ||
            strstr(name, "TARGET_QD") != NULL ||
            strstr(name, "CQ_BATCH") != NULL ||
            strstr(name, "CROSS_SLOT_ENABLED") != NULL ||
            strstr(name, "CROSS_SLOT_CH") != NULL ||
            strcmp(name, "SRC_REAL_CROSS_SLOT") == 0 ||
            strstr(name, "WRITER_RT_POLICY") != NULL ||
            strstr(name, "PRODUCER_RT_POLICY") != NULL ||
            strstr(name, "WRITER_RT_PRIO") != NULL ||
            strstr(name, "PRODUCER_RT_PRIO") != NULL ||
            strstr(name, "STORAGE_RING_BYTES") != NULL ||
            strstr(name, "STORAGE_DMA_DESC_BYTES") != NULL);
}

static void warn_deprecated_once(const char *name)
{
    size_t i;

    if (!name || name[0] == '\0') return;
    for (i = 0u; i < g_deprecated_name_count; ++i) {
        if (strcmp(g_deprecated_names[i], name) == 0) return;
    }
    if (g_deprecated_name_count <
        sizeof(g_deprecated_names) / sizeof(g_deprecated_names[0])) {
        snprintf(g_deprecated_names[g_deprecated_name_count],
                 sizeof(g_deprecated_names[g_deprecated_name_count]), "%s", name);
        ++g_deprecated_name_count;
    }
    fprintf(stderr,
            "warning: deprecated storage environment variable %s; use the CCB_* configuration profile\n",
            name);
}

static bool is_storage_worker_process(void)
{
    return !text_is_false(getenv(CCB_INTERNAL_STORAGE_WORKER));
}

static void warn_profile_overrides_once(void)
{
    static const char *const global_names[] = {
        "SRC_REAL_NVME_CMD_KIB", "SRC_REAL_NVME_QD",
        "SRC_REAL_TARGET_QD", "SRC_REAL_MAX_ACTIVE", "SRC_REAL_CQ_BATCH",
        "SRC_REAL_CROSS_SLOT", "SRC_REAL_CROSS_SLOT_ENABLED",
        "SRC_REAL_WRITER_RT_POLICY", "SRC_REAL_PRODUCER_RT_POLICY",
        "SRC_REAL_STORAGE_RING_BYTES", "SRC_REAL_STORAGE_DMA_DESC_BYTES",
    };
    static const struct {
        const char *prefix;
        const char *suffix;
    } channel_names[] = {
        {"SRC_REAL_NVME_CMD_KIB_CH", ""},
        {"SRC_REAL_NVME_QD_CH", ""},
        {"SRC_REAL_TARGET_QD_CH", ""},
        {"SRC_REAL_MAX_ACTIVE_CH", ""},
        {"SRC_REAL_CQ_BATCH_CH", ""},
        {"SRC_REAL_CROSS_SLOT_CH", ""},
        {"SRC_REAL_CH", "_TARGET_QD"},
        {"SRC_REAL_CH", "_CQ_BATCH"},
        {"SRC_REAL_CH", "_WRITER_RT_PRIO"},
        {"SRC_REAL_CH", "_PRODUCER_RT_PRIO"},
        {"SRC_REAL_STORAGE_RING_BYTES_CH", ""},
        {"SRC_REAL_STORAGE_DMA_DESC_BYTES_CH", ""},
    };
    size_t i;
    uint32_t channel;

    if (is_storage_worker_process()) return;
    for (i = 0u; i < sizeof(global_names) / sizeof(global_names[0]); ++i) {
        const char *value = getenv(global_names[i]);
        if (value && value[0] != '\0') warn_deprecated_once(global_names[i]);
    }
    for (channel = 0u; channel < NUM_CHANNELS; ++channel) {
        for (i = 0u;
             i < sizeof(channel_names) / sizeof(channel_names[0]); ++i) {
            char name[96];
            const char *value;

            (void)snprintf(name, sizeof(name), "%s%u%s",
                           channel_names[i].prefix, channel,
                           channel_names[i].suffix);
            value = getenv(name);
            if (value && value[0] != '\0') warn_deprecated_once(name);
        }
    }
}

const char *storage_config_compat_getenv(const char *name)
{
    const char *value;

    if (!name) return NULL;
    value = getenv(name);
    if (value && value[0] != '\0' && strncmp(name, "SRC_REAL_", 9u) == 0 &&
        !is_storage_worker_process())
        warn_deprecated_once(name);
    if (value && value[0] != '\0' && is_profile_override_name(name) &&
        !storage_config_legacy_compat_enabled())
        return NULL;
    return value;
}

static const char *read_primary_or_legacy(const char *primary, const char *legacy)
{
    const char *value = getenv(primary);

    if (value && value[0] != '\0') return value;
    if (!legacy) return NULL;
    value = getenv(legacy);
    if (value && value[0] != '\0') warn_deprecated_once(legacy);
    return value;
}

static int parse_u32(const char *name, const char *value, uint32_t minimum,
                     uint32_t maximum, uint32_t *out, char *error,
                     size_t error_size)
{
    char *end = NULL;
    unsigned long parsed;

    if (!value || value[0] == '\0') return 0;
    errno = 0;
    parsed = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0' || parsed < minimum ||
        parsed > maximum) {
        if (error && error_size != 0u)
            snprintf(error, error_size, "invalid %s=%s", name, value);
        return -1;
    }
    *out = (uint32_t)parsed;
    return 0;
}

static int parse_legacy_us(const char *name, const char *value, uint32_t *out,
                           char *error, size_t error_size)
{
    uint32_t microseconds;

    if (!value || value[0] == '\0') return 0;
    if (parse_u32(name, value, 1000u, UINT32_MAX, &microseconds,
                  error, error_size) != 0)
        return -1;
    *out = microseconds / 1000u;
    return 0;
}

static int parse_log_level(const char *value, CcbLogLevel *out)
{
    if (!value || value[0] == '\0' || strcmp(value, "info") == 0 ||
        strcmp(value, "summary") == 0) {
        *out = CCB_LOG_INFO;
        return 0;
    }
    if (strcmp(value, "error") == 0 || strcmp(value, "critical") == 0 ||
        strcmp(value, "quiet") == 0) {
        *out = CCB_LOG_ERROR;
        return 0;
    }
    if (strcmp(value, "perf") == 0) {
        *out = CCB_LOG_PERF;
        return 0;
    }
    if (strcmp(value, "debug") == 0 || strcmp(value, "trace") == 0) {
        *out = CCB_LOG_DEBUG;
        return 0;
    }
    return -1;
}

static int parse_profile(const char *value, StorageProfile *out)
{
    if (!value || value[0] == '\0' || strcmp(value, "PERF_QD8") == 0 ||
        strcmp(value, "perf_qd8") == 0 || strcmp(value, "perf") == 0) {
        *out = STORAGE_PROFILE_PERF_QD8;
        return 0;
    }
    if (strcmp(value, "SAFE_QD1") == 0 || strcmp(value, "safe_qd1") == 0 ||
        strcmp(value, "safe") == 0) {
        *out = STORAGE_PROFILE_SAFE_QD1;
        return 0;
    }
    return -1;
}

static void set_channel_profile(AppConfig *config)
{
    uint32_t channel;
    const bool safe = config->storage_profile == STORAGE_PROFILE_SAFE_QD1;

    for (channel = 0u; channel < NUM_CHANNELS; ++channel) {
        ChannelStorageConfig *storage = &config->channels[channel];

        memset(storage, 0, sizeof(*storage));
        storage->channel = channel;
        storage->writer_mode = channel == LOW_SPEED_CHANNEL_ID
                                   ? STORAGE_WRITER_LEGACY
                                   : STORAGE_WRITER_CROSS_SLOT;
        storage->ring_bytes = channel == LOW_SPEED_CHANNEL_ID
                                  ? CHANNEL2_DDR_BYTES
                                  : CHANNEL0_DDR_BYTES;
        storage->descriptor_bytes = channel == LOW_SPEED_CHANNEL_ID
                                        ? 16u * MIB
                                        : 8u * MIB;
        storage->command_bytes = 256u * 1024u;
        storage->nvme_qd = safe ? 1u : 8u;
        storage->max_active_slots = safe ? 1u :
                                    (channel == LOW_SPEED_CHANNEL_ID ? 1u : 4u);
        storage->cq_batch = safe ? 1u : 8u;
        storage->writer_realtime = false;
        storage->writer_priority = 0u;
        storage->producer_realtime = false;
        storage->producer_priority = 0u;
    }
}

int storage_config_load(AppConfig *out, char *error, size_t error_size)
{
    const char *value;
    const char *legacy;

    if (!out) return -1;
    memset(out, 0, sizeof(*out));
    if (error && error_size != 0u) error[0] = '\0';
    value = getenv("UART_DEV_PATH");
    snprintf(out->uart_device, sizeof(out->uart_device), "%s",
             value && value[0] != '\0' ? value : DEFAULT_UART_DEVICE);

    value = read_primary_or_legacy("CCB_LOG_LEVEL", "SRC_REAL_LOG_LEVEL");
    if (parse_log_level(value, &out->log_level) != 0) {
        if (error && error_size != 0u)
            snprintf(error, error_size, "invalid CCB_LOG_LEVEL=%s", value);
        return -1;
    }
    value = getenv("CCB_STORAGE_PROFILE");
    if (parse_profile(value, &out->storage_profile) != 0) {
        if (error && error_size != 0u)
            snprintf(error, error_size, "invalid CCB_STORAGE_PROFILE=%s", value);
        return -1;
    }
    out->legacy_compat_mode = storage_config_legacy_compat_enabled();
    warn_profile_overrides_once();

    out->status_timeout_ms = DEFAULT_STATUS_TIMEOUT_MS;
    value = getenv("CCB_STATUS_TIMEOUT_MS");
    if (value && value[0] != '\0') {
        if (parse_u32("CCB_STATUS_TIMEOUT_MS", value, 1u, 60000u,
                      &out->status_timeout_ms, error, error_size) != 0)
            return -1;
    } else {
        legacy = getenv("SRC_REAL_STATUS_TIMEOUT_US");
        if (legacy && legacy[0] != '\0') warn_deprecated_once("SRC_REAL_STATUS_TIMEOUT_US");
        if (parse_legacy_us("SRC_REAL_STATUS_TIMEOUT_US", legacy,
                            &out->status_timeout_ms, error, error_size) != 0)
            return -1;
    }

    out->first_data_timeout_ms = DEFAULT_FIRST_DATA_TIMEOUT_MS;
    value = getenv("CCB_FIRST_DATA_TIMEOUT_MS");
    if (value && value[0] != '\0') {
        if (parse_u32("CCB_FIRST_DATA_TIMEOUT_MS", value, 1u, 3600000u,
                      &out->first_data_timeout_ms, error, error_size) != 0)
            return -1;
    } else {
        legacy = getenv("SRC_REAL_FIRST_DMA_TIMEOUT_US");
        if (legacy && legacy[0] != '\0') warn_deprecated_once("SRC_REAL_FIRST_DMA_TIMEOUT_US");
        if (parse_legacy_us("SRC_REAL_FIRST_DMA_TIMEOUT_US", legacy,
                            &out->first_data_timeout_ms, error, error_size) != 0)
            return -1;
    }

    out->perf_enabled = !text_is_false(read_primary_or_legacy(
        "CCB_PERF_ENABLE", "SRC_REAL_PERF_LOG_ENABLE"));
    out->perf_interval_ms = DEFAULT_PERF_INTERVAL_MS;
    value = getenv("CCB_PERF_INTERVAL_MS");
    if (value && value[0] != '\0') {
        if (parse_u32("CCB_PERF_INTERVAL_MS", value, 100u, 60000u,
                      &out->perf_interval_ms, error, error_size) != 0)
            return -1;
    } else {
        uint32_t seconds = 0u;
        legacy = getenv("SRC_REAL_PERF_LOG_INTERVAL_SEC");
        if (legacy && legacy[0] != '\0') {
            warn_deprecated_once("SRC_REAL_PERF_LOG_INTERVAL_SEC");
            if (parse_u32("SRC_REAL_PERF_LOG_INTERVAL_SEC", legacy, 1u, 60u,
                          &seconds, error, error_size) != 0)
                return -1;
            out->perf_interval_ms = seconds * 1000u;
        }
    }
    out->dump_diag_on_error = !text_is_false(read_primary_or_legacy(
        "CCB_DUMP_DIAG_ON_ERROR", "SRC_REAL_DUMP_EVENT_RING_ON_ERROR"));
    out->auto_input_complete = true;
    out->idle_scan_interval_ms = 100u;
    out->idle_required_ms = 500u;
    out->idle_required_scans = 5u;
    out->drain_stable_scans = 3u;
    out->drain_stable_us = 100u;
    set_channel_profile(out);
    return 0;
}

int storage_config_load_global(void)
{
    char error[160];

    if (g_config_loaded) return 0;
    if (storage_config_load(&g_config, error, sizeof(error)) != 0) {
        fprintf(stderr, "storage configuration failed: %s\n", error);
        return -1;
    }
    g_config_loaded = true;
    return 0;
}

const AppConfig *storage_config_get(void)
{
    return storage_config_load_global() == 0 ? &g_config : NULL;
}

const ChannelStorageConfig *storage_config_channel(const AppConfig *config,
                                                   uint32_t channel)
{
    return config && channel < NUM_CHANNELS ? &config->channels[channel] : NULL;
}

const char *storage_config_profile_name(StorageProfile profile)
{
    return profile == STORAGE_PROFILE_SAFE_QD1 ? "SAFE_QD1" : "PERF_QD8";
}

const char *storage_config_log_level_name(CcbLogLevel level)
{
    switch (level) {
    case CCB_LOG_ERROR: return "ERROR";
    case CCB_LOG_INFO: return "INFO";
    case CCB_LOG_PERF: return "PERF";
    case CCB_LOG_DEBUG: return "DEBUG";
    default: return "UNKNOWN";
    }
}
