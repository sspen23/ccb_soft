#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "storage_config.h"

static void clear_primary_config(void)
{
    static const char *const names[] = {
        "UART_DEV_PATH", "CCB_LOG_LEVEL", "CCB_STORAGE_PROFILE",
        "CCB_STATUS_TIMEOUT_MS", "CCB_FIRST_DATA_TIMEOUT_MS",
        "CCB_PERF_ENABLE", "CCB_PERF_INTERVAL_MS",
        "CCB_DUMP_DIAG_ON_ERROR", "CCB_STORAGE_COMPAT_MODE",
        CCB_INTERNAL_STORAGE_WORKER,
        "SRC_REAL_LOG_LEVEL", "SRC_REAL_NVME_QD",
        "SRC_REAL_STATUS_TIMEOUT_US", "SRC_REAL_FIRST_DMA_TIMEOUT_US",
        "SRC_REAL_PERF_LOG_ENABLE", "SRC_REAL_PERF_LOG_INTERVAL_SEC",
        "SRC_REAL_DUMP_EVENT_RING_ON_ERROR",
    };
    size_t i;

    for (i = 0u; i < sizeof(names) / sizeof(names[0]); ++i) unsetenv(names[i]);
}

static void test_perf_profile_defaults(void)
{
    AppConfig config;
    char error[160];

    clear_primary_config();
    assert(storage_config_load(&config, error, sizeof(error)) == 0);
    assert(strcmp(config.uart_device, "/dev/ttyUL1") == 0);
    assert(config.storage_profile == STORAGE_PROFILE_PERF_QD8);
    assert(config.log_level == CCB_LOG_INFO);
    assert(config.status_timeout_ms == 100u);
    assert(config.first_data_timeout_ms == 5000u);
    assert(!config.perf_enabled && !config.dump_diag_on_error);
    assert(config.channels[0].writer_mode == STORAGE_WRITER_CROSS_SLOT);
    assert(config.channels[0].nvme_qd == 8u);
    assert(config.channels[0].max_active_slots == 4u);
    assert(config.channels[0].descriptor_bytes == 8u * 1024u * 1024u);
    assert(config.channels[0].command_bytes == 256u * 1024u);
    assert(!config.channels[0].writer_realtime);
    assert(config.channels[0].writer_priority == 0u);
    assert(!config.channels[0].producer_realtime);
    assert(config.channels[0].producer_priority == 0u);
    assert(config.channels[0].nominal_input_mib_s == 1200u);
    assert(config.channels[0].writer_scheduler_weight == 15u);
    assert(config.channels[0].producer_scheduler_weight == 15u);
    assert(config.channels[0].writer_nice == -6);
    assert(config.channels[0].producer_nice == -6);
    assert(config.channels[0].writer_budget_us == 1000u);
    assert(config.channels[0].busy_poll_us == 100u);
    assert(config.channels[0].empty_sleep_us == 0u);
    assert(config.channels[2].writer_mode == STORAGE_WRITER_LEGACY);
    assert(config.channels[2].nvme_qd == 8u);
    assert(config.channels[2].descriptor_bytes == 16u * 1024u * 1024u);
    assert(config.channels[2].nominal_input_mib_s == 80u);
    assert(config.channels[2].writer_scheduler_weight == 3u);
    assert(config.channels[2].producer_scheduler_weight == 1u);
    assert(config.channels[2].writer_nice == 2);
    assert(config.channels[2].producer_nice == 6);
}

static void test_safe_profile_and_primary_values(void)
{
    AppConfig config;
    char error[160];

    clear_primary_config();
    setenv("UART_DEV_PATH", "/dev/ttyTEST", 1);
    setenv("CCB_LOG_LEVEL", "debug", 1);
    setenv("CCB_STORAGE_PROFILE", "SAFE_QD1", 1);
    setenv("CCB_STATUS_TIMEOUT_MS", "75", 1);
    setenv("CCB_FIRST_DATA_TIMEOUT_MS", "1234", 1);
    setenv("CCB_PERF_ENABLE", "1", 1);
    setenv("CCB_PERF_INTERVAL_MS", "500", 1);
    setenv("CCB_DUMP_DIAG_ON_ERROR", "yes", 1);
    assert(storage_config_load(&config, error, sizeof(error)) == 0);
    assert(strcmp(config.uart_device, "/dev/ttyTEST") == 0);
    assert(config.log_level == CCB_LOG_DEBUG);
    assert(config.storage_profile == STORAGE_PROFILE_SAFE_QD1);
    assert(!config.legacy_compat_mode);
    assert(config.status_timeout_ms == 75u);
    assert(config.first_data_timeout_ms == 1234u);
    assert(config.perf_enabled && config.perf_interval_ms == 500u);
    assert(config.dump_diag_on_error);
    assert(config.channels[0].nvme_qd == 1u);
    assert(config.channels[0].max_active_slots == 1u);
    assert(config.channels[2].nvme_qd == 1u);
    assert(!config.channels[0].writer_realtime);
    assert(config.channels[0].writer_priority == 0u);
    assert(!config.channels[0].producer_realtime);
    assert(config.channels[0].producer_priority == 0u);
    assert(config.channels[0].writer_scheduler_weight == 15u);
    assert(config.channels[0].producer_scheduler_weight == 15u);
    assert(config.channels[0].writer_nice == -6);
    assert(config.channels[0].producer_nice == -6);
    assert(config.channels[0].writer_budget_us == 300u);
    assert(config.channels[0].busy_poll_us == 20u);
    assert(config.channels[0].empty_sleep_us == 1u);
    assert(config.auto_input_complete);
    assert(config.idle_scan_interval_ms == 100u);
    assert(config.idle_required_ms == 500u);
    assert(config.idle_required_scans == 5u);
    assert(config.drain_stable_scans == 3u);
    assert(config.drain_stable_us == 100u);
}

static void test_profile_override_requires_compat_mode(void)
{
    clear_primary_config();
    setenv("SRC_REAL_NVME_QD", "8", 1);
    assert(storage_config_compat_getenv("SRC_REAL_NVME_QD") == NULL);
    setenv("CCB_STORAGE_COMPAT_MODE", "1", 1);
    assert(strcmp(storage_config_compat_getenv("SRC_REAL_NVME_QD"), "8") == 0);
}

static void test_legacy_mapping_and_validation(void)
{
    AppConfig config;
    char error[160];

    clear_primary_config();
    setenv("SRC_REAL_LOG_LEVEL", "trace", 1);
    setenv("SRC_REAL_STATUS_TIMEOUT_US", "250000", 1);
    setenv("SRC_REAL_FIRST_DMA_TIMEOUT_US", "9000000", 1);
    setenv("SRC_REAL_PERF_LOG_ENABLE", "1", 1);
    setenv("SRC_REAL_PERF_LOG_INTERVAL_SEC", "2", 1);
    setenv("SRC_REAL_DUMP_EVENT_RING_ON_ERROR", "1", 1);
    assert(storage_config_load(&config, error, sizeof(error)) == 0);
    assert(config.log_level == CCB_LOG_DEBUG);
    assert(config.status_timeout_ms == 250u);
    assert(config.first_data_timeout_ms == 9000u);
    assert(config.perf_enabled && config.perf_interval_ms == 2000u);
    assert(config.dump_diag_on_error);
    assert(strcmp(storage_config_compat_getenv("SRC_REAL_LOG_LEVEL"), "trace") == 0);

    clear_primary_config();
    setenv("CCB_STORAGE_PROFILE", "unknown", 1);
    assert(storage_config_load(&config, error, sizeof(error)) != 0);
    assert(strstr(error, "CCB_STORAGE_PROFILE") != NULL);
}

int main(void)
{
    test_perf_profile_defaults();
    test_safe_profile_and_primary_values();
    test_profile_override_requires_compat_mode();
    test_legacy_mapping_and_validation();
    clear_primary_config();
    puts("mock_storage_profile_test: ok");
    return 0;
}
