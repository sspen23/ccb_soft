#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "ccb_commands.h"

static void clear_cross_slot_env(void)
{
    static const char *const names[] = {
        "SRC_REAL_CROSS_SLOT_CH0", "SRC_REAL_CROSS_SLOT_ENABLED_CH0",
        "SRC_REAL_NVME_CROSS_SLOT_QD_CH0", "SRC_REAL_CROSS_SLOT",
        "SRC_REAL_CROSS_SLOT_ENABLED", "SRC_REAL_NVME_CROSS_SLOT_QD",
        "SRC_REAL_MAX_ACTIVE_CH0", "SRC_REAL_CROSS_SLOT_BATCH_CH0",
        "SRC_REAL_NVME_CROSS_SLOT_MAX_ACTIVE_CH0",
        "SRC_REAL_NVME_CROSS_SLOT_BATCH_CH0",
        "SRC_REAL_CH0_NVME_CROSS_SLOT_MAX_ACTIVE", "SRC_REAL_CH0_CROSS_SLOT_BATCH",
        "SRC_REAL_MAX_ACTIVE", "SRC_REAL_CROSS_SLOT_BATCH",
        "SRC_REAL_CROSS_SLOT_MAX_ACTIVE", "SRC_REAL_NVME_CROSS_SLOT_MAX_ACTIVE",
        "SRC_REAL_NVME_CROSS_SLOT_BATCH",
        "SRC_REAL_TARGET_QD_CH0", "SRC_REAL_CH0_TARGET_QD",
        "SRC_REAL_NVME_CROSS_SLOT_TARGET_QD_CH0", "SRC_REAL_TARGET_QD",
        "SRC_REAL_NVME_CROSS_SLOT_TARGET_QD",
        "SRC_REAL_CQ_BATCH_CH0", "SRC_REAL_CH0_CQ_BATCH",
        "SRC_REAL_NVME_CROSS_SLOT_CQ_BATCH_CH0", "SRC_REAL_CQ_BATCH",
        "SRC_REAL_NVME_CROSS_SLOT_CQ_BATCH",
        "SRC_REAL_WRITER_BUDGET_US_CH0", "SRC_REAL_CH0_WRITER_BUDGET_US",
        "SRC_REAL_NVME_CROSS_SLOT_WRITER_BUDGET_US_CH0",
        "SRC_REAL_WRITER_BUDGET_US", "SRC_REAL_NVME_CROSS_SLOT_WRITER_BUDGET_US",
        "SRC_REAL_BUSY_POLL_US_CH0", "SRC_REAL_CH0_BUSY_POLL_US",
        "SRC_REAL_NVME_CROSS_SLOT_BUSY_POLL_US_CH0",
        "SRC_REAL_BUSY_POLL_US", "SRC_REAL_NVME_CROSS_SLOT_BUSY_POLL_US",
        "SRC_REAL_EMPTY_SLEEP_US_CH0", "SRC_REAL_CH0_EMPTY_SLEEP_US",
        "SRC_REAL_NVME_CROSS_SLOT_EMPTY_SLEEP_US_CH0",
        "SRC_REAL_EMPTY_SLEEP_US", "SRC_REAL_NVME_CROSS_SLOT_EMPTY_SLEEP_US",
        "SRC_REAL_NO_PROGRESS_TIMEOUT_US_CH0", "SRC_REAL_CH0_NO_PROGRESS_TIMEOUT_US",
        "SRC_REAL_NVME_CROSS_SLOT_NO_PROGRESS_TIMEOUT_US_CH0",
        "SRC_REAL_NO_PROGRESS_TIMEOUT_US",
        "SRC_REAL_NVME_CROSS_SLOT_NO_PROGRESS_TIMEOUT_US",
    };
    size_t i;
    int channel;

    unsetenv("CCB_STORAGE_COMPAT_MODE");
    for (i = 0u; i < sizeof(names) / sizeof(names[0]); ++i) unsetenv(names[i]);
    for (channel = 0; channel < 3; ++channel) {
        char name[96];
#define CLEAR_CHANNEL_ENV(format_)                                              \
        do {                                                                    \
            (void)snprintf(name, sizeof(name), (format_), channel);            \
            unsetenv(name);                                                      \
        } while (0)
        CLEAR_CHANNEL_ENV("SRC_REAL_CROSS_SLOT_CH%d");
        CLEAR_CHANNEL_ENV("SRC_REAL_CROSS_SLOT_ENABLED_CH%d");
        CLEAR_CHANNEL_ENV("SRC_REAL_NVME_CROSS_SLOT_QD_CH%d");
        CLEAR_CHANNEL_ENV("SRC_REAL_MAX_ACTIVE_CH%d");
        CLEAR_CHANNEL_ENV("SRC_REAL_CROSS_SLOT_BATCH_CH%d");
        CLEAR_CHANNEL_ENV("SRC_REAL_NVME_CROSS_SLOT_MAX_ACTIVE_CH%d");
        CLEAR_CHANNEL_ENV("SRC_REAL_NVME_CROSS_SLOT_BATCH_CH%d");
        CLEAR_CHANNEL_ENV("SRC_REAL_CH%d_NVME_CROSS_SLOT_MAX_ACTIVE");
        CLEAR_CHANNEL_ENV("SRC_REAL_CH%d_CROSS_SLOT_BATCH");
        CLEAR_CHANNEL_ENV("SRC_REAL_TARGET_QD_CH%d");
        CLEAR_CHANNEL_ENV("SRC_REAL_CH%d_TARGET_QD");
        CLEAR_CHANNEL_ENV("SRC_REAL_NVME_CROSS_SLOT_TARGET_QD_CH%d");
        CLEAR_CHANNEL_ENV("SRC_REAL_CQ_BATCH_CH%d");
        CLEAR_CHANNEL_ENV("SRC_REAL_CH%d_CQ_BATCH");
        CLEAR_CHANNEL_ENV("SRC_REAL_NVME_CROSS_SLOT_CQ_BATCH_CH%d");
        CLEAR_CHANNEL_ENV("SRC_REAL_WRITER_BUDGET_US_CH%d");
        CLEAR_CHANNEL_ENV("SRC_REAL_CH%d_WRITER_BUDGET_US");
        CLEAR_CHANNEL_ENV("SRC_REAL_NVME_CROSS_SLOT_WRITER_BUDGET_US_CH%d");
        CLEAR_CHANNEL_ENV("SRC_REAL_BUSY_POLL_US_CH%d");
        CLEAR_CHANNEL_ENV("SRC_REAL_CH%d_BUSY_POLL_US");
        CLEAR_CHANNEL_ENV("SRC_REAL_NVME_CROSS_SLOT_BUSY_POLL_US_CH%d");
        CLEAR_CHANNEL_ENV("SRC_REAL_EMPTY_SLEEP_US_CH%d");
        CLEAR_CHANNEL_ENV("SRC_REAL_CH%d_EMPTY_SLEEP_US");
        CLEAR_CHANNEL_ENV("SRC_REAL_NVME_CROSS_SLOT_EMPTY_SLEEP_US_CH%d");
        CLEAR_CHANNEL_ENV("SRC_REAL_NO_PROGRESS_TIMEOUT_US_CH%d");
        CLEAR_CHANNEL_ENV("SRC_REAL_CH%d_NO_PROGRESS_TIMEOUT_US");
        CLEAR_CHANNEL_ENV("SRC_REAL_NVME_CROSS_SLOT_NO_PROGRESS_TIMEOUT_US_CH%d");
#undef CLEAR_CHANNEL_ENV
    }
}

static void assert_resolution(int channel, StorageCrossSlotConfigParam param,
                              uint32_t value, StorageCrossSlotSourceKind kind,
                              const char *source_name)
{
    StorageCrossSlotResolution resolution = storage_cross_slot_resolve_config(channel, param);

    assert(resolution.value == value);
    assert(resolution.source_kind == kind);
    assert(strcmp(resolution.source_name, source_name) == 0);
}

static void test_defaults(void)
{
    static const uint32_t expected[3][8] = {
        {1u, 4u, 8u, 8u, 1000u, 50u, 5u, 5000000u},
        {1u, 4u, 8u, 8u, 1000u, 50u, 5u, 5000000u},
        {0u, 1u, 8u, 8u, 300u, 20u, 1u, 5000000u},
    };
    int channel;
    int param;

    clear_cross_slot_env();
    for (channel = 0; channel < 3; ++channel) {
        for (param = STORAGE_CROSS_SLOT_CONFIG_ENABLED;
             param <= STORAGE_CROSS_SLOT_CONFIG_NO_PROGRESS_TIMEOUT_US; ++param) {
            assert_resolution(channel, (StorageCrossSlotConfigParam)param,
                              expected[channel][param], STORAGE_CROSS_SLOT_SOURCE_DEFAULT,
                              param <= STORAGE_CROSS_SLOT_CONFIG_EMPTY_SLEEP_US
                                  ? "profile" : "default");
        }
    }
    assert(storage_cross_slot_enabled_for_channel(0));
    assert(!storage_cross_slot_enabled_for_channel(2));
    assert(storage_cross_slot_active_slots_for_channel(0) == 4u);
    assert(storage_cross_slot_default_target_qd(2) == 8u);
}

static void test_profile_is_authoritative_without_compat(void)
{
    clear_cross_slot_env();
    setenv("SRC_REAL_MAX_ACTIVE_CH0", "6", 1);
    setenv("SRC_REAL_TARGET_QD", "7", 1);
    setenv("SRC_REAL_CQ_BATCH", "9", 1);
    assert_resolution(0, STORAGE_CROSS_SLOT_CONFIG_MAX_ACTIVE, 4u,
                      STORAGE_CROSS_SLOT_SOURCE_DEFAULT, "profile");
    assert_resolution(0, STORAGE_CROSS_SLOT_CONFIG_TARGET_QD, 8u,
                      STORAGE_CROSS_SLOT_SOURCE_DEFAULT, "profile");
    assert_resolution(0, STORAGE_CROSS_SLOT_CONFIG_CQ_BATCH, 8u,
                      STORAGE_CROSS_SLOT_SOURCE_DEFAULT, "profile");
}

static void test_max_active_priority_and_aliases(void)
{
    clear_cross_slot_env();
    setenv("CCB_STORAGE_COMPAT_MODE", "1", 1);
    setenv("SRC_REAL_CROSS_SLOT_BATCH", "9", 1);
    setenv("SRC_REAL_MAX_ACTIVE", "8", 1);
    setenv("SRC_REAL_CROSS_SLOT_BATCH_CH0", "7", 1);
    setenv("SRC_REAL_MAX_ACTIVE_CH0", "6", 1);
    assert_resolution(0, STORAGE_CROSS_SLOT_CONFIG_MAX_ACTIVE, 6u,
                      STORAGE_CROSS_SLOT_SOURCE_CHANNEL_NEW, "SRC_REAL_MAX_ACTIVE_CH0");
    unsetenv("SRC_REAL_MAX_ACTIVE_CH0");
    assert_resolution(0, STORAGE_CROSS_SLOT_CONFIG_MAX_ACTIVE, 7u,
                      STORAGE_CROSS_SLOT_SOURCE_CHANNEL_LEGACY,
                      "SRC_REAL_CROSS_SLOT_BATCH_CH0");
    unsetenv("SRC_REAL_CROSS_SLOT_BATCH_CH0");
    assert_resolution(0, STORAGE_CROSS_SLOT_CONFIG_MAX_ACTIVE, 8u,
                      STORAGE_CROSS_SLOT_SOURCE_GLOBAL_NEW, "SRC_REAL_MAX_ACTIVE");
    unsetenv("SRC_REAL_MAX_ACTIVE");
    assert_resolution(0, STORAGE_CROSS_SLOT_CONFIG_MAX_ACTIVE, 9u,
                      STORAGE_CROSS_SLOT_SOURCE_GLOBAL_LEGACY,
                      "SRC_REAL_CROSS_SLOT_BATCH");

    clear_cross_slot_env();
    setenv("CCB_STORAGE_COMPAT_MODE", "1", 1);
    setenv("SRC_REAL_NVME_CROSS_SLOT_MAX_ACTIVE_CH0", "3", 1);
    assert_resolution(0, STORAGE_CROSS_SLOT_CONFIG_MAX_ACTIVE, 3u,
                      STORAGE_CROSS_SLOT_SOURCE_CHANNEL_LEGACY,
                      "SRC_REAL_NVME_CROSS_SLOT_MAX_ACTIVE_CH0");
}

static void test_generic_priority_and_all_parameters(void)
{
    StorageCrossSlotResolution resolution;
    static const StorageCrossSlotConfigParam params[] = {
        STORAGE_CROSS_SLOT_CONFIG_TARGET_QD,
        STORAGE_CROSS_SLOT_CONFIG_CQ_BATCH,
        STORAGE_CROSS_SLOT_CONFIG_WRITER_BUDGET_US,
        STORAGE_CROSS_SLOT_CONFIG_BUSY_POLL_US,
        STORAGE_CROSS_SLOT_CONFIG_EMPTY_SLEEP_US,
        STORAGE_CROSS_SLOT_CONFIG_NO_PROGRESS_TIMEOUT_US,
    };
    static const char *const legacy_names[] = {
        "SRC_REAL_CH0_TARGET_QD", "SRC_REAL_CH0_CQ_BATCH",
        "SRC_REAL_CH0_WRITER_BUDGET_US", "SRC_REAL_CH0_BUSY_POLL_US",
        "SRC_REAL_CH0_EMPTY_SLEEP_US", "SRC_REAL_CH0_NO_PROGRESS_TIMEOUT_US",
    };
    size_t i;

    clear_cross_slot_env();
    setenv("CCB_STORAGE_COMPAT_MODE", "1", 1);
    setenv("SRC_REAL_NVME_CROSS_SLOT_TARGET_QD", "9", 1);
    setenv("SRC_REAL_TARGET_QD", "8", 1);
    setenv("SRC_REAL_CH0_TARGET_QD", "7", 1);
    setenv("SRC_REAL_TARGET_QD_CH0", "6", 1);
    assert_resolution(0, STORAGE_CROSS_SLOT_CONFIG_TARGET_QD, 6u,
                      STORAGE_CROSS_SLOT_SOURCE_CHANNEL_NEW, "SRC_REAL_TARGET_QD_CH0");
    unsetenv("SRC_REAL_TARGET_QD_CH0");
    assert_resolution(0, STORAGE_CROSS_SLOT_CONFIG_TARGET_QD, 7u,
                      STORAGE_CROSS_SLOT_SOURCE_CHANNEL_LEGACY, "SRC_REAL_CH0_TARGET_QD");
    unsetenv("SRC_REAL_CH0_TARGET_QD");
    assert_resolution(0, STORAGE_CROSS_SLOT_CONFIG_TARGET_QD, 8u,
                      STORAGE_CROSS_SLOT_SOURCE_GLOBAL_NEW, "SRC_REAL_TARGET_QD");
    unsetenv("SRC_REAL_TARGET_QD");
    assert_resolution(0, STORAGE_CROSS_SLOT_CONFIG_TARGET_QD, 9u,
                      STORAGE_CROSS_SLOT_SOURCE_GLOBAL_LEGACY,
                      "SRC_REAL_NVME_CROSS_SLOT_TARGET_QD");

    clear_cross_slot_env();
    setenv("CCB_STORAGE_COMPAT_MODE", "1", 1);
    for (i = 0u; i < sizeof(params) / sizeof(params[0]); ++i) {
        char value[32];
        (void)snprintf(value, sizeof(value), "%u", (unsigned)(i + 2u));
        setenv(legacy_names[i], value, 1);
        resolution = storage_cross_slot_resolve_config(0, params[i]);
        assert(resolution.value == i + 2u);
        assert(resolution.source_kind == STORAGE_CROSS_SLOT_SOURCE_CHANNEL_LEGACY);
        assert(strcmp(resolution.source_name, legacy_names[i]) == 0);
        unsetenv(legacy_names[i]);
    }
}

static void test_enabled_and_invalid_fallback(void)
{
    StorageCrossSlotResolution resolution;

    clear_cross_slot_env();
    setenv("CCB_STORAGE_COMPAT_MODE", "1", 1);
    setenv("SRC_REAL_CROSS_SLOT_ENABLED", "0", 1);
    setenv("SRC_REAL_CROSS_SLOT", "1", 1);
    setenv("SRC_REAL_CROSS_SLOT_ENABLED_CH0", "0", 1);
    setenv("SRC_REAL_CROSS_SLOT_CH0", "1", 1);
    assert_resolution(0, STORAGE_CROSS_SLOT_CONFIG_ENABLED, 1u,
                      STORAGE_CROSS_SLOT_SOURCE_CHANNEL_NEW, "SRC_REAL_CROSS_SLOT_CH0");
    unsetenv("SRC_REAL_CROSS_SLOT_CH0");
    assert_resolution(0, STORAGE_CROSS_SLOT_CONFIG_ENABLED, 0u,
                      STORAGE_CROSS_SLOT_SOURCE_CHANNEL_LEGACY,
                      "SRC_REAL_CROSS_SLOT_ENABLED_CH0");
    unsetenv("SRC_REAL_CROSS_SLOT_ENABLED_CH0");
    assert_resolution(0, STORAGE_CROSS_SLOT_CONFIG_ENABLED, 1u,
                      STORAGE_CROSS_SLOT_SOURCE_GLOBAL_NEW, "SRC_REAL_CROSS_SLOT");
    unsetenv("SRC_REAL_CROSS_SLOT");
    assert_resolution(0, STORAGE_CROSS_SLOT_CONFIG_ENABLED, 0u,
                      STORAGE_CROSS_SLOT_SOURCE_GLOBAL_LEGACY,
                      "SRC_REAL_CROSS_SLOT_ENABLED");

    clear_cross_slot_env();
    setenv("CCB_STORAGE_COMPAT_MODE", "1", 1);
    setenv("SRC_REAL_CQ_BATCH", "55", 1);
    setenv("SRC_REAL_CQ_BATCH_CH0", "invalid", 1);
    resolution = storage_cross_slot_resolve_config(0, STORAGE_CROSS_SLOT_CONFIG_CQ_BATCH);
    assert(resolution.value == 8u);
    assert(resolution.source_kind == STORAGE_CROSS_SLOT_SOURCE_DEFAULT);
    assert(strcmp(resolution.source_name, "profile") == 0);
    assert(strcmp(resolution.invalid_source_name, "SRC_REAL_CQ_BATCH_CH0") == 0);
    assert(strcmp(resolution.fallback_source_name, "profile") == 0);
}

static void test_stop_error_classification(void)
{
    assert(storage_error_class(STORAGE_ERR_TAIL_UNALIGNED) == STORAGE_ERROR_DEFERRED);
    assert(storage_error_class(STORAGE_ERR_STOP_BOUNDARY_TIMEOUT) == STORAGE_ERROR_DEFERRED);
    assert(storage_error_class(STORAGE_ERR_STOP_HARVEST_TIMEOUT) == STORAGE_ERROR_DEFERRED);
    assert(storage_error_class(STORAGE_ERR_UNKNOWN_CID) == STORAGE_ERROR_FATAL);
    assert(storage_error_class(STORAGE_ERR_DUPLICATE_CID) == STORAGE_ERROR_FATAL);
}

int main(void)
{
    test_defaults();
    test_profile_is_authoritative_without_compat();
    test_max_active_priority_and_aliases();
    test_generic_priority_and_all_parameters();
    test_enabled_and_invalid_fallback();
    test_stop_error_classification();
    clear_cross_slot_env();
    puts("mock_storage_config_test: ok");
    return 0;
}
