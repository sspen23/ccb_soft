#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include "ccb_commands.h"

int main(void)
{
    unsetenv("SRC_REAL_NVME_CROSS_SLOT_MAX_ACTIVE_CH0");
    unsetenv("SRC_REAL_MAX_ACTIVE_CH0");
    unsetenv("SRC_REAL_CROSS_SLOT_BATCH_CH0");
    unsetenv("SRC_REAL_CH0_NVME_CROSS_SLOT_MAX_ACTIVE");
    unsetenv("SRC_REAL_CH0_CROSS_SLOT_BATCH");
    unsetenv("SRC_REAL_NVME_CROSS_SLOT_BATCH_CH0");
    unsetenv("SRC_REAL_MAX_ACTIVE");
    unsetenv("SRC_REAL_CROSS_SLOT_MAX_ACTIVE");
    unsetenv("SRC_REAL_CROSS_SLOT_BATCH");
    unsetenv("SRC_REAL_NVME_CROSS_SLOT_MAX_ACTIVE");
    unsetenv("SRC_REAL_NVME_CROSS_SLOT_BATCH");
    unsetenv("SRC_REAL_CROSS_SLOT_CH0");
    unsetenv("SRC_REAL_CROSS_SLOT_ENABLED_CH0");
    unsetenv("SRC_REAL_CROSS_SLOT");
    unsetenv("SRC_REAL_CROSS_SLOT_ENABLED");
    unsetenv("SRC_REAL_NVME_CROSS_SLOT_QD_CH0");
    unsetenv("SRC_REAL_NVME_CROSS_SLOT_QD");
    assert(storage_cross_slot_active_slots_for_channel(0) == 4u);
    assert(storage_cross_slot_enabled_for_channel(0));
    assert(!storage_cross_slot_enabled_for_channel(2));
    assert(storage_cross_slot_default_target_qd(0) == 8u);
    assert(storage_cross_slot_default_target_qd(1) == 8u);
    assert(storage_cross_slot_default_target_qd(2) == 4u);

    setenv("SRC_REAL_NVME_CROSS_SLOT_BATCH", "7", 1);
    setenv("SRC_REAL_NVME_CROSS_SLOT_MAX_ACTIVE", "6", 1);
    setenv("SRC_REAL_NVME_CROSS_SLOT_BATCH_CH0", "5", 1);
    setenv("SRC_REAL_NVME_CROSS_SLOT_MAX_ACTIVE_CH0", "3", 1);
    assert(storage_cross_slot_active_slots_for_channel(0) == 3u);
    unsetenv("SRC_REAL_NVME_CROSS_SLOT_MAX_ACTIVE_CH0");
    assert(storage_cross_slot_active_slots_for_channel(0) == 5u);
    unsetenv("SRC_REAL_NVME_CROSS_SLOT_BATCH_CH0");
    assert(storage_cross_slot_active_slots_for_channel(0) == 6u);
    unsetenv("SRC_REAL_NVME_CROSS_SLOT_MAX_ACTIVE");
    assert(storage_cross_slot_active_slots_for_channel(0) == 7u);
    unsetenv("SRC_REAL_NVME_CROSS_SLOT_BATCH");

    setenv("SRC_REAL_CROSS_SLOT_BATCH", "9", 1);
    setenv("SRC_REAL_MAX_ACTIVE", "8", 1);
    setenv("SRC_REAL_CROSS_SLOT_BATCH_CH0", "7", 1);
    setenv("SRC_REAL_MAX_ACTIVE_CH0", "6", 1);
    assert(storage_cross_slot_active_slots_for_channel(0) == 6u);
    unsetenv("SRC_REAL_MAX_ACTIVE_CH0");
    assert(storage_cross_slot_active_slots_for_channel(0) == 7u);
    unsetenv("SRC_REAL_CROSS_SLOT_BATCH_CH0");
    assert(storage_cross_slot_active_slots_for_channel(0) == 8u);
    unsetenv("SRC_REAL_MAX_ACTIVE");
    assert(storage_cross_slot_active_slots_for_channel(0) == 9u);

    /* The documented channel-level legacy name still outranks an older
     * NVME-prefixed alias when both are present. */
    setenv("SRC_REAL_CROSS_SLOT_BATCH_CH0", "11", 1);
    setenv("SRC_REAL_NVME_CROSS_SLOT_MAX_ACTIVE_CH0", "2", 1);
    assert(storage_cross_slot_active_slots_for_channel(0) == 11u);
    unsetenv("SRC_REAL_CROSS_SLOT_BATCH_CH0");
    unsetenv("SRC_REAL_NVME_CROSS_SLOT_MAX_ACTIVE_CH0");

    setenv("SRC_REAL_CROSS_SLOT", "0", 1);
    assert(!storage_cross_slot_enabled_for_channel(0));
    setenv("SRC_REAL_CROSS_SLOT_CH0", "1", 1);
    assert(storage_cross_slot_enabled_for_channel(0));
    setenv("SRC_REAL_CROSS_SLOT_CH0", "0", 1);
    assert(!storage_cross_slot_enabled_for_channel(0));
    puts("mock_storage_config_test: ok");
    return 0;
}
