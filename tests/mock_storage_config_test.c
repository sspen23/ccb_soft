#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include "ccb_commands.h"

int main(void)
{
    unsetenv("SRC_REAL_NVME_CROSS_SLOT_MAX_ACTIVE_CH0");
    unsetenv("SRC_REAL_CH0_NVME_CROSS_SLOT_MAX_ACTIVE");
    unsetenv("SRC_REAL_NVME_CROSS_SLOT_BATCH_CH0");
    unsetenv("SRC_REAL_NVME_CROSS_SLOT_MAX_ACTIVE");
    unsetenv("SRC_REAL_NVME_CROSS_SLOT_BATCH");
    assert(storage_cross_slot_active_slots_for_channel(0) == 4u);
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
    puts("mock_storage_config_test: ok");
    return 0;
}
