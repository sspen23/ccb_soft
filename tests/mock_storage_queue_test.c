#include <assert.h>
#include <stdio.h>

#include "storage_queue.h"

int main(void)
{
    StorageSlotTable table;

    assert(storage_slot_table_init(&table, 2u) == 0);
    assert(storage_slot_table_valid(&table));
    assert(storage_slot_busy_count(&table) == 0u);
    assert(storage_slot_transition(&table, 0u, STORAGE_SLOT_DMA_WRITABLE,
                                   STORAGE_SLOT_DMA_COMPLETED_UNHARVESTED) == 0);
    assert(storage_slot_busy_count(&table) == 1u);
    assert(storage_slot_transition(&table, 0u, STORAGE_SLOT_DMA_WRITABLE,
                                   STORAGE_SLOT_READY_FOR_NVME) != 0);
    assert(storage_slot_state(&table, 0u) ==
           STORAGE_SLOT_DMA_COMPLETED_UNHARVESTED);
    assert(table.counts.completed_unharvested == 1u);
    assert(storage_slot_transition(&table, 0u,
                                   STORAGE_SLOT_DMA_COMPLETED_UNHARVESTED,
                                   STORAGE_SLOT_READY_FOR_NVME) == 0);
    assert(storage_slot_transition(&table, 0u, STORAGE_SLOT_READY_FOR_NVME,
                                   STORAGE_SLOT_NVME_BUSY) == 0);
    assert(storage_slot_transition(&table, 0u, STORAGE_SLOT_NVME_BUSY,
                                   STORAGE_SLOT_REQUEUE_PENDING) == 0);
    assert(storage_slot_transition(&table, 0u, STORAGE_SLOT_REQUEUE_PENDING,
                                   STORAGE_SLOT_DMA_WRITABLE) == 0);
    assert(storage_slot_busy_count(&table) == 0u);
    assert(storage_slot_state(&table, 2u) == STORAGE_SLOT_INVALID);
    assert(storage_slot_table_valid(&table));
    storage_slot_table_destroy(&table);
    puts("mock_storage_queue_test: ok");
    return 0;
}
