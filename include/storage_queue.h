#ifndef STORAGE_QUEUE_H
#define STORAGE_QUEUE_H

#include "ccb_types.h"

#include <stdbool.h>
#include <stdint.h>

typedef struct {
    uint32_t total;
    uint32_t dma_writable;
    uint32_t completed_unharvested;
    uint32_t ready;
    uint32_t nvme_busy;
    uint32_t requeue_pending;
    uint32_t free_count;
} StorageSlotCounts;

typedef struct {
    uint8_t *states;
    StorageSlotCounts counts;
    uint32_t capacity;
} StorageSlotTable;

int storage_slot_table_init(StorageSlotTable *table, uint32_t slots);
void storage_slot_table_destroy(StorageSlotTable *table);
int storage_slot_transition(StorageSlotTable *table, uint32_t slot,
                            StorageSlotState expected, StorageSlotState next);
StorageSlotState storage_slot_state(const StorageSlotTable *table,
                                    uint32_t slot);
bool storage_slot_table_valid(const StorageSlotTable *table);
uint32_t storage_slot_busy_count(const StorageSlotTable *table);

#endif
