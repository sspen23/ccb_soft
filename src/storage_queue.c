#include "storage_queue.h"

#include <stdlib.h>
#include <string.h>

static uint32_t *count_for_state(StorageSlotCounts *counts,
                                 StorageSlotState state)
{
    switch (state) {
    case STORAGE_SLOT_DMA_WRITABLE: return &counts->dma_writable;
    case STORAGE_SLOT_DMA_COMPLETED_UNHARVESTED:
        return &counts->completed_unharvested;
    case STORAGE_SLOT_READY_FOR_NVME: return &counts->ready;
    case STORAGE_SLOT_NVME_BUSY: return &counts->nvme_busy;
    case STORAGE_SLOT_REQUEUE_PENDING: return &counts->requeue_pending;
    case STORAGE_SLOT_FREE: return &counts->free_count;
    default: return NULL;
    }
}

bool storage_slot_table_valid(const StorageSlotTable *table)
{
    const StorageSlotCounts *counts;
    uint64_t sum;

    if (!table || !table->states || table->capacity == 0u) return false;
    counts = &table->counts;
    sum = (uint64_t)counts->dma_writable +
          counts->completed_unharvested + counts->ready +
          counts->nvme_busy + counts->requeue_pending + counts->free_count;
    return counts->total == table->capacity && sum == counts->total;
}

int storage_slot_table_init(StorageSlotTable *table, uint32_t slots)
{
    if (!table || slots == 0u) return -1;
    memset(table, 0, sizeof(*table));
    table->states = calloc(slots, sizeof(table->states[0]));
    if (!table->states) return -1;
    table->capacity = slots;
    table->counts.total = slots;
    table->counts.dma_writable = slots;
    return 0;
}

void storage_slot_table_destroy(StorageSlotTable *table)
{
    if (!table) return;
    free(table->states);
    memset(table, 0, sizeof(*table));
}

int storage_slot_transition(StorageSlotTable *table, uint32_t slot,
                            StorageSlotState expected, StorageSlotState next)
{
    uint32_t *from;
    uint32_t *to;

    if (!storage_slot_table_valid(table) || slot >= table->capacity ||
        table->states[slot] != (uint8_t)expected || expected == next)
        return -1;
    from = count_for_state(&table->counts, expected);
    to = count_for_state(&table->counts, next);
    if (!from || !to || *from == 0u) return -1;
    --*from;
    ++*to;
    table->states[slot] = (uint8_t)next;
    if (!storage_slot_table_valid(table)) {
        table->states[slot] = (uint8_t)expected;
        ++*from;
        --*to;
        return -1;
    }
    return 0;
}

StorageSlotState storage_slot_state(const StorageSlotTable *table,
                                    uint32_t slot)
{
    if (!table || !table->states || slot >= table->capacity)
        return STORAGE_SLOT_INVALID;
    return (StorageSlotState)table->states[slot];
}

uint32_t storage_slot_busy_count(const StorageSlotTable *table)
{
    if (!storage_slot_table_valid(table)) return 0u;
    return table->counts.completed_unharvested + table->counts.ready +
           table->counts.nvme_busy + table->counts.requeue_pending;
}

int storage_requeue_gate_init(StorageRequeueGate *gate)
{
    if (!gate) return -1;
    memset(gate, 0, sizeof(*gate));
    if (pthread_mutex_init(&gate->lock, NULL) != 0) return -1;
    gate->enabled = true;
    return 0;
}

void storage_requeue_gate_destroy(StorageRequeueGate *gate)
{
    if (!gate) return;
    (void)pthread_mutex_destroy(&gate->lock);
    memset(gate, 0, sizeof(*gate));
}

StorageRequeueResult storage_requeue_gate_run(StorageRequeueGate *gate,
                                               StorageRequeueAction action,
                                               void *opaque)
{
    int rc;

    if (!gate || !action) return STORAGE_REQUEUE_FAILED;
    pthread_mutex_lock(&gate->lock);
    if (!gate->enabled) {
        pthread_mutex_unlock(&gate->lock);
        return STORAGE_REQUEUE_STOPPED;
    }
    rc = action(opaque);
    pthread_mutex_unlock(&gate->lock);
    return rc == 0 ? STORAGE_REQUEUE_EXECUTED : STORAGE_REQUEUE_FAILED;
}

bool storage_requeue_gate_latch_stop(StorageRequeueGate *gate,
                                     uint64_t stop_epoch,
                                     StorageRequeueStopAction action,
                                     void *opaque)
{
    bool first;

    if (!gate) return false;
    pthread_mutex_lock(&gate->lock);
    first = gate->enabled;
    gate->enabled = false;
    if (gate->stop_epoch == 0u)
        gate->stop_epoch = stop_epoch != 0u ? stop_epoch : 1u;
    if (first && action) action(opaque);
    pthread_mutex_unlock(&gate->lock);
    return first;
}
