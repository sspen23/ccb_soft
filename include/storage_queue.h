#ifndef STORAGE_QUEUE_H
#define STORAGE_QUEUE_H

#include "ccb_types.h"

#include <stdbool.h>
#include <pthread.h>
#include <stdint.h>

typedef int (*StorageRequeueAction)(void *opaque);
typedef void (*StorageRequeueStopAction)(void *opaque);

typedef enum {
    STORAGE_REQUEUE_EXECUTED = 0,
    STORAGE_REQUEUE_STOPPED = 1,
    STORAGE_REQUEUE_FAILED = -1
} StorageRequeueResult;

typedef struct {
    pthread_mutex_t lock;
    bool enabled;
    uint64_t stop_epoch;
} StorageRequeueGate;

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
int storage_requeue_gate_init(StorageRequeueGate *gate);
void storage_requeue_gate_destroy(StorageRequeueGate *gate);
StorageRequeueResult storage_requeue_gate_run(StorageRequeueGate *gate,
                                               StorageRequeueAction action,
                                               void *opaque);
bool storage_requeue_gate_latch_stop(StorageRequeueGate *gate,
                                     uint64_t stop_epoch,
                                     StorageRequeueStopAction action,
                                     void *opaque);

#endif
