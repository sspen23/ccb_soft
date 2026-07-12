#ifndef CCB_STORAGE_PIPELINE_H
#define CCB_STORAGE_PIPELINE_H

#include "ccb_types.h"
#include <pthread.h>

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
    uint32_t slot;
    uint64_t bytes;
    uint64_t chunk_index;
    uint64_t file_offset;
    uint64_t start_lba;
    uint64_t sectors;
    uint64_t hw_addr;
} StoragePipelineItem;

typedef struct {
    StoragePipelineItem *items;
    StorageSlotState *states;
    StorageSlotCounts counts;
    uint32_t head;
    uint32_t tail;
    uint32_t count;
    uint32_t capacity;
    int error;
    pthread_mutex_t lock;
    pthread_cond_t not_empty;
} StoragePipeline;

int storage_pipeline_init(StoragePipeline *p, uint32_t slots);
void storage_pipeline_destroy(StoragePipeline *p);
int storage_slot_transition_locked(StoragePipeline *p, uint32_t slot,
                                   StorageSlotState expected, StorageSlotState next);
int storage_pipeline_mark_completed(StoragePipeline *p, uint32_t slot);
int storage_queue_push_batch(StoragePipeline *p, const StoragePipelineItem *items,
                             uint32_t item_count);
int storage_pipeline_pop(StoragePipeline *p, StoragePipelineItem *out, int wait);
int storage_pipeline_complete(StoragePipeline *p, uint32_t slot, int requeue);
int storage_pipeline_counts_valid(const StoragePipeline *p);

#endif
