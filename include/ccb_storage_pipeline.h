#ifndef CCB_STORAGE_PIPELINE_H
#define CCB_STORAGE_PIPELINE_H

#include "ccb_types.h"
#include <stdbool.h>
#include <pthread.h>

typedef enum {
    STORAGE_CAPTURE_ACCEPTING = 0,
    STORAGE_CAPTURE_STOP_LATCHED,
    STORAGE_CAPTURE_DMA_QUIESCING,
    STORAGE_CAPTURE_HARVESTING_COMPLETED,
    STORAGE_CAPTURE_WRITER_DRAINING,
    STORAGE_CAPTURE_FINALIZING
} StorageCaptureState;

typedef struct {
    StorageCaptureState state;
    uint64_t deadline_us;
} StorageStopState;

typedef struct {
    bool writer_enabled;
    bool writer_run_ready;
    bool writer_schedule_failed;
    bool producer_run_ready;
    bool producer_schedule_failed;
    bool running_sent;
} StorageRunState;

/* The producer must decide a first-DMA deadline only after its normal
 * harvest attempt.  Software slot counts can lag a completed hardware BD,
 * so they are diagnostic data, not deadline evidence. */
typedef enum {
    STORAGE_FIRST_DMA_DEADLINE_WAIT = 0,
    STORAGE_FIRST_DMA_DEADLINE_DATA,
    STORAGE_FIRST_DMA_DEADLINE_HARVEST_FAILED,
    STORAGE_FIRST_DMA_DEADLINE_EXPIRED
} StorageFirstDmaDeadlineOutcome;

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
uint32_t storage_harvest_limit_for_remaining(uint64_t remaining_bytes,
                                             uint32_t dma_desc_bytes,
                                             uint32_t max_batch);
void storage_stop_state_init(StorageStopState *state);
bool storage_stop_state_latch(StorageStopState *state, uint64_t deadline_us);
int storage_stop_state_advance(StorageStopState *state, StorageCaptureState next);
bool storage_stop_state_expired(const StorageStopState *state, uint64_t now_us);
void storage_run_state_init(StorageRunState *state);
int storage_run_state_enable_writer(StorageRunState *state);
int storage_run_state_set_writer_ready(StorageRunState *state, bool success);
int storage_run_state_set_producer_ready(StorageRunState *state, bool success);
bool storage_run_state_can_emit_running(const StorageRunState *state);
int storage_run_state_mark_running(StorageRunState *state);
StorageFirstDmaDeadlineOutcome storage_first_dma_deadline_outcome(
    bool deadline_due, bool saw_dma_data, bool stop_requested,
    int harvest_rc, uint32_t harvest_count);

#endif
