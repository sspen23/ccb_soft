#ifndef CCB_STORAGE_PIPELINE_H
#define CCB_STORAGE_PIPELINE_H

#include "ccb_types.h"
#include <stdbool.h>
#include <pthread.h>

typedef enum {
    STORAGE_STOP_NONE = 0,
    STORAGE_STOP_REQUESTED,
    STORAGE_STOP_WAIT_BOUNDARY,
    STORAGE_STOP_DMA_QUIESCING,
    STORAGE_STOP_HARVESTING,
    STORAGE_STOP_PRODUCER_DONE,
    STORAGE_STOP_WRITER_DRAINING,
    STORAGE_STOP_FINALIZING,
    STORAGE_STOP_FINISHED,
    STORAGE_STOP_FAILED
} StorageStopPhase;

typedef struct {
    StorageStopPhase state;
    uint64_t deadline_us;
} StorageStopState;

typedef struct {
    uint32_t consecutive_empty_scans;
    uint64_t empty_since_us;
} StorageStopHarvestState;

typedef enum {
    STORAGE_STOP_TAIL_QUEUE = 0,
    STORAGE_STOP_TAIL_DEFER_UNALIGNED,
    STORAGE_STOP_TAIL_DEFER_LATE
} StorageStopTailDisposition;

typedef struct {
    uint64_t dma_harvested_payload_bytes;
    uint64_t queued_payload_bytes;
    uint64_t nvme_completed_payload_bytes;
    uint64_t file_bytes;
    uint64_t tail_unqueued_bytes;
    uint32_t completed_unharvested;
    uint32_t ready_count;
    uint32_t active_count;
    uint32_t global_inflight;
    uint64_t submit_count;
    uint64_t completion_count;
} StorageDrainInvariant;

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
int storage_stop_state_advance(StorageStopState *state, StorageStopPhase next);
void storage_stop_state_fail(StorageStopState *state);
bool storage_stop_state_expired(const StorageStopState *state, uint64_t now_us);
bool storage_stop_boundary_should_quiesce(const StorageStopState *state,
                                          bool rx_packet_open,
                                          uint64_t now_us);
void storage_stop_harvest_state_init(StorageStopHarvestState *state);
bool storage_stop_harvest_observe(StorageStopHarvestState *state,
                                  bool dma_quiesced,
                                  uint32_t harvested_count,
                                  uint32_t completed_unharvested,
                                  bool rx_packet_open,
                                  uint64_t now_us,
                                  uint32_t required_empty_scans,
                                  uint64_t stable_window_us);
StorageStopTailDisposition storage_stop_tail_disposition(bool stop_active,
                                                         bool tail_already_seen,
                                                         uint64_t payload_bytes,
                                                         uint64_t media_bytes,
                                                         bool padding_coherent);
bool storage_drain_invariant_ok(const StorageDrainInvariant *invariant);
uint32_t storage_ring_pressure_level(uint32_t occupied_slots,
                                     uint32_t total_slots,
                                     uint32_t warning_percent,
                                     uint32_t critical_percent);
uint32_t storage_writer_budget_for_pressure(uint32_t base_budget_us,
                                            uint32_t pressure_level);
bool storage_ring_pressure_should_stop(uint32_t pressure_level,
                                       uint64_t critical_since_us,
                                       uint64_t now_us,
                                       uint64_t critical_duration_us,
                                       bool stop_enabled);
StorageFirstDmaDeadlineOutcome storage_first_dma_deadline_outcome(
    bool deadline_due, bool saw_dma_data, bool stop_requested,
    int harvest_rc, uint32_t harvest_count);

#endif
