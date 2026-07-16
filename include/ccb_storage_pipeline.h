#ifndef CCB_STORAGE_PIPELINE_H
#define CCB_STORAGE_PIPELINE_H

#include "ccb_types.h"
#include "storage_queue.h"
#include "storage_stop.h"
#include "storage_writer.h"
#include <stdbool.h>
#include <pthread.h>

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
    StorageSlotTable slots;
    uint32_t head;
    uint32_t tail;
    uint32_t count;
    int error;
    pthread_mutex_t lock;
    pthread_cond_t not_empty;
} StoragePipeline;

int storage_pipeline_init(StoragePipeline *p, uint32_t slots);
void storage_pipeline_destroy(StoragePipeline *p);
int storage_pipeline_mark_completed(StoragePipeline *p, uint32_t slot);
int storage_queue_push_batch(StoragePipeline *p, const StoragePipelineItem *items,
                             uint32_t item_count);
int storage_pipeline_pop(StoragePipeline *p, StoragePipelineItem *out, int wait);
int storage_pipeline_complete(StoragePipeline *p, uint32_t slot, int requeue);
int storage_pipeline_counts_valid(const StoragePipeline *p);
uint32_t storage_harvest_limit_for_remaining(uint64_t remaining_bytes,
                                             uint32_t dma_desc_bytes,
                                             uint32_t max_batch);
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
bool storage_ring_pressure_requires_degraded_drain(uint32_t pressure_level,
                                                   bool drain_requested);
bool storage_receive_failure_allows_drain(bool safe_discard,
                                          bool pressure_drain,
                                          bool dma_error,
                                          bool descriptor_error);
StorageFirstDmaDeadlineOutcome storage_first_dma_deadline_outcome(
    bool deadline_due, bool saw_dma_data, bool stop_requested,
    int harvest_rc, uint32_t harvest_count);
uint32_t storage_dma_harvest_batch_limit(uint32_t base_limit,
                                         uint32_t total_slots,
                                         uint32_t completed_unharvested,
                                         uint32_t available_limit);
bool storage_dma_emergency_harvest(uint32_t dma_writable,
                                   uint32_t completed_unharvested,
                                   uint32_t total_slots);
bool storage_dma_producer_may_idle(uint32_t harvested,
                                   uint32_t completed_unharvested);

#endif
