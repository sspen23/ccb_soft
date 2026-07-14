#ifndef STORAGE_WRITER_H
#define STORAGE_WRITER_H

#include <stdbool.h>
#include <stdint.h>

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

typedef enum {
    STORAGE_CROSS_SLOT_WRITER_CONTINUE = 0,
    STORAGE_CROSS_SLOT_WRITER_WAIT_FOR_QUEUE,
    STORAGE_CROSS_SLOT_WRITER_DRAINED,
    STORAGE_CROSS_SLOT_WRITER_QUEUE_ERROR
} StorageCrossSlotWriterDecision;

bool storage_drain_invariant_ok(const StorageDrainInvariant *invariant);
StorageCrossSlotWriterDecision storage_cross_slot_writer_decide(
    bool producer_done, uint32_t queue_count, bool queue_error,
    uint32_t engine_active, uint32_t engine_inflight);

#endif
