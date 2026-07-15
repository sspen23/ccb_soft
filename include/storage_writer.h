#ifndef STORAGE_WRITER_H
#define STORAGE_WRITER_H

#include <stdbool.h>
#include <stdint.h>

typedef struct {
    bool input_complete;
    bool dma_quiesced;
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
    uint64_t ring_occupied_bytes;
} StorageDrainInvariant;

typedef struct {
    uint32_t consecutive_scans;
    uint64_t empty_since_us;
} StorageDrainStableState;

typedef enum {
    STORAGE_CROSS_SLOT_WRITER_CONTINUE = 0,
    STORAGE_CROSS_SLOT_WRITER_WAIT_FOR_QUEUE,
    STORAGE_CROSS_SLOT_WRITER_DRAINED,
    STORAGE_CROSS_SLOT_WRITER_QUEUE_ERROR
} StorageCrossSlotWriterDecision;

bool storage_drain_invariant_ok(const StorageDrainInvariant *invariant);
bool storage_rate_mib_s(uint64_t bytes, uint64_t elapsed_us, double *rate);
void storage_drain_stable_init(StorageDrainStableState *state);
bool storage_drain_stable_observe(StorageDrainStableState *state,
                                  const StorageDrainInvariant *invariant,
                                  uint64_t now_us,
                                  uint32_t required_scans,
                                  uint64_t stable_us);
StorageCrossSlotWriterDecision storage_cross_slot_writer_decide(
    bool producer_done, uint32_t queue_count, bool queue_error,
    uint32_t engine_active, uint32_t engine_inflight);

#endif
