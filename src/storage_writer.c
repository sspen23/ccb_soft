#include "storage_writer.h"

#include <limits.h>
#include <string.h>

bool storage_drain_invariant_ok(const StorageDrainInvariant *invariant)
{
    if (!invariant || invariant->completion_count > invariant->submit_count)
        return false;
    if (invariant->tail_unqueued_bytes > UINT64_MAX -
            invariant->queued_payload_bytes)
        return false;
    return invariant->input_complete && invariant->dma_quiesced &&
           invariant->dma_harvested_payload_bytes ==
               invariant->queued_payload_bytes + invariant->tail_unqueued_bytes &&
           invariant->queued_payload_bytes ==
               invariant->nvme_completed_payload_bytes &&
           invariant->nvme_completed_payload_bytes == invariant->file_bytes &&
           invariant->completed_unharvested == 0u &&
           invariant->ready_count == 0u && invariant->active_count == 0u &&
           invariant->global_inflight == 0u &&
           invariant->submit_count == invariant->completion_count &&
           invariant->ring_occupied_bytes == 0u;
}

bool storage_rate_mib_s(uint64_t bytes, uint64_t elapsed_us, double *rate)
{
    if (rate) *rate = 0.0;
    if (!rate || elapsed_us < 100u) return false;
    *rate = (double)bytes * 1000000.0 / (double)elapsed_us / 1048576.0;
    return true;
}

void storage_drain_stable_init(StorageDrainStableState *state)
{
    if (state) memset(state, 0, sizeof(*state));
}

bool storage_drain_stable_observe(StorageDrainStableState *state,
                                  const StorageDrainInvariant *invariant,
                                  uint64_t now_us,
                                  uint32_t required_scans,
                                  uint64_t stable_us)
{
    if (!state || !storage_drain_invariant_ok(invariant)) {
        storage_drain_stable_init(state);
        return false;
    }
    if (required_scans == 0u) required_scans = 1u;
    if (state->consecutive_scans == 0u) state->empty_since_us = now_us;
    if (state->consecutive_scans != UINT32_MAX) ++state->consecutive_scans;
    return state->consecutive_scans >= required_scans &&
           now_us >= state->empty_since_us &&
           now_us - state->empty_since_us >= stable_us;
}

StorageCrossSlotWriterDecision storage_cross_slot_writer_decide(
    bool producer_done, uint32_t queue_count, bool queue_error,
    uint32_t engine_active, uint32_t engine_inflight)
{
    if (queue_error) return STORAGE_CROSS_SLOT_WRITER_QUEUE_ERROR;
    if (queue_count != 0u || engine_active != 0u || engine_inflight != 0u)
        return STORAGE_CROSS_SLOT_WRITER_CONTINUE;
    if (producer_done) return STORAGE_CROSS_SLOT_WRITER_DRAINED;
    return STORAGE_CROSS_SLOT_WRITER_WAIT_FOR_QUEUE;
}

bool storage_metrics_publish_due(uint64_t now_us, uint64_t next_publish_us,
                                 bool force)
{
    return force || next_publish_us == 0u || now_us >= next_publish_us;
}

uint64_t storage_metrics_next_publish_us(uint64_t now_us,
                                         uint64_t interval_us)
{
    if (UINT64_MAX - now_us < interval_us) return UINT64_MAX;
    return now_us + interval_us;
}
