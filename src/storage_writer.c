#include "storage_writer.h"

bool storage_drain_invariant_ok(const StorageDrainInvariant *invariant)
{
    if (!invariant || invariant->completion_count > invariant->submit_count)
        return false;
    return invariant->dma_harvested_payload_bytes ==
               invariant->queued_payload_bytes &&
           invariant->queued_payload_bytes ==
               invariant->nvme_completed_payload_bytes &&
           invariant->nvme_completed_payload_bytes == invariant->file_bytes &&
           invariant->tail_unqueued_bytes == 0u &&
           invariant->completed_unharvested == 0u &&
           invariant->ready_count == 0u && invariant->active_count == 0u &&
           invariant->global_inflight == 0u &&
           invariant->submit_count - invariant->completion_count ==
               invariant->global_inflight;
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
