#include "storage_stop.h"

#include <limits.h>
#include <string.h>

void storage_stop_state_init(StorageStopState *state)
{
    if (!state) return;
    memset(state, 0, sizeof(*state));
    state->state = STORAGE_STOP_NONE;
}

int storage_dma_quiesce_once(StorageDmaQuiesceState *state,
                             uint64_t drain_epoch,
                             StorageDmaQuiesceFn quiesce,
                             void *opaque)
{
    if (!state || drain_epoch == 0u || !quiesce) return -1;
    if (state->started) {
        if (state->drain_epoch != drain_epoch || !state->finished) return -1;
        return state->result;
    }
    state->drain_epoch = drain_epoch;
    state->started = true;
    state->result = quiesce(opaque);
    state->finished = true;
    return state->result;
}

bool storage_stop_state_latch(StorageStopState *state, uint64_t deadline_us)
{
    if (!state || state->state != STORAGE_STOP_NONE) return false;
    state->state = STORAGE_STOP_REQUESTED;
    state->deadline_us = deadline_us;
    return true;
}

int storage_stop_state_advance(StorageStopState *state, StorageStopPhase next)
{
    if (!state || state->state == STORAGE_STOP_FAILED ||
        next != (StorageStopPhase)(state->state + 1) ||
        next > STORAGE_STOP_FINISHED) return -1;
    state->state = next;
    return 0;
}

void storage_stop_state_fail(StorageStopState *state)
{
    if (state) state->state = STORAGE_STOP_FAILED;
}

bool storage_stop_state_expired(const StorageStopState *state, uint64_t now_us)
{
    return state && state->state != STORAGE_STOP_NONE &&
           state->deadline_us != 0u && now_us >= state->deadline_us;
}

bool storage_stop_boundary_should_quiesce(const StorageStopState *state,
                                          bool rx_packet_open,
                                          uint64_t now_us)
{
    return state && state->state == STORAGE_STOP_WAIT_BOUNDARY &&
           (!rx_packet_open || storage_stop_state_expired(state, now_us));
}

void storage_stop_harvest_state_init(StorageStopHarvestState *state)
{
    if (state) memset(state, 0, sizeof(*state));
}

bool storage_stop_harvest_observe(StorageStopHarvestState *state,
                                  bool dma_quiesced,
                                  uint32_t harvested_count,
                                  uint32_t completed_unharvested,
                                  bool rx_packet_open,
                                  uint64_t now_us,
                                  uint32_t required_empty_scans,
                                  uint64_t stable_window_us)
{
    if (!state || !dma_quiesced || harvested_count != 0u ||
        completed_unharvested != 0u || rx_packet_open) {
        if (state) storage_stop_harvest_state_init(state);
        return false;
    }
    if (required_empty_scans == 0u) required_empty_scans = 1u;
    if (state->consecutive_empty_scans == 0u)
        state->empty_since_us = now_us;
    if (state->consecutive_empty_scans != UINT32_MAX)
        ++state->consecutive_empty_scans;
    return state->consecutive_empty_scans >= required_empty_scans &&
           now_us >= state->empty_since_us &&
           now_us - state->empty_since_us >= stable_window_us;
}

StorageStopTailDisposition storage_stop_tail_disposition(bool stop_active,
                                                         bool tail_already_seen,
                                                         uint64_t payload_bytes,
                                                         uint64_t media_bytes,
                                                         bool padding_coherent)
{
    if (!stop_active) return STORAGE_STOP_TAIL_QUEUE;
    if (tail_already_seen) return STORAGE_STOP_TAIL_DEFER_LATE;
    if (payload_bytes != 0u && media_bytes > payload_bytes && !padding_coherent)
        return STORAGE_STOP_TAIL_DEFER_UNALIGNED;
    return STORAGE_STOP_TAIL_QUEUE;
}
