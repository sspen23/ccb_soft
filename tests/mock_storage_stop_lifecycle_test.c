#include <assert.h>
#include <stdio.h>

#include "ccb_storage_pipeline.h"

static void test_stable_empty_requires_repeated_observation(void)
{
    StorageStopHarvestState state;

    storage_stop_harvest_state_init(&state);
    assert(!storage_stop_harvest_observe(&state, true, 0u, 0u, false,
                                         1000u, 3u, 100u));
    assert(!storage_stop_harvest_observe(&state, true, 1u, 0u, false,
                                         1050u, 3u, 100u));
    assert(!storage_stop_harvest_observe(&state, true, 0u, 0u, false,
                                         1100u, 3u, 100u));
    assert(!storage_stop_harvest_observe(&state, true, 0u, 0u, false,
                                         1150u, 3u, 100u));
    assert(storage_stop_harvest_observe(&state, true, 0u, 0u, false,
                                        1200u, 3u, 100u));
}

static void test_unaligned_tail_does_not_abort_full_prefix(void)
{
    StoragePipeline pipeline;
    StoragePipelineItem full = {
        .slot = 0u,
        .bytes = 8u * 1024u * 1024u,
        .sectors = 8u * 1024u * 1024u / 512u,
    };
    StoragePipelineItem out;
    bool producer_done = false;

    assert(storage_pipeline_init(&pipeline, 2u) == 0);
    assert(storage_pipeline_mark_completed(&pipeline, 0u) == 0);
    assert(storage_pipeline_mark_completed(&pipeline, 1u) == 0);
    assert(storage_queue_push_batch(&pipeline, &full, 1u) == 0);
    assert(storage_stop_tail_disposition(true, false, 419692512u,
                                         419692544u, false) ==
           STORAGE_STOP_TAIL_DEFER_UNALIGNED);
    assert(storage_slot_transition_locked(
               &pipeline, 1u, STORAGE_SLOT_DMA_COMPLETED_UNHARVESTED,
               STORAGE_SLOT_FREE) == 0);
    assert(!pipeline.error && !producer_done && pipeline.count == 1u);
    assert(storage_pipeline_pop(&pipeline, &out, 0) == 0 && out.slot == 0u);
    assert(storage_pipeline_complete(&pipeline, 0u, 0) == 0);
    producer_done = true;
    assert(producer_done && pipeline.count == 0u &&
           pipeline.counts.completed_unharvested == 0u &&
           pipeline.counts.ready == 0u && pipeline.counts.nvme_busy == 0u &&
           storage_pipeline_counts_valid(&pipeline));
    storage_pipeline_destroy(&pipeline);
}

static void test_stop_is_idempotent_and_inflight_blocks_finish(void)
{
    StorageStopState stop;
    StorageDrainInvariant invariant = {
        .dma_harvested_payload_bytes = 4096u,
        .queued_payload_bytes = 4096u,
        .nvme_completed_payload_bytes = 4096u,
        .file_bytes = 4096u,
        .submit_count = 7u,
        .completion_count = 6u,
        .global_inflight = 1u,
    };
    uint64_t submitted = 7u;
    uint64_t completed = 6u;
    uint64_t global_inflight = 1u;

    storage_stop_state_init(&stop);
    assert(storage_stop_state_latch(&stop, 1000u));
    assert(!storage_stop_state_latch(&stop, 2000u));
    assert(submitted - completed == global_inflight);
    assert(global_inflight != 0u);
    assert(!storage_drain_invariant_ok(&invariant));
    ++completed;
    --global_inflight;
    assert(submitted == completed && global_inflight == 0u);
    invariant.completion_count = 7u;
    invariant.global_inflight = 0u;
    assert(storage_drain_invariant_ok(&invariant));
    invariant.completed_unharvested = 1u;
    assert(!storage_drain_invariant_ok(&invariant));
    invariant.completed_unharvested = 0u;
    invariant.ready_count = 1u;
    assert(!storage_drain_invariant_ok(&invariant));
}

static void test_ring_pressure_policy_is_bounded(void)
{
    assert(storage_ring_pressure_level(74u, 100u, 75u, 90u) == 0u);
    assert(storage_ring_pressure_level(75u, 100u, 75u, 90u) == 1u);
    assert(storage_ring_pressure_level(90u, 100u, 75u, 90u) == 2u);
    assert(storage_ring_pressure_level(100u, 100u, 75u, 90u) == 3u);
    assert(storage_writer_budget_for_pressure(300u, 0u) == 300u);
    assert(storage_writer_budget_for_pressure(300u, 1u) == 450u);
    assert(storage_writer_budget_for_pressure(300u, 2u) == 600u);
    assert(storage_writer_budget_for_pressure(UINT32_MAX, 2u) == UINT32_MAX);
    assert(!storage_ring_pressure_should_stop(2u, 1000u, 100999u,
                                              100000u, true));
    assert(storage_ring_pressure_should_stop(2u, 1000u, 101000u,
                                             100000u, true));
    assert(!storage_ring_pressure_should_stop(2u, 1000u, 101000u,
                                              100000u, false));
}

int main(void)
{
    test_stable_empty_requires_repeated_observation();
    test_unaligned_tail_does_not_abort_full_prefix();
    test_stop_is_idempotent_and_inflight_blocks_finish();
    test_ring_pressure_policy_is_bounded();
    puts("mock_storage_stop_lifecycle_test: ok");
    return 0;
}
