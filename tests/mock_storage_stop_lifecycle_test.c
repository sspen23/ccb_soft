#include <assert.h>
#include <stdio.h>

#include "ccb_storage_pipeline.h"

static int quiesce_call_count;

static int count_quiesce(void *opaque)
{
    int *result = opaque;

    ++quiesce_call_count;
    return *result;
}

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

static void test_boundary_deadline_is_independent_of_harvest_activity(void)
{
    StorageStopState stop;

    storage_stop_state_init(&stop);
    assert(storage_stop_state_latch(&stop, 1100u));
    assert(storage_stop_state_advance(&stop, STORAGE_STOP_WAIT_BOUNDARY) == 0);
    assert(!storage_stop_boundary_should_quiesce(&stop, true, 1099u));
    assert(storage_stop_boundary_should_quiesce(&stop, true, 1100u));
    assert(storage_stop_boundary_should_quiesce(&stop, false, 1001u));
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
    const uint64_t descriptor_bytes = 8u * 1024u * 1024u;
    const uint64_t partial_bytes = descriptor_bytes - 32u;
    uint64_t harvested_bytes = partial_bytes;
    uint64_t queued_bytes = 0u;
    uint64_t tail_unqueued_bytes = 0u;
    uint32_t i;

    assert(storage_pipeline_init(&pipeline, 2u) == 0);
    assert(storage_pipeline_mark_completed(&pipeline, 0u) == 0);
    assert(storage_pipeline_mark_completed(&pipeline, 1u) == 0);
    assert(storage_queue_push_batch(&pipeline, &full, 1u) == 0);
    assert(storage_stop_tail_disposition(true, false, 419692512u,
                                         419692544u, false) ==
           STORAGE_STOP_TAIL_DEFER_UNALIGNED);
    assert(storage_stop_tail_disposition(false, false, 262112u,
                                         262144u, false) ==
           STORAGE_STOP_TAIL_DEFER_UNALIGNED);
    assert(storage_stop_tail_disposition(false, true, 4096u, 4096u, false) ==
           STORAGE_STOP_TAIL_QUEUE);
    assert(storage_stop_tail_disposition(true, false, partial_bytes,
                                         descriptor_bytes, false) ==
           STORAGE_STOP_TAIL_DEFER_UNALIGNED);
    tail_unqueued_bytes += partial_bytes;
    for (i = 0u; i < 64u; ++i) {
        assert(storage_stop_tail_disposition(true, true, descriptor_bytes,
                                             descriptor_bytes, false) ==
               STORAGE_STOP_TAIL_QUEUE);
        harvested_bytes += descriptor_bytes;
        queued_bytes += descriptor_bytes;
    }
    assert(tail_unqueued_bytes < descriptor_bytes);
    assert(harvested_bytes == queued_bytes + tail_unqueued_bytes);
    assert(storage_slot_transition(&pipeline.slots, 1u,
                                   STORAGE_SLOT_DMA_COMPLETED_UNHARVESTED,
                                   STORAGE_SLOT_FREE) == 0);
    assert(!pipeline.error && !producer_done && pipeline.count == 1u);
    assert(storage_pipeline_pop(&pipeline, &out, 0) == 0 && out.slot == 0u);
    assert(storage_pipeline_complete(&pipeline, 0u, 0) == 0);
    producer_done = true;
    assert(producer_done && pipeline.count == 0u &&
           pipeline.slots.counts.completed_unharvested == 0u &&
           pipeline.slots.counts.ready == 0u &&
           pipeline.slots.counts.nvme_busy == 0u &&
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
    int quiesce_result = 1;

    storage_stop_state_init(&stop);
    assert(storage_stop_state_latch(&stop, 1000u));
    assert(!storage_stop_state_latch(&stop, 2000u));
    quiesce_call_count = 0;
    assert(storage_dma_quiesce_once(&stop.quiesce, 77u, count_quiesce,
                                    &quiesce_result) == 1);
    quiesce_result = -1;
    assert(storage_dma_quiesce_once(&stop.quiesce, 77u, count_quiesce,
                                    &quiesce_result) == 1);
    assert(quiesce_call_count == 1);
    assert(storage_dma_quiesce_once(&stop.quiesce, 78u, count_quiesce,
                                    &quiesce_result) == -1);
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

static void test_input_idle_candidate_and_reactivation(void)
{
    StorageInputIdleState idle;
    uint32_t scan;

    storage_input_idle_init(&idle);
    for (scan = 0u; scan < 10u; ++scan) {
        assert(storage_input_idle_observe(&idle, scan * 100000u, 0u, 0u,
                                          false, false, 500000u, 5u) ==
               STORAGE_INPUT_IDLE_NO_CHANGE);
    }
    assert(!idle.first_data_seen && !idle.candidate);

    assert(storage_input_idle_observe(&idle, 1000000u, 4096u, 1u,
                                      false, false, 500000u, 5u) ==
           STORAGE_INPUT_IDLE_NO_CHANGE);
    for (scan = 1u; scan < 5u; ++scan) {
        assert(storage_input_idle_observe(&idle,
                                          1500000u + scan * 100000u,
                                          4096u, 1u, false, false,
                                          500000u, 5u) ==
               STORAGE_INPUT_IDLE_NO_CHANGE);
    }
    assert(storage_input_idle_observe(&idle, 1900000u, 4096u, 1u,
                                      false, false, 500000u, 5u) ==
           STORAGE_INPUT_IDLE_CANDIDATE);
    assert(idle.candidate);
    assert(storage_input_idle_observe(&idle, 2000000u, 8192u, 2u,
                                      false, false, 500000u, 5u) ==
           STORAGE_INPUT_ACTIVE);
    assert(!idle.candidate && idle.idle_scan_count == 0u);
}

int main(void)
{
    test_stable_empty_requires_repeated_observation();
    test_boundary_deadline_is_independent_of_harvest_activity();
    test_unaligned_tail_does_not_abort_full_prefix();
    test_stop_is_idempotent_and_inflight_blocks_finish();
    test_ring_pressure_policy_is_bounded();
    test_input_idle_candidate_and_reactivation();
    puts("mock_storage_stop_lifecycle_test: ok");
    return 0;
}
