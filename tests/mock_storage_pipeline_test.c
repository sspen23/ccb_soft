#include <assert.h>
#include <stdio.h>
#include "ccb_storage_pipeline.h"

int main(void)
{
    StoragePipeline p; StoragePipelineItem a[2] = {
        {.slot=0,.bytes=512,.sectors=1,.chunk_index=7,.file_offset=0,.start_lba=100},
        {.slot=1,.bytes=512,.sectors=1,.chunk_index=8,.file_offset=512,.start_lba=101}}, out;
    StorageStopState stop;
    StorageRunState run;

    storage_stop_state_init(&stop);
    assert(stop.state == STORAGE_CAPTURE_ACCEPTING);
    assert(storage_stop_state_latch(&stop, 100u));
    assert(!storage_stop_state_latch(&stop, 200u));
    assert(stop.deadline_us == 100u);
    assert(storage_stop_state_advance(&stop, STORAGE_CAPTURE_DMA_QUIESCING) == 0);
    assert(storage_stop_state_advance(&stop, STORAGE_CAPTURE_HARVESTING_COMPLETED) == 0);
    assert(storage_stop_state_advance(&stop, STORAGE_CAPTURE_WRITER_DRAINING) == 0);
    assert(storage_stop_state_advance(&stop, STORAGE_CAPTURE_FINALIZING) == 0);
    assert(!storage_stop_state_expired(&stop, 99u));
    assert(storage_stop_state_expired(&stop, 100u));

    storage_run_state_init(&run);
    assert(!storage_run_state_can_emit_running(&run));
    assert(storage_run_state_enable_writer(&run) == 0);
    assert(!storage_run_state_can_emit_running(&run));
    assert(storage_run_state_set_writer_ready(&run, true) == 0);
    assert(!storage_run_state_can_emit_running(&run));
    assert(storage_run_state_set_producer_ready(&run, true) == 0);
    assert(storage_run_state_can_emit_running(&run));
    assert(storage_run_state_mark_running(&run) == 0);
    assert(!storage_run_state_can_emit_running(&run));

    storage_run_state_init(&run);
    assert(storage_run_state_enable_writer(&run) == 0);
    assert(storage_run_state_set_writer_ready(&run, false) != 0);
    assert(!storage_run_state_can_emit_running(&run));
    storage_run_state_init(&run);
    assert(storage_run_state_enable_writer(&run) == 0);
    assert(storage_run_state_set_writer_ready(&run, true) == 0);
    assert(storage_run_state_set_producer_ready(&run, false) != 0);
    assert(!storage_run_state_can_emit_running(&run));

    assert(storage_pipeline_init(&p, 2u) == 0);
    assert(storage_harvest_limit_for_remaining(8192u, 4096u, 4u) == 2u);
    assert(storage_harvest_limit_for_remaining(16384u, 4096u, 4u) == 4u);
    assert(storage_harvest_limit_for_remaining(6144u, 4096u, 4u) == 2u);
    assert(storage_harvest_limit_for_remaining(1u, 4096u, 4u) == 1u);
    assert(storage_harvest_limit_for_remaining(UINT64_MAX, UINT32_MAX, 4u) == 4u);
    assert(storage_harvest_limit_for_remaining(0u, 4096u, 4u) == 0u);
    assert(storage_pipeline_mark_completed(&p, 0u) == 0);
    assert(storage_pipeline_mark_completed(&p, 1u) == 0);
    assert(storage_queue_push_batch(&p, a, 2u) == 0);
    assert(storage_pipeline_counts_valid(&p));
    assert(storage_pipeline_pop(&p, &out, 0) == 0 && out.slot == 0u &&
           out.chunk_index == 7u && out.file_offset == 0u && out.start_lba == 100u);
    assert(storage_pipeline_complete(&p, 0u, 1) == 0);
    assert(storage_slot_transition_locked(&p, 0u, STORAGE_SLOT_REQUEUE_PENDING,
                                          STORAGE_SLOT_DMA_WRITABLE) == 0);
    assert(storage_pipeline_pop(&p, &out, 0) == 0 && out.slot == 1u &&
           out.chunk_index == 8u && out.file_offset == 512u && out.start_lba == 101u);
    assert(storage_pipeline_complete(&p, 1u, 0) == 0);
    assert(storage_pipeline_counts_valid(&p));
    storage_pipeline_destroy(&p);

    assert(storage_pipeline_init(&p, 2u) == 0);
    assert(storage_pipeline_mark_completed(&p, 0u) == 0);
    {
        StoragePipelineItem duplicate[2] = {
            {.slot=0u,.bytes=512u,.sectors=1u}, {.slot=0u,.bytes=512u,.sectors=1u}};
        assert(storage_queue_push_batch(&p, duplicate, 2u) != 0);
        assert(p.count == 0u && p.tail == 0u && p.states[0] == STORAGE_SLOT_DMA_COMPLETED_UNHARVESTED);
        assert(storage_pipeline_counts_valid(&p));
    }
    storage_pipeline_destroy(&p);

    assert(storage_pipeline_init(&p, 1u) == 0);
    {
        StoragePipelineItem wrong_state = {.slot=0u,.bytes=512u,.sectors=1u};
        assert(storage_queue_push_batch(&p, &wrong_state, 1u) != 0);
        assert(p.count == 0u && p.states[0] == STORAGE_SLOT_DMA_WRITABLE);
        assert(storage_pipeline_counts_valid(&p));
    }
    storage_pipeline_destroy(&p);

    assert(storage_pipeline_init(&p, 1u) == 0);
    assert(storage_pipeline_mark_completed(&p, 0u) == 0);
    {
        StoragePipelineItem partial = {.slot=0u,.bytes=1u,.sectors=1u,
                                       .chunk_index=0u,.file_offset=0u,.start_lba=7u};
        assert(storage_queue_push_batch(&p, &partial, 1u) == 0);
        assert(storage_pipeline_pop(&p, &out, 0) == 0 && out.bytes == 1u &&
               out.sectors == 1u && out.start_lba == 7u);
        assert(storage_pipeline_counts_valid(&p));
    }
    storage_pipeline_destroy(&p);

    assert(storage_pipeline_init(&p, 1u) == 0);
    assert(storage_pipeline_mark_completed(&p, 0u) == 0);
    {
        StoragePipelineItem bad = {.slot=0u,.bytes=512u,.sectors=2u};
        assert(storage_queue_push_batch(&p, &bad, 1u) != 0);
        assert(p.count == 0u && p.states[0] == STORAGE_SLOT_DMA_COMPLETED_UNHARVESTED);
    }
    storage_pipeline_destroy(&p);

    assert(storage_pipeline_init(&p, 1u) == 0);
    assert(storage_pipeline_mark_completed(&p, 0u) == 0);
    assert(storage_queue_push_batch(&p, a, 1u) == 0);
    assert(storage_queue_push_batch(&p, a, 1u) != 0);
    assert(p.count == 1u);
    storage_pipeline_destroy(&p);
    puts("mock_storage_pipeline_test: ok"); return 0;
}
