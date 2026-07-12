#include <assert.h>
#include <stdio.h>
#include "ccb_storage_pipeline.h"

int main(void)
{
    StoragePipeline p; StoragePipelineItem a[2] = {{.slot=0,.bytes=512},{.slot=1,.bytes=512}}, out;
    assert(storage_pipeline_init(&p, 2u) == 0);
    assert(storage_pipeline_mark_completed(&p, 0u) == 0);
    assert(storage_pipeline_mark_completed(&p, 1u) == 0);
    assert(storage_queue_push_batch(&p, a, 2u) == 0);
    assert(storage_pipeline_counts_valid(&p));
    assert(storage_pipeline_pop(&p, &out, 0) == 0 && out.slot == 0u);
    assert(storage_pipeline_complete(&p, 0u, 1) == 0);
    assert(storage_slot_transition_locked(&p, 0u, STORAGE_SLOT_REQUEUE_PENDING,
                                          STORAGE_SLOT_DMA_WRITABLE) == 0);
    assert(storage_pipeline_pop(&p, &out, 0) == 0 && out.slot == 1u);
    assert(storage_pipeline_complete(&p, 1u, 0) == 0);
    assert(storage_pipeline_counts_valid(&p));
    storage_pipeline_destroy(&p);
    puts("mock_storage_pipeline_test: ok"); return 0;
}
