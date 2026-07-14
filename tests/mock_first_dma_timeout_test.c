#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "ccb_hw.h"
#include "ccb_storage_pipeline.h"

#define DESC_CMPLT (1u << 31)
#define DESC_ERROR (1u << 28)

static void init_runtime(ChannelRuntime *rt, ChannelConfig *cfg, DmaSgDesc *desc,
                         uint32_t *dma_regs)
{
    memset(rt, 0, sizeof(*rt));
    memset(cfg, 0, sizeof(*cfg));
    memset(desc, 0, 2u * sizeof(*desc));
    memset(dma_regs, 0, 64u * sizeof(*dma_regs));
    cfg->id = 0;
    rt->cfg = cfg;
    rt->desc.virt = (volatile uint8_t *)desc;
    rt->desc.valid = true;
    rt->dma.virt = (volatile uint8_t *)dma_regs;
    rt->dma.valid = true;
    rt->dma_desc_count = 2u;
    rt->dma_desc_bytes = 4096u;
    rt->dma_hw_desc_count = 2u;
}

int main(void)
{
    ChannelRuntime rt;
    ChannelConfig cfg;
    DmaSgDesc desc[2];
    uint32_t dma_regs[64];
    DmaHarvestItem item[2];
    DmaBdSnapshot snapshot;
    StorageSlotCounts stale_counts;
    StoragePipeline pipeline;
    StoragePipelineItem queued;
    uint32_t count;
    uint64_t dma_received_bytes;

    /* A: deadline with no hardware completion is a real first-DMA timeout. */
    init_runtime(&rt, &cfg, desc, dma_regs);
    count = 0u;
    assert(dma_harvest_completed_batch(&rt, item, 2u, &count) == 0);
    assert(count == 0u);
    assert(storage_first_dma_deadline_outcome(true, false, false, 0, count) ==
           STORAGE_FIRST_DMA_DEADLINE_EXPIRED);

    /* B: maintained O(1) counts may still say zero while the next BD is
     * complete.  The normal harvest owns it once, queues it once and advances
     * payload accounting once. */
    init_runtime(&rt, &cfg, desc, dma_regs);
    desc[0].status = DESC_CMPLT | 512u;
    memset(&stale_counts, 0, sizeof(stale_counts));
    stale_counts.total = 2u;
    stale_counts.dma_writable = 2u;
    assert(dma_get_bd_snapshot_o1(&rt, &stale_counts, &snapshot) == 0);
    assert(snapshot.completed_unharvested == 0u);
    count = 0u;
    assert(dma_harvest_completed_batch(&rt, item, 2u, &count) == 0);
    assert(count == 1u && item[0].slot == 0u && item[0].actual_bytes == 512u);
    assert(storage_first_dma_deadline_outcome(true, false, false, 0, count) ==
           STORAGE_FIRST_DMA_DEADLINE_DATA);
    assert(storage_pipeline_init(&pipeline, 2u) == 0);
    assert(storage_pipeline_mark_completed(&pipeline, item[0].slot) == 0);
    memset(&queued, 0, sizeof(queued));
    queued.slot = item[0].slot;
    queued.bytes = item[0].actual_bytes;
    queued.sectors = 1u;
    assert(storage_queue_push_batch(&pipeline, &queued, 1u) == 0);
    dma_received_bytes = item[0].actual_bytes;
    assert(dma_harvest_completed_batch(&rt, item, 2u, &count) == 0);
    assert(count == 0u && dma_received_bytes == 512u);
    storage_pipeline_destroy(&pipeline);

    /* C: a completion becoming visible at the deadline boundary is consumed
     * by that same normal harvest and continues through the usual queue. */
    init_runtime(&rt, &cfg, desc, dma_regs);
    desc[0].status = DESC_CMPLT | 1024u;
    count = 0u;
    assert(dma_harvest_completed_batch(&rt, item, 2u, &count) == 0);
    assert(storage_first_dma_deadline_outcome(true, false, false, 0, count) ==
           STORAGE_FIRST_DMA_DEADLINE_DATA);
    assert(storage_pipeline_init(&pipeline, 2u) == 0);
    assert(storage_pipeline_mark_completed(&pipeline, item[0].slot) == 0);
    memset(&queued, 0, sizeof(queued));
    queued.slot = item[0].slot;
    queued.bytes = item[0].actual_bytes;
    queued.sectors = 2u;
    assert(storage_queue_push_batch(&pipeline, &queued, 1u) == 0);
    assert(storage_pipeline_pop(&pipeline, &queued, 0) == 0);
    assert(queued.bytes == 1024u);
    storage_pipeline_destroy(&pipeline);

    /* D: a descriptor error observed by the real harvest is never rewritten
     * as first_dma_timeout. */
    init_runtime(&rt, &cfg, desc, dma_regs);
    desc[0].status = DESC_CMPLT | DESC_ERROR;
    count = 0u;
    assert(dma_harvest_completed_batch(&rt, item, 2u, &count) != 0);
    assert(storage_first_dma_deadline_outcome(true, false, false, -1, count) ==
           STORAGE_FIRST_DMA_DEADLINE_HARVEST_FAILED);

    puts("mock_first_dma_timeout_test: ok");
    return 0;
}
