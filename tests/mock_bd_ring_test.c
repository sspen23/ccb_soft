#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "ccb_hw.h"

#define TEST_S2MM_DMACR 0x30u
#define TEST_S2MM_DMASR 0x34u
#define TEST_S2MM_CURDESC 0x38u
#define TEST_S2MM_TAILDESC 0x40u
#define TEST_DESC_CMPLT (1u << 31)

static void init_runtime(ChannelRuntime *rt,
                         ChannelConfig *cfg,
                         DmaSgDesc *desc,
                         uint32_t *dma_regs)
{
    memset(rt, 0, sizeof(*rt));
    memset(cfg, 0, sizeof(*cfg));
    memset(desc, 0, 4u * sizeof(*desc));
    memset(dma_regs, 0, 0x100u);
    cfg->id = 0;
    cfg->desc_dma_base = 0x10000000u;
    cfg->ddr_hw_base = 0x20000000u;
    rt->cfg = cfg;
    rt->desc.virt = (volatile uint8_t *)desc;
    rt->desc.valid = true;
    rt->dma.virt = (volatile uint8_t *)dma_regs;
    rt->dma.valid = true;
    rt->dma_desc_count = 4u;
    rt->dma_desc_bytes = 4096u;
    rt->dma_hw_desc_count = 4u;
    rt->dma_requeue_enabled = true;
    dma_regs[TEST_S2MM_DMACR / 4u] = 1u;
    dma_regs[TEST_S2MM_CURDESC / 4u] = (uint32_t)cfg->desc_dma_base;
    dma_regs[TEST_S2MM_TAILDESC / 4u] =
        (uint32_t)(cfg->desc_dma_base + 3u * sizeof(DmaSgDesc));
}

static void reproduce_completed_unharvested_quiesce_failure(uint32_t completed)
{
    ChannelRuntime rt;
    ChannelConfig cfg;
    DmaSgDesc desc[128];
    uint32_t dma_regs[0x100u / 4u];
    uint8_t state[128];
    DmaStopReport stop_report;
    uint32_t i;

    assert(completed <= 128u);
    init_runtime(&rt, &cfg, desc, dma_regs);
    memset(desc, 0, sizeof(desc));
    memset(state, STORAGE_SLOT_DMA_WRITABLE, sizeof(state));
    rt.dma_desc_count = 128u;
    rt.dma_hw_desc_count = 128u;
    dma_regs[TEST_S2MM_TAILDESC / 4u] =
        (uint32_t)(cfg.desc_dma_base + 127u * sizeof(DmaSgDesc));
    for (i = 0u; i < completed; ++i)
        desc[i].status = TEST_DESC_CMPLT | 512u;

    dma_latch_stop(&rt);
    memset(&stop_report, 0, sizeof(stop_report));
    assert(dma_quiesce_s2mm_with_state(&rt, 1u, state, &stop_report) ==
           DMA_STOP_FAILED);
    assert(stop_report.completed_unharvested == completed);
    assert(strcmp(stop_report.reason, "completed_unharvested") == 0);
}

int main(void)
{
    ChannelRuntime rt;
    ChannelConfig cfg;
    DmaSgDesc desc[4];
    uint32_t dma_regs[0x100u / 4u];
    uint8_t state[4];
    DmaBdSnapshot snapshot;
    DmaStopReport stop_report;
    DmaHarvestItem harvested[4];
    uint32_t harvested_count = 0u;

    assert(nvme_default_qd_for_channel(HIGH_I_CHANNEL_ID) == 8u);
    assert(nvme_default_qd_for_channel(HIGH_Q_CHANNEL_ID) == 8u);
    assert(nvme_default_qd_for_channel(LOW_SPEED_CHANNEL_ID) == 4u);

    init_runtime(&rt, &cfg, desc, dma_regs);
    memset(state, STORAGE_SLOT_DMA_WRITABLE, sizeof(state));
    assert(dma_get_bd_snapshot(&rt, state, &snapshot) == 0);
    assert(snapshot.dma_writable == 4u && snapshot.completed_unharvested == 0u);

    desc[1].status = TEST_DESC_CMPLT | 1024u;
    assert(dma_get_bd_snapshot(&rt, state, &snapshot) == 0);
    assert(snapshot.dma_writable == 3u && snapshot.completed_unharvested == 1u);

    desc[0].status = desc[2].status = desc[3].status = TEST_DESC_CMPLT | 1024u;
    assert(dma_get_bd_snapshot(&rt, state, &snapshot) == 0);
    assert(snapshot.dma_writable == 0u && snapshot.completed_unharvested == 4u);
    {
        int integrity_ok = 1;
        if (snapshot.dma_writable == 0u) integrity_ok = 0;
        assert(integrity_ok == 0);
    }

    memset(desc, 0, sizeof(desc));
    desc[0].status = TEST_DESC_CMPLT | 512u;
    state[0] = STORAGE_SLOT_DMA_WRITABLE;
    state[1] = STORAGE_SLOT_READY_FOR_NVME;
    state[2] = STORAGE_SLOT_NVME_BUSY;
    state[3] = STORAGE_SLOT_REQUEUE_PENDING;
    assert(dma_get_bd_snapshot(&rt, state, &snapshot) == 0);
    assert(snapshot.completed_unharvested == 1u && snapshot.ready_slots == 1u);
    assert(snapshot.nvme_busy_slots == 1u && snapshot.requeue_pending == 1u);
    assert(snapshot.dma_writable + snapshot.completed_unharvested +
           snapshot.ready_slots + snapshot.nvme_busy_slots +
           snapshot.requeue_pending + snapshot.free_slots == snapshot.total_slots);

    {
        StorageSlotCounts counts = {
            .total = 4u, .dma_writable = 0u, .completed_unharvested = 1u,
            .ready = 1u, .nvme_busy = 1u, .requeue_pending = 1u, .free_count = 0u,
        };
        /* Normal capture snapshots use maintained counts and do not inspect
         * the descriptor array; the full scan above remains an audit path. */
        memset(desc, 0, sizeof(desc));
        assert(dma_get_bd_snapshot_o1(&rt, &counts, &snapshot) == 0);
        assert(snapshot.completed_unharvested == 1u && snapshot.ready_slots == 1u &&
               snapshot.nvme_busy_slots == 1u && snapshot.requeue_pending == 1u);
        counts.free_count = 1u;
        assert(dma_get_bd_snapshot_o1(&rt, &counts, &snapshot) != 0);
    }

    state[3] = 0xffu;
    assert(dma_get_bd_snapshot(&rt, state, &snapshot) != 0);

    init_runtime(&rt, &cfg, desc, dma_regs);
    desc[0].status = TEST_DESC_CMPLT | 512u;
    desc[1].status = TEST_DESC_CMPLT | 1024u;
    desc[2].status = 64u;
    dma_regs[TEST_S2MM_DMASR / 4u] = 1u;
    memset(&stop_report, 0, sizeof(stop_report));
    assert(dma_quiesce_s2mm(&rt, 1u, &stop_report) == 0);
    assert((dma_regs[TEST_S2MM_DMACR / 4u] & 1u) == 0u);
    assert(desc[0].status == (TEST_DESC_CMPLT | 512u));
    assert(desc[1].status == (TEST_DESC_CMPLT | 1024u));
    assert(dma_harvest_completed_batch(&rt, harvested, 4u, &harvested_count) == 0);
    assert(harvested_count == 2u);
    assert(harvested[0].slot == 0u && harvested[1].slot == 1u);
    assert(rt.next_harvest_bd == 2u);
    assert(dma_s2mm_tail_incomplete(&rt));

    init_runtime(&rt, &cfg, desc, dma_regs);
    desc[0].status = desc[1].status = desc[2].status = desc[3].status =
        TEST_DESC_CMPLT | 512u;
    dma_regs[TEST_S2MM_DMASR / 4u] = 1u;
    assert(dma_quiesce_s2mm(&rt, 1u, &stop_report) == 0);
    assert(dma_harvest_completed_batch(&rt, harvested, 4u, &harvested_count) == 0);
    assert(harvested_count == 4u);
    assert(dma_harvest_completed_batch(&rt, harvested, 4u, &harvested_count) == 0);
    assert(harvested_count == 0u);

    /* STOP harvesting must not treat the first empty observation as final:
     * a descriptor can become visible on a later scan after S2MM quiesces. */
    init_runtime(&rt, &cfg, desc, dma_regs);
    dma_regs[TEST_S2MM_DMASR / 4u] = 1u;
    assert(dma_quiesce_s2mm(&rt, 1u, &stop_report) == 0);
    assert(dma_harvest_completed_batch(&rt, harvested, 4u, &harvested_count) == 0);
    assert(harvested_count == 0u && rt.next_harvest_bd == 0u);
    desc[0].status = TEST_DESC_CMPLT | 768u;
    assert(dma_harvest_completed_batch(&rt, harvested, 4u, &harvested_count) == 0);
    assert(harvested_count == 1u && harvested[0].slot == 0u &&
           harvested[0].actual_bytes == 768u && rt.next_harvest_bd == 1u);
    assert(dma_harvest_completed_batch(&rt, harvested, 4u, &harvested_count) == 0);
    assert(harvested_count == 0u && rt.next_harvest_bd == 1u);

    init_runtime(&rt, &cfg, desc, dma_regs);
    desc[0].status = TEST_DESC_CMPLT | 512u;
    dma_regs[TEST_S2MM_DMASR / 4u] = 1u;
    assert(dma_requeue_one(&rt, 0u) != 0);

    init_runtime(&rt, &cfg, desc, dma_regs);
    dma_latch_stop(&rt);
    assert(dma_requeue_one(&rt, 0u) == -2);
    assert(dma_requeue_after_stop_count(&rt) == 1u);

    /* A non-halted DMA with an unharvested completion is not eligible for
     * reset recovery: the reset could discard valid DDR data. */
    init_runtime(&rt, &cfg, desc, dma_regs);
    dma_latch_stop(&rt);
    desc[0].status = TEST_DESC_CMPLT | 512u;
    dma_regs[TEST_S2MM_DMASR / 4u] = 0u;
    memset(state, STORAGE_SLOT_DMA_WRITABLE, sizeof(state));
    memset(&stop_report, 0, sizeof(stop_report));
    assert(dma_quiesce_s2mm_with_state(&rt, 1u, state, &stop_report) == DMA_STOP_FAILED);
    assert(strcmp(stop_report.reason, "completed_unharvested") == 0);

    /* A halted path must apply the same ownership audit before the final
     * reset; it must not discard a completion that the producer did not
     * harvest merely because quiesce itself reached HALTED. */
    init_runtime(&rt, &cfg, desc, dma_regs);
    dma_latch_stop(&rt);
    desc[0].status = TEST_DESC_CMPLT | 512u;
    memset(state, STORAGE_SLOT_DMA_WRITABLE, sizeof(state));
    memset(&stop_report, 0, sizeof(stop_report));
    assert(dma_finalize_stop_s2mm_with_state(&rt, state, &stop_report) == DMA_STOP_FAILED);
    assert(strcmp(stop_report.reason, "completed_unharvested") == 0);

    /* An open AXIS packet is likewise a conservative hard failure. */
    init_runtime(&rt, &cfg, desc, dma_regs);
    dma_latch_stop(&rt);
    rt.dma_rx_packet_open = true;
    dma_regs[TEST_S2MM_DMASR / 4u] = 0u;
    memset(&stop_report, 0, sizeof(stop_report));
    assert(dma_quiesce_s2mm_with_state(&rt, 1u, state, &stop_report) == DMA_STOP_FAILED);
    assert(strcmp(stop_report.reason, "tail_descriptor_incomplete") == 0);

    /* Board regressions A/B stopped with 114/119 completed descriptors.  The
     * current implementation reproduces both failures before harvest runs. */
    reproduce_completed_unharvested_quiesce_failure(114u);
    reproduce_completed_unharvested_quiesce_failure(119u);

    puts("mock_bd_ring_test: ok");
    return 0;
}
