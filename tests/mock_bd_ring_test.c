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
    dma_regs[TEST_S2MM_DMACR / 4u] = 1u;
    dma_regs[TEST_S2MM_CURDESC / 4u] = (uint32_t)cfg->desc_dma_base;
    dma_regs[TEST_S2MM_TAILDESC / 4u] =
        (uint32_t)(cfg->desc_dma_base + 3u * sizeof(DmaSgDesc));
}

int main(void)
{
    ChannelRuntime rt;
    ChannelConfig cfg;
    DmaSgDesc desc[4];
    uint32_t dma_regs[0x100u / 4u];
    uint8_t state[4];
    DmaBdSnapshot snapshot;

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

    state[3] = 0xffu;
    assert(dma_get_bd_snapshot(&rt, state, &snapshot) != 0);

    init_runtime(&rt, &cfg, desc, dma_regs);
    desc[0].status = TEST_DESC_CMPLT | 512u;
    dma_regs[TEST_S2MM_DMASR / 4u] = 1u;
    assert(dma_requeue_one(&rt, 0u) != 0);

    puts("mock_bd_ring_test: ok");
    return 0;
}
