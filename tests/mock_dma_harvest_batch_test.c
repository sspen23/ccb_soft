#include <assert.h>
#include <stdio.h>
#include <string.h>
#include "ccb_hw.h"

#define CMPLT (1u << 31)
#define DESC_ERROR (1u << 28)
int main(void)
{
    ChannelRuntime rt; ChannelConfig cfg; DmaSgDesc desc[4]; uint32_t regs[64];
    DmaHarvestItem out[4]; uint32_t count = 0u;
    memset(&rt, 0, sizeof(rt)); memset(&cfg, 0, sizeof(cfg)); memset(desc, 0, sizeof(desc)); memset(regs, 0, sizeof(regs));
    cfg.id = 0; rt.cfg = &cfg; rt.desc.virt = (volatile uint8_t *)desc; rt.dma.virt = (volatile uint8_t *)regs;
    rt.dma_desc_count = 4u; rt.dma_desc_bytes = 4096u; rt.dma_hw_desc_count = 4u;
    desc[0].status = CMPLT | 512u; desc[1].status = CMPLT | 1024u;
    assert(dma_harvest_batch(&rt, out, 4u, 0u, &count) == 0 && count == 2u);
    assert(out[0].slot == 0u && out[1].actual_bytes == 1024u && rt.dma_hw_desc_count == 2u);
    assert(out[0].submission_sequence == 0u &&
           out[1].submission_sequence == 1u);

    memset(desc, 0, sizeof(desc));
    rt.next_harvest_bd = 0u; rt.next_requeue_bd = 0u;
    rt.dma_hw_desc_count = 4u; count = 0u;
    memset(rt.dma_bd_harvested, 0, sizeof(rt.dma_bd_harvested));
    desc[0].status = CMPLT | 512u;
    desc[1].status = CMPLT | 1024u;
    desc[2].status = CMPLT | DESC_ERROR;
    assert(dma_harvest_batch(&rt, out, 4u, 0u, &count) != 0 && count == 2u);
    assert(out[0].slot == 0u && out[1].slot == 1u);
    assert(out[0].submission_sequence == 2u &&
           out[1].submission_sequence == 3u);
    assert(rt.next_harvest_bd == 2u && rt.dma_hw_desc_count == 2u);
    puts("mock_dma_harvest_batch_test: ok"); return 0;
}
