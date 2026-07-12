#include <assert.h>
#include <stdio.h>
#include <string.h>
#include "ccb_hw.h"

#define CMPLT (1u << 31)
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
    puts("mock_dma_harvest_batch_test: ok"); return 0;
}
