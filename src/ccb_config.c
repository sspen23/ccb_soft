#include "ccb_config.h"

/*
 * Hardware topology from the current Vivado block design plus deployed DDR
 * capacities.
 *
 * Compared with the older bare-metal table in src_mb, the device tree is the
 * authority here for MMIO bases and descriptor BRAM CPU addresses. Channel
 * data DDR is not mapped into the CPU address space; software passes only
 * channel-local DDR offsets or DMA/NVMe hardware addresses. ch0/ch1 are
 * clamped to the stable low 1 GiB window; larger environment requests are
 * rejected at runtime.
 *
 * Descriptor BRAM uses the device-tree CPU address for /dev/mem access, while
 * AXI DMA still uses the old descriptor hardware-view base 0x10000000.
 * Data buffer descriptors still use channel-local DDR hardware offsets from
 * 0x00000000, matching the bare-metal S2MM/NVMe PRP programming model.
 */
const ChannelConfig kChannels[NUM_CHANNELS] = {
    {
        .id = HIGH_I_CHANNEL_ID,
        .name = "HIGH_I",
        .file_type = 1u,
        .dma_base = 0x41e00000ull,
        .axis_switch_base = 0x44a10000ull,
        .nvme_base = 0x44a00000ull,
        .pcie_bridge_base = 0xb0000000ull,
        .desc_cpu_base = 0x20000000ull,
        .desc_dma_base = 0x10000000ull,
        .desc_cpu_size = 0x4000ull,
        .ddr_hw_base = 0x00000000ull,
        .dma_ring_bytes = CHANNEL0_DDR_BYTES,
        .dma_ring_bytes_max = CHANNEL0_DDR_BYTES_MAX,
        .dma_desc_bytes_default = DMA_DESC_BYTES_CH0_DEFAULT,
        .nvme_manual_prp = true,
        .prp_list_cpu_base = 0xc0000000ull,
        .prp_list_hw_base = 0xc0000000ull,
        .prp_list_size = 0x8000u,
    },
    {
        .id = HIGH_Q_CHANNEL_ID,
        .name = "HIGH_Q",
        .file_type = 2u,
        .dma_base = 0xa0060000ull,
        .axis_switch_base = 0xa0070000ull,
        .nvme_base = 0xa0080000ull,
        .pcie_bridge_base = 0ull,
        .desc_cpu_base = 0x30000000ull,
        .desc_dma_base = 0x10000000ull,
        .desc_cpu_size = 0x4000ull,
        .ddr_hw_base = 0x00000000ull,
        .dma_ring_bytes = CHANNEL1_DDR_BYTES,
        .dma_ring_bytes_max = CHANNEL1_DDR_BYTES_MAX,
        .dma_desc_bytes_default = DMA_DESC_BYTES_CH0_DEFAULT,
        .nvme_manual_prp = true,
        .prp_list_cpu_base = 0xc2000000ull,
        .prp_list_hw_base = 0xc2000000ull,
        .prp_list_size = 0x8000u,
    },
    {
        .id = LOW_SPEED_CHANNEL_ID,
        .name = "LOW_SPEED",
        .file_type = 0u,
        .dma_base = 0x00030000ull,
        .axis_switch_base = 0x00040000ull,
        .nvme_base = 0x00010000ull,
        .pcie_bridge_base = 0ull,
        .desc_cpu_base = 0x20004000ull,
        .desc_dma_base = 0x10000000ull,
        .desc_cpu_size = 0x4000ull,
        .ddr_hw_base = 0x00000000ull,
        .dma_ring_bytes = CHANNEL2_DDR_BYTES,
        .dma_ring_bytes_max = CHANNEL2_DDR_BYTES_MAX,
        .dma_desc_bytes_default = DMA_DESC_BYTES_CH2_DEFAULT,
    },
};

const ChannelConfig *find_channel(int id) {
    size_t i;
    /* Small fixed table lookup; linear scan keeps this trivial and explicit. */
    for (i = 0; i < NUM_CHANNELS; ++i) {
        if (kChannels[i].id == id) {
            return &kChannels[i];
        }
    }
    return 0;
}
