#ifndef CCB_HW_H
#define CCB_HW_H

#include "ccb_types.h"
#include <stdio.h>

/* Utility conversion helpers. */
uint64_t bytes_to_sectors(uint64_t bytes);
uint64_t cpu_to_hw_addr(const ChannelConfig *cfg, uint64_t cpu_addr);
int ddr_addr_validate(const ChannelConfig *cfg, uint64_t cpu_addr, uint64_t size);

/* Build/release mapped runtime state for one channel. */
int channel_runtime_open(ChannelRuntime *rt, const ChannelConfig *cfg, GlobalOptions gopt);
void channel_runtime_close(ChannelRuntime *rt);
void storage_print_pcie_link_status(ChannelRuntime *rt, const char *reason);

/* Configure data path source and probe NVMe host capabilities. */
void axis_switch_select(ChannelRuntime *rt, SourceMode src);
int nvme_probe(ChannelRuntime *rt);
void nvme_clear_stop_request(void);
void nvme_request_stop(void);
int nvme_stop_requested(void);

/* Submit segmented NVMe read/write covering the requested sector range. */
int nvme_rw(ChannelRuntime *rt, bool is_write, uint64_t lba, uint64_t sectors, uint64_t hw_addr);
int nvme_submit_write_async(ChannelRuntime *rt,
                            uint16_t cid,
                            uint64_t lba,
                            uint32_t sectors,
                            uint64_t ddr_addr);
int nvme_poll_cq(ChannelRuntime *rt, NvmeCompletion *out_cpl, uint32_t timeout_us);
int nvme_write_slot_qd(ChannelRuntime *rt,
                       uint32_t slot,
                       uint64_t lba,
                       uint64_t sectors,
                       uint64_t hw_addr);
int nvme_write_contiguous_tight_qd(ChannelRuntime *rt,
                                   uint64_t ddr_hw_start,
                                   uint64_t start_lba,
                                   uint64_t bytes,
                                   uint32_t qd);

typedef struct {
    uint32_t slot;
    uint64_t start_lba;
    uint64_t sectors;
    uint64_t hw_addr;
    uint64_t bytes;
    uint64_t chunk_index;
    uint64_t file_offset;
} NvmeWriteSlotReq;

typedef int (*NvmeWriteSlotDoneCb)(void *opaque, const NvmeWriteSlotReq *req);

int nvme_write_slots_qd(ChannelRuntime *rt,
                        const NvmeWriteSlotReq *reqs,
                        uint32_t req_count,
                        NvmeWriteSlotDoneCb done_cb,
                        void *opaque);
void nvme_reset_sw_timing(ChannelRuntime *rt);
void nvme_print_sw_timing(const ChannelRuntime *rt);
void nvme_print_sw_timing_to(FILE *out, const ChannelRuntime *rt);
uint64_t nvme_perf_calc_begin(ChannelRuntime *rt, uint64_t bytes);
void nvme_perf_calc_print(ChannelRuntime *rt,
                          uint64_t bytes,
                          uint64_t expected_cmds,
                          uint64_t wall_us);
void nvme_perf_calc_fprint(FILE *out,
                           ChannelRuntime *rt,
                           uint64_t bytes,
                           uint64_t expected_cmds,
                           uint64_t wall_us);

typedef enum {
    DMA_STOP_FAILED = -1,
    DMA_STOP_OK = 0,
    DMA_STOP_RESET_RECOVERED = 1
} DmaStopResult;

typedef struct {
    uint32_t s2mm_cr_before;
    uint32_t s2mm_sr_before;
    uint32_t s2mm_cr_after;
    uint32_t s2mm_sr_after;
    uint32_t curdesc;
    uint32_t taildesc;
    uint32_t next_bd;
    uint32_t next_bd_status;
    uint32_t hw_owned;
    uint64_t rxsof_count;
    uint64_t rxeof_count;
    bool rx_packet_open;
    bool reset_attempted;
} DmaStopReport;

/* Initialize and harvest AXI DMA S2MM SG ring buffers. */
int dma_init_s2mm_ring(ChannelRuntime *rt, uint32_t dma_desc_bytes);
int dma_prepare_s2mm_ring(ChannelRuntime *rt, uint32_t dma_desc_bytes);
int dma_start_s2mm_ring(ChannelRuntime *rt);
int dma_harvest_one(ChannelRuntime *rt, uint32_t *slot, uint32_t *actual_bytes);
int dma_requeue_one(ChannelRuntime *rt, uint32_t slot);
int dma_get_bd_snapshot(ChannelRuntime *rt,
                        const uint8_t *software_slot_state,
                        DmaBdSnapshot *out);
bool dma_s2mm_tail_incomplete(const ChannelRuntime *rt);
DmaStopResult dma_stop_s2mm(ChannelRuntime *rt, DmaStopReport *report);

#endif
