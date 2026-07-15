#ifndef CCB_HW_H
#define CCB_HW_H

#include "ccb_types.h"
#include "ccb_storage_pipeline.h"
#include <stdio.h>

/* Utility conversion helpers. */
uint64_t bytes_to_sectors(uint64_t bytes);
/* nvme_max_lba is kept as an exclusive capacity limit in software. */
bool nvme_lba_range_valid(const ChannelRuntime *rt, uint64_t start_lba,
                          uint64_t sectors);
uint64_t cpu_to_hw_addr(const ChannelConfig *cfg, uint64_t cpu_addr);
int ddr_addr_validate(const ChannelConfig *cfg, uint64_t cpu_addr, uint64_t size);

/* Build/release mapped runtime state for one channel. */
int channel_runtime_open(ChannelRuntime *rt, const ChannelConfig *cfg, GlobalOptions gopt);
void channel_runtime_close(ChannelRuntime *rt);
uint32_t nvme_default_qd_for_channel(int channel_id);
void storage_print_pcie_link_status(ChannelRuntime *rt, const char *reason);

/* Configure data path source and probe NVMe host capabilities. */
void axis_switch_select(ChannelRuntime *rt, SourceMode src);
int nvme_probe(ChannelRuntime *rt);
void nvme_clear_stop_request(void);
void nvme_request_stop(void);
int nvme_stop_requested(void);

/* Submit segmented NVMe read/write covering the requested sector range. */
int nvme_rw(ChannelRuntime *rt, bool is_write, uint64_t lba, uint64_t sectors, uint64_t hw_addr);
/* The return value follows NvmeSubmitResult.  Once the doorbell is written,
 * STOP and timeout are acceptance-unknown, never ordinary cancellation. */
int nvme_submit_command_async(ChannelRuntime *rt, uint8_t opcode,
                              uint16_t cid, uint64_t lba,
                              uint32_t sectors, uint64_t ddr_addr);
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
int nvme_write_slot_qd_payload(ChannelRuntime *rt,
                               uint32_t slot,
                               uint64_t lba,
                               uint64_t sectors,
                               uint64_t payload_bytes,
                               uint64_t hw_addr);
int nvme_write_contiguous_tight_qd(ChannelRuntime *rt,
                                   uint64_t ddr_hw_start,
                                   uint64_t start_lba,
                                   uint64_t bytes,
                                   uint32_t qd);
int nvme_write_contiguous_tight_qd_payload(ChannelRuntime *rt,
                                            uint64_t ddr_hw_start,
                                            uint64_t start_lba,
                                            uint64_t media_bytes,
                                            uint64_t payload_bytes,
                                            uint32_t qd);

typedef struct {
    uint32_t slot;
    uint64_t start_lba;
    uint64_t sectors;
    uint64_t hw_addr;
    uint64_t bytes;
    uint64_t chunk_index;
    uint64_t file_offset;
    uint64_t media_bytes;
} NvmeWriteSlotReq;

typedef int (*NvmeWriteSlotDoneCb)(void *opaque, const NvmeWriteSlotReq *req);
typedef struct NvmeCrossSlotEngine NvmeCrossSlotEngine;
typedef struct {
    uint32_t max_active_slots;
    uint32_t target_qd;
    uint32_t cq_batch;
    uint32_t writer_budget_us;
    uint32_t busy_poll_us;
    uint32_t empty_sleep_us;
    uint64_t no_progress_timeout_us;
} NvmeCrossSlotConfig;
typedef struct {
    uint64_t sq_full_wait_count;
    uint64_t sq_full_wait_max_us;
    uint64_t cq_empty_wait_count;
    uint64_t cq_empty_wait_max_us;
    uint64_t submit_mmio_count;
    uint64_t submit_mmio_max_us;
    uint64_t completion_process_count;
    uint64_t completion_process_max_us;
    uint64_t no_progress_sleep_count;
} NvmeCrossSlotStats;
typedef enum {
    NVME_CROSS_SLOT_RUNNING = 0,
    NVME_CROSS_SLOT_ABORT_REQUESTED,
    NVME_CROSS_SLOT_DRAINING_INFLIGHT,
    NVME_CROSS_SLOT_RESETTING,
    NVME_CROSS_SLOT_QUIESCED,
    NVME_CROSS_SLOT_FAILED
} NvmeCrossSlotState;
/* submit() result contract.  Only ACCEPTED owns the CID normally.  An
 * ACCEPTANCE_UNKNOWN result follows a posted doorbell timeout: the engine
 * retains the CID as inflight and must drain or obtain a confirmed reset
 * before the buffer/CID may be reused. */
typedef enum {
    NVME_SUBMIT_ACCEPTED = 0,
    NVME_SUBMIT_RETRY_SQ_FULL = 1,
    NVME_SUBMIT_NOT_ACCEPTED = -1,
    NVME_SUBMIT_STOPPED_BEFORE_DOORBELL = -2,
    NVME_SUBMIT_ACCEPTANCE_UNKNOWN = -3
} NvmeSubmitResult;
typedef struct {
    int (*submit)(void *opaque, uint16_t cid, uint64_t lba, uint32_t sectors, uint64_t ddr_addr);
    int (*poll_completion)(void *opaque, NvmeCompletion *out);
    uint64_t (*monotonic_us)(void *opaque);
    void (*sleep_us)(void *opaque, uint32_t us);
    void (*yield_cpu)(void *opaque);
    /* Return 0 only after hardware can no longer access submitted DDR buffers. */
    int (*reset)(void *opaque);
} NvmeCrossSlotOps;

NvmeCrossSlotEngine *nvme_cross_slot_engine_create(ChannelRuntime *rt);
NvmeCrossSlotEngine *nvme_cross_slot_engine_create_with_config(
    ChannelRuntime *rt, const NvmeCrossSlotConfig *config);
NvmeCrossSlotEngine *nvme_cross_slot_engine_create_with_ops(ChannelRuntime *rt,
                                                             const NvmeCrossSlotOps *ops, void *opaque);
NvmeCrossSlotEngine *nvme_cross_slot_engine_create_with_ops_config(
    ChannelRuntime *rt, const NvmeCrossSlotConfig *config,
    const NvmeCrossSlotOps *ops, void *opaque);
void nvme_cross_slot_engine_destroy(NvmeCrossSlotEngine *engine);
int nvme_cross_slot_engine_add(NvmeCrossSlotEngine *engine, const NvmeWriteSlotReq *req);
int nvme_cross_slot_engine_step(NvmeCrossSlotEngine *engine, uint32_t budget_us,
                                NvmeWriteSlotDoneCb done_cb, void *opaque);
uint32_t nvme_cross_slot_engine_active(const NvmeCrossSlotEngine *engine);
uint32_t nvme_cross_slot_engine_capacity(const NvmeCrossSlotEngine *engine);
bool nvme_cross_slot_engine_can_accept(const NvmeCrossSlotEngine *engine);
const char *nvme_cross_slot_engine_last_error(const NvmeCrossSlotEngine *engine);
void nvme_cross_slot_engine_get_stats(const NvmeCrossSlotEngine *engine,
                                      NvmeCrossSlotStats *out);
void nvme_cross_slot_engine_request_abort(NvmeCrossSlotEngine *engine,
                                          const char *reason);
int nvme_cross_slot_engine_drain_abort(NvmeCrossSlotEngine *engine,
                                       uint64_t absolute_deadline_us);
uint32_t nvme_cross_slot_engine_inflight(const NvmeCrossSlotEngine *engine);
bool nvme_cross_slot_engine_is_quiesced(const NvmeCrossSlotEngine *engine);
NvmeCrossSlotState nvme_cross_slot_engine_state(const NvmeCrossSlotEngine *engine);

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
    uint64_t curdesc_addr;
    uint64_t taildesc_addr;
    uint32_t next_bd;
    uint32_t next_bd_status;
    uint32_t hw_owned;
    uint64_t rxsof_count;
    uint64_t rxeof_count;
    bool rx_packet_open;
    bool reset_attempted;
    DmaStopResult result;
    bool safe_reset_confirmed;
    uint32_t completed_unharvested;
    char reason[64];
} DmaStopReport;

/* Initialize and harvest AXI DMA S2MM SG ring buffers. */
int dma_init_s2mm_ring(ChannelRuntime *rt, uint32_t dma_desc_bytes);
int dma_prepare_s2mm_ring(ChannelRuntime *rt, uint32_t dma_desc_bytes);
int dma_start_s2mm_ring(ChannelRuntime *rt);
typedef struct {
    uint32_t slot;
    uint32_t actual_bytes;
    uint32_t descriptor_status;
    uint64_t submission_sequence;
} DmaHarvestItem;
int dma_harvest_batch(ChannelRuntime *rt, DmaHarvestItem *items,
                      uint32_t max_items, uint32_t budget_us, uint32_t *out_count);
int dma_harvest_completed_batch(ChannelRuntime *rt, DmaHarvestItem *items,
                                uint32_t max_items, uint32_t *out_count);
int dma_harvest_one(ChannelRuntime *rt, uint32_t *slot, uint32_t *actual_bytes);
int dma_requeue_one(ChannelRuntime *rt, uint32_t slot);
void dma_latch_stop(ChannelRuntime *rt);
uint64_t dma_requeue_after_stop_count(const ChannelRuntime *rt);
int dma_get_bd_snapshot(ChannelRuntime *rt,
                        const uint8_t *software_slot_state,
                        DmaBdSnapshot *out);
/* O(1) normal-path snapshot.  Descriptor memory is deliberately not scanned;
 * callers must pass the queue-maintained six-state counts under its lock. */
int dma_get_bd_snapshot_o1(ChannelRuntime *rt, const StorageSlotCounts *counts,
                           DmaBdSnapshot *out);
bool dma_s2mm_tail_incomplete(const ChannelRuntime *rt);
int dma_quiesce_s2mm(ChannelRuntime *rt, uint64_t deadline_us, DmaStopReport *report);
DmaStopResult dma_quiesce_s2mm_with_state(ChannelRuntime *rt, uint64_t deadline_us,
                                          const uint8_t *software_slot_state,
                                          DmaStopReport *report);
DmaStopResult dma_finalize_stop_s2mm(ChannelRuntime *rt, DmaStopReport *report);
DmaStopResult dma_finalize_stop_s2mm_with_state(ChannelRuntime *rt,
                                                const uint8_t *software_slot_state,
                                                DmaStopReport *report);
DmaStopResult dma_stop_s2mm(ChannelRuntime *rt, DmaStopReport *report);

#endif
