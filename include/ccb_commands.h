#ifndef CCB_COMMANDS_H
#define CCB_COMMANDS_H

#include "ccb_types.h"

typedef struct {
    int channel_id;
    uint32_t metadata_slot;
    uint64_t start_lba;
    uint64_t sector_count;
    uint64_t size_bytes;
    uint32_t file_index;
    bool data_persisted;
    bool receive_integrity_ok;
    bool storage_integrity_ok;
    bool integrity_ok;
    bool dma_stop_recovered;
    uint64_t dma_received_bytes;
    uint64_t nvme_completed_bytes;
    uint64_t file_bytes;
    uint64_t expected_bytes;
    bool expected_available;
    uint64_t dma_bd_exhaustion_count;
    uint64_t dma_error_count;
    uint64_t descriptor_error_count;
    uint32_t min_dma_writable;
    uint32_t max_completed_unharvested;
    uint64_t max_occupied_bytes_est;
    uint64_t writer_schedule_gap_max_us;
    uint64_t submit_stall_max_us;
    char integrity_risk[64];
    char task_no[12];
} WriteResult;

/* Execute one write acquisition + SSD persist flow. */
int execute_write(const ParsedArgs *args, GlobalOptions gopt);
int execute_write_with_result(const ParsedArgs *args, GlobalOptions gopt, WriteResult *result);
int execute_ddr_pattern_store_with_result(const ParsedArgs *args, GlobalOptions gopt, WriteResult *result);
int execute_ssd_lba_wrap_test(const ParsedArgs *args, GlobalOptions gopt);
int execute_ssd_continuous_pattern_test(const ParsedArgs *args, GlobalOptions gopt);
int execute_dma_rx_benchmark(const ParsedArgs *args, GlobalOptions gopt);
void storage_write_reset_stop(void);
void storage_write_request_stop(void);

/* Read one file (by metadata key or explicit LBA) back to DDR. */
int execute_read(const ParsedArgs *args, GlobalOptions gopt);

/* Print metadata entries for one channel or all channels. */
int execute_list(const ParsedArgs *args, GlobalOptions gopt);

/* Explicitly clear metadata area for a channel. */
int execute_init_meta(const ParsedArgs *args, GlobalOptions gopt);

#endif
