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

typedef enum {
    STORAGE_WRITE_STANDALONE = 0,
    STORAGE_WRITE_SUPERVISED
} StorageWriteMode;

static inline bool storage_write_mode_commits_locally(StorageWriteMode mode)
{
    return mode == STORAGE_WRITE_STANDALONE;
}

/* Execute one write acquisition + SSD persist flow. */
int execute_write(const ParsedArgs *args, GlobalOptions gopt);
int execute_write_with_result(const ParsedArgs *args, GlobalOptions gopt, WriteResult *result);
int execute_write_with_result_mode(const ParsedArgs *args, GlobalOptions gopt,
                                   WriteResult *result, StorageWriteMode mode);
int execute_ddr_pattern_store_with_result(const ParsedArgs *args, GlobalOptions gopt, WriteResult *result);
int execute_ssd_lba_wrap_test(const ParsedArgs *args, GlobalOptions gopt);
int execute_ssd_continuous_pattern_test(const ParsedArgs *args, GlobalOptions gopt);
int execute_dma_rx_benchmark(const ParsedArgs *args, GlobalOptions gopt);
void storage_write_reset_stop(void);
void storage_write_request_stop(void);
void storage_write_flush_deferred_diag(void);
uint32_t storage_cross_slot_active_slots_for_channel(int channel_id);
uint32_t storage_cross_slot_default_target_qd(int channel_id);

/* Read one file (by metadata key or explicit LBA) back to DDR. */
int execute_read(const ParsedArgs *args, GlobalOptions gopt);

/* Print metadata entries for one channel or all channels. */
int execute_list(const ParsedArgs *args, GlobalOptions gopt);

/* Explicitly clear metadata area for a channel. */
int execute_init_meta(const ParsedArgs *args, GlobalOptions gopt);

#endif
