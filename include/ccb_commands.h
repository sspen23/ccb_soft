#ifndef CCB_COMMANDS_H
#define CCB_COMMANDS_H

#include "ccb_types.h"
#include "storage_error.h"
#include "storage_writer.h"

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
    uint64_t dma_observed_bytes;
    uint64_t dma_harvested_payload_bytes;
    uint64_t queued_payload_bytes;
    /* Payload bytes are the valid data bytes.  Media bytes include sector
     * padding and are kept separate so integrity checks never compare a
     * padded write with the received payload. */
    uint64_t nvme_completed_bytes;
    uint64_t nvme_media_bytes;
    uint64_t nvme_padding_bytes;
    uint64_t tail_unqueued_bytes;
    uint64_t completed_unharvested_bytes;
    uint64_t stop_epoch;
    uint64_t stop_request_us;
    uint64_t packet_boundary_us;
    uint64_t dma_quiesced_us;
    uint64_t last_bd_complete_us;
    uint64_t last_bd_harvest_us;
    uint64_t producer_done_us;
    uint64_t writer_drained_us;
    uint64_t final_us;
    uint64_t harvested_bd_count;
    uint64_t queued_slot_count;
    uint64_t completed_slot_count;
    uint64_t recycled_slot_count;
    uint32_t final_ready_count;
    uint32_t final_active_count;
    uint32_t final_global_inflight;
    uint64_t submit_count;
    uint64_t completion_count;
    uint32_t final_completed_unharvested;
    uint32_t final_free_dma_bd;
    uint64_t final_ring_occupied_bytes;
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
    StorageErrorCode primary_error;
    StorageErrorCode secondary_error;
    char integrity_risk[64];
    char secondary_reason[64];
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
bool storage_write_fatal_event_sent(void);

typedef enum {
    STORAGE_CROSS_SLOT_CONFIG_ENABLED = 0,
    STORAGE_CROSS_SLOT_CONFIG_MAX_ACTIVE,
    STORAGE_CROSS_SLOT_CONFIG_TARGET_QD,
    STORAGE_CROSS_SLOT_CONFIG_CQ_BATCH,
    STORAGE_CROSS_SLOT_CONFIG_WRITER_BUDGET_US,
    STORAGE_CROSS_SLOT_CONFIG_BUSY_POLL_US,
    STORAGE_CROSS_SLOT_CONFIG_EMPTY_SLEEP_US,
    STORAGE_CROSS_SLOT_CONFIG_NO_PROGRESS_TIMEOUT_US
} StorageCrossSlotConfigParam;

typedef enum {
    STORAGE_CROSS_SLOT_SOURCE_CHANNEL_NEW = 0,
    STORAGE_CROSS_SLOT_SOURCE_CHANNEL_LEGACY,
    STORAGE_CROSS_SLOT_SOURCE_GLOBAL_NEW,
    STORAGE_CROSS_SLOT_SOURCE_GLOBAL_LEGACY,
    STORAGE_CROSS_SLOT_SOURCE_DEFAULT
} StorageCrossSlotSourceKind;

typedef struct {
    uint32_t value;
    StorageCrossSlotSourceKind source_kind;
    char source_name[96];
    char invalid_source_name[96];
    char fallback_source_name[96];
} StorageCrossSlotResolution;

StorageCrossSlotResolution storage_cross_slot_resolve_config(
    int channel_id, StorageCrossSlotConfigParam param);
const char *storage_cross_slot_source_kind_name(StorageCrossSlotSourceKind kind);
bool storage_cross_slot_enabled_for_channel(int channel_id);
uint32_t storage_cross_slot_active_slots_for_channel(int channel_id);
uint32_t storage_cross_slot_default_target_qd(int channel_id);

/* Read one file (by metadata key or explicit LBA) back to DDR. */
int execute_read(const ParsedArgs *args, GlobalOptions gopt);

/* Print metadata entries for one channel or all channels. */
int execute_list(const ParsedArgs *args, GlobalOptions gopt);

/* Explicitly clear metadata area for a channel. */
int execute_init_meta(const ParsedArgs *args, GlobalOptions gopt);

#endif
