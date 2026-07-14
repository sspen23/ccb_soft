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
    /* Payload bytes are the valid data bytes.  Media bytes include sector
     * padding and are kept separate so integrity checks never compare a
     * padded write with the received payload. */
    uint64_t nvme_completed_bytes;
    uint64_t nvme_media_bytes;
    uint64_t nvme_padding_bytes;
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

typedef enum {
    STORAGE_CROSS_SLOT_WRITER_CONTINUE = 0,
    STORAGE_CROSS_SLOT_WRITER_WAIT_FOR_QUEUE,
    STORAGE_CROSS_SLOT_WRITER_DRAINED,
    STORAGE_CROSS_SLOT_WRITER_QUEUE_ERROR
} StorageCrossSlotWriterDecision;

/* Pure decision helper used by the locked writer termination check and host
 * lifecycle tests.  Only DRAINED permits STOP_DRAINED and thread exit. */
StorageCrossSlotWriterDecision storage_cross_slot_writer_decide(
    bool producer_done, uint32_t queue_count, bool queue_error,
    uint32_t engine_active, uint32_t engine_inflight);

/* Read one file (by metadata key or explicit LBA) back to DDR. */
int execute_read(const ParsedArgs *args, GlobalOptions gopt);

/* Print metadata entries for one channel or all channels. */
int execute_list(const ParsedArgs *args, GlobalOptions gopt);

/* Explicitly clear metadata area for a channel. */
int execute_init_meta(const ParsedArgs *args, GlobalOptions gopt);

#endif
