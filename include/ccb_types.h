#ifndef CCB_TYPES_H
#define CCB_TYPES_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* Supported data channels in the current FPGA image: ch0, ch1, and ch2. */
#define NUM_CHANNELS 3
#define HIGH_I_CHANNEL_ID 0
#define HIGH_Q_CHANNEL_ID 1
#define LOW_SPEED_CHANNEL_ID 2

/* CPU-visible data-buffer DDR windows are limited by the Linux address map. */
#define CHANNEL_CPU_DDR_BYTES (64ull * 1024ull * 1024ull)

/*
 * Usable DMA/NVMe S2MM ring windows from the FPGA address map. ch0/ch1 are
 * limited in software to the currently reliable low 1 GiB window.
 */
#define CHANNEL0_DDR_BYTES (1ull * 1024ull * 1024ull * 1024ull)
#define CHANNEL1_DDR_BYTES (1ull * 1024ull * 1024ull * 1024ull)
#define CHANNEL2_DDR_BYTES (512ull * 1024ull * 1024ull)
#define CHANNEL0_DDR_BYTES_MAX (2ull * 1024ull * 1024ull * 1024ull)
#define CHANNEL1_DDR_BYTES_MAX (2ull * 1024ull * 1024ull * 1024ull)
#define CHANNEL2_DDR_BYTES_MAX CHANNEL2_DDR_BYTES

/* DMA descriptor payload size defaults and CLI upper bound. */
#define DMA_DESC_BYTES_CH0_DEFAULT (16u * 1024u * 1024u)
#define DMA_DESC_BYTES_CH2_DEFAULT (16u * 1024u * 1024u)
#define DMA_DESC_BYTES_DEFAULT DMA_DESC_BYTES_CH2_DEFAULT
#define DMA_DESC_BYTES_MAX DMA_DESC_BYTES_CH2_DEFAULT

/*
 * Legacy descriptor limits used by the TCP MM2S helper.
 * Storage S2MM channels use ChannelConfig.desc_cpu_size instead.
 */
#define DMA_DESC_ENTRY_BYTES 64u
#define DMA_DESC_BRAM_BYTES 8192u
#define DMA_DESC_COUNT_MAX (DMA_DESC_BRAM_BYTES / DMA_DESC_ENTRY_BYTES)

/* SSD metadata layout constants. */
#define SECTOR_SIZE 512u
#define FILE_ENTRY_SIZE 32u
#define MAX_FILES_TOTAL 128u
#define ENTRIES_PER_SECTOR (SECTOR_SIZE / FILE_ENTRY_SIZE)
#define META_SECTOR_COUNT (MAX_FILES_TOTAL / ENTRIES_PER_SECTOR)
#define METADATA_START_LBA 0ull
#define DATA_START_LBA 1000ull
#define FILE_MAX_SECTORS ((uint64_t)UINT32_MAX)
#define FILE_MAX_BYTES (FILE_MAX_SECTORS * (uint64_t)SECTOR_SIZE)

/* File-list backend, kept compatible with process_test. */
#ifdef _WIN32
#define PROCESS_META_DIR_DEFAULT ".\\process_meta"
#define PATH_SEPARATOR "\\"
#else
#define PROCESS_META_DIR_DEFAULT "/run/ccb_nvme_process_test"
#define PATH_SEPARATOR "/"
#endif

/* Default hardware polling timeout (microseconds). */
#define DEFAULT_TIMEOUT_US 5000000u

typedef enum {
    SOURCE_TRANSFER = 0,
    SOURCE_TEST = 1
} SourceMode;

typedef enum {
    CMD_WRITE = 0,
    CMD_READ = 1,
    CMD_LIST = 2,
    CMD_INIT_META = 3,
    CMD_STORAGE_WRITE = 4,
    CMD_NETWORK_SEND = 5,
    CMD_DDR_PATTERN_STORE = 6,
    CMD_SSD_LBA_WRAP_TEST = 7,
    CMD_SSD_CONTINUOUS_PATTERN_TEST = 8
} CommandType;

/* AXI DMA SG descriptor format (must be exactly 64 bytes). */
typedef struct {
    uint32_t next_desc;
    uint32_t next_desc_msb;
    uint32_t buffer_addr;
    uint32_t buffer_addr_msb;
    uint32_t reserved1[2];
    uint32_t control;
    uint32_t status;
    uint32_t app[5];
    uint32_t reserved2[3];
} __attribute__((packed)) DmaSgDesc;

/* On-disk metadata entry (kept compatible with existing bare-metal format). */
typedef struct {
    char task_no[11];
    uint8_t file_cnt;
    uint8_t file_type;
    uint16_t file_index;
    uint32_t file_size_bytes;
    uint64_t start_lba;
    uint32_t sector_count;
    uint8_t valid;
} __attribute__((packed)) FileEntry;

/* Per-channel static hardware topology and address map. */
typedef struct {
    int id;
    const char *name;
    uint32_t file_type;
    uint64_t dma_base;
    uint64_t axis_switch_base;
    uint64_t nvme_base;
    uint64_t pcie_bridge_base;
    uint64_t desc_cpu_base;
    uint64_t desc_dma_base;
    uint64_t desc_cpu_size;
    uint64_t ddr_cpu_base;
    uint64_t ddr_hw_base;
    uint64_t ddr_cpu_size;
    uint64_t dma_ring_bytes;
    uint64_t dma_ring_bytes_max;
    uint32_t dma_desc_bytes_default;
} ChannelConfig;

/* Generic mapped physical-memory region descriptor. */
typedef struct {
    int fd;
    void *map_base;
    size_t map_len;
    size_t map_off;
    volatile uint8_t *virt;
    uint64_t phys;
    size_t size;
    bool valid;
} MappedRegion;

/* Global switches parsed before subcommand dispatch. */
typedef struct {
    bool dry_run;
    bool skip_link_check;
    uint32_t timeout_us;
} GlobalOptions;

/* Parsed subcommand arguments (presence flags + values). */
typedef struct {
    bool has_lba;
    bool lba_auto;
    uint64_t lba;

    bool has_size;
    uint64_t size_bytes;

    bool has_task_no;
    char task_no[12];

    bool has_file_index;
    uint32_t file_index;

    bool has_proto_file_type;
    uint32_t proto_file_type;

    bool has_calibration_type;
    uint32_t calibration_type;

    bool has_source;
    SourceMode source;

    bool has_dma_desc_bytes;
    uint32_t dma_desc_bytes;

    bool has_ddr_offset;
    uint64_t ddr_offset;

    bool has_channel;
    int channel_id;
    bool channel_all;
} ParsedArgs;

typedef struct {
    uint16_t cid;
    uint16_t status;
    uint8_t status_code;
    bool error;
    uint32_t raw;
} NvmeCompletion;

typedef enum {
    NVME_FEED_MODE_LEGACY = 0,
    NVME_FEED_MODE_TIGHT = 1
} NvmeFeedMode;

/* Runtime context for one channel during command execution. */
typedef struct {
    const ChannelConfig *cfg;
    GlobalOptions gopt;

    MappedRegion dma;
    MappedRegion axis_switch;
    MappedRegion nvme;
    MappedRegion pcie_bridge;
    MappedRegion desc;
    MappedRegion ddr;
    uint64_t pcie_bridge_base_effective;

    uint32_t next_harvest_bd;
    uint16_t next_cmd_id;
    uint32_t nvme_block_size;
    uint32_t nvme_max_dts_bytes;
    uint64_t nvme_max_lba;
    uint32_t nvme_cmd_size_bytes;
    uint32_t nvme_cmd_sectors;
    uint32_t nvme_qd_requested;
    uint32_t nvme_qd_effective;
    uint32_t nvme_feed_mode;
    uint32_t nvme_cq_pop_batch;
    bool nvme_diag_timing;
    bool nvme_skip_const_ctx;
    uint32_t nvme_busy_poll_us;
    uint32_t nvme_poll_sleep_us;
    uint64_t nvme_cmd_count;
    uint64_t nvme_cmd_bytes_total;
    uint64_t nvme_write_bytes_done;
    uint64_t nvme_cmd_latency_total_us;
    uint64_t nvme_cmd_latency_min_us;
    uint64_t nvme_cmd_latency_max_us;
    uint32_t nvme_active_qd_max;
    uint32_t nvme_active_qd_current;
    uint64_t nvme_active_qd_integral_us;
    uint64_t nvme_active_qd_observed_us;
    uint64_t nvme_active_qd_last_update_us;
    uint64_t nvme_submit_calls;
    uint64_t nvme_submit_total_us;
    uint64_t nvme_submit_pending_wait_us;
    uint64_t nvme_submit_sq_full_count;
    uint64_t nvme_submit_stall_count;
    uint64_t nvme_submit_stall_max_us;
    uint64_t nvme_cq_poll_calls;
    uint64_t nvme_cq_empty_polls;
    uint64_t nvme_cq_wait_total_us;
    uint64_t nvme_cq_pop_total_us;
    uint64_t nvme_cq_completed;
    uint64_t nvme_active_qd_event_sum;
    uint64_t nvme_active_qd_event_samples;
    uint32_t nvme_active_qd_event_min;
    uint64_t nvme_refill_count;
    uint64_t nvme_completion_count;
    char nvme_last_error[64];
    uint32_t dma_desc_bytes;
    uint64_t dma_ring_bytes;
    uint32_t dma_desc_count;
    uint32_t dma_hw_desc_count;
    uint32_t dma_last_completed_bd;
    uint32_t dma_last_completed_status;
    uint64_t dma_rxsof_count;
    uint64_t dma_rxeof_count;
    bool dma_rx_packet_open;
} ChannelRuntime;

#endif
