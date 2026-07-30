#include "ccb_cli.h"
#include "ccb_config.h"
#include "storage_config.h"

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Parse either literal "auto" or an integer with base auto-detection. */
static int parse_u64_auto(const char *s, bool *is_auto, uint64_t *value) {
    char *end = 0;
    if (strcmp(s, "auto") == 0) {
        *is_auto = true;
        *value = 0;
        return 0;
    }
    errno = 0;
    *value = strtoull(s, &end, 0);
    if (errno != 0 || end == s || *end != '\0') {
        return -1;
    }
    *is_auto = false;
    return 0;
}

/* Parse channel argument: single channel id or "all" for list command. */
static int parse_channel_arg(const char *s, bool *is_all, int *id) {
    char *end = 0;
    long v;
    if (strcmp(s, "all") == 0) {
        *is_all = true;
        *id = -1;
        return 0;
    }
    errno = 0;
    v = strtol(s, &end, 10);
    if (errno != 0 || end == s || *end != '\0' ||
        (v != HIGH_I_CHANNEL_ID &&
         v != HIGH_Q_CHANNEL_ID &&
         v != LOW_SPEED_CHANNEL_ID)) {
        return -1;
    }
    *is_all = false;
    *id = (int)v;
    return 0;
}

/* Common check used by validators that require --channel. */
static int ensure_channel_required(const ParsedArgs *args, bool allow_all) {
    if (!args->has_channel) {
        fprintf(stderr, "Missing required option: --channel\n");
        return -1;
    }
    if (!allow_all && args->channel_all) {
        fprintf(stderr, "--channel all is only valid for list\n");
        return -1;
    }
    return 0;
}

static int validate_dma_desc_bytes_for_channel(const ParsedArgs *a)
{
    const ChannelConfig *cfg;
    uint32_t max_desc_count;
    uint32_t desc_count;

    if (!a->has_dma_desc_bytes) {
        return 0;
    }
    if ((a->dma_desc_bytes % SECTOR_SIZE) != 0u) {
        fprintf(stderr, "--dma-desc-bytes must be aligned to %u bytes\n", SECTOR_SIZE);
        return -1;
    }
    if (a->dma_desc_bytes > DMA_DESC_BYTES_MAX) {
        fprintf(stderr, "--dma-desc-bytes too large: max is %u bytes\n", (unsigned)DMA_DESC_BYTES_MAX);
        return -1;
    }
    cfg = find_channel(a->channel_id);
    if (!cfg) {
        fprintf(stderr, "Invalid channel: %d\n", a->channel_id);
        return -1;
    }
    if ((cfg->dma_ring_bytes % (uint64_t)a->dma_desc_bytes) != 0u) {
        fprintf(stderr,
                "--dma-desc-bytes must divide channel %d ring size %" PRIu64 "\n",
                cfg->id,
                cfg->dma_ring_bytes);
        return -1;
    }
    desc_count = (uint32_t)(cfg->dma_ring_bytes / (uint64_t)a->dma_desc_bytes);
    max_desc_count = (uint32_t)(cfg->desc_cpu_size / (uint64_t)sizeof(DmaSgDesc));
    if (desc_count == 0u || desc_count > max_desc_count) {
        fprintf(stderr,
                "--dma-desc-bytes creates %u descriptors on channel %d, max is %u\n",
                (unsigned)desc_count,
                cfg->id,
                (unsigned)max_desc_count);
        return -1;
    }
    return 0;
}

static int validate_ddr_offset_for_channel(const ParsedArgs *a,
                                           uint64_t default_size)
{
    const ChannelConfig *cfg;
    uint64_t size;

    if (!a->has_ddr_offset) {
        return 0;
    }
    if ((a->ddr_offset % SECTOR_SIZE) != 0u) {
        fprintf(stderr, "--ddr-offset must be aligned to %u bytes\n", SECTOR_SIZE);
        return -1;
    }
    cfg = a->has_channel ? find_channel(a->channel_id) : find_channel(LOW_SPEED_CHANNEL_ID);
    if (!cfg) {
        fprintf(stderr, "Invalid channel for --ddr-offset\n");
        return -1;
    }
    size = a->has_size ? a->size_bytes : default_size;
    if (a->ddr_offset > cfg->dma_ring_bytes ||
        size > (cfg->dma_ring_bytes - a->ddr_offset)) {
        fprintf(stderr,
                "--ddr-offset range exceeds channel %d ring: offset=%" PRIu64
                " size=%" PRIu64 " ring=%" PRIu64 "\n",
                cfg->id,
                a->ddr_offset,
                size,
                cfg->dma_ring_bytes);
        return -1;
    }
    return 0;
}

void usage(void) {
    fprintf(stderr,
            "Usage:\n"
            "  ccb_nvme_tool [--dry-run] [--skip-link-check] [--timeout-us N] write --channel 0|1|2 --size <bytes> "
            "--task-no <id> --file-index <n> --ssd-lba auto|0x... --source transfer|test [--dma-desc-bytes <bytes>]\n"
            "  ccb_nvme_tool [--dry-run] [--skip-link-check] [--timeout-us N] storage-write --channel 0|1|2 [--size <bytes>] "
            "--task-no <id> --file-index <n> --ssd-lba auto|0x... --source transfer|test --proto-file-type <0|1|2|3> [--calibration-type N] [--dma-desc-bytes <bytes>]\n"
            "  ccb_nvme_tool [--dry-run] [--skip-link-check] [--timeout-us N] ddr-pattern-store [--channel 0|1|2] [--size <bytes>] "
            "--task-no <id> --file-index <n> [--ssd-lba auto|0x...] --proto-file-type <0|1|3> [--calibration-type N] [--ddr-offset <bytes>]\n"
            "  ccb_nvme_tool [--dry-run] [--skip-link-check] [--timeout-us N] ssd-lba-wrap-test "
            "--ssd-lba 0x... [--size <bytes>]  # destructive ch2 test at LBA and LBA+0x100000\n"
            "  ccb_nvme_tool [--dry-run] [--skip-link-check] [--timeout-us N] ssd-continuous-pattern-test "
            "--ssd-lba 0x... [--size <bytes>]  # destructive ch2 continuous SSD write/read test\n"
            "  ccb_nvme_tool [--dry-run] [--skip-link-check] dma-rx-benchmark --channel 0|1|2"
            " --duration-sec N --source transfer|test\n"
            "  ccb_nvme_tool [--dry-run] [--skip-link-check] [--timeout-us N] network-send --task-no <id> --file-index <n> --proto-file-type <0|1|2|3>\n"
            "  ccb_nvme_tool [--dry-run] [--skip-link-check] [--timeout-us N] read --channel 0|1|2 "
            "[--task-no <id> --file-index <n> | --ssd-lba 0x... --size <bytes>] [--ddr-offset <bytes>]\n"
            "  ccb_nvme_tool [--dry-run] [--skip-link-check] [--timeout-us N] list --channel all|0|1|2\n"
            "  ccb_nvme_tool [--dry-run] [--skip-link-check] [--timeout-us N] init-meta --channel all|0|1|2\n"
            "\n"
            "storage-write DMA descriptor defaults: ch0/ch1=%u bytes, ch2=%u bytes\n",
            (unsigned)(8u * 1024u * 1024u),
            (unsigned)DMA_DESC_BYTES_CH2_DEFAULT);
}

int parse_global_options(int *argc, char ***argv, GlobalOptions *out) {
    int i = 1;
    memset(out, 0, sizeof(*out));
    out->timeout_us = DEFAULT_TIMEOUT_US;

    /* Parse only leading global options; stop at first unknown token (command). */
    while (i < *argc) {
        const char *arg = (*argv)[i];
        if (strcmp(arg, "--dry-run") == 0) {
            out->dry_run = true;
            ++i;
            continue;
        }
        if (strcmp(arg, "--skip-link-check") == 0) {
            out->skip_link_check = true;
            ++i;
            continue;
        }
        if (strcmp(arg, "--timeout-us") == 0) {
            char *end = 0;
            unsigned long v;
            if ((i + 1) >= *argc) {
                fprintf(stderr, "Missing value for --timeout-us\n");
                return -1;
            }
            errno = 0;
            v = strtoul((*argv)[i + 1], &end, 0);
            if (errno != 0 || end == (*argv)[i + 1] || *end != '\0' || v == 0u) {
                fprintf(stderr, "Invalid --timeout-us value: %s\n", (*argv)[i + 1]);
                return -1;
            }
            out->timeout_us = (uint32_t)v;
            i += 2;
            continue;
        }
        break;
    }

    *argc -= i;
    *argv += i;
    return 0;
}

int parse_command_type(const char *s, CommandType *type) {
    if (strcmp(s, "write") == 0) {
        *type = CMD_WRITE;
        return 0;
    }
    if (strcmp(s, "read") == 0) {
        *type = CMD_READ;
        return 0;
    }
    if (strcmp(s, "list") == 0) {
        *type = CMD_LIST;
        return 0;
    }
    if (strcmp(s, "init-meta") == 0) {
        *type = CMD_INIT_META;
        return 0;
    }
    if (strcmp(s, "storage-write") == 0) {
        *type = CMD_STORAGE_WRITE;
        return 0;
    }
    if (strcmp(s, "network-send") == 0) {
        *type = CMD_NETWORK_SEND;
        return 0;
    }
    if (strcmp(s, "ddr-pattern-store") == 0) {
        *type = CMD_DDR_PATTERN_STORE;
        return 0;
    }
    if (strcmp(s, "ssd-lba-wrap-test") == 0) {
        *type = CMD_SSD_LBA_WRAP_TEST;
        return 0;
    }
    if (strcmp(s, "ssd-continuous-pattern-test") == 0) {
        *type = CMD_SSD_CONTINUOUS_PATTERN_TEST;
        return 0;
    }
    if (strcmp(s, "dma-rx-benchmark") == 0) {
        *type = CMD_DMA_RX_BENCHMARK;
        return 0;
    }
    return -1;
}

int parse_subcommand_args(int argc, char **argv, ParsedArgs *out) {
    int c;
    static struct option long_opts[] = {
        {"channel", required_argument, 0, 'c'},
        {"size", required_argument, 0, 's'},
        {"task-no", required_argument, 0, 'T'},
        {"file-index", required_argument, 0, 'i'},
        {"ssd-lba", required_argument, 0, 'l'},
        {"source", required_argument, 0, 'S'},
        {"dma-desc-bytes", required_argument, 0, 'b'},
        {"ddr-offset", required_argument, 0, 'O'},
        {"proto-file-type", required_argument, 0, 'p'},
        {"calibration-type", required_argument, 0, 'C'},
        {"duration-sec", required_argument, 0, 'd'},
        {0, 0, 0, 0},
    };

    memset(out, 0, sizeof(*out));
    opterr = 0;
    optind = 1;

    /* Long options are the public CLI contract for all subcommands. */
    while ((c = getopt_long(argc, argv, "c:s:T:i:l:S:b:O:p:C:d:", long_opts, 0)) != -1) {
        switch (c) {
        case 'c':
            out->has_channel = true;
            if (parse_channel_arg(optarg, &out->channel_all, &out->channel_id) != 0) {
                fprintf(stderr, "Invalid --channel value: %s\n", optarg);
                return -1;
            }
            break;
        case 's': {
            char *end = 0;
            uint64_t v;
            errno = 0;
            v = strtoull(optarg, &end, 0);
            if (errno != 0 || end == optarg || *end != '\0' || v == 0u) {
                fprintf(stderr, "Invalid --size value: %s\n", optarg);
                return -1;
            }
            out->has_size = true;
            out->size_bytes = v;
            break;
        }
        case 'T':
            out->has_task_no = true;
            strncpy(out->task_no, optarg, sizeof(out->task_no) - 1u);
            out->task_no[sizeof(out->task_no) - 1u] = '\0';
            break;
        case 'i': {
            char *end = 0;
            unsigned long v;
            errno = 0;
            v = strtoul(optarg, &end, 0);
            if (errno != 0 || end == optarg || *end != '\0') {
                fprintf(stderr, "Invalid --file-index value: %s\n", optarg);
                return -1;
            }
            out->has_file_index = true;
            out->file_index = (uint32_t)v;
            break;
        }
        case 'l':
            out->has_lba = true;
            if (parse_u64_auto(optarg, &out->lba_auto, &out->lba) != 0) {
                fprintf(stderr, "Invalid --ssd-lba value: %s\n", optarg);
                return -1;
            }
            break;
        case 'S':
            out->has_source = true;
            if (strcmp(optarg, "transfer") == 0) {
                out->source = SOURCE_TRANSFER;
            } else if (strcmp(optarg, "test") == 0) {
                out->source = SOURCE_TEST;
            } else {
                fprintf(stderr, "Invalid --source value: %s\n", optarg);
                return -1;
            }
            break;
        case 'b': {
            char *end = 0;
            unsigned long v;
            errno = 0;
            v = strtoul(optarg, &end, 0);
            if (errno != 0 || end == optarg || *end != '\0' || v == 0u || v > 0xFFFFFFFFu) {
                fprintf(stderr, "Invalid --dma-desc-bytes value: %s\n", optarg);
                return -1;
            }
            out->has_dma_desc_bytes = true;
            out->dma_desc_bytes = (uint32_t)v;
            break;
        }
        case 'O': {
            char *end = 0;
            uint64_t v;
            errno = 0;
            v = strtoull(optarg, &end, 0);
            if (errno != 0 || end == optarg || *end != '\0') {
                fprintf(stderr, "Invalid --ddr-offset value: %s\n", optarg);
                return -1;
            }
            out->has_ddr_offset = true;
            out->ddr_offset = v;
            break;
        }
        case 'p': {
            char *end = 0;
            unsigned long v;
            errno = 0;
            v = strtoul(optarg, &end, 0);
            if (errno != 0 || end == optarg || *end != '\0' || v > 0xFFu) {
                fprintf(stderr, "Invalid --proto-file-type value: %s\n", optarg);
                return -1;
            }
            out->has_proto_file_type = true;
            out->proto_file_type = (uint32_t)v;
            break;
        }
        case 'C': {
            char *end = 0;
            unsigned long v;
            errno = 0;
            v = strtoul(optarg, &end, 0);
            if (errno != 0 || end == optarg || *end != '\0' || v > 0xFFu) {
                fprintf(stderr, "Invalid --calibration-type value: %s\n", optarg);
                return -1;
            }
            out->has_calibration_type = true;
            out->calibration_type = (uint32_t)v;
            break;
        }
        case 'd': {
            char *end = NULL;
            unsigned long v;
            errno = 0;
            v = strtoul(optarg, &end, 0);
            if (errno != 0 || end == optarg || *end != '\0' || v == 0u || v > 86400u) {
                fprintf(stderr, "Invalid --duration-sec value: %s\n", optarg);
                return -1;
            }
            out->has_duration_sec = true;
            out->duration_sec = (uint32_t)v;
            break;
        }
        default:
            fprintf(stderr, "Unknown or invalid option\n");
            return -1;
        }
    }

    /* Disallow positional leftovers to keep CLI behavior deterministic. */
    if (optind != argc) {
        fprintf(stderr, "Unexpected trailing arguments\n");
        return -1;
    }
    return 0;
}

int validate_write_args(const ParsedArgs *a) {
    if (ensure_channel_required(a, false) != 0) {
        return -1;
    }
    if (!a->has_size || !a->has_task_no || !a->has_file_index || !a->has_lba || !a->has_source) {
        fprintf(stderr, "write requires --channel --size --task-no --file-index --ssd-lba --source\n");
        return -1;
    }
    /* task_no field in metadata is fixed-width 11 bytes. */
    if (strlen(a->task_no) > 11u) {
        fprintf(stderr, "--task-no length must be <= 11\n");
        return -1;
    }
    if ((a->size_bytes % SECTOR_SIZE) != 0u) {
        fprintf(stderr, "--size must be aligned to %u bytes\n", SECTOR_SIZE);
        return -1;
    }
    if (a->size_bytes > FILE_MAX_BYTES) {
        fprintf(stderr, "--size too large: max is %" PRIu64 " bytes\n", FILE_MAX_BYTES);
        return -1;
    }
    if (validate_dma_desc_bytes_for_channel(a) != 0) {
        return -1;
    }
    return 0;
}

int validate_read_args(const ParsedArgs *a) {
    bool by_task;
    bool by_lba;
    if (ensure_channel_required(a, false) != 0) {
        return -1;
    }
    by_task = a->has_task_no && a->has_file_index;
    by_lba = a->has_lba && !a->lba_auto && a->has_size;
    /* Read supports exactly two modes: metadata lookup or direct LBA mode. */
    if (!by_task && !by_lba) {
        fprintf(stderr, "read requires either (--task-no --file-index) or (--ssd-lba --size)\n");
        return -1;
    }
    if (by_lba && (a->size_bytes % SECTOR_SIZE) != 0u) {
        fprintf(stderr, "--size must be aligned to %u bytes\n", SECTOR_SIZE);
        return -1;
    }
    if (validate_ddr_offset_for_channel(a, 0u) != 0) {
        return -1;
    }
    return 0;
}

int validate_list_args(const ParsedArgs *a) {
    return ensure_channel_required(a, true);
}

int validate_init_meta_args(const ParsedArgs *a) {
    return ensure_channel_required(a, true);
}

int validate_storage_write_args(const ParsedArgs *a) {
    if (ensure_channel_required(a, false) != 0) {
        return -1;
    }
    if (!a->has_task_no || !a->has_file_index || !a->has_lba || !a->has_source) {
        fprintf(stderr, "storage-write requires --channel --task-no --file-index --ssd-lba --source\n");
        return -1;
    }
    if (strlen(a->task_no) > 11u) {
        fprintf(stderr, "--task-no length must be <= 11\n");
        return -1;
    }
    if (a->has_size) {
        if ((a->size_bytes % SECTOR_SIZE) != 0u) {
            fprintf(stderr, "--size must be aligned to %u bytes\n", SECTOR_SIZE);
            return -1;
        }
        if (a->size_bytes > FILE_MAX_BYTES) {
            fprintf(stderr, "--size too large: max is %" PRIu64 " bytes\n", FILE_MAX_BYTES);
            return -1;
        }
    } else if (!a->lba_auto) {
        fprintf(stderr, "storage-write without --size requires --ssd-lba auto\n");
        return -1;
    }
    if (!a->has_proto_file_type) {
        fprintf(stderr, "storage-write requires --proto-file-type\n");
        return -1;
    }
    if (validate_dma_desc_bytes_for_channel(a) != 0) {
        return -1;
    }
    return 0;
}

int validate_network_send_args(const ParsedArgs *a) {
    if (!a->has_task_no || !a->has_file_index || !a->has_proto_file_type) {
        fprintf(stderr, "network-send requires --task-no --file-index --proto-file-type\n");
        return -1;
    }
    if (strlen(a->task_no) > 11u) {
        fprintf(stderr, "--task-no length must be <= 11\n");
        return -1;
    }
    return 0;
}

int validate_ddr_pattern_store_args(const ParsedArgs *a) {
    uint64_t size = a->has_size ? a->size_bytes : (32ull * 1024ull * 1024ull);
    const ChannelConfig *cfg = a->has_channel ? find_channel(a->channel_id) : find_channel(LOW_SPEED_CHANNEL_ID);
    const char *raw_env = storage_config_compat_getenv("SRC_REAL_DDR_RAW_STORE");
    bool raw_store = raw_env && raw_env[0] != '\0' && strcmp(raw_env, "0") != 0;
    uint64_t size_limit = cfg ? cfg->dma_ring_bytes : 0u;

    if (!a->has_task_no || !a->has_file_index || !a->has_proto_file_type) {
        fprintf(stderr, "ddr-pattern-store requires --task-no --file-index --proto-file-type\n");
        return -1;
    }
    if (a->has_channel && a->channel_all) {
        fprintf(stderr, "ddr-pattern-store does not support --channel all\n");
        return -1;
    }
    if (a->has_channel && !find_channel(a->channel_id)) {
        fprintf(stderr, "Invalid channel: %d\n", a->channel_id);
        return -1;
    }
    if (!raw_store) {
        fprintf(stderr,
                "ddr-pattern-store requires SRC_REAL_DDR_RAW_STORE=1 because"
                " data DDR is not CPU-accessible\n");
        return -1;
    }
    if (strlen(a->task_no) > 11u) {
        fprintf(stderr, "--task-no length must be <= 11\n");
        return -1;
    }
    if ((size % SECTOR_SIZE) != 0u) {
        fprintf(stderr, "--size must be aligned to %u bytes\n", SECTOR_SIZE);
        return -1;
    }
    if (size == 0u || size > size_limit) {
        fprintf(stderr, "--size must be 1..%" PRIu64 " bytes\n", size_limit);
        return -1;
    }
    if (validate_ddr_offset_for_channel(a, 32ull * 1024ull * 1024ull) != 0) {
        return -1;
    }
    return 0;
}

int validate_ssd_lba_wrap_test_args(const ParsedArgs *a) {
    uint64_t size = a->has_size ? a->size_bytes : (4ull * 1024ull * 1024ull);
    uint64_t aligned = (size + 4095ull) & ~4095ull;
    const ChannelConfig *cfg = find_channel(LOW_SPEED_CHANNEL_ID);
    uint64_t max_size = cfg ? cfg->dma_ring_bytes / 4u : 0u;

    if (!a->has_lba || a->lba_auto) {
        fprintf(stderr, "ssd-lba-wrap-test requires explicit --ssd-lba 0x... and will overwrite test ranges\n");
        return -1;
    }
    if (a->has_channel && (!a->channel_all && a->channel_id != LOW_SPEED_CHANNEL_ID)) {
        fprintf(stderr, "ssd-lba-wrap-test only supports channel 2 DDR\n");
        return -1;
    }
    if (a->has_channel && a->channel_all) {
        fprintf(stderr, "ssd-lba-wrap-test does not support --channel all\n");
        return -1;
    }
    if ((size % SECTOR_SIZE) != 0u) {
        fprintf(stderr, "--size must be aligned to %u bytes\n", SECTOR_SIZE);
        return -1;
    }
    if (size == 0u || aligned > max_size) {
        fprintf(stderr, "--size must fit four buffers in ch2 DDR ring; max=%" PRIu64 " bytes\n",
                max_size);
        return -1;
    }
    return 0;
}

int validate_ssd_continuous_pattern_test_args(const ParsedArgs *a) {
    uint64_t size = a->has_size ? a->size_bytes : (640ull * 1024ull * 1024ull);

    if (!a->has_lba || a->lba_auto) {
        fprintf(stderr, "ssd-continuous-pattern-test requires explicit --ssd-lba 0x... and will overwrite that range\n");
        return -1;
    }
    if (a->has_channel && (!a->channel_all && a->channel_id != LOW_SPEED_CHANNEL_ID)) {
        fprintf(stderr, "ssd-continuous-pattern-test only supports channel 2 DDR\n");
        return -1;
    }
    if (a->has_channel && a->channel_all) {
        fprintf(stderr, "ssd-continuous-pattern-test does not support --channel all\n");
        return -1;
    }
    if ((size % SECTOR_SIZE) != 0u) {
        fprintf(stderr, "--size must be aligned to %u bytes\n", SECTOR_SIZE);
        return -1;
    }
    if (size == 0u) {
        fprintf(stderr, "--size must be > 0\n");
        return -1;
    }
    return 0;
}
