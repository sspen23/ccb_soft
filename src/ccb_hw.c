#include "ccb_hw.h"

#include "debug_uart.h"

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <inttypes.h>
#include <sys/time.h>
#include <stdlib.h>
#include <signal.h>
#include <sched.h>
#include <time.h>

/*
 * Hardware access layer:
 * - Maps channel MMIO/DDR regions from /dev/mem
 * - Configures AXIS switch + AXI DMA S2MM SG
 * - Submits NVMe read/write commands via host registers
 */
#ifdef __linux__
#include <sys/mman.h>
#define HAVE_POSIX_MMAP 1
#else
#define HAVE_POSIX_MMAP 0
#ifndef MAP_FAILED
#define MAP_FAILED ((void *)-1)
#endif
#endif

#define NVME_BUSY_POLL_US_DEFAULT 0u
#define NVME_POLL_SLEEP_US_DEFAULT 10u
#define NVME_SECTOR_BYTES 512u
#define NVME_CMD_KIB_DEFAULT 256u
#define NVME_CMD_KIB_HIGH_DEFAULT 1024u
#define NVME_CMD_KIB_MAX 4096u
#define NVME_QD_DEFAULT 8u
#define NVME_QD_HIGH_DEFAULT 8u
#define NVME_QD_SAFETY_MAX 32u
#define NVME_PENDING_CAPACITY 32u
#define DDR_MMAP_DEFAULT_BYTES (64ull * 1024ull * 1024ull)

#define PAGE_ALIGN_DOWN(v, p) ((v) & ~((uint64_t)((p) - 1u)))
#define PAGE_ALIGN_UP(v, p) (((v) + ((p) - 1u)) & ~((uint64_t)((p) - 1u)))

/* AXI DMA register offsets */
#define MM2S_DMACR 0x00u
#define MM2S_DMASR 0x04u
#define S2MM_DMACR 0x30u
#define S2MM_DMASR 0x34u
#define S2MM_CURDESC 0x38u
#define S2MM_CURDESC_MSB 0x3Cu
#define S2MM_TAILDESC 0x40u
#define S2MM_TAILDESC_MSB 0x44u

#define DMA_CR_RS_BIT (1u << 0)
#define DMA_CR_RESET_BIT (1u << 2)
#define DMA_SR_HALT_BIT (1u << 0)
#define DMA_ERROR_MASK_S2MM 0x00004770u
#define DMA_IRQ_STATUS_MASK 0x00007000u
#define DESC_STS_CMPLT (1u << 31)
#define DESC_STS_ERROR_MASK 0x70000000u
#define DESC_STS_RXSOF (1u << 27)
#define DESC_STS_RXEOF (1u << 26)
#define DESC_STS_LEN_MASK 0x03FFFFFFu

/* AXIS switch register offsets */
#define AXIS_SWITCH_CTRL_REG_OFFSET 0x0000u
#define AXIS_SWITCH_MI0_MUX_OFFSET 0x0040u
#define AXIS_SWITCH_REG_UPDATE (1u << 1)

/* Xilinx/AMD AXI Bridge for PCIe Gen3 PG194 PHY Status/Control register. */
#define PCIE_BRIDGE_PHY_STATUS_CONTROL 0x144u
#define PCIE_BRIDGE_MMAP_BYTES 0x1000u

/* NVMe host register offsets/bits */
#define GENERIC_REG_OFFSET 0x00u
#define GENERIC_NVM_STATUS 0x14u
#define GENERIC_MAXLBA_L 0x18u
#define GENERIC_MAXLBA_H 0x1Cu

#define GENERIC_PCIE_LINK_STATUS (1u << 12)
#define GENERIC_NVME_LINK_STATUS (1u << 8)

#define QUEUE_REG_OFFSET 0x80u
#define QUEUE_INT_STATUS 0x00u
#define QUEUE_CUR_CQ_CID 0x0Cu
#define QUEUE_PRP1_L 0x10u
#define QUEUE_PRP1_H 0x14u
#define QUEUE_CTX0 0x20u
#define QUEUE_CTX1 0x24u
#define QUEUE_LBA_L 0x28u
#define QUEUE_LBA_H 0x2Cu
#define QUEUE_TX_CTRL 0x30u
#define QUEUE_TX_STATUS 0x34u
#define QUEUE_SQ_PTRS 0x38u
#define QUEUE_CQ_PTRS 0x3Cu
#define PERF_CALC_TIMER 0x40u
#define PERF_CALC_CTRL 0x44u

#define QUEUE_TX_CTRL_CMD_PENDING (1u << 0)
#define QUEUE_CQ_FIFO_EMPTY (1u << 2)
#define QUEUE_SQ_FIFO_FULL (1u << 1)
#define PERF_CALC_OVERFLOW (1u << 31)
#define NVME_PERF_CLOCK_PERIOD_NS_DEFAULT 4u
#define NVME_SUBMIT_STALL_US 1000u

#define NVM_WRITE 0x01u
#define NVM_READ 0x02u

#if !HAVE_POSIX_MMAP
#define MAYBE_UNUSED __attribute__((unused))
#else
#define MAYBE_UNUSED
#endif

_Static_assert(sizeof(DmaSgDesc) == DMA_DESC_ENTRY_BYTES, "DmaSgDesc size must be 64 bytes");

static volatile sig_atomic_t g_nvme_stop_requested = 0;

void nvme_clear_stop_request(void) {
    g_nvme_stop_requested = 0;
}

void nvme_request_stop(void) {
    g_nvme_stop_requested = 1;
}

int nvme_stop_requested(void) {
    return g_nvme_stop_requested != 0;
}

static const char *nvme_opcode_name(uint8_t opcode) {
    switch (opcode) {
    case NVM_WRITE:
        return "write";
    case NVM_READ:
        return "read";
    default:
        return "unknown";
    }
}

/* Simple 32-bit MMIO helpers used by all register operations. */
static inline uint32_t reg_read32(const MappedRegion *r, uint32_t off) {
    return *(volatile uint32_t *)(r->virt + off);
}

static inline void reg_write32(const MappedRegion *r, uint32_t off, uint32_t val) {
    *(volatile uint32_t *)(r->virt + off) = val;
}

static uint64_t wall_time_us(void) {
    struct timespec ts;
#ifdef CLOCK_MONOTONIC_RAW
    if (clock_gettime(CLOCK_MONOTONIC_RAW, &ts) != 0 &&
        clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
#else
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
#endif
        return 0u;
    }
    return ((uint64_t)ts.tv_sec * 1000000ull) + (uint64_t)ts.tv_nsec / 1000ull;
}

static int hw_storage_log_at_least_debug(void)
{
    const char *level = getenv("SRC_REAL_LOG_LEVEL");

    return level && (strcmp(level, "debug") == 0 || strcmp(level, "trace") == 0);
}

static int hw_storage_log_trace(void)
{
    const char *level = getenv("SRC_REAL_LOG_LEVEL");
    return level && strcmp(level, "trace") == 0;
}

static uint32_t elapsed_us_since(uint64_t start_us) {
    uint64_t now_us = wall_time_us();
    uint64_t diff;

    if (now_us <= start_us) {
        return 0u;
    }
    diff = now_us - start_us;
    if (diff > UINT32_MAX) {
        return UINT32_MAX;
    }
    return (uint32_t)diff;
}

static uint64_t env_u64_limit(const char *name, uint64_t fallback, uint64_t max_value) {
    const char *v = getenv(name);
    char *end = NULL;
    unsigned long long parsed;

    if (!v || v[0] == '\0') {
        return fallback;
    }
    errno = 0;
    parsed = strtoull(v, &end, 0);
    if (errno != 0 || end == v || *end != '\0' || parsed == 0ull) {
        return fallback;
    }
    if (parsed > max_value) {
        parsed = max_value;
    }
    return (uint64_t)parsed;
}

static int hw_env_flag_enabled(const char *name) {
    const char *v = getenv(name);

    if (!v || v[0] == '\0' || strcmp(v, "0") == 0 ||
        strcmp(v, "false") == 0 || strcmp(v, "FALSE") == 0 ||
        strcmp(v, "no") == 0 || strcmp(v, "NO") == 0) {
        return 0;
    }
    return 1;
}

static int env_u64_exact(const char *name, uint64_t *out) {
    const char *v = getenv(name);
    char *end = NULL;
    unsigned long long parsed;

    if (!v || v[0] == '\0') {
        return 0;
    }
    errno = 0;
    parsed = strtoull(v, &end, 0);
    if (errno != 0 || end == v || *end != '\0') {
        fprintf(stderr, "warning: invalid %s=%s; ignoring PCIe bridge override\n", name, v);
        return -1;
    }
    *out = (uint64_t)parsed;
    return 1;
}

static uint64_t pcie_bridge_base_for_channel(const ChannelConfig *cfg) {
    char name[64];
    uint64_t base = 0u;
    int rc;

    if (!cfg) {
        return 0u;
    }
    snprintf(name, sizeof(name), "SRC_REAL_PCIE_BRIDGE_BASE_CH%d", cfg->id);
    rc = env_u64_exact(name, &base);
    if (rc > 0) {
        return base;
    }
    return cfg->pcie_bridge_base;
}

static const char *pcie_link_speed_name(uint32_t phy_reg) {
    bool link_up = (phy_reg & (1u << 11u)) != 0u;
    bool gen3 = (phy_reg & (1u << 12u)) != 0u;
    bool gen2 = (phy_reg & (1u << 0u)) != 0u;

    if (!link_up) {
        return "down";
    }
    if (gen3) {
        return "Gen3_8GT";
    }
    if (gen2) {
        return "Gen2_5GT";
    }
    return "Gen1_2p5GT";
}

static const char *pcie_link_width_name(uint32_t phy_reg) {
    uint32_t width_code;

    if ((phy_reg & (1u << 13u)) != 0u) {
        return "x16";
    }
    width_code = (phy_reg >> 1u) & 0x3u;
    switch (width_code) {
    case 0u:
        return "x1";
    case 1u:
        return "x2";
    case 2u:
        return "x4";
    case 3u:
        return "x8";
    default:
        return "unknown";
    }
}

void storage_print_pcie_link_status(ChannelRuntime *rt, const char *reason) {
    uint64_t base;
    uint32_t phy_reg;
    uint32_t link_up;
    uint32_t ltssm;

    if (!rt || !rt->cfg) {
        return;
    }
    if (!reason || reason[0] == '\0') {
        reason = "unknown";
    }
    base = rt->pcie_bridge_base_effective;
    if (base == 0u) {
        printf("pcie_link_status channel=%d reason=%s error=no_bridge_base\n",
               rt->cfg->id,
               reason);
        fflush(stdout);
        return;
    }
    if (!rt->pcie_bridge.valid) {
        printf("pcie_link_status channel=%d reason=%s bridge_base=0x%08" PRIx64
               " error=read_failed\n",
               rt->cfg->id,
               reason,
               base);
        fflush(stdout);
        return;
    }

    phy_reg = reg_read32(&rt->pcie_bridge, PCIE_BRIDGE_PHY_STATUS_CONTROL);
    link_up = (phy_reg >> 11u) & 0x1u;
    ltssm = (phy_reg >> 3u) & 0x3fu;
    printf("pcie_link_status channel=%d reason=%s bridge_base=0x%08" PRIx64
           " phy_reg=0x%08x link_up=%u speed=%s width=%s ltssm=0x%02x\n",
           rt->cfg->id,
           reason,
           base,
           phy_reg,
           link_up,
           pcie_link_speed_name(phy_reg),
           pcie_link_width_name(phy_reg),
           ltssm);
    fflush(stdout);
}

static bool nvme_cmd_kib_allowed(uint32_t kib) {
    return kib == 256u || kib == 512u || kib == 1024u || kib == 2048u || kib == 4096u;
}

static uint32_t channel_default_nvme_cmd_kib(const ChannelRuntime *rt) {
    if (rt && (rt->cfg->id == HIGH_I_CHANNEL_ID || rt->cfg->id == HIGH_Q_CHANNEL_ID)) {
        return NVME_CMD_KIB_HIGH_DEFAULT;
    }
    return NVME_CMD_KIB_DEFAULT;
}

static uint32_t channel_default_nvme_qd(const ChannelRuntime *rt) {
    if (rt && (rt->cfg->id == HIGH_I_CHANNEL_ID || rt->cfg->id == HIGH_Q_CHANNEL_ID)) {
        return NVME_QD_HIGH_DEFAULT;
    }
    return NVME_QD_DEFAULT;
}

static const char *channel_env(const ChannelRuntime *rt, const char *prefix, char *buf, size_t buf_size) {
    if (!rt || !prefix || !buf || buf_size == 0u) {
        return NULL;
    }
    snprintf(buf, buf_size, "%s_CH%d", prefix, rt->cfg->id);
    return getenv(buf);
}

static uint32_t parse_channel_u32_env(ChannelRuntime *rt,
                                      const char *prefix,
                                      uint32_t fallback,
                                      uint32_t max_value) {
    char channel_name[64];
    const char *env_name = prefix;
    const char *env = channel_env(rt, prefix, channel_name, sizeof(channel_name));
    char *end = NULL;
    unsigned long parsed;

    if (env && env[0] != '\0') {
        env_name = channel_name;
    } else {
        env = getenv(prefix);
        env_name = prefix;
    }
    if (!env || env[0] == '\0') {
        return fallback;
    }

    errno = 0;
    parsed = strtoul(env, &end, 0);
    if (errno != 0 || end == env || *end != '\0' || parsed > max_value) {
        fprintf(stderr,
                "warning: invalid %s=%s; fallback=%u max=%u\n",
                env_name,
                env,
                fallback,
                max_value);
        return fallback;
    }
    return (uint32_t)parsed;
}

static uint64_t parse_storage_ring_bytes(ChannelRuntime *rt) {
    char channel_name[64];
    const char *env_name = "SRC_REAL_STORAGE_RING_BYTES";
    const char *env = channel_env(rt, env_name, channel_name, sizeof(channel_name));
    uint64_t fallback = rt->cfg->dma_ring_bytes;
    uint64_t max_ring_bytes = rt->cfg->dma_ring_bytes_max;
    uint64_t ring_bytes = fallback;
    char *end = NULL;
    unsigned long long parsed;

    if (env && env[0] != '\0') {
        env_name = channel_name;
    } else {
        env = getenv("SRC_REAL_STORAGE_RING_BYTES");
        env_name = "SRC_REAL_STORAGE_RING_BYTES";
    }
    if (rt->cfg->id == HIGH_I_CHANNEL_ID || rt->cfg->id == HIGH_Q_CHANNEL_ID) {
        max_ring_bytes = fallback;
    }
    if (env && env[0] != '\0') {
        errno = 0;
        parsed = strtoull(env, &end, 0);
        if (errno != 0 || end == env || *end != '\0' || parsed == 0ull ||
            (parsed % SECTOR_SIZE) != 0ull) {
            fprintf(stderr,
                    "warning: invalid %s=%s; fallback=%" PRIu64 " max=%" PRIu64 "\n",
                    env_name,
                    env,
                    fallback,
                    max_ring_bytes);
        } else if (parsed > max_ring_bytes) {
            fprintf(stderr,
                    "warning: %s=%s exceeds supported ring window on channel %d;"
                    " clamp=%" PRIu64 " max=%" PRIu64 "\n",
                    env_name,
                    env,
                    rt->cfg->id,
                    fallback,
                    max_ring_bytes);
            ring_bytes = fallback;
        } else {
            ring_bytes = (uint64_t)parsed;
        }
    }

    printf("storage_ring_config channel=%d ring_bytes=%" PRIu64
           " default_ring_bytes=%" PRIu64 " max_ring_bytes=%" PRIu64
           " source=%s\n",
           rt->cfg->id,
           ring_bytes,
           fallback,
           max_ring_bytes,
           ring_bytes == fallback ? "default" : env_name);
    fflush(stdout);
    return ring_bytes;
}

static uint32_t parse_nvme_cmd_kib(ChannelRuntime *rt) {
    char channel_name[48];
    const char *env_name = "SRC_REAL_NVME_CMD_KIB";
    const char *env = channel_env(rt, env_name, channel_name, sizeof(channel_name));
    uint32_t fallback = channel_default_nvme_cmd_kib(rt);
    char *end = NULL;
    unsigned long parsed;

    if (env && env[0] != '\0') {
        env_name = channel_name;
    } else {
        env = getenv("SRC_REAL_NVME_CMD_KIB");
        env_name = "SRC_REAL_NVME_CMD_KIB";
    }
    if (!env || env[0] == '\0') {
        return fallback;
    }
    errno = 0;
    parsed = strtoul(env, &end, 0);
    if (errno != 0 || end == env || *end != '\0' || parsed > UINT32_MAX ||
        !nvme_cmd_kib_allowed((uint32_t)parsed)) {
        fprintf(stderr,
                "warning: invalid %s=%s; fallback=%u\n",
                env_name,
                env,
                fallback);
        return fallback;
    }
    return (uint32_t)parsed;
}

static uint32_t parse_nvme_qd(ChannelRuntime *rt) {
    char channel_name[48];
    const char *env_name = "SRC_REAL_NVME_QD";
    const char *env = channel_env(rt, env_name, channel_name, sizeof(channel_name));
    uint32_t fallback = channel_default_nvme_qd(rt);
    char *end = NULL;
    unsigned long parsed;

    if (env && env[0] != '\0') {
        env_name = channel_name;
    } else {
        env = getenv("SRC_REAL_NVME_QD");
        env_name = "SRC_REAL_NVME_QD";
    }
    if (!env || env[0] == '\0') {
        return fallback;
    }
    errno = 0;
    parsed = strtoul(env, &end, 0);
    if (errno != 0 || end == env || *end != '\0' || parsed == 0u ||
        parsed > NVME_QD_SAFETY_MAX ||
        (parsed != 1u && parsed != 2u && parsed != 4u && parsed != 8u &&
         parsed != 16u && parsed != 32u)) {
        fprintf(stderr,
                "warning: invalid %s=%s; fallback=%u\n",
                env_name,
                env,
                fallback);
        return fallback;
    }
    return (uint32_t)parsed;
}

static const char *nvme_feed_mode_name(uint32_t mode) {
    return mode == NVME_FEED_MODE_TIGHT ? "tight" : "legacy";
}

static uint32_t parse_nvme_feed_mode(ChannelRuntime *rt) {
    char channel_name[64];
    const char *env_name = "SRC_REAL_NVME_FEED_MODE";
    const char *env = channel_env(rt, env_name, channel_name, sizeof(channel_name));

    if (env && env[0] != '\0') {
        env_name = channel_name;
    } else {
        env = getenv("SRC_REAL_NVME_FEED_MODE");
        env_name = "SRC_REAL_NVME_FEED_MODE";
    }
    if (!env || env[0] == '\0' || strcmp(env, "legacy") == 0) {
        return NVME_FEED_MODE_LEGACY;
    }
    if (strcmp(env, "tight") == 0) {
        return NVME_FEED_MODE_TIGHT;
    }
    fprintf(stderr, "warning: invalid %s=%s; fallback=legacy\n", env_name, env);
    return NVME_FEED_MODE_LEGACY;
}

static void nvme_configure_runtime(ChannelRuntime *rt) {
    uint32_t requested_kib = parse_nvme_cmd_kib(rt);
    uint64_t requested_bytes = (uint64_t)requested_kib * 1024ull;
    uint64_t safe_limit = (uint64_t)NVME_CMD_KIB_MAX * 1024ull;
    uint64_t effective_bytes;

    if (rt->nvme_max_dts_bytes > 0u && safe_limit > rt->nvme_max_dts_bytes) {
        safe_limit = rt->nvme_max_dts_bytes;
    }
    safe_limit -= safe_limit % NVME_SECTOR_BYTES;
    if (safe_limit < NVME_SECTOR_BYTES) {
        safe_limit = NVME_SECTOR_BYTES;
    }

    effective_bytes = requested_bytes;
    if (effective_bytes > safe_limit) {
        uint32_t candidate_kib;

        effective_bytes = safe_limit;
        for (candidate_kib = NVME_CMD_KIB_DEFAULT; candidate_kib <= NVME_CMD_KIB_MAX;
             candidate_kib *= 2u) {
            uint64_t candidate_bytes = (uint64_t)candidate_kib * 1024ull;
            if (candidate_bytes > safe_limit || candidate_bytes > requested_bytes) {
                break;
            }
            effective_bytes = candidate_bytes;
        }
        fprintf(stderr,
                "warning: SRC_REAL_NVME_CMD_KIB=%u exceeds NVMe safe limit=%" PRIu64
                " bytes (max_dts=%u); effective=%" PRIu64 " bytes\n",
                requested_kib,
                safe_limit,
                (unsigned)rt->nvme_max_dts_bytes,
                effective_bytes);
    }

    rt->nvme_cmd_size_bytes = (uint32_t)effective_bytes;
    rt->nvme_cmd_sectors = rt->nvme_cmd_size_bytes / NVME_SECTOR_BYTES;
    rt->nvme_qd_requested = parse_nvme_qd(rt);
    rt->nvme_qd_effective = rt->nvme_qd_requested;
    if (rt->nvme_qd_effective > NVME_QD_SAFETY_MAX) {
        rt->nvme_qd_effective = NVME_QD_SAFETY_MAX;
    }
    rt->nvme_feed_mode = parse_nvme_feed_mode(rt);
    rt->nvme_cq_pop_batch = parse_channel_u32_env(rt,
                                                  "SRC_REAL_NVME_CQ_POP_BATCH",
                                                  1u,
                                                  NVME_QD_SAFETY_MAX);
    if (rt->nvme_cq_pop_batch == 0u) {
        rt->nvme_cq_pop_batch = 1u;
    }
    rt->nvme_diag_timing = hw_env_flag_enabled("SRC_REAL_NVME_DIAG_TIMING") != 0;
    rt->nvme_skip_const_ctx = hw_env_flag_enabled("SRC_REAL_NVME_SKIP_CONST_CTX") != 0;
    rt->nvme_busy_poll_us = parse_channel_u32_env(rt,
                                                  "SRC_REAL_NVME_BUSY_POLL_US",
                                                  NVME_BUSY_POLL_US_DEFAULT,
                                                  1000000u);
    rt->nvme_poll_sleep_us = parse_channel_u32_env(rt,
                                                   "SRC_REAL_NVME_POLL_SLEEP_US",
                                                   NVME_POLL_SLEEP_US_DEFAULT,
                                                   1000000u);
    printf("nvme_storage_config channel=%d nvme_cmd_size_requested_bytes=%" PRIu64
           " nvme_cmd_size_bytes=%u nvme_max_dts_bytes=%u"
           " nvme_qd_requested=%u nvme_qd_effective=%u"
           " nvme_qd_limit_reason=%s\n",
           rt->cfg->id,
           requested_bytes,
           (unsigned)rt->nvme_cmd_size_bytes,
           (unsigned)rt->nvme_max_dts_bytes,
           (unsigned)rt->nvme_qd_requested,
           (unsigned)rt->nvme_qd_effective,
           rt->nvme_qd_requested > NVME_QD_SAFETY_MAX
               ? "code_safety_limit_32"
               : "none_sq_fifo_full_runtime_guard");
    printf("nvme_poll_config channel=%d busy_poll_us=%u poll_sleep_us=%u\n",
           rt->cfg->id,
           (unsigned)rt->nvme_busy_poll_us,
           (unsigned)rt->nvme_poll_sleep_us);
    printf("nvme_scheduler_config channel=%d mode=%s qd=%u cmd_size=%u"
           " max_dts=%u feed_window=%u\n",
           rt->cfg->id,
           nvme_feed_mode_name(rt->nvme_feed_mode),
           (unsigned)rt->nvme_qd_effective,
           (unsigned)rt->nvme_cmd_size_bytes,
           (unsigned)rt->nvme_max_dts_bytes,
           (unsigned)rt->nvme_qd_effective);
    fflush(stdout);
}

static void atomic_update_min_u64(uint64_t *target, uint64_t value) {
    uint64_t current = __atomic_load_n(target, __ATOMIC_RELAXED);

    while ((current == 0u || value < current) &&
           !__atomic_compare_exchange_n(target,
                                        &current,
                                        value,
                                        false,
                                        __ATOMIC_RELAXED,
                                        __ATOMIC_RELAXED)) {
    }
}

static void atomic_update_max_u64(uint64_t *target, uint64_t value) {
    uint64_t current = __atomic_load_n(target, __ATOMIC_RELAXED);

    while (value > current &&
           !__atomic_compare_exchange_n(target,
                                        &current,
                                        value,
                                        false,
                                        __ATOMIC_RELAXED,
                                        __ATOMIC_RELAXED)) {
    }
}

static void nvme_set_last_error(ChannelRuntime *rt, const char *reason) {
    if (rt && reason) {
        (void)snprintf(rt->nvme_last_error, sizeof(rt->nvme_last_error), "%s", reason);
    }
}

static void nvme_record_submit_stall(ChannelRuntime *rt, uint64_t submit_us) {
    if (!rt || submit_us <= NVME_SUBMIT_STALL_US) {
        return;
    }
    (void)__atomic_add_fetch(&rt->nvme_submit_stall_count, 1u, __ATOMIC_RELAXED);
    atomic_update_max_u64(&rt->nvme_submit_stall_max_us, submit_us);
}

static void nvme_poll_pause(ChannelRuntime *rt, uint64_t start_us) {
    uint32_t waited_us;

    if (!rt) {
        usleep(NVME_POLL_SLEEP_US_DEFAULT);
        return;
    }
    waited_us = elapsed_us_since(start_us);
    if (waited_us < rt->nvme_busy_poll_us) {
        return;
    }
    if (rt->nvme_poll_sleep_us == 0u) {
        return;
    }
    usleep(rt->nvme_poll_sleep_us);
}

void nvme_reset_sw_timing(ChannelRuntime *rt) {
    if (!rt) {
        return;
    }
    rt->nvme_submit_calls = 0u;
    rt->nvme_submit_total_us = 0u;
    rt->nvme_submit_pending_wait_us = 0u;
    rt->nvme_submit_sq_full_count = 0u;
    rt->nvme_submit_stall_count = 0u;
    rt->nvme_submit_stall_max_us = 0u;
    rt->nvme_cq_poll_calls = 0u;
    rt->nvme_cq_empty_polls = 0u;
    rt->nvme_cq_wait_total_us = 0u;
    rt->nvme_cq_pop_total_us = 0u;
    rt->nvme_cq_completed = 0u;
    rt->nvme_active_qd_event_sum = 0u;
    rt->nvme_active_qd_event_samples = 0u;
    rt->nvme_active_qd_event_min = UINT32_MAX;
    rt->nvme_refill_count = 0u;
    rt->nvme_completion_count = 0u;
}

void nvme_print_sw_timing_to(FILE *out, const ChannelRuntime *rt) {
    double submit_avg_us = 0.0;
    double pending_avg_us = 0.0;
    double cq_wait_avg_us = 0.0;

    if (!out || !rt) {
        return;
    }
    if (rt->nvme_submit_calls != 0u) {
        submit_avg_us = (double)rt->nvme_submit_total_us / (double)rt->nvme_submit_calls;
        pending_avg_us = (double)rt->nvme_submit_pending_wait_us / (double)rt->nvme_submit_calls;
    }
    if (rt->nvme_cq_completed != 0u) {
        cq_wait_avg_us = (double)rt->nvme_cq_wait_total_us / (double)rt->nvme_cq_completed;
    }
    fprintf(out,
            "nvme_sw_timing channel=%d submit_calls=%" PRIu64
            " submit_total_us=%" PRIu64
            " submit_pending_wait_us=%" PRIu64
            " submit_avg_us=%.3f pending_wait_avg_us=%.3f"
            " cq_completed=%" PRIu64
            " cq_poll_calls=%" PRIu64
            " cq_wait_total_us=%" PRIu64
            " cq_wait_avg_us=%.3f cq_pop_total_us=%" PRIu64
            " cq_empty_polls=%" PRIu64
            " sq_full_count=%" PRIu64 "\n",
            rt->cfg->id,
            rt->nvme_submit_calls,
            rt->nvme_submit_total_us,
            rt->nvme_submit_pending_wait_us,
            submit_avg_us,
            pending_avg_us,
            rt->nvme_cq_completed,
            rt->nvme_cq_poll_calls,
            rt->nvme_cq_wait_total_us,
            cq_wait_avg_us,
            rt->nvme_cq_pop_total_us,
            rt->nvme_cq_empty_polls,
            rt->nvme_submit_sq_full_count);
    fflush(out);
}

void nvme_print_sw_timing(const ChannelRuntime *rt) {
    nvme_print_sw_timing_to(stdout, rt);
}

uint64_t nvme_perf_calc_begin(ChannelRuntime *rt, uint64_t bytes) {
    uint64_t cmd_size;
    uint64_t expected_cmds;

    if (!rt || rt->gopt.dry_run) {
        return 0u;
    }
    cmd_size = rt->nvme_cmd_size_bytes ? rt->nvme_cmd_size_bytes : NVME_SECTOR_BYTES;
    expected_cmds = (bytes + cmd_size - 1u) / cmd_size;
    if (expected_cmds > UINT32_MAX) {
        expected_cmds = UINT32_MAX;
    }
    reg_write32(&rt->nvme, QUEUE_REG_OFFSET + PERF_CALC_CTRL, (uint32_t)expected_cmds);
    return expected_cmds;
}

void nvme_perf_calc_fprint(FILE *out,
                           ChannelRuntime *rt,
                           uint64_t bytes,
                           uint64_t expected_cmds,
                           uint64_t wall_us) {
    uint32_t timer_cycles;
    uint32_t ctrl;
    uint32_t clock_period_ns;
    uint64_t hostcore_total_ns;
    double hostcore_total_ms;
    double hostcore_ns_per_cmd = 0.0;
    double hostcore_mib_s = 0.0;
    double wall_ms;
    double wall_mib_s = 0.0;
    double software_gap_ms = 0.0;

    if (!out || !rt || rt->gopt.dry_run || expected_cmds == 0u) {
        return;
    }
    timer_cycles = reg_read32(&rt->nvme, QUEUE_REG_OFFSET + PERF_CALC_TIMER);
    ctrl = reg_read32(&rt->nvme, QUEUE_REG_OFFSET + PERF_CALC_CTRL);
    clock_period_ns = (uint32_t)env_u64_limit("SRC_REAL_NVME_PERF_CLOCK_PERIOD_NS",
                                              NVME_PERF_CLOCK_PERIOD_NS_DEFAULT,
                                              1000000u);
    hostcore_total_ns = (uint64_t)timer_cycles * (uint64_t)clock_period_ns;
    hostcore_total_ms = (double)hostcore_total_ns / 1000000.0;
    hostcore_ns_per_cmd = (double)hostcore_total_ns / (double)expected_cmds;
    if (hostcore_total_ns != 0u) {
        hostcore_mib_s = ((double)bytes * 1000000000.0) /
                         ((double)hostcore_total_ns * 1048576.0);
    }
    wall_ms = (double)wall_us / 1000.0;
    if (wall_us != 0u) {
        wall_mib_s = ((double)bytes * 1000000.0) / ((double)wall_us * 1048576.0);
    }
    if (wall_ms > hostcore_total_ms) {
        software_gap_ms = wall_ms - hostcore_total_ms;
    }
    fprintf(out,
            "nvme_perf_calc channel=%d expected_cmds=%" PRIu64
            " timer_cycles=%u clock_period_ns=%u overflow=%u"
            " bytes=%" PRIu64
            " hostcore_ns_per_cmd=%.3f hostcore_mib_s=%.3f"
            " wall_ms=%.3f wall_mib_s=%.3f software_gap_ms=%.3f\n",
            rt->cfg->id,
            expected_cmds,
            (unsigned)timer_cycles,
            (unsigned)clock_period_ns,
            (unsigned)((ctrl & PERF_CALC_OVERFLOW) != 0u),
            bytes,
            hostcore_ns_per_cmd,
            hostcore_mib_s,
            wall_ms,
            wall_mib_s,
            software_gap_ms);
    fflush(out);
}

void nvme_perf_calc_print(ChannelRuntime *rt,
                          uint64_t bytes,
                          uint64_t expected_cmds,
                          uint64_t wall_us) {
    nvme_perf_calc_fprint(stdout, rt, bytes, expected_cmds, wall_us);
}

static uint64_t MAYBE_UNUSED channel_ddr_mmap_bytes(const ChannelConfig *cfg) {
    uint64_t fallback = cfg->ddr_cpu_size;

    if (fallback > DDR_MMAP_DEFAULT_BYTES) {
        fallback = DDR_MMAP_DEFAULT_BYTES;
    }
    return env_u64_limit("CCB_DDR_MMAP_BYTES", fallback, cfg->ddr_cpu_size);
}

static int MAYBE_UNUSED map_region_common(int fd,
                                          uint64_t phys,
                                          size_t size,
                                          MappedRegion *out,
                                          bool log_error) {
#if HAVE_POSIX_MMAP
    long page = sysconf(_SC_PAGE_SIZE);
    uint64_t aligned;
    size_t off;
    size_t map_len;
    void *base;

    memset(out, 0, sizeof(*out));
    if (page <= 0) {
        page = 4096;
    }
    /* mmap requires page-aligned offset, keep original offset separately. */
    aligned = PAGE_ALIGN_DOWN(phys, (uint64_t)page);
    off = (size_t)(phys - aligned);
    map_len = (size_t)PAGE_ALIGN_UP(size + off, (uint64_t)page);

    base = mmap(0, map_len, PROT_READ | PROT_WRITE, MAP_SHARED, fd, (off_t)aligned);
    if (base == MAP_FAILED) {
        if (log_error) {
            fprintf(stderr, "mmap failed: phys=0x%016" PRIx64 " size=0x%zx, errno=%d (%s)\n",
                    phys, size, errno, strerror(errno));
        }
        return -1;
    }

    out->fd = fd;
    out->map_base = base;
    out->map_len = map_len;
    out->map_off = off;
    out->virt = (volatile uint8_t *)base + off;
    out->phys = phys;
    out->size = size;
    out->valid = true;
    return 0;
#else
    (void)fd;
    (void)phys;
    (void)size;
    (void)out;
    fprintf(stderr, "mmap backend is only supported on Linux builds\n");
    return -1;
#endif
}

static int MAYBE_UNUSED map_region(int fd, uint64_t phys, size_t size, MappedRegion *out) {
    return map_region_common(fd, phys, size, out, true);
}

static int map_region_optional(int fd, uint64_t phys, size_t size, MappedRegion *out) {
    return map_region_common(fd, phys, size, out, false);
}

static void unmap_region(MappedRegion *r) {
#if HAVE_POSIX_MMAP
    if (r->valid && r->map_base && r->map_len > 0u) {
        munmap(r->map_base, r->map_len);
    }
#endif
    memset(r, 0, sizeof(*r));
}

/* Poll register bits until expected value appears or timeout expires. */
static int wait_reg_bits(const MappedRegion *r, uint32_t off, uint32_t mask, uint32_t expected, uint32_t timeout_us) {
    uint64_t start_us = wall_time_us();
    while (elapsed_us_since(start_us) < timeout_us) {
        uint32_t v = reg_read32(r, off);
        if ((v & mask) == expected) {
            return 0;
        }
        usleep(NVME_POLL_SLEEP_US_DEFAULT);
    }
    return -1;
}

typedef struct {
    bool valid;
    uint16_t cid;
    uint32_t slot;
    uint64_t slot_offset;
    uint64_t lba;
    uint64_t ddr_addr;
    uint32_t sectors;
    uint32_t bytes;
    uint64_t submit_us;
} NvmePendingCmd;

typedef struct {
    uint32_t slot;
    uint64_t base_lba;
    uint64_t base_ddr_addr;
    uint64_t total_bytes;
    uint64_t total_sectors;
    uint64_t next_submit_sector;
    uint32_t submitted_cmds;
    uint32_t completed_cmds;
    uint32_t failed_cmds;
    uint32_t inflight_cmds;
} NvmeSlotWriteContext;

static void nvme_dump_write_timeout_context(ChannelRuntime *rt,
                                            const NvmePendingCmd *pending,
                                            uint32_t capacity,
                                            const NvmeSlotWriteContext *slot_ctx) {
    uint32_t i;
    uint64_t now_us = wall_time_us();

    if (!rt || !pending || !slot_ctx) {
        return;
    }
    printf("nvme_write_timeout_context channel=%d slot=%u base_lba=0x%08" PRIx64
           " base_ddr=0x%08" PRIx64 " total_sectors=%" PRIu64
           " next_submit_sector=%" PRIu64 " submitted=%u completed=%u"
           " inflight=%u failed=%u qd=%u cmd_sectors=%u"
           " tx_status=0x%08x int_status=0x%08x sq_ptrs=0x%08x cq_ptrs=0x%08x\n",
           rt->cfg->id,
           (unsigned)slot_ctx->slot,
           slot_ctx->base_lba,
           slot_ctx->base_ddr_addr,
           slot_ctx->total_sectors,
           slot_ctx->next_submit_sector,
           (unsigned)slot_ctx->submitted_cmds,
           (unsigned)slot_ctx->completed_cmds,
           (unsigned)slot_ctx->inflight_cmds,
           (unsigned)slot_ctx->failed_cmds,
           (unsigned)rt->nvme_qd_effective,
           (unsigned)rt->nvme_cmd_sectors,
           reg_read32(&rt->nvme, QUEUE_REG_OFFSET + QUEUE_TX_STATUS),
           reg_read32(&rt->nvme, QUEUE_REG_OFFSET + QUEUE_INT_STATUS),
           reg_read32(&rt->nvme, QUEUE_REG_OFFSET + QUEUE_SQ_PTRS),
           reg_read32(&rt->nvme, QUEUE_REG_OFFSET + QUEUE_CQ_PTRS));
    for (i = 0u; i < capacity; ++i) {
        if (!pending[i].valid) {
            continue;
        }
        printf("nvme_write_timeout_pending channel=%d index=%u cid=%u slot=%u"
               " slot_offset=%" PRIu64 " lba=0x%08" PRIx64
               " sectors=%u bytes=%u ddr=0x%08" PRIx64
               " age_us=%" PRIu64 "\n",
               rt->cfg->id,
               (unsigned)i,
               (unsigned)pending[i].cid,
               (unsigned)pending[i].slot,
               pending[i].slot_offset,
               pending[i].lba,
               (unsigned)pending[i].sectors,
               (unsigned)pending[i].bytes,
               pending[i].ddr_addr,
               now_us >= pending[i].submit_us ? now_us - pending[i].submit_us : 0u);
    }
    fflush(stdout);
}

static void nvme_update_active_qd(ChannelRuntime *rt, uint32_t new_qd, uint64_t now_us) {
    uint64_t last_us = rt->nvme_active_qd_last_update_us;
    uint32_t old_qd = __atomic_load_n(&rt->nvme_active_qd_current, __ATOMIC_RELAXED);

    if (last_us != 0u && now_us >= last_us) {
        uint64_t elapsed_us = now_us - last_us;
        (void)__atomic_add_fetch(&rt->nvme_active_qd_integral_us,
                                 elapsed_us * old_qd,
                                 __ATOMIC_RELAXED);
        (void)__atomic_add_fetch(&rt->nvme_active_qd_observed_us,
                                 elapsed_us,
                                 __ATOMIC_RELAXED);
    }
    rt->nvme_active_qd_last_update_us = now_us;
    __atomic_store_n(&rt->nvme_active_qd_current, new_qd, __ATOMIC_RELEASE);
    if (new_qd > __atomic_load_n(&rt->nvme_active_qd_max, __ATOMIC_RELAXED)) {
        __atomic_store_n(&rt->nvme_active_qd_max, new_qd, __ATOMIC_RELEASE);
    }
}

static void nvme_record_active_qd_event(ChannelRuntime *rt, uint32_t active_qd) {
    if (!rt) {
        return;
    }
    rt->nvme_active_qd_event_sum += active_qd;
    rt->nvme_active_qd_event_samples++;
    if (active_qd < rt->nvme_active_qd_event_min) {
        rt->nvme_active_qd_event_min = active_qd;
    }
    if (active_qd > __atomic_load_n(&rt->nvme_active_qd_max, __ATOMIC_RELAXED)) {
        __atomic_store_n(&rt->nvme_active_qd_max, active_qd, __ATOMIC_RELEASE);
    }
    __atomic_store_n(&rt->nvme_active_qd_current, active_qd, __ATOMIC_RELEASE);
}

static void nvme_reset_active_qd_events(ChannelRuntime *rt) {
    if (!rt) {
        return;
    }
    rt->nvme_active_qd_event_sum = 0u;
    rt->nvme_active_qd_event_samples = 0u;
    rt->nvme_active_qd_event_min = UINT32_MAX;
    rt->nvme_refill_count = 0u;
    rt->nvme_completion_count = 0u;
}

static void nvme_print_active_qd_stats(const ChannelRuntime *rt,
                                       uint64_t submitted,
                                       uint64_t completed,
                                       uint32_t requested_qd,
                                       uint32_t effective_qd) {
    double avg = 0.0;
    uint32_t min_qd;

    if (!rt || !hw_storage_log_trace()) {
        return;
    }
    if (rt->nvme_active_qd_event_samples != 0u) {
        avg = (double)rt->nvme_active_qd_event_sum /
              (double)rt->nvme_active_qd_event_samples;
    }
    min_qd = rt->nvme_active_qd_event_min == UINT32_MAX
                 ? 0u
                 : rt->nvme_active_qd_event_min;
    printf("nvme_active_qd_stats channel=%d mode=%s requested_qd=%u"
           " effective_qd=%u active_qd_avg=%.3f active_qd_max=%u"
           " active_qd_min=%u submitted=%" PRIu64 " completed=%" PRIu64
           " sq_full_count=%" PRIu64 " cq_empty_polls=%" PRIu64
           " refill_count=%" PRIu64 " completion_count=%" PRIu64 "\n",
           rt->cfg->id,
           nvme_feed_mode_name(rt->nvme_feed_mode),
           (unsigned)requested_qd,
           (unsigned)effective_qd,
           avg,
           (unsigned)__atomic_load_n(&rt->nvme_active_qd_max, __ATOMIC_ACQUIRE),
           (unsigned)min_qd,
           submitted,
           completed,
           rt->nvme_submit_sq_full_count,
           rt->nvme_cq_empty_polls,
           rt->nvme_refill_count,
           rt->nvme_completion_count);
}

static int nvme_submit_command_async(ChannelRuntime *rt,
                                     uint8_t opcode,
                                     uint16_t cid,
                                     uint64_t lba,
                                     uint32_t sectors,
                                     uint64_t hw_addr) {
    uint64_t submit_start_us;
    uint64_t pending_start_us;

    if (sectors == 0u || sectors > UINT16_MAX) {
        return -1;
    }
    if (rt->gopt.dry_run) {
        return 0;
    }
    if (nvme_stop_requested()) {
        return -2;
    }
    if ((reg_read32(&rt->nvme, QUEUE_REG_OFFSET + QUEUE_TX_STATUS) & QUEUE_SQ_FIFO_FULL) != 0u) {
        rt->nvme_submit_sq_full_count++;
        return 1;
    }

    submit_start_us = wall_time_us();
    rt->nvme_submit_calls++;
    dbg_verbose_printf("[DBG][NVME] async submit ch=%d cid=%u op=%s lba=0x%08" PRIx64
                       " sectors=%u hw=0x%08" PRIx64 "\n",
                       rt->cfg->id,
                       (unsigned)cid,
                       nvme_opcode_name(opcode),
                       lba,
                       (unsigned)sectors,
                       hw_addr);
    reg_write32(&rt->nvme,
                QUEUE_REG_OFFSET + QUEUE_CTX0,
                ((uint32_t)(sectors - 1u) << 16u) | opcode);
    reg_write32(&rt->nvme, QUEUE_REG_OFFSET + QUEUE_CTX1, cid);
    reg_write32(&rt->nvme, QUEUE_REG_OFFSET + QUEUE_PRP1_L, (uint32_t)(hw_addr & 0xFFFFFFFFull));
    reg_write32(&rt->nvme, QUEUE_REG_OFFSET + QUEUE_PRP1_H, (uint32_t)(hw_addr >> 32u));
    reg_write32(&rt->nvme, QUEUE_REG_OFFSET + QUEUE_LBA_L, (uint32_t)(lba & 0xFFFFFFFFull));
    reg_write32(&rt->nvme, QUEUE_REG_OFFSET + QUEUE_LBA_H, (uint32_t)(lba >> 32u));
    __sync_synchronize();
    reg_write32(&rt->nvme, QUEUE_REG_OFFSET + QUEUE_TX_CTRL, QUEUE_TX_CTRL_CMD_PENDING);

    pending_start_us = wall_time_us();
    while ((reg_read32(&rt->nvme, QUEUE_REG_OFFSET + QUEUE_TX_CTRL) &
            QUEUE_TX_CTRL_CMD_PENDING) != 0u) {
        if (nvme_stop_requested()) {
            uint64_t submit_us = elapsed_us_since(submit_start_us);
            rt->nvme_submit_pending_wait_us += elapsed_us_since(pending_start_us);
            rt->nvme_submit_total_us += submit_us;
            nvme_record_submit_stall(rt, submit_us);
            return -2;
        }
        if (elapsed_us_since(pending_start_us) >= rt->gopt.timeout_us) {
            uint64_t submit_us = elapsed_us_since(submit_start_us);
            rt->nvme_submit_pending_wait_us += elapsed_us_since(pending_start_us);
            rt->nvme_submit_total_us += submit_us;
            nvme_record_submit_stall(rt, submit_us);
            nvme_set_last_error(rt, "tx_pending_timeout");
            fprintf(stderr,
                    "NVMe TX pending timeout channel=%d cid=%u ctrl=0x%08x status=0x%08x\n",
                    rt->cfg->id,
                    (unsigned)cid,
                    reg_read32(&rt->nvme, QUEUE_REG_OFFSET + QUEUE_TX_CTRL),
                    reg_read32(&rt->nvme, QUEUE_REG_OFFSET + QUEUE_TX_STATUS));
            return -1;
        }
        nvme_poll_pause(rt, pending_start_us);
    }
    {
        uint64_t submit_us = elapsed_us_since(submit_start_us);
        rt->nvme_submit_pending_wait_us += elapsed_us_since(pending_start_us);
        rt->nvme_submit_total_us += submit_us;
        nvme_record_submit_stall(rt, submit_us);
    }
    {
        uint64_t zero = 0u;
        uint64_t submitted_us = wall_time_us();
        (void)__atomic_compare_exchange_n(&rt->nvme_first_submit_us,
                                          &zero,
                                          submitted_us,
                                          false,
                                          __ATOMIC_RELEASE,
                                          __ATOMIC_RELAXED);
    }
    return 0;
}

int nvme_submit_write_async(ChannelRuntime *rt,
                            uint16_t cid,
                            uint64_t lba,
                            uint32_t sectors,
                            uint64_t ddr_addr) {
    return nvme_submit_command_async(rt, NVM_WRITE, cid, lba, sectors, ddr_addr);
}

static void nvme_tight_error(ChannelRuntime *rt,
                             const char *reason,
                             uint16_t cid,
                             uint64_t submitted,
                             uint64_t completed,
                             uint32_t active_qd,
                             uint64_t last_lba,
                             uint64_t last_ddr_addr) {
    if (rt && reason) {
        nvme_set_last_error(rt, reason);
        fprintf(stderr,
                "nvme_tight_error channel=%d reason=%s cid=%u submitted=%" PRIu64
                " completed=%" PRIu64 " active_qd=%u last_lba=0x%08" PRIx64
                " last_ddr_addr=0x%08" PRIx64 " tx_status=0x%08x int_status=0x%08x"
                " sq_ptrs=0x%08x cq_ptrs=0x%08x\n",
                rt->cfg->id,
                reason,
                (unsigned)cid,
                submitted,
                completed,
                (unsigned)active_qd,
                last_lba,
                last_ddr_addr,
                reg_read32(&rt->nvme, QUEUE_REG_OFFSET + QUEUE_TX_STATUS),
                reg_read32(&rt->nvme, QUEUE_REG_OFFSET + QUEUE_INT_STATUS),
                reg_read32(&rt->nvme, QUEUE_REG_OFFSET + QUEUE_SQ_PTRS),
                reg_read32(&rt->nvme, QUEUE_REG_OFFSET + QUEUE_CQ_PTRS));
    }
}

static int nvme_submit_write_fast(ChannelRuntime *rt,
                                  uint16_t cid,
                                  uint64_t lba,
                                  uint32_t sectors,
                                  uint64_t hw_addr,
                                  uint32_t *last_ctx0,
                                  bool *last_ctx0_valid) {
    uint32_t tx_status;
    uint32_t ctx0;
    uint64_t submit_start_us;
    uint64_t pending_start_us = 0u;

    if (!rt || sectors == 0u || sectors > UINT16_MAX) {
        return -1;
    }
    if (rt->gopt.dry_run) {
        return 0;
    }
    if (nvme_stop_requested()) {
        return -2;
    }
    tx_status = reg_read32(&rt->nvme, QUEUE_REG_OFFSET + QUEUE_TX_STATUS);
    if ((tx_status & QUEUE_SQ_FIFO_FULL) != 0u) {
        rt->nvme_submit_sq_full_count++;
        return 1;
    }

    submit_start_us = wall_time_us();
    rt->nvme_submit_calls++;
    ctx0 = ((uint32_t)(sectors - 1u) << 16u) | NVM_WRITE;
    if (!rt->nvme_skip_const_ctx || !last_ctx0 || !last_ctx0_valid ||
        !*last_ctx0_valid || *last_ctx0 != ctx0) {
        reg_write32(&rt->nvme, QUEUE_REG_OFFSET + QUEUE_CTX0, ctx0);
        if (last_ctx0) {
            *last_ctx0 = ctx0;
        }
        if (last_ctx0_valid) {
            *last_ctx0_valid = true;
        }
    }
    reg_write32(&rt->nvme, QUEUE_REG_OFFSET + QUEUE_CTX1, cid);
    reg_write32(&rt->nvme, QUEUE_REG_OFFSET + QUEUE_PRP1_L, (uint32_t)(hw_addr & 0xFFFFFFFFull));
    reg_write32(&rt->nvme, QUEUE_REG_OFFSET + QUEUE_PRP1_H, (uint32_t)(hw_addr >> 32u));
    reg_write32(&rt->nvme, QUEUE_REG_OFFSET + QUEUE_LBA_L, (uint32_t)(lba & 0xFFFFFFFFull));
    reg_write32(&rt->nvme, QUEUE_REG_OFFSET + QUEUE_LBA_H, (uint32_t)(lba >> 32u));
    __sync_synchronize();
    reg_write32(&rt->nvme, QUEUE_REG_OFFSET + QUEUE_TX_CTRL, QUEUE_TX_CTRL_CMD_PENDING);

    while ((reg_read32(&rt->nvme, QUEUE_REG_OFFSET + QUEUE_TX_CTRL) &
            QUEUE_TX_CTRL_CMD_PENDING) != 0u) {
        if (nvme_stop_requested()) {
            nvme_record_submit_stall(rt, elapsed_us_since(submit_start_us));
            return -2;
        }
        if (pending_start_us == 0u) {
            pending_start_us = wall_time_us();
        } else if (elapsed_us_since(pending_start_us) >= rt->gopt.timeout_us) {
            nvme_set_last_error(rt, "submit_timeout");
            nvme_record_submit_stall(rt, elapsed_us_since(submit_start_us));
            return -1;
        }
        if (rt->nvme_poll_sleep_us != 0u &&
            pending_start_us != 0u &&
            elapsed_us_since(pending_start_us) >= rt->nvme_busy_poll_us) {
            usleep(rt->nvme_poll_sleep_us);
        }
    }
    nvme_record_submit_stall(rt, elapsed_us_since(submit_start_us));
    {
        uint64_t zero = 0u;
        uint64_t submitted_us = wall_time_us();
        (void)__atomic_compare_exchange_n(&rt->nvme_first_submit_us,
                                          &zero,
                                          submitted_us,
                                          false,
                                          __ATOMIC_RELEASE,
                                          __ATOMIC_RELAXED);
    }
    return 0;
}

static int nvme_try_poll_cq(ChannelRuntime *rt, NvmeCompletion *out_cpl) {
    uint32_t cq;
    uint64_t pop_start_us;

    if (!rt || !out_cpl || rt->gopt.dry_run) {
        return -1;
    }
    rt->nvme_cq_poll_calls++;
    if (nvme_stop_requested()) {
        return -2;
    }
    if ((reg_read32(&rt->nvme, QUEUE_REG_OFFSET + QUEUE_TX_STATUS) &
         QUEUE_CQ_FIFO_EMPTY) != 0u) {
        rt->nvme_cq_empty_polls++;
        return 0;
    }

    /* Reading CUR_CQ_CID atomically pops one Host Core CQ FIFO entry. */
    pop_start_us = wall_time_us();
    cq = reg_read32(&rt->nvme, QUEUE_REG_OFFSET + QUEUE_CUR_CQ_CID);
    out_cpl->raw = cq;
    out_cpl->cid = (uint16_t)(cq & 0xFFFFu);
    out_cpl->status = (uint16_t)(cq >> 16u);
    out_cpl->status_code = (uint8_t)((cq >> 18u) & 0xFFu);
    out_cpl->error = ((cq >> 17u) & 0x01u) != 0u || out_cpl->status_code != 0u;
    rt->nvme_cq_pop_total_us += elapsed_us_since(pop_start_us);
    rt->nvme_cq_completed++;
    return 1;
}

static int nvme_try_poll_cq_fast(ChannelRuntime *rt, NvmeCompletion *out_cpl) {
    uint32_t cq;

    if (!rt || !out_cpl || rt->gopt.dry_run) {
        return -1;
    }
    rt->nvme_cq_poll_calls++;
    if (nvme_stop_requested()) {
        return -2;
    }
    if ((reg_read32(&rt->nvme, QUEUE_REG_OFFSET + QUEUE_TX_STATUS) &
         QUEUE_CQ_FIFO_EMPTY) != 0u) {
        rt->nvme_cq_empty_polls++;
        return 0;
    }
    cq = reg_read32(&rt->nvme, QUEUE_REG_OFFSET + QUEUE_CUR_CQ_CID);
    out_cpl->raw = cq;
    out_cpl->cid = (uint16_t)(cq & 0xFFFFu);
    out_cpl->status = (uint16_t)(cq >> 16u);
    out_cpl->status_code = (uint8_t)((cq >> 18u) & 0xFFu);
    out_cpl->error = ((cq >> 17u) & 0x01u) != 0u || out_cpl->status_code != 0u;
    rt->nvme_cq_completed++;
    return 1;
}

int nvme_poll_cq(ChannelRuntime *rt, NvmeCompletion *out_cpl, uint32_t timeout_us) {
    uint64_t start_us;

    if (!rt || !out_cpl || timeout_us == 0u || rt->gopt.dry_run) {
        return -1;
    }
    start_us = wall_time_us();
    while (1) {
        int poll_rc = nvme_try_poll_cq(rt, out_cpl);

        if (poll_rc > 0) {
            rt->nvme_cq_wait_total_us += elapsed_us_since(start_us);
            return 0;
        }
        if (poll_rc < 0) {
            return poll_rc;
        }
        if (elapsed_us_since(start_us) >= timeout_us) {
            nvme_set_last_error(rt, "cq_timeout");
            dbg_printf("[DBG][NVME] CQ timeout ch=%d status=0x%08x int=0x%08x"
                       " sq_ptrs=0x%08x cq_ptrs=0x%08x active_qd=%u\n",
                       rt->cfg->id,
                       reg_read32(&rt->nvme, QUEUE_REG_OFFSET + QUEUE_TX_STATUS),
                       reg_read32(&rt->nvme, QUEUE_REG_OFFSET + QUEUE_INT_STATUS),
                       reg_read32(&rt->nvme, QUEUE_REG_OFFSET + QUEUE_SQ_PTRS),
                       reg_read32(&rt->nvme, QUEUE_REG_OFFSET + QUEUE_CQ_PTRS),
                       (unsigned)__atomic_load_n(&rt->nvme_active_qd_current, __ATOMIC_ACQUIRE));
            return -1;
        }
        nvme_poll_pause(rt, start_us);
    }
}

static int nvme_alloc_cid(ChannelRuntime *rt,
                          const NvmePendingCmd *pending,
                          uint32_t capacity,
                          uint16_t *out_cid) {
    uint32_t attempt;

    for (attempt = 0u; attempt < UINT16_MAX; ++attempt) {
        uint16_t cid = rt->next_cmd_id++;
        uint32_t i;
        bool in_use = false;

        if (cid == 0u) {
            continue;
        }
        for (i = 0u; i < capacity; ++i) {
            if (pending[i].valid && pending[i].cid == cid) {
                in_use = true;
                break;
            }
        }
        if (!in_use) {
            *out_cid = cid;
            return 0;
        }
    }
    return -1;
}

static NvmePendingCmd *nvme_find_pending_by_cid(NvmePendingCmd *pending,
                                                 uint32_t capacity,
                                                 uint16_t cid) {
    uint32_t i;

    for (i = 0u; i < capacity; ++i) {
        if (pending[i].valid && pending[i].cid == cid) {
            return &pending[i];
        }
    }
    return NULL;
}

static NvmePendingCmd *nvme_find_free_pending(NvmePendingCmd *pending, uint32_t capacity) {
    uint32_t i;

    for (i = 0u; i < capacity; ++i) {
        if (!pending[i].valid) {
            return &pending[i];
        }
    }
    return NULL;
}

static void nvme_record_write_completion(ChannelRuntime *rt,
                                         const NvmePendingCmd *pending,
                                         uint64_t completion_us) {
    uint64_t latency_us = pending->submit_us != 0u && completion_us >= pending->submit_us
                              ? completion_us - pending->submit_us
                              : 0u;

    (void)__atomic_add_fetch(&rt->nvme_cmd_count, 1u, __ATOMIC_RELAXED);
    (void)__atomic_add_fetch(&rt->nvme_cmd_bytes_total, pending->bytes, __ATOMIC_RELAXED);
    (void)__atomic_add_fetch(&rt->nvme_write_bytes_done, pending->bytes, __ATOMIC_RELEASE);
    __atomic_store_n(&rt->nvme_last_completion_us, completion_us, __ATOMIC_RELEASE);
    if (pending->submit_us != 0u) {
        (void)__atomic_add_fetch(&rt->nvme_cmd_latency_total_us, latency_us, __ATOMIC_RELAXED);
        (void)__atomic_add_fetch(&rt->nvme_latency_sample_count, 1u, __ATOMIC_RELAXED);
        atomic_update_min_u64(&rt->nvme_cmd_latency_min_us, latency_us);
        atomic_update_max_u64(&rt->nvme_cmd_latency_max_us, latency_us);
    }
    /* TODO: add a bounded latency histogram before exporting p95. */
}

static int nvme_handle_write_completion(ChannelRuntime *rt,
                                        NvmePendingCmd *pending,
                                        uint32_t capacity,
                                        NvmeSlotWriteContext *slot_ctx,
                                        const NvmeCompletion *completion) {
    NvmePendingCmd *entry;
    uint64_t completion_us = wall_time_us();

    entry = nvme_find_pending_by_cid(pending, capacity, completion->cid);
    if (!entry) {
        nvme_set_last_error(rt, "completion_without_pending_cid");
        fprintf(stderr,
                "NVMe completion has no pending CID channel=%d slot=%u cid=%u cq=0x%08x"
                " inflight=%u submitted=%u completed=%u\n",
                rt->cfg->id,
                (unsigned)slot_ctx->slot,
                (unsigned)completion->cid,
                completion->raw,
                (unsigned)slot_ctx->inflight_cmds,
                (unsigned)slot_ctx->submitted_cmds,
                (unsigned)slot_ctx->completed_cmds);
        return -1;
    }
    if (completion->error) {
        nvme_set_last_error(rt, "completion_status_error");
        ++slot_ctx->failed_cmds;
        fprintf(stderr,
                "NVMe async completion error channel=%d slot=%u cid=%u cq=0x%08x"
                " status_code=0x%02x lba=0x%08" PRIx64 " sectors=%u ddr=0x%08" PRIx64 "\n",
                rt->cfg->id,
                (unsigned)entry->slot,
                (unsigned)entry->cid,
                completion->raw,
                (unsigned)completion->status_code,
                entry->lba,
                (unsigned)entry->sectors,
                entry->ddr_addr);
        entry->valid = false;
        --slot_ctx->inflight_cmds;
        nvme_update_active_qd(rt, slot_ctx->inflight_cmds, completion_us);
        nvme_record_active_qd_event(rt, slot_ctx->inflight_cmds);
        return -1;
    }

    nvme_record_write_completion(rt, entry, completion_us);
    dbg_verbose_printf("[DBG][NVME] async complete ch=%d slot=%u cid=%u"
                       " lba=0x%08" PRIx64 " sectors=%u inflight_before=%u\n",
                       rt->cfg->id,
                       (unsigned)entry->slot,
                       (unsigned)entry->cid,
                       entry->lba,
                       (unsigned)entry->sectors,
                       (unsigned)slot_ctx->inflight_cmds);
    entry->valid = false;
    ++slot_ctx->completed_cmds;
    --slot_ctx->inflight_cmds;
    nvme_update_active_qd(rt, slot_ctx->inflight_cmds, completion_us);
    nvme_record_active_qd_event(rt, slot_ctx->inflight_cmds);
    return 0;
}

static int nvme_simulate_write_qd(ChannelRuntime *rt, uint64_t sectors, uint64_t command_sectors) {
    uint64_t command_count = (sectors + command_sectors - 1u) / command_sectors;
    uint64_t command_bytes = sectors * NVME_SECTOR_BYTES;
    uint32_t max_qd = command_count < rt->nvme_qd_effective
                          ? (uint32_t)command_count
                          : rt->nvme_qd_effective;

    (void)__atomic_add_fetch(&rt->nvme_cmd_count, command_count, __ATOMIC_RELAXED);
    (void)__atomic_add_fetch(&rt->nvme_cmd_bytes_total, command_bytes, __ATOMIC_RELAXED);
    (void)__atomic_add_fetch(&rt->nvme_write_bytes_done, command_bytes, __ATOMIC_RELEASE);
    __atomic_store_n(&rt->nvme_active_qd_max, max_qd, __ATOMIC_RELEASE);
    return 0;
}

int nvme_write_contiguous_tight_qd(ChannelRuntime *rt,
                                   uint64_t ddr_hw_start,
                                   uint64_t start_lba,
                                   uint64_t bytes,
                                   uint32_t qd) {
    NvmePendingCmd pending[NVME_PENDING_CAPACITY];
    uint64_t total_cmds;
    uint64_t submitted = 0u;
    uint64_t completed = 0u;
    uint64_t next_lba = start_lba;
    uint64_t next_ddr = ddr_hw_start;
    uint64_t bytes_left = bytes;
    uint32_t active_qd = 0u;
    uint64_t no_progress_start_us = 0u;
    uint32_t last_ctx0 = 0u;
    bool last_ctx0_valid = false;

    if (!rt || bytes == 0u) {
        return 0;
    }
    if (qd == 0u || qd > NVME_PENDING_CAPACITY) {
        nvme_tight_error(rt, "invalid_qd", 0u, 0u, 0u, 0u, start_lba, ddr_hw_start);
        return -1;
    }
    if (rt->nvme_cmd_size_bytes == 0u ||
        rt->nvme_cmd_size_bytes > rt->nvme_max_dts_bytes) {
        nvme_tight_error(rt,
                         "lba_or_ddr_range",
                         0u,
                         0u,
                         0u,
                         0u,
                         start_lba,
                         ddr_hw_start);
        return -1;
    }
    if (ddr_hw_start < rt->cfg->ddr_hw_base ||
        (ddr_hw_start - rt->cfg->ddr_hw_base) > rt->dma_ring_bytes ||
        bytes > (rt->dma_ring_bytes - (ddr_hw_start - rt->cfg->ddr_hw_base))) {
        nvme_tight_error(rt,
                         "lba_or_ddr_range",
                         0u,
                         0u,
                         0u,
                         0u,
                         start_lba,
                         ddr_hw_start);
        return -1;
    }
    if (rt->nvme_max_lba > 0u &&
        (start_lba + bytes_to_sectors(bytes)) > rt->nvme_max_lba) {
        nvme_tight_error(rt,
                         "lba_or_ddr_range",
                         0u,
                         0u,
                         0u,
                         0u,
                         start_lba,
                         ddr_hw_start);
        return -1;
    }
    if (rt->gopt.dry_run) {
        uint64_t sectors = bytes_to_sectors(bytes);
        int rc = nvme_simulate_write_qd(rt, sectors, rt->nvme_cmd_sectors);
        total_cmds = (bytes + rt->nvme_cmd_size_bytes - 1u) / rt->nvme_cmd_size_bytes;
        nvme_print_active_qd_stats(rt, total_cmds, total_cmds, rt->nvme_qd_requested, qd);
        return rc;
    }

    memset(pending, 0, sizeof(pending));
    nvme_reset_active_qd_events(rt);
    total_cmds = (bytes + rt->nvme_cmd_size_bytes - 1u) / rt->nvme_cmd_size_bytes;
    nvme_record_active_qd_event(rt, 0u);

    while (completed < total_cmds) {
        bool made_progress = false;
        uint32_t pops = 0u;

        while (active_qd < qd && submitted < total_cmds) {
            NvmePendingCmd *entry;
            uint64_t cmd_bytes64;
            uint32_t cmd_bytes;
            uint32_t cmd_sectors;
            uint16_t cid;
            int submit_rc;

            if ((reg_read32(&rt->nvme, QUEUE_REG_OFFSET + QUEUE_TX_STATUS) &
                 QUEUE_SQ_FIFO_FULL) != 0u) {
                rt->nvme_submit_sq_full_count++;
                break;
            }
            entry = nvme_find_free_pending(pending, NVME_PENDING_CAPACITY);
            if (!entry || nvme_alloc_cid(rt, pending, NVME_PENDING_CAPACITY, &cid) != 0) {
                nvme_tight_error(rt,
                                 "submit_timeout",
                                 0u,
                                 submitted,
                                 completed,
                                 active_qd,
                                 next_lba,
                                 next_ddr);
                return -1;
            }
            if (nvme_find_pending_by_cid(pending, NVME_PENDING_CAPACITY, cid)) {
                nvme_tight_error(rt,
                                 "duplicate_cid",
                                 cid,
                                 submitted,
                                 completed,
                                 active_qd,
                                 next_lba,
                                 next_ddr);
                return -1;
            }
            cmd_bytes64 = bytes_left > rt->nvme_cmd_size_bytes
                              ? rt->nvme_cmd_size_bytes
                              : bytes_left;
            if (cmd_bytes64 == 0u || cmd_bytes64 > UINT32_MAX) {
                nvme_tight_error(rt,
                                 "lba_or_ddr_range",
                                 cid,
                                 submitted,
                                 completed,
                                 active_qd,
                                 next_lba,
                                 next_ddr);
                return -1;
            }
            cmd_bytes = (uint32_t)cmd_bytes64;
            cmd_sectors = (uint32_t)bytes_to_sectors(cmd_bytes64);
            memset(entry, 0, sizeof(*entry));
            entry->valid = true;
            entry->cid = cid;
            entry->slot = UINT32_MAX;
            entry->slot_offset = bytes - bytes_left;
            entry->lba = next_lba;
            entry->ddr_addr = next_ddr;
            entry->sectors = cmd_sectors;
            entry->bytes = cmd_bytes;
            entry->submit_us = rt->nvme_diag_timing ? wall_time_us() : 0u;

            submit_rc = nvme_submit_write_fast(rt,
                                               cid,
                                               next_lba,
                                               cmd_sectors,
                                               next_ddr,
                                               &last_ctx0,
                                               &last_ctx0_valid);
            if (submit_rc == 1) {
                entry->valid = false;
                rt->nvme_submit_sq_full_count++;
                break;
            }
            if (submit_rc != 0) {
                entry->valid = false;
                nvme_tight_error(rt,
                                 submit_rc == -2 ? "submit_timeout" : "submit_timeout",
                                 cid,
                                 submitted,
                                 completed,
                                 active_qd,
                                 next_lba,
                                 next_ddr);
                return submit_rc;
            }
            next_lba += cmd_sectors;
            next_ddr += cmd_bytes64;
            bytes_left -= cmd_bytes64;
            ++submitted;
            ++active_qd;
            rt->nvme_refill_count++;
            nvme_record_active_qd_event(rt, active_qd);
            made_progress = true;
            no_progress_start_us = 0u;
        }

        while (pops < rt->nvme_cq_pop_batch && active_qd > 0u) {
            NvmeCompletion completion;
            NvmePendingCmd *entry;
            int poll_rc = rt->nvme_diag_timing ? nvme_try_poll_cq(rt, &completion)
                                               : nvme_try_poll_cq_fast(rt, &completion);

            if (poll_rc < 0) {
                nvme_tight_error(rt,
                                 "cq_error",
                                 0u,
                                 submitted,
                                 completed,
                                 active_qd,
                                 next_lba,
                                 next_ddr);
                return poll_rc;
            }
            if (poll_rc == 0) {
                break;
            }
            entry = nvme_find_pending_by_cid(pending, NVME_PENDING_CAPACITY, completion.cid);
            if (!entry) {
                nvme_tight_error(rt,
                                 "unknown_cid",
                                 completion.cid,
                                 submitted,
                                 completed,
                                 active_qd,
                                 next_lba,
                                 next_ddr);
                return -1;
            }
            if (completion.error) {
                nvme_tight_error(rt,
                                 "cq_error",
                                 completion.cid,
                                 submitted,
                                 completed,
                                 active_qd,
                                 entry->lba,
                                 entry->ddr_addr);
                entry->valid = false;
                return -1;
            }
            nvme_record_write_completion(rt,
                                         entry,
                                         rt->nvme_diag_timing ? wall_time_us() : 0u);
            entry->valid = false;
            ++completed;
            --active_qd;
            rt->nvme_completion_count++;
            nvme_record_active_qd_event(rt, active_qd);
            made_progress = true;
            no_progress_start_us = 0u;
            ++pops;

            if (rt->nvme_cq_pop_batch == 1u) {
                break;
            }
        }

        if (made_progress) {
            continue;
        }
        if (submitted < total_cmds && active_qd == 0u) {
            continue;
        }
        if (no_progress_start_us == 0u) {
            no_progress_start_us = wall_time_us();
        } else if (elapsed_us_since(no_progress_start_us) >= rt->gopt.timeout_us) {
            nvme_tight_error(rt,
                             "cq_error",
                             0u,
                             submitted,
                             completed,
                             active_qd,
                             next_lba,
                             next_ddr);
            return -1;
        }
        if (rt->nvme_poll_sleep_us != 0u &&
            elapsed_us_since(no_progress_start_us) >= rt->nvme_busy_poll_us) {
            usleep(rt->nvme_poll_sleep_us);
        } else {
            sched_yield();
        }
    }

    if (submitted != total_cmds || completed != total_cmds || active_qd != 0u ||
        bytes_left != 0u) {
        nvme_tight_error(rt,
                         "lba_or_ddr_range",
                         0u,
                         submitted,
                         completed,
                         active_qd,
                         next_lba,
                         next_ddr);
        return -1;
    }
    nvme_print_active_qd_stats(rt, submitted, completed, rt->nvme_qd_requested, qd);
    return 0;
}

static int nvme_write_slot_qd_legacy(ChannelRuntime *rt,
                                     uint32_t slot,
                                     uint64_t lba,
                                     uint64_t sectors,
                                     uint64_t hw_addr) {
    NvmePendingCmd pending[NVME_PENDING_CAPACITY];
    NvmeSlotWriteContext slot_ctx;
    uint32_t qd = rt->nvme_qd_effective;
    uint64_t command_limit = rt->nvme_cmd_sectors;

    if (sectors == 0u) {
        return 0;
    }
    if (qd == 0u || qd > NVME_PENDING_CAPACITY) {
        return -1;
    }
    if (command_limit == 0u) {
        command_limit = NVME_CMD_KIB_DEFAULT * 1024u / NVME_SECTOR_BYTES;
    }
    command_limit = env_u64_limit("CCB_NVME_MAX_SECTORS", command_limit, command_limit);
    if (rt->gopt.dry_run) {
        return nvme_simulate_write_qd(rt, sectors, command_limit);
    }

    memset(pending, 0, sizeof(pending));
    nvme_reset_active_qd_events(rt);
    memset(&slot_ctx, 0, sizeof(slot_ctx));
    slot_ctx.slot = slot;
    slot_ctx.base_lba = lba;
    slot_ctx.base_ddr_addr = hw_addr;
    slot_ctx.total_sectors = sectors;
    slot_ctx.total_bytes = sectors * NVME_SECTOR_BYTES;
    nvme_record_active_qd_event(rt, 0u);

    while (slot_ctx.next_submit_sector < slot_ctx.total_sectors || slot_ctx.inflight_cmds > 0u) {
        bool made_progress = false;
        bool sq_full = false;

        /*
         * Keep the host core fed: fill up to the configured queue depth, then
         * retire one completion before refilling. This matches the vendor test
         * flow more closely than draining the CQ FIFO after every submit.
         */
        while (slot_ctx.inflight_cmds < qd &&
               slot_ctx.next_submit_sector < slot_ctx.total_sectors) {
            NvmePendingCmd *entry = nvme_find_free_pending(pending, NVME_PENDING_CAPACITY);
            uint64_t remaining = slot_ctx.total_sectors - slot_ctx.next_submit_sector;
            uint32_t command_sectors = (uint32_t)(remaining > command_limit
                                                      ? command_limit
                                                      : remaining);
            uint16_t cid;
            int submit_rc;

            if (!entry || nvme_alloc_cid(rt, pending, NVME_PENDING_CAPACITY, &cid) != 0) {
                nvme_set_last_error(rt, "pending_or_cid_allocation_failed");
                fprintf(stderr, "NVMe pending/CID allocation failed channel=%d slot=%u\n",
                        rt->cfg->id, (unsigned)slot);
                return -1;
            }
            memset(entry, 0, sizeof(*entry));
            entry->valid = true;
            entry->cid = cid;
            entry->slot = slot;
            entry->slot_offset = slot_ctx.next_submit_sector * NVME_SECTOR_BYTES;
            entry->lba = lba + slot_ctx.next_submit_sector;
            entry->ddr_addr = hw_addr + entry->slot_offset;
            entry->sectors = command_sectors;
            entry->bytes = command_sectors * NVME_SECTOR_BYTES;
            entry->submit_us = wall_time_us();

            submit_rc = nvme_submit_write_async(rt,
                                                entry->cid,
                                                entry->lba,
                                                entry->sectors,
                                                entry->ddr_addr);
            if (submit_rc == 1) {
                entry->valid = false;
                sq_full = true;
                break;
            } else if (submit_rc != 0) {
                entry->valid = false;
                if (submit_rc != -2 && rt->nvme_last_error[0] == '\0') {
                    nvme_set_last_error(rt, "async_submit_failed");
                }
                return submit_rc;
            } else {
                slot_ctx.next_submit_sector += command_sectors;
                ++slot_ctx.submitted_cmds;
                ++slot_ctx.inflight_cmds;
                nvme_update_active_qd(rt, slot_ctx.inflight_cmds, wall_time_us());
                nvme_record_active_qd_event(rt, slot_ctx.inflight_cmds);
                made_progress = true;
            }
        }

        if (slot_ctx.inflight_cmds > 0u) {
            NvmeCompletion completion;
            int poll_rc = nvme_try_poll_cq(rt, &completion);

            if (poll_rc < 0) {
                return poll_rc;
            }
            if (poll_rc > 0) {
                if (nvme_handle_write_completion(rt,
                                                 pending,
                                                 NVME_PENDING_CAPACITY,
                                                 &slot_ctx,
                                                 &completion) != 0) {
                    return -1;
                }
                made_progress = true;
            }
        }

        if (!made_progress) {
            NvmeCompletion completion;
            int poll_rc;

            if (slot_ctx.inflight_cmds == 0u) {
                nvme_set_last_error(rt, "sq_full_without_software_pending");
                fprintf(stderr,
                        "NVMe SQ full with no software pending command channel=%d slot=%u submitted=%u\n",
                        rt->cfg->id,
                        (unsigned)slot,
                        (unsigned)slot_ctx.submitted_cmds);
                return -1;
            }
            if (!sq_full && slot_ctx.inflight_cmds < qd &&
                slot_ctx.next_submit_sector < slot_ctx.total_sectors) {
                continue;
            }
            poll_rc = nvme_poll_cq(rt, &completion, rt->gopt.timeout_us);
            if (poll_rc != 0) {
                nvme_dump_write_timeout_context(rt, pending, NVME_PENDING_CAPACITY, &slot_ctx);
                return poll_rc;
            }
            if (nvme_handle_write_completion(rt,
                                             pending,
                                             NVME_PENDING_CAPACITY,
                                             &slot_ctx,
                                             &completion) != 0) {
                return -1;
            }
        }
    }

    if (slot_ctx.next_submit_sector != slot_ctx.total_sectors ||
        slot_ctx.completed_cmds != slot_ctx.submitted_cmds ||
        slot_ctx.inflight_cmds != 0u || slot_ctx.failed_cmds != 0u) {
        nvme_set_last_error(rt, "slot_completion_invariant_failed");
        fprintf(stderr,
                "NVMe slot completion invariant failed channel=%d slot=%u next=%" PRIu64
                "/%" PRIu64 " submitted=%u completed=%u inflight=%u failed=%u\n",
                rt->cfg->id,
                (unsigned)slot,
                slot_ctx.next_submit_sector,
                slot_ctx.total_sectors,
                (unsigned)slot_ctx.submitted_cmds,
                (unsigned)slot_ctx.completed_cmds,
                (unsigned)slot_ctx.inflight_cmds,
                (unsigned)slot_ctx.failed_cmds);
        return -1;
    }
    nvme_print_active_qd_stats(rt,
                               slot_ctx.submitted_cmds,
                               slot_ctx.completed_cmds,
                               rt->nvme_qd_requested,
                               qd);
    return 0;
}

int nvme_write_slot_qd(ChannelRuntime *rt,
                       uint32_t slot,
                       uint64_t lba,
                       uint64_t sectors,
                       uint64_t hw_addr) {
    if (!rt || sectors == 0u) {
        return 0;
    }
    if (rt->nvme_feed_mode == NVME_FEED_MODE_TIGHT) {
        return nvme_write_contiguous_tight_qd(rt,
                                             hw_addr,
                                             lba,
                                             sectors * NVME_SECTOR_BYTES,
                                             rt->nvme_qd_effective);
    }
    return nvme_write_slot_qd_legacy(rt, slot, lba, sectors, hw_addr);
}

static NvmeSlotWriteContext *nvme_find_slot_context(NvmeSlotWriteContext *contexts,
                                                    uint32_t context_count,
                                                    uint32_t slot) {
    uint32_t i;

    for (i = 0u; i < context_count; ++i) {
        if (contexts[i].slot == slot) {
            return &contexts[i];
        }
    }
    return NULL;
}

static int nvme_handle_multi_write_completion(ChannelRuntime *rt,
                                              NvmePendingCmd *pending,
                                              uint32_t capacity,
                                              NvmeSlotWriteContext *contexts,
                                              uint32_t context_count,
                                              uint32_t *global_inflight,
                                              const NvmeCompletion *completion) {
    NvmePendingCmd *entry;
    NvmeSlotWriteContext *slot_ctx;
    uint64_t completion_us = wall_time_us();

    entry = nvme_find_pending_by_cid(pending, capacity, completion->cid);
    if (!entry) {
        nvme_set_last_error(rt, "completion_without_pending_cid");
        fprintf(stderr,
                "NVMe completion has no pending CID channel=%d cid=%u cq=0x%08x active_qd=%u\n",
                rt->cfg->id,
                (unsigned)completion->cid,
                completion->raw,
                (unsigned)(global_inflight ? *global_inflight : 0u));
        return -1;
    }
    slot_ctx = nvme_find_slot_context(contexts, context_count, entry->slot);
    if (!slot_ctx) {
        nvme_set_last_error(rt, "completion_without_slot_context");
        fprintf(stderr,
                "NVMe completion has no slot context channel=%d slot=%u cid=%u cq=0x%08x\n",
                rt->cfg->id,
                (unsigned)entry->slot,
                (unsigned)entry->cid,
                completion->raw);
        entry->valid = false;
        return -1;
    }
    if (completion->error) {
        nvme_set_last_error(rt, "completion_status_error");
        ++slot_ctx->failed_cmds;
        fprintf(stderr,
                "NVMe async completion error channel=%d slot=%u cid=%u cq=0x%08x"
                " status_code=0x%02x lba=0x%08" PRIx64 " sectors=%u ddr=0x%08" PRIx64 "\n",
                rt->cfg->id,
                (unsigned)entry->slot,
                (unsigned)entry->cid,
                completion->raw,
                (unsigned)completion->status_code,
                entry->lba,
                (unsigned)entry->sectors,
                entry->ddr_addr);
        entry->valid = false;
        if (slot_ctx->inflight_cmds > 0u) {
            --slot_ctx->inflight_cmds;
        }
        if (global_inflight && *global_inflight > 0u) {
            --(*global_inflight);
        }
        nvme_update_active_qd(rt, global_inflight ? *global_inflight : 0u, completion_us);
        return -1;
    }

    nvme_record_write_completion(rt, entry, completion_us);
    dbg_verbose_printf("[DBG][NVME] async complete ch=%d slot=%u cid=%u"
                       " lba=0x%08" PRIx64 " sectors=%u global_inflight_before=%u\n",
                       rt->cfg->id,
                       (unsigned)entry->slot,
                       (unsigned)entry->cid,
                       entry->lba,
                       (unsigned)entry->sectors,
                       (unsigned)(global_inflight ? *global_inflight : 0u));
    entry->valid = false;
    ++slot_ctx->completed_cmds;
    if (slot_ctx->inflight_cmds > 0u) {
        --slot_ctx->inflight_cmds;
    }
    if (global_inflight && *global_inflight > 0u) {
        --(*global_inflight);
    }
    nvme_update_active_qd(rt, global_inflight ? *global_inflight : 0u, completion_us);
    return 0;
}

static int nvme_finish_completed_slots(ChannelRuntime *rt,
                                       NvmeSlotWriteContext *contexts,
                                       const NvmeWriteSlotReq *reqs,
                                       uint32_t req_count,
                                       bool *done,
                                       uint32_t *active_count,
                                       NvmeWriteSlotDoneCb done_cb,
                                       void *opaque) {
    uint32_t i;

    for (i = 0u; i < req_count; ++i) {
        NvmeSlotWriteContext *ctx = &contexts[i];

        if (done[i]) {
            continue;
        }
        if (ctx->next_submit_sector == ctx->total_sectors &&
            ctx->completed_cmds == ctx->submitted_cmds &&
            ctx->inflight_cmds == 0u &&
            ctx->failed_cmds == 0u) {
            if (done_cb && done_cb(opaque, &reqs[i]) != 0) {
                nvme_set_last_error(rt, "slot_done_callback_failed");
                return -1;
            }
            done[i] = true;
            if (active_count && *active_count > 0u) {
                --(*active_count);
            }
        }
    }
    return 0;
}

static int nvme_find_next_submit_context(NvmeSlotWriteContext *contexts,
                                         const bool *done,
                                         uint32_t context_count,
                                         uint32_t *rr_index) {
    uint32_t attempt;

    if (context_count == 0u) {
        return -1;
    }
    for (attempt = 0u; attempt < context_count; ++attempt) {
        uint32_t index = (*rr_index + attempt) % context_count;
        if (!done[index] &&
            contexts[index].next_submit_sector < contexts[index].total_sectors) {
            *rr_index = (index + 1u) % context_count;
            return (int)index;
        }
    }
    return -1;
}

struct NvmeCrossSlotEngine {
    ChannelRuntime *rt;
    NvmeCrossSlotOps ops;
    void *ops_opaque;
    NvmePendingCmd pending[NVME_PENDING_CAPACITY];
    NvmeSlotWriteContext contexts[NVME_PENDING_CAPACITY];
    NvmeWriteSlotReq reqs[NVME_PENDING_CAPACITY];
    bool active[NVME_PENDING_CAPACITY];
    uint32_t active_count;
    NvmeCrossSlotConfig config;
    uint32_t global_inflight;
    uint32_t rr_index;
    bool completed_cid_seen[UINT16_MAX + 1u];
    uint64_t sq_full_start_us;
    uint64_t cq_empty_start_us;
    uint64_t no_progress_start_us;
    NvmeCrossSlotStats stats;
    char last_error[64];
};

static void cross_slot_update_max(uint64_t *value, uint64_t sample)
{
    if (sample > *value) *value = sample;
}

static void cross_slot_record_cq_empty(NvmeCrossSlotEngine *e, uint64_t now_us)
{
    if (e->cq_empty_start_us == 0u) {
        e->cq_empty_start_us = now_us;
        ++e->stats.cq_empty_wait_count;
    }
    cross_slot_update_max(&e->stats.cq_empty_wait_max_us, now_us - e->cq_empty_start_us);
}

static void cross_slot_clear_cq_empty(NvmeCrossSlotEngine *e, uint64_t now_us)
{
    if (e->cq_empty_start_us != 0u) {
        cross_slot_update_max(&e->stats.cq_empty_wait_max_us, now_us - e->cq_empty_start_us);
        e->cq_empty_start_us = 0u;
    }
}

static int cross_slot_fail(NvmeCrossSlotEngine *e, const char *reason)
{
    if (e) {
        snprintf(e->last_error, sizeof(e->last_error), "%s", reason);
        nvme_set_last_error(e->rt, reason);
    }
    return -1;
}

static int cross_slot_validate(NvmeCrossSlotEngine *e)
{
    uint32_t i, active_count = 0u, pending_count = 0u, inflight_count = 0u;
    if (!e) return -1;
    for (i = 0u; i < NVME_PENDING_CAPACITY; ++i) {
        NvmeSlotWriteContext *ctx = &e->contexts[i];
        if (!e->active[i]) continue;
        ++active_count;
        if (ctx->slot != e->reqs[i].slot || ctx->next_submit_sector > ctx->total_sectors ||
            ctx->completed_cmds > ctx->submitted_cmds ||
            ctx->submitted_cmds != ctx->completed_cmds + ctx->inflight_cmds)
            return cross_slot_fail(e, "context_count_invariant_failed");
        inflight_count += ctx->inflight_cmds;
    }
    if (active_count != e->active_count || active_count > e->config.max_active_slots)
        return cross_slot_fail(e, "context_count_invariant_failed");
    for (i = 0u; i < NVME_PENDING_CAPACITY; ++i) {
        uint32_t j;
        if (!e->pending[i].valid) continue;
        ++pending_count;
        for (j = 0u; j < NVME_PENDING_CAPACITY; ++j)
            if (e->active[j] && e->contexts[j].slot == e->pending[i].slot) break;
        if (j == NVME_PENDING_CAPACITY) return cross_slot_fail(e, "pending_cid_state_inconsistent");
    }
    if (pending_count != e->global_inflight || inflight_count != e->global_inflight)
        return cross_slot_fail(e, "submitted_completed_inflight_invariant_failed");
    return 0;
}

static int cross_slot_handle_completion(NvmeCrossSlotEngine *e, const NvmeCompletion *completion)
{
    NvmePendingCmd *entry;
    NvmeSlotWriteContext *ctx;
    if (!e || !completion) return -1;
    entry = nvme_find_pending_by_cid(e->pending, NVME_PENDING_CAPACITY, completion->cid);
    if (!entry) return cross_slot_fail(e, e->completed_cid_seen[completion->cid] ?
                                      "duplicate_completion_cid" : "unknown_completion_cid");
    ctx = nvme_find_slot_context(e->contexts, NVME_PENDING_CAPACITY, entry->slot);
    if (!ctx || ctx->inflight_cmds == 0u || e->global_inflight == 0u)
        return cross_slot_fail(e, "pending_cid_state_inconsistent");
    if (completion->error) {
        ++ctx->failed_cmds;
        entry->valid = false;
        return cross_slot_fail(e, "completion_status_error");
    }
    nvme_record_write_completion(e->rt, entry, e->ops.monotonic_us(e->ops_opaque));
    entry->valid = false;
    e->completed_cid_seen[completion->cid] = true;
    --ctx->inflight_cmds;
    ++ctx->completed_cmds;
    --e->global_inflight;
    return cross_slot_validate(e);
}

static int cross_slot_real_submit(void *opaque, uint16_t cid, uint64_t lba,
                                  uint32_t sectors, uint64_t ddr_addr)
{ return nvme_submit_write_async((ChannelRuntime *)opaque, cid, lba, sectors, ddr_addr); }
static int cross_slot_real_poll(void *opaque, NvmeCompletion *out)
{ return nvme_try_poll_cq((ChannelRuntime *)opaque, out); }
static uint64_t cross_slot_real_time(void *opaque) { (void)opaque; return wall_time_us(); }
static void cross_slot_real_sleep(void *opaque, uint32_t us) { (void)opaque; usleep(us); }
static void cross_slot_real_yield(void *opaque) { (void)opaque; sched_yield(); }

NvmeCrossSlotEngine *nvme_cross_slot_engine_create(ChannelRuntime *rt)
{
    NvmeCrossSlotOps ops = { cross_slot_real_submit, cross_slot_real_poll,
                             cross_slot_real_time, cross_slot_real_sleep, cross_slot_real_yield };
    return nvme_cross_slot_engine_create_with_ops(rt, &ops, rt);
}

NvmeCrossSlotEngine *nvme_cross_slot_engine_create_with_config(
    ChannelRuntime *rt, const NvmeCrossSlotConfig *config)
{
    NvmeCrossSlotOps ops = { cross_slot_real_submit, cross_slot_real_poll,
                             cross_slot_real_time, cross_slot_real_sleep, cross_slot_real_yield };
    return nvme_cross_slot_engine_create_with_ops_config(rt, config, &ops, rt);
}

NvmeCrossSlotEngine *nvme_cross_slot_engine_create_with_ops(ChannelRuntime *rt,
                                                             const NvmeCrossSlotOps *ops, void *opaque)
{
    NvmeCrossSlotConfig config = {
        .max_active_slots = 4u, .target_qd = rt ? rt->nvme_qd_effective : 0u,
        .cq_batch = rt ? rt->nvme_cq_pop_batch : 1u, .writer_budget_us = 300u,
        .busy_poll_us = rt ? rt->nvme_busy_poll_us : 0u,
        .empty_sleep_us = rt ? rt->nvme_poll_sleep_us : 0u,
        .no_progress_timeout_us = rt ? rt->gopt.timeout_us : 0u,
    };
    return nvme_cross_slot_engine_create_with_ops_config(rt, &config, ops, opaque);
}

NvmeCrossSlotEngine *nvme_cross_slot_engine_create_with_ops_config(
    ChannelRuntime *rt, const NvmeCrossSlotConfig *config,
    const NvmeCrossSlotOps *ops, void *opaque)
{
    NvmeCrossSlotEngine *e;
    if (!rt || !ops || !ops->submit || !ops->poll_completion || !ops->monotonic_us ||
        !config || config->max_active_slots == 0u || config->target_qd == 0u ||
        config->cq_batch == 0u || config->max_active_slots > NVME_PENDING_CAPACITY ||
        config->target_qd > NVME_PENDING_CAPACITY) return NULL;
    e = calloc(1u, sizeof(*e));
    if (e) {
        uint32_t i;
        e->rt = rt;
        e->ops = *ops;
        e->ops_opaque = opaque;
        e->config = *config;
        for (i = 0u; i < NVME_PENDING_CAPACITY; ++i) e->contexts[i].slot = UINT32_MAX;
    }
    return e;
}

void nvme_cross_slot_engine_destroy(NvmeCrossSlotEngine *engine)
{ free(engine); }

uint32_t nvme_cross_slot_engine_active(const NvmeCrossSlotEngine *engine)
{ return engine ? engine->active_count : 0u; }
uint32_t nvme_cross_slot_engine_capacity(const NvmeCrossSlotEngine *engine)
{ return engine ? engine->config.max_active_slots : 0u; }
bool nvme_cross_slot_engine_can_accept(const NvmeCrossSlotEngine *engine)
{ return engine && engine->active_count < engine->config.max_active_slots; }
const char *nvme_cross_slot_engine_last_error(const NvmeCrossSlotEngine *engine)
{ return engine ? engine->last_error : "invalid_engine"; }
void nvme_cross_slot_engine_get_stats(const NvmeCrossSlotEngine *engine,
                                      NvmeCrossSlotStats *out)
{
    if (out) memset(out, 0, sizeof(*out));
    if (engine && out) *out = engine->stats;
}

int nvme_cross_slot_engine_add(NvmeCrossSlotEngine *e, const NvmeWriteSlotReq *req)
{
    uint32_t i;
    if (!e || !req || req->sectors == 0u || req->bytes == 0u) return -1;
    if (!nvme_cross_slot_engine_can_accept(e)) return 1;
    for (i = 0u; i < NVME_PENDING_CAPACITY; ++i) {
        if (e->active[i] && e->reqs[i].slot == req->slot)
            return cross_slot_fail(e, "duplicate_active_slot");
        if (!e->active[i]) {
            e->active[i] = true; e->reqs[i] = *req;
            memset(&e->contexts[i], 0, sizeof(e->contexts[i]));
            e->contexts[i].slot = req->slot; e->contexts[i].base_lba = req->start_lba;
            e->contexts[i].base_ddr_addr = req->hw_addr; e->contexts[i].total_sectors = req->sectors;
            e->contexts[i].total_bytes = req->bytes; ++e->active_count;
            return cross_slot_validate(e);
        }
    }
    return 1;
}

int nvme_cross_slot_engine_step(NvmeCrossSlotEngine *e, uint32_t budget_us,
                                NvmeWriteSlotDoneCb done_cb, void *opaque)
{
    uint64_t start_us;
    uint64_t command_limit;
    uint32_t qd;
    uint32_t effective_budget;
    if (!e || !e->rt) return -1;
    qd = e->config.target_qd;
    command_limit = e->rt->nvme_cmd_sectors ? e->rt->nvme_cmd_sectors : 512u;
    effective_budget = budget_us != 0u ? budget_us : e->config.writer_budget_us;
    start_us = e->ops.monotonic_us(e->ops_opaque);
    while (e->active_count > 0u) {
        uint32_t i, pops = 0u;
        bool progress = false;
        if (cross_slot_validate(e) != 0) return -1;
        /* CQ first: completions may free QD before refill. */
        while (pops < e->config.cq_batch && e->global_inflight > 0u) {
            NvmeCompletion cpl;
            int prc = e->ops.poll_completion(e->ops_opaque, &cpl);
            uint64_t poll_end_us = e->ops.monotonic_us(e->ops_opaque);
            if (prc < 0) return cross_slot_fail(e, "completion_poll_failed");
            if (prc == 0) {
                cross_slot_record_cq_empty(e, poll_end_us);
                break;
            }
            cross_slot_clear_cq_empty(e, poll_end_us);
            {
                uint64_t process_start_us = poll_end_us;
                if (cross_slot_handle_completion(e, &cpl) != 0) return -1;
                ++e->stats.completion_process_count;
                cross_slot_update_max(&e->stats.completion_process_max_us,
                                      e->ops.monotonic_us(e->ops_opaque) - process_start_us);
            }
            progress = true;
            ++pops;
        }
        /* Only fully drained contexts may invoke the slot callback. */
        for (i = 0u; i < NVME_PENDING_CAPACITY; ++i) {
            NvmeSlotWriteContext *ctx = &e->contexts[i];
            if (e->active[i] && ctx->next_submit_sector == ctx->total_sectors &&
                ctx->completed_cmds == ctx->submitted_cmds && ctx->inflight_cmds == 0u) {
                if (ctx->failed_cmds != 0u) return cross_slot_fail(e, "completion_status_error");
                if (done_cb && done_cb(opaque, &e->reqs[i]) != 0)
                    return cross_slot_fail(e, "slot_callback_failed");
                e->active[i] = false; e->contexts[i].slot = UINT32_MAX;
                --e->active_count; progress = true;
            }
        }
        /* Refill after processing CQ and completed contexts. */
        while (e->global_inflight < qd) {
            bool done[NVME_PENDING_CAPACITY];
            int index;
            NvmeSlotWriteContext *ctx; NvmePendingCmd *p; uint64_t remaining; uint32_t sectors; uint16_t cid; int src;
            for (i = 0u; i < NVME_PENDING_CAPACITY; ++i) done[i] = !e->active[i];
            index = nvme_find_next_submit_context(e->contexts, done,
                                                   NVME_PENDING_CAPACITY, &e->rr_index);
            if (index < 0) break;
            ctx = &e->contexts[index]; p = nvme_find_free_pending(e->pending, NVME_PENDING_CAPACITY);
            if (!p || nvme_alloc_cid(e->rt, e->pending, NVME_PENDING_CAPACITY, &cid) != 0)
                return cross_slot_fail(e, "pending_cid_state_inconsistent");
            remaining = ctx->total_sectors - ctx->next_submit_sector;
            sectors = (uint32_t)(remaining > command_limit ? command_limit : remaining);
            memset(p, 0, sizeof(*p)); p->valid = true; p->cid = cid; p->slot = ctx->slot;
            p->slot_offset = ctx->next_submit_sector * NVME_SECTOR_BYTES;
            p->lba = ctx->base_lba + ctx->next_submit_sector; p->ddr_addr = ctx->base_ddr_addr + p->slot_offset;
            p->sectors = sectors; p->bytes = sectors * NVME_SECTOR_BYTES;
            {
                uint64_t submit_start_us = e->ops.monotonic_us(e->ops_opaque);
                src = e->ops.submit(e->ops_opaque, cid, p->lba, sectors, p->ddr_addr);
                uint64_t submit_end_us = e->ops.monotonic_us(e->ops_opaque);
                ++e->stats.submit_mmio_count;
                cross_slot_update_max(&e->stats.submit_mmio_max_us,
                                      submit_end_us - submit_start_us);
                if (src == 1) {
                    if (e->sq_full_start_us == 0u) {
                        e->sq_full_start_us = submit_end_us;
                        ++e->stats.sq_full_wait_count;
                    }
                    cross_slot_update_max(&e->stats.sq_full_wait_max_us,
                                          submit_end_us - e->sq_full_start_us);
                    p->valid = false;
                    break;
                }
                if (e->sq_full_start_us != 0u) {
                    cross_slot_update_max(&e->stats.sq_full_wait_max_us,
                                          submit_end_us - e->sq_full_start_us);
                    e->sq_full_start_us = 0u;
                }
            }
            if (src != 0) { p->valid = false; return cross_slot_fail(e, "submit_failed"); }
            e->completed_cid_seen[cid] = false;
            ctx->next_submit_sector += sectors; ++ctx->submitted_cmds; ++ctx->inflight_cmds;
            ++e->global_inflight; progress = true;
        }
        if (progress) {
            e->no_progress_start_us = 0u;
            if (effective_budget == 0u ||
                e->ops.monotonic_us(e->ops_opaque) - start_us < effective_budget) continue;
            if (e->ops.yield_cpu) e->ops.yield_cpu(e->ops_opaque); else sched_yield();
            break;
        }
        {
            uint64_t now_us = e->ops.monotonic_us(e->ops_opaque);
            uint64_t busy_deadline_us = now_us + e->config.busy_poll_us;
            bool busy_progress = false;

            if (e->no_progress_start_us == 0u) e->no_progress_start_us = now_us;
            while (e->global_inflight > 0u && e->ops.monotonic_us(e->ops_opaque) < busy_deadline_us) {
                NvmeCompletion cpl;
                int prc = e->ops.poll_completion(e->ops_opaque, &cpl);
                uint64_t poll_end_us = e->ops.monotonic_us(e->ops_opaque);
                if (prc < 0) return cross_slot_fail(e, "completion_poll_failed");
                if (prc == 0) {
                    cross_slot_record_cq_empty(e, poll_end_us);
                    continue;
                }
                cross_slot_clear_cq_empty(e, poll_end_us);
                {
                    uint64_t process_start_us = poll_end_us;
                    if (cross_slot_handle_completion(e, &cpl) != 0) return -1;
                    ++e->stats.completion_process_count;
                    cross_slot_update_max(&e->stats.completion_process_max_us,
                                          e->ops.monotonic_us(e->ops_opaque) - process_start_us);
                }
                busy_progress = true;
                break;
            }
            if (busy_progress) {
                e->no_progress_start_us = 0u;
                continue;
            }
            if (e->ops.yield_cpu) e->ops.yield_cpu(e->ops_opaque); else sched_yield();
            if (e->global_inflight > 0u) {
                NvmeCompletion cpl;
                int prc = e->ops.poll_completion(e->ops_opaque, &cpl);
                if (prc < 0) return cross_slot_fail(e, "completion_poll_failed");
                if (prc > 0) {
                    uint64_t process_start_us = e->ops.monotonic_us(e->ops_opaque);
                    if (cross_slot_handle_completion(e, &cpl) != 0) return -1;
                    ++e->stats.completion_process_count;
                    cross_slot_update_max(&e->stats.completion_process_max_us,
                                          e->ops.monotonic_us(e->ops_opaque) - process_start_us);
                    e->no_progress_start_us = 0u;
                    continue;
                }
                cross_slot_record_cq_empty(e, e->ops.monotonic_us(e->ops_opaque));
            }
            if (e->config.empty_sleep_us != 0u) {
                e->ops.sleep_us(e->ops_opaque, e->config.empty_sleep_us);
                ++e->stats.no_progress_sleep_count;
            }
            now_us = e->ops.monotonic_us(e->ops_opaque);
            if (e->config.no_progress_timeout_us != 0u &&
                now_us - e->no_progress_start_us >= e->config.no_progress_timeout_us)
                return cross_slot_fail(e, "no_progress_timeout");
            if (effective_budget != 0u && now_us - start_us >= effective_budget) break;
        }
    }
    return 0;
}

int nvme_write_slots_qd(ChannelRuntime *rt,
                        const NvmeWriteSlotReq *reqs,
                        uint32_t req_count,
                        NvmeWriteSlotDoneCb done_cb,
                        void *opaque) {
    NvmePendingCmd pending[NVME_PENDING_CAPACITY];
    NvmeSlotWriteContext contexts[NVME_PENDING_CAPACITY];
    bool done[NVME_PENDING_CAPACITY];
    uint32_t qd = rt ? rt->nvme_qd_effective : 0u;
    uint64_t command_limit = rt ? rt->nvme_cmd_sectors : 0u;
    uint32_t active_count = req_count;
    uint32_t global_inflight = 0u;
    uint32_t rr_index = 0u;
    uint32_t i;

    if (!rt || !reqs || req_count == 0u) {
        return 0;
    }
    if (req_count > NVME_PENDING_CAPACITY || qd == 0u || qd > NVME_PENDING_CAPACITY) {
        nvme_set_last_error(rt, "invalid_multi_slot_qd_request");
        return -1;
    }
    if (command_limit == 0u) {
        command_limit = NVME_CMD_KIB_DEFAULT * 1024u / NVME_SECTOR_BYTES;
    }
    command_limit = env_u64_limit("CCB_NVME_MAX_SECTORS", command_limit, command_limit);

    memset(pending, 0, sizeof(pending));
    memset(contexts, 0, sizeof(contexts));
    memset(done, 0, sizeof(done));
    for (i = 0u; i < req_count; ++i) {
        uint32_t j;

        if (reqs[i].sectors == 0u || reqs[i].bytes == 0u) {
            done[i] = true;
            if (active_count > 0u) {
                --active_count;
            }
            continue;
        }
        if (rt->nvme_max_lba > 0u && (reqs[i].start_lba + reqs[i].sectors) > rt->nvme_max_lba) {
            nvme_set_last_error(rt, "multi_slot_lba_exceeds_max");
            fprintf(stderr,
                    "Requested SSD range exceeds max LBA: channel=%d slot=%u"
                    " start=0x%08" PRIx64 " sectors=%" PRIu64 " max=0x%08" PRIx64 "\n",
                    rt->cfg->id,
                    (unsigned)reqs[i].slot,
                    reqs[i].start_lba,
                    reqs[i].sectors,
                    rt->nvme_max_lba);
            return -1;
        }
        for (j = i + 1u; j < req_count; ++j) {
            if (reqs[i].slot == reqs[j].slot) {
                nvme_set_last_error(rt, "duplicate_multi_slot_request");
                fprintf(stderr,
                        "Duplicate NVMe multi-slot request channel=%d slot=%u\n",
                        rt->cfg->id,
                        (unsigned)reqs[i].slot);
                return -1;
            }
        }
        contexts[i].slot = reqs[i].slot;
        contexts[i].base_lba = reqs[i].start_lba;
        contexts[i].base_ddr_addr = reqs[i].hw_addr;
        contexts[i].total_sectors = reqs[i].sectors;
        contexts[i].total_bytes = reqs[i].bytes;
    }

    if (rt->gopt.dry_run) {
        for (i = 0u; i < req_count; ++i) {
            if (!done[i]) {
                if (nvme_simulate_write_qd(rt, reqs[i].sectors, command_limit) != 0 ||
                    (done_cb && done_cb(opaque, &reqs[i]) != 0)) {
                    return -1;
                }
            }
        }
        return 0;
    }

    while (active_count > 0u) {
        bool made_progress = false;
        bool sq_full = false;

        while (global_inflight < qd) {
            int ctx_index = nvme_find_next_submit_context(contexts, done, req_count, &rr_index);
            NvmeSlotWriteContext *ctx;
            NvmePendingCmd *entry;
            uint64_t remaining;
            uint32_t command_sectors;
            uint16_t cid;
            int submit_rc;

            if (ctx_index < 0) {
                break;
            }
            ctx = &contexts[(uint32_t)ctx_index];
            entry = nvme_find_free_pending(pending, NVME_PENDING_CAPACITY);
            if (!entry || nvme_alloc_cid(rt, pending, NVME_PENDING_CAPACITY, &cid) != 0) {
                nvme_set_last_error(rt, "pending_or_cid_allocation_failed");
                fprintf(stderr,
                        "NVMe pending/CID allocation failed channel=%d slot=%u\n",
                        rt->cfg->id,
                        (unsigned)ctx->slot);
                return -1;
            }
            remaining = ctx->total_sectors - ctx->next_submit_sector;
            command_sectors = (uint32_t)(remaining > command_limit ? command_limit : remaining);
            memset(entry, 0, sizeof(*entry));
            entry->valid = true;
            entry->cid = cid;
            entry->slot = ctx->slot;
            entry->slot_offset = ctx->next_submit_sector * NVME_SECTOR_BYTES;
            entry->lba = ctx->base_lba + ctx->next_submit_sector;
            entry->ddr_addr = ctx->base_ddr_addr + entry->slot_offset;
            entry->sectors = command_sectors;
            entry->bytes = command_sectors * NVME_SECTOR_BYTES;
            entry->submit_us = wall_time_us();

            submit_rc = nvme_submit_write_async(rt,
                                                entry->cid,
                                                entry->lba,
                                                entry->sectors,
                                                entry->ddr_addr);
            if (submit_rc == 1) {
                entry->valid = false;
                sq_full = true;
                break;
            }
            if (submit_rc != 0) {
                entry->valid = false;
                if (submit_rc != -2 && rt->nvme_last_error[0] == '\0') {
                    nvme_set_last_error(rt, "async_submit_failed");
                }
                return submit_rc;
            }

            ctx->next_submit_sector += command_sectors;
            ++ctx->submitted_cmds;
            ++ctx->inflight_cmds;
            ++global_inflight;
            nvme_update_active_qd(rt, global_inflight, wall_time_us());
            made_progress = true;
        }

        if (global_inflight > 0u) {
            NvmeCompletion completion;
            int poll_rc = nvme_try_poll_cq(rt, &completion);

            if (poll_rc < 0) {
                return poll_rc;
            }
            if (poll_rc > 0) {
                if (nvme_handle_multi_write_completion(rt,
                                                       pending,
                                                       NVME_PENDING_CAPACITY,
                                                       contexts,
                                                       req_count,
                                                       &global_inflight,
                                                       &completion) != 0) {
                    return -1;
                }
                if (nvme_finish_completed_slots(rt,
                                                contexts,
                                                reqs,
                                                req_count,
                                                done,
                                                &active_count,
                                                done_cb,
                                                opaque) != 0) {
                    return -1;
                }
                made_progress = true;
            }
        }

        if (!made_progress) {
            NvmeCompletion completion;
            int poll_rc;

            if (global_inflight == 0u) {
                nvme_set_last_error(rt, "sq_full_without_software_pending");
                fprintf(stderr,
                        "NVMe SQ full with no software pending command channel=%d"
                        " active_slots=%u\n",
                        rt->cfg->id,
                        (unsigned)active_count);
                return -1;
            }
            if (!sq_full && global_inflight < qd &&
                nvme_find_next_submit_context(contexts, done, req_count, &rr_index) >= 0) {
                continue;
            }
            poll_rc = nvme_poll_cq(rt, &completion, rt->gopt.timeout_us);
            if (poll_rc != 0) {
                uint32_t dump_index = 0u;
                for (i = 0u; i < req_count; ++i) {
                    if (!done[i]) {
                        dump_index = i;
                        break;
                    }
                }
                nvme_dump_write_timeout_context(rt,
                                                pending,
                                                NVME_PENDING_CAPACITY,
                                                &contexts[dump_index]);
                return poll_rc;
            }
            if (nvme_handle_multi_write_completion(rt,
                                                   pending,
                                                   NVME_PENDING_CAPACITY,
                                                   contexts,
                                                   req_count,
                                                   &global_inflight,
                                                   &completion) != 0) {
                return -1;
            }
            if (nvme_finish_completed_slots(rt,
                                            contexts,
                                            reqs,
                                            req_count,
                                            done,
                                            &active_count,
                                            done_cb,
                                            opaque) != 0) {
                return -1;
            }
        }
    }

    for (i = 0u; i < req_count; ++i) {
        NvmeSlotWriteContext *ctx = &contexts[i];
        if (!done[i] ||
            ctx->next_submit_sector != ctx->total_sectors ||
            ctx->completed_cmds != ctx->submitted_cmds ||
            ctx->inflight_cmds != 0u ||
            ctx->failed_cmds != 0u) {
            nvme_set_last_error(rt, "multi_slot_completion_invariant_failed");
            fprintf(stderr,
                    "NVMe multi-slot completion invariant failed channel=%d slot=%u"
                    " next=%" PRIu64 "/%" PRIu64 " submitted=%u completed=%u"
                    " inflight=%u failed=%u done=%u\n",
                    rt->cfg->id,
                    (unsigned)ctx->slot,
                    ctx->next_submit_sector,
                    ctx->total_sectors,
                    (unsigned)ctx->submitted_cmds,
                    (unsigned)ctx->completed_cmds,
                    (unsigned)ctx->inflight_cmds,
                    (unsigned)ctx->failed_cmds,
                    done[i] ? 1u : 0u);
            return -1;
        }
    }
    return 0;
}

/* Round up bytes to logical sectors used by NVMe commands. */
uint64_t bytes_to_sectors(uint64_t bytes) {
    return (bytes + (SECTOR_SIZE - 1u)) / SECTOR_SIZE;
}

/* Convert CPU DDR address to hardware view used by DMA/NVMe PRP. */
uint64_t cpu_to_hw_addr(const ChannelConfig *cfg, uint64_t cpu_addr) {
    return cfg->ddr_hw_base + (cpu_addr - cfg->ddr_cpu_base);
}

/* Bounds-check CPU address range against mapped DDR window. */
int ddr_addr_validate(const ChannelConfig *cfg, uint64_t cpu_addr, uint64_t size) {
    uint64_t start = cfg->ddr_cpu_base;
    uint64_t end = cfg->ddr_cpu_base + cfg->ddr_cpu_size;
    if (cpu_addr < start || cpu_addr >= end) {
        return -1;
    }
    if (size > 0u && (cpu_addr + size) > end) {
        return -1;
    }
    return 0;
}

int channel_runtime_open(ChannelRuntime *rt, const ChannelConfig *cfg, GlobalOptions gopt) {
    memset(rt, 0, sizeof(*rt));
    rt->cfg = cfg;
    rt->gopt = gopt;
    rt->next_cmd_id = 1u;
    rt->next_harvest_bd = 0u;
    rt->nvme_block_size = 512u;
    rt->nvme_max_dts_bytes = 256u * 1024u;
    rt->nvme_cmd_size_bytes = NVME_CMD_KIB_DEFAULT * 1024u;
    rt->nvme_cmd_sectors = rt->nvme_cmd_size_bytes / NVME_SECTOR_BYTES;
    rt->nvme_qd_requested = NVME_QD_DEFAULT;
    rt->nvme_qd_effective = NVME_QD_DEFAULT;
    rt->dma_desc_bytes = cfg->dma_desc_bytes_default;
    rt->dma_ring_bytes = parse_storage_ring_bytes(rt);
    rt->dma_desc_count = (uint32_t)(rt->dma_ring_bytes / cfg->dma_desc_bytes_default);
    rt->dma_hw_desc_count = 0u;
    rt->pcie_bridge_base_effective = pcie_bridge_base_for_channel(cfg);

    if (gopt.dry_run) {
        /* Dry-run skips all /dev/mem and MMIO setup. */
        storage_print_pcie_link_status(rt, "startup");
        return 0;
    }
#if !HAVE_POSIX_MMAP
    fprintf(stderr, "Non-dry-run mode requires Linux (/dev/mem + mmap)\n");
    return -1;
#else
    {
        int fd = open("/dev/mem", O_RDWR | O_SYNC);
        if (fd < 0) {
            fprintf(stderr, "Failed to open /dev/mem, errno=%d (%s)\n", errno, strerror(errno));
            return -1;
        }
        /*
         * Map all per-channel regions:
         * DMA, switch, NVMe host, descriptor BRAM, DDR data window.
         */
        uint64_t ddr_mmap_bytes = channel_ddr_mmap_bytes(cfg);
        if (ddr_mmap_bytes > (uint64_t)SIZE_MAX) {
            fprintf(stderr,
                    "DDR mmap size too large for userspace: channel=%d size=%" PRIu64 "\n",
                    cfg->id,
                    ddr_mmap_bytes);
            close(fd);
            return -1;
        }
        if (ddr_mmap_bytes < cfg->ddr_cpu_size) {
            dbg_verbose_printf("[DBG][HW] DDR mmap capped ch=%d map=%" PRIu64
                               " total=%" PRIu64 "\n",
                               cfg->id,
                               ddr_mmap_bytes,
                               cfg->ddr_cpu_size);
        }
        if (map_region(fd, cfg->dma_base, 0x10000u, &rt->dma) != 0 ||
            map_region(fd, cfg->axis_switch_base, 0x10000u, &rt->axis_switch) != 0 ||
            map_region(fd, cfg->nvme_base, 0x10000u, &rt->nvme) != 0 ||
            map_region(fd, cfg->desc_cpu_base, (size_t)cfg->desc_cpu_size, &rt->desc) != 0 ||
            map_region(fd, cfg->ddr_cpu_base, (size_t)ddr_mmap_bytes, &rt->ddr) != 0) {
            unmap_region(&rt->ddr);
            unmap_region(&rt->desc);
            unmap_region(&rt->nvme);
            unmap_region(&rt->axis_switch);
            unmap_region(&rt->dma);
            close(fd);
            return -1;
        }
        rt->dma.fd = fd;
        rt->axis_switch.fd = fd;
        rt->nvme.fd = fd;
        rt->desc.fd = fd;
        rt->ddr.fd = fd;
        if (rt->pcie_bridge_base_effective != 0u) {
            if (map_region_optional(fd,
                                    rt->pcie_bridge_base_effective,
                                    PCIE_BRIDGE_MMAP_BYTES,
                                    &rt->pcie_bridge) == 0) {
                rt->pcie_bridge.fd = fd;
            }
        }
    }
    storage_print_pcie_link_status(rt, "startup");
    return 0;
#endif
}

void channel_runtime_close(ChannelRuntime *rt) {
    int fd = -1;
    if (!rt->gopt.dry_run && rt->dma.valid) {
        fd = rt->dma.fd;
    }
    unmap_region(&rt->ddr);
    unmap_region(&rt->desc);
    unmap_region(&rt->nvme);
    unmap_region(&rt->pcie_bridge);
    unmap_region(&rt->axis_switch);
    unmap_region(&rt->dma);
    if (fd >= 0) {
        close(fd);
    }
}

void axis_switch_select(ChannelRuntime *rt, SourceMode src) {
    uint32_t input;
    if (rt->gopt.dry_run) {
        return;
    }
    /* transfer -> input 0, test -> input 1 */
    input = (src == SOURCE_TEST) ? 1u : 0u;
    reg_write32(&rt->axis_switch, AXIS_SWITCH_MI0_MUX_OFFSET, input & 0x0Fu);
    reg_write32(&rt->axis_switch, AXIS_SWITCH_CTRL_REG_OFFSET, AXIS_SWITCH_REG_UPDATE);
    (void)wait_reg_bits(&rt->axis_switch,
                        AXIS_SWITCH_CTRL_REG_OFFSET,
                        AXIS_SWITCH_REG_UPDATE,
                        0u,
                        rt->gopt.timeout_us);
}

static int nvme_wait_links(ChannelRuntime *rt) {
    if (!rt->gopt.skip_link_check) {
        if (wait_reg_bits(&rt->nvme,
                          GENERIC_REG_OFFSET + GENERIC_NVM_STATUS,
                          GENERIC_PCIE_LINK_STATUS,
                          GENERIC_PCIE_LINK_STATUS,
                          rt->gopt.timeout_us) != 0) {
            uint32_t status = reg_read32(&rt->nvme, GENERIC_REG_OFFSET + GENERIC_NVM_STATUS);
            fprintf(stderr,
                    "Channel %d PCIe link not ready status=0x%08x nvme_base=0x%08" PRIx64 "\n",
                    rt->cfg->id,
                    status,
                    rt->cfg->nvme_base);
            return -1;
        }
        if (wait_reg_bits(&rt->nvme,
                          GENERIC_REG_OFFSET + GENERIC_NVM_STATUS,
                          GENERIC_NVME_LINK_STATUS,
                          GENERIC_NVME_LINK_STATUS,
                          rt->gopt.timeout_us) != 0) {
            uint32_t status = reg_read32(&rt->nvme, GENERIC_REG_OFFSET + GENERIC_NVM_STATUS);
            fprintf(stderr,
                    "Channel %d NVMe link not ready status=0x%08x nvme_base=0x%08" PRIx64 "\n",
                    rt->cfg->id,
                    status,
                    rt->cfg->nvme_base);
            return -1;
        }
    }
    return 0;
}

static void nvme_read_capability(ChannelRuntime *rt, const char *tag) {
    uint32_t status = reg_read32(&rt->nvme, GENERIC_REG_OFFSET + GENERIC_NVM_STATUS);
    uint32_t blk_exp = (status >> 29u) & 0x1Fu;
    uint32_t max_dts_blocks = (status >> 16u) & 0x1FFFu;
    uint32_t lba_l;
    uint32_t lba_h;

    rt->nvme_block_size = 1u << (blk_exp + 9u);
    if (rt->nvme_block_size == 0u) {
        rt->nvme_block_size = 512u;
    }
    if (max_dts_blocks == 0u) {
        max_dts_blocks = 512u;
    }
    rt->nvme_max_dts_bytes = max_dts_blocks * rt->nvme_block_size;
    lba_l = reg_read32(&rt->nvme, GENERIC_REG_OFFSET + GENERIC_MAXLBA_L);
    lba_h = reg_read32(&rt->nvme, GENERIC_REG_OFFSET + GENERIC_MAXLBA_H);
    rt->nvme_max_lba = ((uint64_t)lba_h << 32u) | (uint64_t)lba_l;

    dbg_verbose_printf("[DBG][NVME] probe %s ch=%d status=0x%08x block=%u max_dts=%u max_lba=0x%08" PRIx64
                       " tx_status=0x%08x int=0x%08x\n",
                       tag,
                       rt->cfg->id,
                       status,
                       rt->nvme_block_size,
                       rt->nvme_max_dts_bytes,
                       rt->nvme_max_lba,
                       reg_read32(&rt->nvme, QUEUE_REG_OFFSET + QUEUE_TX_STATUS),
                       reg_read32(&rt->nvme, QUEUE_REG_OFFSET + QUEUE_INT_STATUS));
}

int nvme_probe(ChannelRuntime *rt) {
    if (rt->gopt.dry_run) {
        rt->nvme_block_size = 512u;
        rt->nvme_max_dts_bytes = NVME_CMD_KIB_MAX * 1024u;
        rt->nvme_max_lba = 1024ull * 1024ull * 1024ull;
        nvme_configure_runtime(rt);
        storage_print_pcie_link_status(rt, "after_nvme_probe");
        return 0;
    }

    if (nvme_wait_links(rt) != 0) {
        return -1;
    }

    nvme_read_capability(rt, "initial");
    nvme_configure_runtime(rt);
    storage_print_pcie_link_status(rt, "after_nvme_probe");

    {
        uint32_t status = reg_read32(&rt->nvme, GENERIC_REG_OFFSET + GENERIC_NVM_STATUS);
        if (rt->nvme_max_lba == 0u) {
            dbg_printf("[DBG][NVME] probe max_lba unavailable ch=%d status=0x%08x\n",
                       rt->cfg->id,
                       status);
        }
    }
    return 0;
}

static int nvme_issue_one_sync(ChannelRuntime *rt,
                               uint8_t opcode,
                               uint64_t lba,
                               uint32_t sectors,
                               uint64_t hw_addr) {
    NvmePendingCmd pending[1];
    NvmeCompletion completion;
    uint16_t cid;
    uint64_t start_us;
    int submit_rc;
    int poll_rc;

    if (rt->gopt.dry_run) {
        return 0;
    }
    memset(pending, 0, sizeof(pending));
    if (nvme_alloc_cid(rt, pending, 1u, &cid) != 0) {
        return -1;
    }
    start_us = wall_time_us();
    do {
        submit_rc = nvme_submit_command_async(rt, opcode, cid, lba, sectors, hw_addr);
        if (submit_rc == 1) {
            if (elapsed_us_since(start_us) >= rt->gopt.timeout_us) {
                fprintf(stderr, "NVMe SQ full timeout channel=%d op=%s\n",
                        rt->cfg->id, nvme_opcode_name(opcode));
                return -1;
            }
            nvme_poll_pause(rt, start_us);
        }
    } while (submit_rc == 1);
    if (submit_rc != 0) {
        return submit_rc;
    }

    poll_rc = nvme_poll_cq(rt, &completion, rt->gopt.timeout_us);
    if (poll_rc != 0) {
        return poll_rc;
    }
    if (completion.cid != cid) {
        fprintf(stderr,
                "NVMe synchronous completion CID mismatch channel=%d expected=%u actual=%u cq=0x%08x\n",
                rt->cfg->id,
                (unsigned)cid,
                (unsigned)completion.cid,
                completion.raw);
        return -1;
    }
    if (completion.error) {
        fprintf(stderr,
                "NVMe synchronous completion error channel=%d cid=%u cq=0x%08x status_code=0x%02x\n",
                rt->cfg->id,
                (unsigned)cid,
                completion.raw,
                (unsigned)completion.status_code);
        return -1;
    }
    return 0;
}

int nvme_rw(ChannelRuntime *rt, bool is_write, uint64_t lba, uint64_t sectors, uint64_t hw_addr) {
    uint64_t max_sectors = rt->nvme_cmd_sectors;
    uint64_t submitted = 0u;
    uint64_t cur_lba = lba;
    uint64_t cur_hw = hw_addr;

    if (max_sectors == 0u) {
        max_sectors = NVME_CMD_KIB_DEFAULT * 1024u / NVME_SECTOR_BYTES;
    }
    /* Preserve the legacy environment variable as an optional reducing cap. */
    max_sectors = env_u64_limit("CCB_NVME_MAX_SECTORS", max_sectors, max_sectors);
    dbg_verbose_printf("[DBG][NVME] rw start ch=%d op=%s lba=0x%08" PRIx64
                       " sectors=%" PRIu64 " hw=0x%08" PRIx64
                       " max_sectors=%" PRIu64 " max_dts=%u block=%u\n",
                       rt->cfg->id,
                       is_write ? "write" : "read",
                       lba,
                       sectors,
                       hw_addr,
                       max_sectors,
                       rt->nvme_max_dts_bytes,
                       rt->nvme_block_size);

    if (is_write) {
        return nvme_write_slot_qd(rt, UINT32_MAX, lba, sectors, hw_addr);
    }

    while (submitted < sectors) {
        uint64_t rem = sectors - submitted;
        uint16_t this_sectors = (uint16_t)((rem > max_sectors) ? max_sectors : rem);
        int issue_rc;

        if (nvme_stop_requested()) {
            dbg_printf("[DBG][NVME] rw stop ch=%d op=%s submitted=%" PRIu64 "/%" PRIu64 "\n",
                       rt->cfg->id,
                       is_write ? "write" : "read",
                       submitted,
                       sectors);
            return -2;
        }

        issue_rc = nvme_issue_one_sync(rt, NVM_READ, cur_lba, this_sectors, cur_hw);
        if (issue_rc != 0) {
            if (issue_rc == -2) {
                dbg_printf("[DBG][NVME] rw stopped ch=%d op=%s submitted=%" PRIu64 "/%" PRIu64 "\n",
                           rt->cfg->id,
                           is_write ? "write" : "read",
                           submitted,
                           sectors);
                return -2;
            }
            return -1;
        }
        submitted += this_sectors;
        cur_lba += this_sectors;
        cur_hw += this_sectors * (uint64_t)NVME_SECTOR_BYTES;
    }
    dbg_verbose_printf("[DBG][NVME] rw done ch=%d op=%s lba=0x%08" PRIx64
                       " sectors=%" PRIu64 " hw=0x%08" PRIx64 "\n",
                       rt->cfg->id,
                       is_write ? "write" : "read",
                       lba,
                       sectors,
                       hw_addr);
    return 0;
}

static int dma_reset_full(ChannelRuntime *rt, const char *tag) {
    int mm2s_ok;
    int s2mm_ok;
    uint32_t mm2s_cr;
    uint32_t s2mm_cr;

    if (!rt || rt->gopt.dry_run) {
        return 0;
    }

    dbg_verbose_printf("[DBG][DMA] reset start ch=%d tag=%s mm2s_cr=0x%08x mm2s_sr=0x%08x s2mm_cr=0x%08x s2mm_sr=0x%08x\n",
                       rt->cfg->id,
                       tag ? tag : "none",
                       reg_read32(&rt->dma, MM2S_DMACR),
                       reg_read32(&rt->dma, MM2S_DMASR),
                       reg_read32(&rt->dma, S2MM_DMACR),
                       reg_read32(&rt->dma, S2MM_DMASR));

    mm2s_cr = reg_read32(&rt->dma, MM2S_DMACR);
    s2mm_cr = reg_read32(&rt->dma, S2MM_DMACR);
    if (((mm2s_cr | s2mm_cr) & DMA_CR_RESET_BIT) != 0u) {
        fprintf(stderr,
                "DMA reset already in progress on channel %d: tag=%s mm2s_cr=0x%08x s2mm_cr=0x%08x\n",
                rt->cfg->id,
                tag ? tag : "none",
                mm2s_cr,
                s2mm_cr);
        return -1;
    }

    /* Keep the legacy known-good sequence: reset both channel register banks. */
    reg_write32(&rt->dma, MM2S_DMACR, DMA_CR_RESET_BIT);
    reg_write32(&rt->dma, S2MM_DMACR, DMA_CR_RESET_BIT);

    mm2s_ok = (wait_reg_bits(&rt->dma, MM2S_DMACR, DMA_CR_RESET_BIT, 0u, rt->gopt.timeout_us) == 0);
    s2mm_ok = (wait_reg_bits(&rt->dma, S2MM_DMACR, DMA_CR_RESET_BIT, 0u, rt->gopt.timeout_us) == 0);

    dbg_verbose_printf("[DBG][DMA] reset done ch=%d tag=%s mm2s_ok=%d s2mm_ok=%d mm2s_cr=0x%08x mm2s_sr=0x%08x s2mm_cr=0x%08x s2mm_sr=0x%08x\n",
                       rt->cfg->id,
                       tag ? tag : "none",
                       mm2s_ok,
                       s2mm_ok,
                       reg_read32(&rt->dma, MM2S_DMACR),
                       reg_read32(&rt->dma, MM2S_DMASR),
                       reg_read32(&rt->dma, S2MM_DMACR),
                       reg_read32(&rt->dma, S2MM_DMASR));

    if (!mm2s_ok || !s2mm_ok) {
        fprintf(stderr,
                "DMA full reset timeout on channel %d: tag=%s mm2s_ok=%d s2mm_ok=%d"
                " mm2s_cr=0x%08x s2mm_cr=0x%08x mm2s_sr=0x%08x s2mm_sr=0x%08x\n",
                rt->cfg->id,
                tag ? tag : "none",
                mm2s_ok,
                s2mm_ok,
                reg_read32(&rt->dma, MM2S_DMACR),
                reg_read32(&rt->dma, S2MM_DMACR),
                reg_read32(&rt->dma, MM2S_DMASR),
                reg_read32(&rt->dma, S2MM_DMASR));
        return -1;
    }
    return 0;
}

int dma_prepare_s2mm_ring(ChannelRuntime *rt, uint32_t dma_desc_bytes) {
    uint32_t desc_count = (uint32_t)(rt->dma_ring_bytes / (uint64_t)dma_desc_bytes);
    uint32_t desc_capacity = (uint32_t)(rt->cfg->desc_cpu_size / sizeof(DmaSgDesc));
    /*
     * Descriptor count is constrained by:
     * - channel total hardware DDR coverage
     * - descriptor BRAM capacity
     */
    if (rt->dma_ring_bytes == 0u ||
        (rt->dma_ring_bytes % (uint64_t)dma_desc_bytes) != 0u ||
        desc_count == 0u || desc_capacity == 0u || desc_count > desc_capacity) {
        fprintf(stderr,
                "Invalid dma_desc_bytes=%u for channel %d: ring=%" PRIu64 " bytes, desc_count=%u (max descriptors=%u)\n",
                (unsigned)dma_desc_bytes,
                rt->cfg->id,
                rt->dma_ring_bytes,
                (unsigned)desc_count,
                (unsigned)desc_capacity);
        return -1;
    }
    rt->dma_desc_bytes = dma_desc_bytes;
    rt->dma_desc_count = desc_count;
    rt->dma_rx_packet_open = false;
    rt->dma_last_completed_bd = UINT32_MAX;
    rt->dma_last_completed_status = 0u;
    rt->dma_rxsof_count = 0u;
    rt->dma_rxeof_count = 0u;
    __atomic_store_n(&rt->dma_hw_desc_count, desc_count, __ATOMIC_RELEASE);
    if (rt->gopt.dry_run) {
        rt->next_harvest_bd = 0u;
        return 0;
    }

    {
        DmaSgDesc *desc = (DmaSgDesc *)(void *)rt->desc.virt;
        uint32_t i;
        /* Build a circular SG ring with contiguous buffer slices. */
        for (i = 0; i < rt->dma_desc_count; ++i) {
            uint32_t next = (i + 1u) % rt->dma_desc_count;
            memset(&desc[i], 0, sizeof(DmaSgDesc));
            {
                uint64_t next_addr = rt->cfg->desc_dma_base +
                                     (uint64_t)next * sizeof(DmaSgDesc);
                uint64_t buffer_addr = rt->cfg->ddr_hw_base +
                                       (uint64_t)i * dma_desc_bytes;
                desc[i].next_desc = (uint32_t)next_addr;
                desc[i].next_desc_msb = (uint32_t)(next_addr >> 32u);
                desc[i].buffer_addr = (uint32_t)buffer_addr;
                desc[i].buffer_addr_msb = (uint32_t)(buffer_addr >> 32u);
            }
            desc[i].control = dma_desc_bytes;
            desc[i].status = 0u;
        }
        __sync_synchronize();
        for (i = 0; i < rt->dma_desc_count; ++i) {
            uint32_t next = (i + 1u) % rt->dma_desc_count;
            uint64_t expected_next = rt->cfg->desc_dma_base +
                                     (uint64_t)next * sizeof(DmaSgDesc);
            uint64_t expected_buffer = rt->cfg->ddr_hw_base +
                                       (uint64_t)i * dma_desc_bytes;
            uint64_t buffer_end = expected_buffer + dma_desc_bytes;

            if (desc[i].next_desc != (uint32_t)expected_next ||
                desc[i].next_desc_msb != (uint32_t)(expected_next >> 32u) ||
                desc[i].buffer_addr != (uint32_t)expected_buffer ||
                desc[i].buffer_addr_msb != (uint32_t)(expected_buffer >> 32u) ||
                desc[i].control != dma_desc_bytes || desc[i].status != 0u ||
                expected_buffer < rt->cfg->ddr_hw_base ||
                buffer_end > rt->cfg->ddr_hw_base + rt->dma_ring_bytes) {
                fprintf(stderr,
                        "DMA descriptor init validation failed channel=%d slot=%u"
                        " next=0x%08x buffer=0x%08x control=0x%08x status=0x%08x\n",
                        rt->cfg->id,
                        (unsigned)i,
                        desc[i].next_desc,
                        desc[i].buffer_addr,
                        desc[i].control,
                        desc[i].status);
                return -1;
            }
        }
    }

    /* Match src_real (1): every storage task initializes DMA from full reset. */
    if (dma_reset_full(rt, "s2mm_init") != 0) {
        return -1;
    }

    /* Clear stale IRQ status after the channel is halted. */
    reg_write32(&rt->dma, S2MM_DMASR, DMA_IRQ_STATUS_MASK);

    reg_write32(&rt->dma, S2MM_CURDESC, (uint32_t)rt->cfg->desc_dma_base);
    reg_write32(&rt->dma, S2MM_CURDESC_MSB, (uint32_t)(rt->cfg->desc_dma_base >> 32u));
    return 0;
}

int dma_start_s2mm_ring(ChannelRuntime *rt)
{
    if (!rt || rt->dma_desc_count == 0u) return -1;
    if (rt->gopt.dry_run) return 0;
    reg_write32(&rt->dma, S2MM_DMACR, DMA_CR_RS_BIT | 0x1000u);
    reg_write32(&rt->dma,
                S2MM_TAILDESC,
                (uint32_t)(rt->cfg->desc_dma_base + (uint64_t)((rt->dma_desc_count - 1u) * sizeof(DmaSgDesc))));
    reg_write32(&rt->dma,
                S2MM_TAILDESC_MSB,
                (uint32_t)((rt->cfg->desc_dma_base +
                            (uint64_t)(rt->dma_desc_count - 1u) * sizeof(DmaSgDesc)) >> 32u));
    __sync_synchronize();
    if (wait_reg_bits(&rt->dma,
                      S2MM_DMASR,
                      DMA_SR_HALT_BIT,
                      0u,
                      rt->gopt.timeout_us) != 0) {
        fprintf(stderr, "DMA start did not leave halted state channel=%d\n", rt->cfg->id);
        return -1;
    }
    {
        uint32_t cr = reg_read32(&rt->dma, S2MM_DMACR);
        uint32_t sr = reg_read32(&rt->dma, S2MM_DMASR);
        uint32_t cur = reg_read32(&rt->dma, S2MM_CURDESC);
        uint32_t tail = reg_read32(&rt->dma, S2MM_TAILDESC);
        printf("storage_dma_started channel=%d s2mm_dmacr=0x%08x"
               " s2mm_dmasr=0x%08x curdesc=0x%08x taildesc=0x%08x\n",
               rt->cfg->id, cr, sr, cur, tail);
        if ((cr & DMA_CR_RS_BIT) == 0u || (sr & DMA_ERROR_MASK_S2MM) != 0u) {
            return -1;
        }
    }
    __atomic_store_n(&rt->dma_hw_desc_count, rt->dma_desc_count, __ATOMIC_RELEASE);
    rt->next_harvest_bd = 0u;
    return 0;
}

int dma_init_s2mm_ring(ChannelRuntime *rt, uint32_t dma_desc_bytes)
{
    if (dma_prepare_s2mm_ring(rt, dma_desc_bytes) != 0) return -1;
    return dma_start_s2mm_ring(rt);
}

static int dma_harvest_batch_impl(ChannelRuntime *rt, DmaHarvestItem *items,
                                  uint32_t max_items, uint32_t budget_us,
                                  uint32_t *out_count, bool allow_halted)
{
    uint32_t count = 0u;
    uint64_t start_us = 0u;
    if (!rt || !items || !out_count || max_items == 0u) return -1;
    *out_count = 0u;
    if (budget_us != 0u) start_us = wall_time_us();
    if (rt->gopt.dry_run) {
        for (; count < max_items; ++count) {
            items[count].slot = rt->next_harvest_bd;
            items[count].actual_bytes = rt->dma_desc_bytes;
            items[count].descriptor_status = 0u;
            rt->next_harvest_bd = (rt->next_harvest_bd + 1u) % rt->dma_desc_count;
        }
        *out_count = count;
        return 0;
    }
    while (count < max_items) {
        volatile DmaSgDesc *desc;
        uint32_t idx, st, dsr;
        if (__atomic_load_n(&rt->dma_hw_desc_count, __ATOMIC_ACQUIRE) == 0u) break;
        dsr = reg_read32(&rt->dma, S2MM_DMASR);
        if ((dsr & DMA_ERROR_MASK_S2MM) != 0u ||
            (!allow_halted && (dsr & DMA_SR_HALT_BIT) != 0u)) {
            *out_count = count; return -1;
        }
        desc = (volatile DmaSgDesc *)(void *)rt->desc.virt;
        idx = rt->next_harvest_bd; st = desc[idx].status;
        if ((st & DESC_STS_CMPLT) == 0u) break;
        rt->dma_last_completed_bd = idx; rt->dma_last_completed_status = st;
        if ((st & DESC_STS_ERROR_MASK) != 0u) { *out_count = count; return -1; }
        items[count].slot = idx;
        items[count].actual_bytes = st & DESC_STS_LEN_MASK;
        items[count].descriptor_status = st;
        if ((st & DESC_STS_RXSOF) != 0u) { ++rt->dma_rxsof_count; rt->dma_rx_packet_open = true; }
        if ((st & DESC_STS_RXEOF) != 0u) { ++rt->dma_rxeof_count; rt->dma_rx_packet_open = false; }
        (void)__atomic_sub_fetch(&rt->dma_hw_desc_count, 1u, __ATOMIC_ACQ_REL);
        rt->next_harvest_bd = (idx + 1u) % rt->dma_desc_count;
        ++count;
        if (budget_us != 0u && wall_time_us() - start_us >= budget_us) break;
    }
    *out_count = count;
    return 0;
}

int dma_harvest_batch(ChannelRuntime *rt, DmaHarvestItem *items,
                      uint32_t max_items, uint32_t budget_us, uint32_t *out_count)
{
    return dma_harvest_batch_impl(rt, items, max_items, budget_us, out_count, false);
}

int dma_harvest_completed_batch(ChannelRuntime *rt, DmaHarvestItem *items,
                                uint32_t max_items, uint32_t *out_count)
{
    return dma_harvest_batch_impl(rt, items, max_items, 0u, out_count, true);
}

int dma_harvest_one(ChannelRuntime *rt, uint32_t *slot, uint32_t *actual_bytes) {
    DmaHarvestItem item;
    uint32_t count = 0u;
    int rc;
    if (!slot || !actual_bytes) return -1;
    rc = dma_harvest_batch(rt, &item, 1u, 0u, &count);
    if (count != 0u) { *slot = item.slot; *actual_bytes = item.actual_bytes; }
    return rc != 0 ? -1 : (count != 0u ? 1 : 0);
}

int dma_requeue_one(ChannelRuntime *rt, uint32_t slot) {
    if (!rt || rt->gopt.dry_run || slot >= rt->dma_desc_count) {
        return (rt && rt->gopt.dry_run && slot < rt->dma_desc_count) ? 0 : -1;
    }

    {
        volatile DmaSgDesc *desc = (volatile DmaSgDesc *)(void *)rt->desc.virt;
        uint32_t hw_owned;

        /* PG021 Tail Pointer Mode: clear completion before moving TAILDESC. */
        desc[slot].status = 0u;
        __sync_synchronize();
        reg_write32(&rt->dma,
                    S2MM_TAILDESC,
                    (uint32_t)(rt->cfg->desc_dma_base + (uint64_t)(slot * sizeof(DmaSgDesc))));
        reg_write32(&rt->dma,
                    S2MM_TAILDESC_MSB,
                    (uint32_t)((rt->cfg->desc_dma_base +
                                (uint64_t)slot * sizeof(DmaSgDesc)) >> 32u));
        __sync_synchronize();
        {
            uint32_t sr = reg_read32(&rt->dma, S2MM_DMASR);
            uint32_t cr = reg_read32(&rt->dma, S2MM_DMACR);
            if ((sr & (DMA_ERROR_MASK_S2MM | DMA_SR_HALT_BIT)) != 0u ||
                (cr & DMA_CR_RS_BIT) == 0u) {
                fprintf(stderr,
                        "DMA requeue failed channel=%d slot=%u dmacr=0x%08x dmasr=0x%08x\n",
                        rt->cfg->id, (unsigned)slot, cr, sr);
                return -1;
            }
        }
        hw_owned = __atomic_add_fetch(&rt->dma_hw_desc_count, 1u, __ATOMIC_ACQ_REL);
        if (hw_owned > rt->dma_desc_count) {
            fprintf(stderr,
                    "DMA descriptor ownership overflow on channel %d: hw_owned=%u total=%u\n",
                    rt->cfg->id,
                    (unsigned)hw_owned,
                    (unsigned)rt->dma_desc_count);
            __atomic_store_n(&rt->dma_hw_desc_count, rt->dma_desc_count, __ATOMIC_RELEASE);
            return -1;
        }
    }
    return 0;
}

static uint32_t dma_desc_address_to_index(const ChannelRuntime *rt, uint64_t address)
{
    uint64_t offset;

    if (!rt || address < rt->cfg->desc_dma_base) {
        return UINT32_MAX;
    }
    offset = address - rt->cfg->desc_dma_base;
    if ((offset % sizeof(DmaSgDesc)) != 0u ||
        offset / sizeof(DmaSgDesc) >= rt->dma_desc_count) {
        return UINT32_MAX;
    }
    return (uint32_t)(offset / sizeof(DmaSgDesc));
}

int dma_get_bd_snapshot(ChannelRuntime *rt,
                        const uint8_t *software_slot_state,
                        DmaBdSnapshot *out)
{
    volatile const DmaSgDesc *desc;
    uint32_t i;
    uint64_t curdesc;
    uint64_t taildesc;

    if (!rt || !out || rt->dma_desc_count == 0u) {
        return -1;
    }
    memset(out, 0, sizeof(*out));
    out->total_slots = rt->dma_desc_count;
    out->curdesc_index = UINT32_MAX;
    out->taildesc_index = UINT32_MAX;
    if (!rt->gopt.dry_run) {
        out->s2mm_dmasr = reg_read32(&rt->dma, S2MM_DMASR);
        out->s2mm_dmacr = reg_read32(&rt->dma, S2MM_DMACR);
        curdesc = (uint64_t)reg_read32(&rt->dma, S2MM_CURDESC) |
                  ((uint64_t)reg_read32(&rt->dma, S2MM_CURDESC_MSB) << 32u);
        taildesc = (uint64_t)reg_read32(&rt->dma, S2MM_TAILDESC) |
                   ((uint64_t)reg_read32(&rt->dma, S2MM_TAILDESC_MSB) << 32u);
        out->curdesc_index = dma_desc_address_to_index(rt, curdesc);
        out->taildesc_index = dma_desc_address_to_index(rt, taildesc);
    }
    __sync_synchronize();
    desc = (volatile const DmaSgDesc *)(const void *)rt->desc.virt;
    for (i = 0u; i < rt->dma_desc_count; ++i) {
        uint8_t state = software_slot_state ? software_slot_state[i]
                                            : STORAGE_SLOT_DMA_WRITABLE;
        if (state == STORAGE_SLOT_DMA_WRITABLE) {
            uint32_t status = rt->gopt.dry_run ? 0u : desc[i].status;
            if ((status & DESC_STS_CMPLT) != 0u) {
                ++out->completed_unharvested;
            } else {
                ++out->dma_writable;
            }
        } else if (state == STORAGE_SLOT_DMA_COMPLETED_UNHARVESTED) {
            ++out->completed_unharvested;
        } else if (state == STORAGE_SLOT_READY_FOR_NVME) {
            ++out->ready_slots;
        } else if (state == STORAGE_SLOT_NVME_BUSY) {
            ++out->nvme_busy_slots;
        } else if (state == STORAGE_SLOT_REQUEUE_PENDING) {
            ++out->requeue_pending;
        } else if (state == STORAGE_SLOT_FREE) {
            ++out->free_slots;
        } else {
            return -1;
        }
    }
    out->occupied_bytes_est =
        (uint64_t)(out->completed_unharvested + out->ready_slots +
                   out->nvme_busy_slots + out->requeue_pending) * rt->dma_desc_bytes;
    return 0;
}

bool dma_s2mm_tail_incomplete(const ChannelRuntime *rt) {
    volatile const DmaSgDesc *desc;
    uint32_t status;

    if (!rt || rt->gopt.dry_run || rt->dma_desc_count == 0u || !rt->desc.valid) {
        return false;
    }
    desc = (volatile const DmaSgDesc *)(const void *)rt->desc.virt;
    status = desc[rt->next_harvest_bd].status;

    /*
     * PG021 does not guarantee that a partial S2MM BD exposes its byte count
     * before completion. RXSOF without RXEOF is therefore also retained as a
     * conservative indication that stopping may discard an unfinished frame.
     */
    return rt->dma_rx_packet_open ||
           (((status & DESC_STS_CMPLT) == 0u) && ((status & DESC_STS_LEN_MASK) != 0u));
}

int dma_quiesce_s2mm(ChannelRuntime *rt, uint64_t deadline_us, DmaStopReport *report)
{
    volatile const DmaSgDesc *desc = NULL;
    uint32_t control;
    uint32_t next_status = 0u;
    uint32_t hw_owned;

    if (!rt || rt->gopt.dry_run) {
        if (report) {
            memset(report, 0, sizeof(*report));
        }
        return rt ? 0 : -1;
    }

    if (rt->desc.valid && rt->dma_desc_count > 0u &&
        rt->next_harvest_bd < rt->dma_desc_count) {
        desc = (volatile const DmaSgDesc *)(const void *)rt->desc.virt;
        next_status = desc[rt->next_harvest_bd].status;
    }
    hw_owned = __atomic_load_n(&rt->dma_hw_desc_count, __ATOMIC_ACQUIRE);
    control = reg_read32(&rt->dma, S2MM_DMACR);
    if (report) {
        memset(report, 0, sizeof(*report));
        report->s2mm_cr_before = control;
        report->s2mm_sr_before = reg_read32(&rt->dma, S2MM_DMASR);
        report->curdesc = reg_read32(&rt->dma, S2MM_CURDESC);
        report->taildesc = reg_read32(&rt->dma, S2MM_TAILDESC);
        report->next_bd = rt->next_harvest_bd;
        report->next_bd_status = next_status;
        report->hw_owned = hw_owned;
        report->rxsof_count = rt->dma_rxsof_count;
        report->rxeof_count = rt->dma_rxeof_count;
        report->rx_packet_open = rt->dma_rx_packet_open;
    }
    if (hw_storage_log_at_least_debug()) {
        printf("storage_dma_stop_begin channel=%d s2mm_cr=0x%08x s2mm_sr=0x%08x"
               " curdesc=0x%08x taildesc=0x%08x next_bd=%u next_bd_status=0x%08x"
               " hw_owned=%u rxsof_count=%" PRIu64 " rxeof_count=%" PRIu64
               " rx_packet_open=%u\n",
               rt->cfg->id,
               control,
               reg_read32(&rt->dma, S2MM_DMASR),
               reg_read32(&rt->dma, S2MM_CURDESC),
               reg_read32(&rt->dma, S2MM_TAILDESC),
               (unsigned)rt->next_harvest_bd,
               next_status,
               (unsigned)hw_owned,
               rt->dma_rxsof_count,
               rt->dma_rxeof_count,
               rt->dma_rx_packet_open ? 1u : 0u);
        fflush(stdout);
    }

    reg_write32(&rt->dma, S2MM_DMACR, control & ~DMA_CR_RS_BIT);
    __sync_synchronize();
    for (;;) {
        uint32_t status = reg_read32(&rt->dma, S2MM_DMASR);

        if ((status & DMA_ERROR_MASK_S2MM) != 0u) {
            errno = EIO;
            return -1;
        }
        if ((status & DMA_SR_HALT_BIT) != 0u) break;
        if (deadline_us != 0u && wall_time_us() >= deadline_us) {
            errno = ETIMEDOUT;
            return -1;
        }
        sched_yield();
    }

    if (report) {
        report->s2mm_cr_after = reg_read32(&rt->dma, S2MM_DMACR);
        report->s2mm_sr_after = reg_read32(&rt->dma, S2MM_DMASR);
    }
    return 0;
}

DmaStopResult dma_finalize_stop_s2mm(ChannelRuntime *rt, DmaStopReport *report)
{
    DmaStopResult result = DMA_STOP_FAILED;

    if (!rt || rt->gopt.dry_run) return rt ? DMA_STOP_OK : DMA_STOP_FAILED;
    __atomic_store_n(&rt->dma_hw_desc_count, 0u, __ATOMIC_RELEASE);
    if (report) report->reset_attempted = true;
    /* Final reset is deliberately after the halted descriptor state was harvested. */
    if (dma_reset_full(rt, "s2mm_stop") != 0) {
        return DMA_STOP_FAILED;
    }
    result = DMA_STOP_OK;
    if (report) {
        report->s2mm_cr_after = reg_read32(&rt->dma, S2MM_DMACR);
        report->s2mm_sr_after = reg_read32(&rt->dma, S2MM_DMASR);
    }
    if (hw_storage_log_at_least_debug() || result != DMA_STOP_OK) {
        printf("storage_dma_stop_done channel=%d result=%s s2mm_cr=0x%08x s2mm_sr=0x%08x"
               " next_bd_status=0x%08x rxsof_count=%" PRIu64 " rxeof_count=%" PRIu64
               " rx_packet_open=%u reset_attempted=%u\n",
               rt->cfg->id,
               result == DMA_STOP_OK
                   ? "full_reset"
                   : (result == DMA_STOP_RESET_RECOVERED ? "soft_reset_recovered" : "failed"),
               reg_read32(&rt->dma, S2MM_DMACR),
               reg_read32(&rt->dma, S2MM_DMASR),
               report ? report->next_bd_status : 0u,
               rt->dma_rxsof_count,
               rt->dma_rxeof_count,
               rt->dma_rx_packet_open ? 1u : 0u,
               report && report->reset_attempted ? 1u : 0u);
        fflush(stdout);
    }
    return result;
}

DmaStopResult dma_stop_s2mm(ChannelRuntime *rt, DmaStopReport *report)
{
    uint64_t deadline_us;

    if (!rt) return DMA_STOP_FAILED;
    deadline_us = wall_time_us() + (rt->gopt.timeout_us ? rt->gopt.timeout_us
                                                        : DEFAULT_TIMEOUT_US);
    if (dma_quiesce_s2mm(rt, deadline_us, report) != 0) return DMA_STOP_FAILED;
    return dma_finalize_stop_s2mm(rt, report);
}
