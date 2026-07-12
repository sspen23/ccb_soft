#include "ccb_commands.h"

#include "ccb_config.h"
#include "ccb_hw.h"
#include "ccb_metadata.h"
#include "ccb_storage_ipc.h"
#include "ccb_storage_pipeline.h"
#include "debug_uart.h"

#include <stdio.h>
#include <string.h>
#include <inttypes.h>
#include <pthread.h>
#include <signal.h>
#include <sys/time.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include <sched.h>
#include <stdarg.h>
#include <time.h>
#include <limits.h>
#include <fcntl.h>

#define STORAGE_STOP_DRAIN_POLLS_DEFAULT 1000u
#define STORAGE_STOP_DRAIN_SLEEP_US 1000u
#define STORAGE_IDLE_NOTICE_MS_DEFAULT 5000u
#define STORAGE_PIPELINE_STATS_SEC_DEFAULT 5u
#define STORAGE_POLL_SLEEP_US_DEFAULT 100u
#define STORAGE_HIGH_WATERMARK_POLL_US_DEFAULT 10u
#define STORAGE_CRITICAL_WATERMARK_POLL_US_DEFAULT 0u
#define NVME_CROSS_SLOT_BATCH_DEFAULT 8u
#define STORAGE_READY_QUEUE_DEPTH_DEFAULT 0u
#define STORAGE_HARVEST_BATCH_MAX_DEFAULT 1u
#define STORAGE_HIGH_DESC_BYTES_DEFAULT (8u * 1024u * 1024u)
#define DDR_PATTERN_STORE_DEFAULT_BYTES (32ull * 1024ull * 1024ull)
#define SSD_CONTINUOUS_PATTERN_TEST_DEFAULT_BYTES (640ull * 1024ull * 1024ull)
#define STORAGE_DESC_STS_RXSOF (1u << 27)
#define STORAGE_DESC_STS_RXEOF (1u << 26)
static volatile sig_atomic_t g_storage_stop_requested = 0;
static bool g_storage_control_stop_latched = false;

typedef struct {
    uint32_t slot;
    uint64_t bytes;
    uint64_t chunk_index;
    uint64_t file_offset;
    uint64_t start_lba;
    uint64_t sectors;
    uint64_t hw_addr;
} PendingDdrSlot;

typedef enum {
    STORAGE_LOG_QUIET = 0,
    STORAGE_LOG_SUMMARY = 1,
    STORAGE_LOG_DEBUG = 2,
    STORAGE_LOG_TRACE = 3,
} StorageLogLevel;

typedef struct {
    uint32_t ready_depth_current;
    uint32_t ready_depth_avg;
    uint32_t ready_depth_max;
    uint32_t free_slots;
    uint32_t dma_writable_slots;
    uint32_t completed_unharvested_slots;
    uint32_t ready_for_nvme_slots;
    uint32_t nvme_busy_slots;
    uint32_t requeue_pending_slots;
    uint32_t busy_count;
    uint64_t buffered_bytes;
    uint64_t writer_idle_us;
    uint64_t writer_active_us;
    uint64_t ready_q_nonempty_us;
    uint64_t writer_drain_loop_count;
    uint64_t writer_slots_drained;
    bool writer_rt_enabled;
    int writer_rt_policy;
    uint32_t writer_rt_prio;
} StorageQueueSnapshot;

typedef struct {
    ChannelRuntime *rt;
    PendingDdrSlot *items;
    bool *slot_busy;
    uint8_t *slot_state;
    StorageSlotCounts slot_counts;
    uint32_t capacity;
    uint32_t head;
    uint32_t tail;
    uint32_t count;
    uint64_t ready_depth_sum;
    uint64_t ready_depth_samples;
    uint32_t ready_depth_max;
    uint32_t busy_count;
    uint32_t max_busy_count;
    uint64_t buffered_bytes;
    uint64_t max_buffered_bytes;
    uint64_t bytes_written;
    uint64_t nvme_write_us;
    uint64_t writer_idle_us;
    uint64_t writer_active_us;
    uint64_t ready_q_nonempty_us;
    uint64_t writer_drain_loop_count;
    uint64_t writer_slots_drained;
    bool writer_rt_enabled;
    int writer_rt_policy;
    uint32_t writer_rt_prio;
    bool backlog_mode;
    uint32_t chunks;
    uint32_t file_index;
    int metadata_slot;
    const char *task_no;
    bool cross_slot_qd;
    uint32_t cross_slot_batch;
    bool producer_done;
    bool run_enabled;
    bool error;
    pthread_mutex_t lock;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} StorageWriteQueue;

typedef struct {
    uint32_t interval_ms;
    uint64_t next_log_us;
    uint64_t window_start_us;
    uint64_t window_received_bytes;
    uint64_t window_nvme_bytes;
    uint64_t dma_desc_completed_count;
    uint64_t dma_harvest_interval_count;
    uint64_t dma_harvest_interval_total_us;
    uint64_t dma_harvest_interval_min_us;
    uint64_t dma_harvest_interval_max_us;
    uint64_t first_dma_desc_us;
    uint64_t last_dma_desc_us;
    uint64_t window_backlog_bytes;
    uint64_t ring_full_count;
    uint64_t ring_full_total_us;
    uint64_t ring_full_start_us;
    uint64_t ring_full_first_at_bytes;
    uint64_t ring_full_last_at_bytes;
    bool ring_full_active;
    bool integrity_risk_ring_full;
    uint32_t watermark_level;
    uint64_t dma_no_free_slot_count;
    uint64_t dma_harvest_batches;
    uint64_t dma_harvest_batch_total;
    uint32_t dma_harvest_batch_max;
    uint32_t dma_harvest_batch_current;
    uint64_t window_nvme_cmd_count;
    uint64_t window_nvme_cq_completed;
    bool idle_printed;
    bool ring_warning_emitted;
    uint64_t last_ring_warning_us;
    uint64_t dma_error_count;
    uint64_t descriptor_error_count;
    uint32_t max_completed_unharvested;
    uint32_t min_dma_writable;
    uint64_t max_occupied_bytes_est;
    bool receive_integrity_ok;
    bool dma_bd_low_active;
    uint64_t first_receive_failure_us;
    uint64_t first_receive_failure_bytes;
    char receive_integrity_risk[64];
    DmaBdSnapshot first_failure_snapshot;
    DmaBdSnapshot last_bd_snapshot;
    uint64_t last_bd_snapshot_us;
} StorageProducerStats;

typedef struct {
    uint64_t submit_calls;
    uint64_t submit_total_us;
    uint64_t submit_pending_wait_us;
    uint64_t submit_sq_full_count;
    uint64_t cq_poll_calls;
    uint64_t cq_empty_polls;
    uint64_t cq_wait_total_us;
    uint64_t cq_pop_total_us;
    uint64_t cq_completed;
    uint64_t cmd_count;
    uint64_t active_qd_integral_us;
    uint64_t active_qd_observed_us;
} StorageNvmeTimingSnapshot;

static int flush_slot_to_nvme(ChannelRuntime *rt,
                              const PendingDdrSlot *item,
                              uint32_t file_index,
                              int metadata_slot,
                              const char *task_no);
static void storage_mark_writer_error(StorageWriteQueue *q);
static void storage_trace_flush_start(const ChannelRuntime *rt, const PendingDdrSlot *item);
static void storage_trace_flush_done(const ChannelRuntime *rt,
                                     const PendingDdrSlot *item,
                                     uint32_t file_index,
                                     int metadata_slot,
                                     const char *task_no);
void storage_write_request_stop(void);

static uint64_t storage_wall_time_us(void) {
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

static uint64_t storage_elapsed_us(uint64_t start_us) {
    uint64_t now = storage_wall_time_us();
    if (now < start_us) {
        return 0u;
    }
    return now - start_us;
}

static int storage_env_fd(const char *name)
{
    const char *value = getenv(name);
    char *end = NULL;
    long fd;
    if (!value || value[0] == '\0') return -1;
    fd = strtol(value, &end, 10);
    return end != value && *end == '\0' && fd >= 0 && fd <= INT_MAX ? (int)fd : -1;
}

static void storage_emit_event(StorageWorkerEventType type, const ChannelRuntime *rt,
                               int error_code, uint64_t bytes, const char *reason)
{
    StorageWorkerEvent event;
    int fd = storage_env_fd("SRC_REAL_STORAGE_EVENT_FD");
    if (fd < 0) return;
    storage_ipc_make_event(&event, type, rt && rt->cfg ? (uint32_t)rt->cfg->id : UINT32_MAX,
                           error_code, bytes, reason);
    (void)storage_ipc_write_event(fd, &event);
}

static int storage_wait_start_gate(ChannelRuntime *rt, uint64_t *start_skew_us, const char **gate_mode)
{
    const char *value = getenv("SRC_REAL_START_FD");
    char *end = NULL;
    long fd;
    uint64_t parent_start_us = 0u;
    size_t used = 0u;

    int control_fd = storage_env_fd("SRC_REAL_STORAGE_CONTROL_FD");
    StorageControlMessage msg;
    if (start_skew_us) *start_skew_us = 0u;
    if (gate_mode) *gate_mode = "standalone_immediate";
    if (control_fd >= 0) {
        storage_emit_event(STORAGE_WORKER_READY, rt, 0, 0u, "ready");
        if (storage_ipc_read_control(control_fd, &msg) != 0 || msg.type != STORAGE_CTRL_ARM) return -1;
        if (dma_start_s2mm_ring(rt) != 0) return -1;
        storage_emit_event(STORAGE_WORKER_ARMED, rt, 0, 0u, "armed");
        if (storage_ipc_read_control(control_fd, &msg) != 0 || msg.type != STORAGE_CTRL_RUN) return -1;
        { int flags = fcntl(control_fd, F_GETFL, 0); if (flags >= 0) (void)fcntl(control_fd, F_SETFL, flags | O_NONBLOCK); }
        if (gate_mode) *gate_mode = "software_two_phase_barrier";
        if (start_skew_us) {
            uint64_t now_us = storage_wall_time_us();
            *start_skew_us = now_us >= msg.timestamp_us ? now_us - msg.timestamp_us : 0u;
        }
        storage_emit_event(STORAGE_WORKER_RUNNING, rt, 0, 0u, "running");
        return 0;
    }
    if (!value || value[0] == '\0') return 0;
    fd = strtol(value, &end, 10);
    if (end == value || *end != '\0' || fd < 0 || fd > INT_MAX) return -1;
    while (used < sizeof(parent_start_us)) {
        ssize_t n = read((int)fd, (uint8_t *)&parent_start_us + used,
                         sizeof(parent_start_us) - used);
        if (n > 0) {
            used += (size_t)n;
        } else if (n < 0 && errno == EINTR) {
            continue;
        } else {
            return -1;
        }
    }
    if (gate_mode) *gate_mode = "software_barrier";
    if (start_skew_us) {
        uint64_t now_us = storage_wall_time_us();
        *start_skew_us = now_us >= parent_start_us ? now_us - parent_start_us : 0u;
    }
    return 0;
}

static bool storage_control_stop_requested(void)
{
    StorageControlMessage msg;
    int fd = storage_env_fd("SRC_REAL_STORAGE_CONTROL_FD");
    if (g_storage_control_stop_latched) return true;
    if (fd >= 0 && storage_ipc_read_control(fd, &msg) == 0 && msg.type == STORAGE_CTRL_STOP) {
        g_storage_control_stop_latched = true;
    }
    return g_storage_control_stop_latched;
}

static void storage_emit_line(const char *fmt, ...)
{
    char line[2048];
    va_list ap;
    int n;
    size_t len;

    if (!fmt) {
        return;
    }
    va_start(ap, fmt);
    n = vsnprintf(line, sizeof(line), fmt, ap);
    va_end(ap);
    if (n < 0) {
        return;
    }
    len = (size_t)n;
    if (len >= sizeof(line)) {
        len = sizeof(line) - 2u;
    }
    if (len == 0u || line[len - 1u] != '\n') {
        line[len++] = '\n';
    }
    line[len] = '\0';
    {
        ssize_t written = write(STDOUT_FILENO, line, len);
        (void)written;
    }
}

static uint32_t storage_idle_notice_ms(void) {
    const char *env = getenv("SRC_REAL_STORAGE_IDLE_NOTICE_MS");
    char *end = NULL;
    unsigned long parsed;

    if (!env || !env[0]) {
        return STORAGE_IDLE_NOTICE_MS_DEFAULT;
    }

    errno = 0;
    parsed = strtoul(env, &end, 0);
    if (errno != 0 || end == env || *end != '\0' || parsed > UINT32_MAX) {
        return STORAGE_IDLE_NOTICE_MS_DEFAULT;
    }
    return (uint32_t)parsed;
}

static StorageLogLevel storage_log_level(void)
{
    static int cached = -1;
    const char *env;

    if (cached >= 0) {
        return (StorageLogLevel)cached;
    }
    env = getenv("SRC_REAL_LOG_LEVEL");
    if (!env || env[0] == '\0' || strcmp(env, "quiet") == 0) {
        cached = STORAGE_LOG_QUIET;
    } else if (strcmp(env, "summary") == 0) {
        cached = STORAGE_LOG_SUMMARY;
    } else if (strcmp(env, "debug") == 0) {
        cached = STORAGE_LOG_DEBUG;
    } else if (strcmp(env, "trace") == 0) {
        cached = STORAGE_LOG_TRACE;
    } else {
        fprintf(stderr, "warning: invalid SRC_REAL_LOG_LEVEL=%s; fallback=quiet\n", env);
        cached = STORAGE_LOG_QUIET;
    }
    return (StorageLogLevel)cached;
}

static bool storage_log_enabled(StorageLogLevel need)
{
    return storage_log_level() >= need;
}

static uint32_t storage_pipeline_stats_ms(void)
{
    const char *env = getenv("SRC_REAL_PIPELINE_STATS_SEC");
    char *end = NULL;
    unsigned long parsed;

    if (!storage_log_enabled(STORAGE_LOG_SUMMARY)) {
        return 0u;
    }
    if (!env || !env[0]) {
        return STORAGE_PIPELINE_STATS_SEC_DEFAULT * 1000u;
    }
    errno = 0;
    parsed = strtoul(env, &end, 0);
    if (errno != 0 || end == env || *end != '\0' || parsed > (UINT32_MAX / 1000u)) {
        fprintf(stderr,
                "warning: invalid SRC_REAL_PIPELINE_STATS_SEC=%s; fallback=%u\n",
                env,
                STORAGE_PIPELINE_STATS_SEC_DEFAULT);
        return STORAGE_PIPELINE_STATS_SEC_DEFAULT * 1000u;
    }
    return (uint32_t)parsed * 1000u;
}

static uint32_t storage_env_u32_limit(const char *name, uint32_t fallback, uint32_t max_value)
{
    const char *value = getenv(name);
    char *end = NULL;
    unsigned long parsed;

    if (!value || value[0] == '\0') {
        return fallback;
    }
    errno = 0;
    parsed = strtoul(value, &end, 0);
    if (errno != 0 || end == value || *end != '\0' || parsed > max_value) {
        fprintf(stderr,
                "warning: invalid %s=%s; fallback=%u max=%u\n",
                name,
                value,
                fallback,
                max_value);
        return fallback;
    }
    return (uint32_t)parsed;
}

static uint32_t storage_channel_env_u32(const ChannelConfig *cfg,
                                        const char *suffix,
                                        const char *global_fallback,
                                        uint32_t fallback,
                                        uint32_t max_value)
{
    char name[96];

    if (!cfg || !suffix) return fallback;
    snprintf(name, sizeof(name), "SRC_REAL_CH%d_%s", cfg->id, suffix);
    if (getenv(name)) return storage_env_u32_limit(name, fallback, max_value);
    if (global_fallback && getenv(global_fallback)) {
        return storage_env_u32_limit(global_fallback, fallback, max_value);
    }
    return fallback;
}

static int storage_env_flag_enabled(const char *name)
{
    const char *value = getenv(name);

    if (!value || value[0] == '\0') {
        return 0;
    }
    if (strcmp(value, "0") == 0 ||
        strcmp(value, "false") == 0 ||
        strcmp(value, "FALSE") == 0 ||
        strcmp(value, "off") == 0 ||
        strcmp(value, "OFF") == 0 ||
        strcmp(value, "no") == 0 ||
        strcmp(value, "NO") == 0) {
        return 0;
    }
    return 1;
}

static bool storage_env_string_is(const char *name, const char *expected)
{
    const char *value = getenv(name);

    return value && expected && strcmp(value, expected) == 0;
}

static FILE *storage_slot_perf_log_open(void)
{
    static bool initialized = false;
    static FILE *log = NULL;
    const char *path;

    if (initialized) {
        return log;
    }
    initialized = true;
    path = getenv("SRC_REAL_SLOT_WRITE_PERF_LOG");
    if ((!path || path[0] == '\0') &&
        storage_env_flag_enabled("SRC_REAL_SLOT_WRITE_PERF")) {
        path = "/tmp/storage_slot_perf.log";
    }
    if (!path || path[0] == '\0') {
        return NULL;
    }
    log = fopen(path, "a");
    if (!log) {
        fprintf(stderr,
                "warning: failed to open SRC_REAL_SLOT_WRITE_PERF_LOG=%s: %s\n",
                path,
                strerror(errno));
        return NULL;
    }
    setvbuf(log, NULL, _IOLBF, 0);
    return log;
}

static uint32_t storage_slot_perf_interval(void)
{
    if (!storage_env_flag_enabled("SRC_REAL_SLOT_WRITE_PERF") ||
        !storage_log_enabled(STORAGE_LOG_DEBUG)) {
        return 0u;
    }
    return storage_env_u32_limit("SRC_REAL_SLOT_WRITE_PERF_SAMPLE", 0u, 1000000u);
}

static bool storage_should_print_periodic_stats(ChannelRuntime *rt)
{
    (void)rt;
    return storage_log_enabled(STORAGE_LOG_SUMMARY) &&
           storage_pipeline_stats_ms() != 0u;
}

static bool storage_should_print_slot_perf(ChannelRuntime *rt, uint64_t chunk_index)
{
    uint32_t sample;

    (void)rt;
    sample = storage_slot_perf_interval();
    if (sample == 0u) {
        return false;
    }
    if (sample == 1u && !storage_log_enabled(STORAGE_LOG_TRACE)) {
        return false;
    }
    return (chunk_index % (uint64_t)sample) == 0u;
}

static uint32_t storage_writer_rt_prio(const ChannelRuntime *rt)
{
    char name[64];

    if (!rt) {
        return 0u;
    }
    snprintf(name, sizeof(name), "SRC_REAL_CH%d_WRITER_RT_PRIO", rt->cfg->id);
    if (getenv(name)) {
        return storage_env_u32_limit(name, 0u, 99u);
    }
    return 60u;
}

static uint32_t storage_producer_rt_prio(const ChannelRuntime *rt)
{
    char name[64];

    if (!rt) {
        return 0u;
    }
    snprintf(name, sizeof(name), "SRC_REAL_CH%d_PRODUCER_RT_PRIO", rt->cfg->id);
    if (getenv(name)) {
        return storage_env_u32_limit(name, 0u, 99u);
    }
    return 60u;
}

static int storage_rt_policy(const char *name, int fallback)
{
    const char *value = getenv(name);

    if (!value || value[0] == '\0') {
        return fallback;
    }
    if (strcmp(value, "rr") == 0) {
        return SCHED_RR;
    }
    if (strcmp(value, "fifo") == 0) {
        return SCHED_FIFO;
    }
    if (strcmp(value, "other") == 0) {
        return SCHED_OTHER;
    }
    fprintf(stderr, "warning: invalid %s=%s; fallback=rr\n", name, value);
    return fallback;
}

static const char *storage_rt_policy_name(int policy)
{
    if (policy == SCHED_RR) return "rr";
    if (policy == SCHED_FIFO) return "fifo";
    return "other";
}

static uint64_t storage_requested_ring_bytes(const ChannelRuntime *rt)
{
    char name[64];
    const char *env;
    char *end = NULL;
    unsigned long long parsed;

    if (!rt) {
        return 0u;
    }
    snprintf(name, sizeof(name), "SRC_REAL_STORAGE_RING_BYTES_CH%d", rt->cfg->id);
    env = getenv(name);
    if (!env || env[0] == '\0') {
        env = getenv("SRC_REAL_STORAGE_RING_BYTES");
    }
    if (!env || env[0] == '\0') {
        return rt->cfg->dma_ring_bytes;
    }
    errno = 0;
    parsed = strtoull(env, &end, 0);
    if (errno != 0 || end == env || *end != '\0') {
        return rt->cfg->dma_ring_bytes;
    }
    return (uint64_t)parsed;
}

static const char *storage_ring_clamp_reason(const ChannelRuntime *rt, uint64_t requested)
{
    if (!rt) {
        return "unknown";
    }
    if (requested == rt->dma_ring_bytes) {
        return "none";
    }
    if (requested > rt->dma_ring_bytes) {
        return "exceeds_hw_span";
    }
    return "configured";
}

static uint32_t storage_default_dma_desc_bytes(const ChannelConfig *cfg)
{
    if (cfg && (cfg->id == HIGH_I_CHANNEL_ID || cfg->id == HIGH_Q_CHANNEL_ID)) {
        return STORAGE_HIGH_DESC_BYTES_DEFAULT;
    }
    return cfg ? cfg->dma_desc_bytes_default : DMA_DESC_BYTES_DEFAULT;
}

static const char *storage_ring_layout_error_reason(const ChannelRuntime *rt,
                                                    uint32_t dma_desc_bytes,
                                                    uint32_t *out_desc_count,
                                                    uint32_t *out_desc_capacity)
{
    uint32_t desc_count = 0u;
    uint32_t desc_capacity = 0u;

    if (out_desc_count) {
        *out_desc_count = 0u;
    }
    if (out_desc_capacity) {
        *out_desc_capacity = 0u;
    }
    if (!rt || !rt->cfg || dma_desc_bytes == 0u) {
        return "invalid_slot_bytes";
    }
    desc_capacity = (uint32_t)(rt->cfg->desc_cpu_size / sizeof(DmaSgDesc));
    if (rt->dma_ring_bytes == 0u) {
        return "zero_ring";
    }
    if ((rt->dma_ring_bytes % (uint64_t)dma_desc_bytes) != 0u) {
        return "slot_bytes_not_divisor";
    }
    desc_count = (uint32_t)(rt->dma_ring_bytes / (uint64_t)dma_desc_bytes);
    if (out_desc_count) {
        *out_desc_count = desc_count;
    }
    if (out_desc_capacity) {
        *out_desc_capacity = desc_capacity;
    }
    if (desc_count == 0u) {
        return "zero_slots";
    }
    if (desc_capacity == 0u) {
        return "zero_desc_capacity";
    }
    if (desc_count > desc_capacity) {
        return "max_slots";
    }
    if (rt->dma_ring_bytes > rt->cfg->dma_ring_bytes) {
        return "exceeds_hw_span";
    }
    return NULL;
}

static uint32_t storage_dma_idle_done_ms(void)
{
    return storage_env_u32_limit("SRC_REAL_DMA_IDLE_DONE_MS", 5000u, 3600000u);
}

static void storage_apply_writer_rt(StorageWriteQueue *q)
{
    struct sched_param sp;
    uint32_t prio;
    int policy;
    int rc;

    if (!q || !q->rt) {
        return;
    }
    prio = storage_writer_rt_prio(q->rt);
    __atomic_store_n(&q->writer_rt_prio, prio, __ATOMIC_RELEASE);
    if (prio == 0u) {
        __atomic_store_n(&q->writer_rt_policy, SCHED_OTHER, __ATOMIC_RELEASE);
        storage_emit_line("storage_writer_scheduler channel=%d effective_policy=other"
                          " effective_prio=0 rt_enabled=0",
                          q->rt->cfg->id);
        return;
    }
    memset(&sp, 0, sizeof(sp));
    policy = storage_rt_policy("SRC_REAL_WRITER_RT_POLICY", SCHED_RR);
    __atomic_store_n(&q->writer_rt_policy, policy, __ATOMIC_RELEASE);
    if (policy == SCHED_OTHER) {
        sp.sched_priority = 0;
    } else {
        sp.sched_priority = (int)prio;
    }
    rc = pthread_setschedparam(pthread_self(), policy, &sp);
    if (rc == 0) {
        __atomic_store_n(&q->writer_rt_enabled, true, __ATOMIC_RELEASE);
    } else {
        fprintf(stderr,
                "warning: writer RT setup failed channel=%d policy=%s prio=%u errno=%d\n",
                q->rt->cfg->id,
                storage_rt_policy_name(policy),
                (unsigned)prio,
                rc);
        __atomic_store_n(&q->writer_rt_policy, SCHED_OTHER, __ATOMIC_RELEASE);
        __atomic_store_n(&q->writer_rt_prio, 0u, __ATOMIC_RELEASE);
    }
    storage_emit_line("storage_writer_scheduler channel=%d effective_policy=%s"
                      " effective_prio=%u rt_enabled=%u",
                      q->rt->cfg->id,
                      storage_rt_policy_name(__atomic_load_n(&q->writer_rt_policy,
                                                              __ATOMIC_ACQUIRE)),
                      (unsigned)__atomic_load_n(&q->writer_rt_prio, __ATOMIC_ACQUIRE),
                      __atomic_load_n(&q->writer_rt_enabled, __ATOMIC_ACQUIRE) ? 1u : 0u);
}

static void storage_apply_producer_rt(const ChannelRuntime *rt,
                                      int *effective_policy,
                                      uint32_t *effective_prio)
{
    struct sched_param sp;
    uint32_t producer_prio;
    int policy;
    int rc;

    if (!rt) return;
    producer_prio = storage_producer_rt_prio(rt);
    policy = storage_rt_policy("SRC_REAL_PRODUCER_RT_POLICY", SCHED_RR);
    memset(&sp, 0, sizeof(sp));
    sp.sched_priority = policy == SCHED_OTHER ? 0 : (int)producer_prio;
    rc = pthread_setschedparam(pthread_self(), policy, &sp);
    if (rc != 0) {
        fprintf(stderr,
                "warning: producer RT setup failed channel=%d policy=%s prio=%u errno=%d\n",
                rt->cfg->id, storage_rt_policy_name(policy), producer_prio, rc);
        policy = SCHED_OTHER;
        producer_prio = 0u;
    }
    if (effective_policy) *effective_policy = policy;
    if (effective_prio) *effective_prio = producer_prio;
}

static void storage_nvme_timing_snapshot(const ChannelRuntime *rt,
                                         StorageNvmeTimingSnapshot *out)
{
    if (!out) {
        return;
    }
    memset(out, 0, sizeof(*out));
    if (!rt) {
        return;
    }
    out->submit_calls = rt->nvme_submit_calls;
    out->submit_total_us = rt->nvme_submit_total_us;
    out->submit_pending_wait_us = rt->nvme_submit_pending_wait_us;
    out->submit_sq_full_count = rt->nvme_submit_sq_full_count;
    out->cq_poll_calls = rt->nvme_cq_poll_calls;
    out->cq_empty_polls = rt->nvme_cq_empty_polls;
    out->cq_wait_total_us = rt->nvme_cq_wait_total_us;
    out->cq_pop_total_us = rt->nvme_cq_pop_total_us;
    out->cq_completed = rt->nvme_cq_completed;
    out->cmd_count = __atomic_load_n(&rt->nvme_cmd_count, __ATOMIC_ACQUIRE);
    out->active_qd_integral_us = __atomic_load_n(&rt->nvme_active_qd_integral_us,
                                                 __ATOMIC_ACQUIRE);
    out->active_qd_observed_us = __atomic_load_n(&rt->nvme_active_qd_observed_us,
                                                 __ATOMIC_ACQUIRE);
}

static double storage_nvme_active_qd_avg_delta(const StorageNvmeTimingSnapshot *before,
                                               const StorageNvmeTimingSnapshot *after)
{
    uint64_t observed_delta;

    if (!before || !after || after->active_qd_observed_us < before->active_qd_observed_us) {
        return 0.0;
    }
    observed_delta = after->active_qd_observed_us - before->active_qd_observed_us;
    if (observed_delta == 0u ||
        after->active_qd_integral_us < before->active_qd_integral_us) {
        return 0.0;
    }
    return (double)(after->active_qd_integral_us - before->active_qd_integral_us) /
           (double)observed_delta;
}

static void storage_print_slot_sw_timing(FILE *log,
                                         const ChannelRuntime *rt,
                                         const StorageNvmeTimingSnapshot *before,
                                         const StorageNvmeTimingSnapshot *after)
{
    uint64_t submit_calls;
    uint64_t cq_completed;
    double submit_avg_us = 0.0;
    double pending_avg_us = 0.0;
    double cq_wait_avg_us = 0.0;

    if (!log || !rt || !before || !after) {
        return;
    }
    submit_calls = after->submit_calls - before->submit_calls;
    cq_completed = after->cq_completed - before->cq_completed;
    if (submit_calls != 0u) {
        submit_avg_us = (double)(after->submit_total_us - before->submit_total_us) /
                        (double)submit_calls;
        pending_avg_us = (double)(after->submit_pending_wait_us -
                                  before->submit_pending_wait_us) /
                         (double)submit_calls;
    }
    if (cq_completed != 0u) {
        cq_wait_avg_us = (double)(after->cq_wait_total_us - before->cq_wait_total_us) /
                         (double)cq_completed;
    }
    fprintf(log,
            "slot_sw_timing channel=%d submit_calls=%" PRIu64
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
            submit_calls,
            after->submit_total_us - before->submit_total_us,
            after->submit_pending_wait_us - before->submit_pending_wait_us,
            submit_avg_us,
            pending_avg_us,
            cq_completed,
            after->cq_poll_calls - before->cq_poll_calls,
            after->cq_wait_total_us - before->cq_wait_total_us,
            cq_wait_avg_us,
            after->cq_pop_total_us - before->cq_pop_total_us,
            after->cq_empty_polls - before->cq_empty_polls,
            after->submit_sq_full_count - before->submit_sq_full_count);
}

static int storage_event_logs_enabled(void)
{
    static int cached = -1;

    if (cached < 0) {
        cached = storage_env_flag_enabled("SRC_REAL_STORAGE_EVENTS") ||
                 dbg_category_enabled("WRITE");
    }
    return cached != 0;
}

static int storage_env_u64(const char *name, uint64_t *out)
{
    const char *value = getenv(name);
    char *end = NULL;
    unsigned long long parsed;

    if (!out || !value || value[0] == '\0') {
        return 0;
    }
    errno = 0;
    parsed = strtoull(value, &end, 0);
    if (errno != 0 || end == value || *end != '\0') {
        return 0;
    }
    *out = (uint64_t)parsed;
    return 1;
}

static int storage_trace_chunk_enabled(uint64_t chunk_index, uint32_t slot)
{
    uint64_t trace_chunk;
    uint64_t trace_slot;
    int has_chunk;
    int has_slot = storage_env_u64("SRC_REAL_STORAGE_TRACE_SLOT", &trace_slot);

    if (!storage_log_enabled(STORAGE_LOG_TRACE)) {
        return 0;
    }
    has_chunk = storage_env_u64("SRC_REAL_STORAGE_TRACE_CHUNK", &trace_chunk);

    if (!has_chunk && !has_slot) {
        return 0;
    }
    if (has_chunk && trace_chunk != chunk_index) {
        return 0;
    }
    if (has_slot && trace_slot != (uint64_t)slot) {
        return 0;
    }
    return 1;
}

static void format_hex_prefix(const volatile uint8_t *data, uint32_t bytes, char *out, size_t out_len)
{
    static const char hex[] = "0123456789ABCDEF";
    uint32_t i;
    size_t pos = 0u;

    if (!out || out_len == 0u) {
        return;
    }
    out[0] = '\0';
    if (!data) {
        return;
    }
    for (i = 0u; i < bytes && (pos + 2u) < out_len; ++i) {
        uint8_t v = data[i];
        out[pos++] = hex[(v >> 4u) & 0x0fu];
        out[pos++] = hex[v & 0x0fu];
    }
    out[pos] = '\0';
}

static const char *storage_magic_hint(const volatile uint8_t *data, uint32_t bytes)
{
    uint32_t i;

    if (!data || bytes < 4u) {
        return "unknown";
    }
    for (i = 0u; i + 3u < bytes; ++i) {
        if (data[i] == 0x18u && data[i + 1u] == 0xEFu &&
            data[i + 2u] == 0xDCu && data[i + 3u] == 0x01u) {
            return "ch2_18efdc01";
        }
        if (data[i] == 0x18u && data[i + 1u] == 0xEFu &&
            data[i + 2u] == 0x01u && data[i + 3u] == 0xDCu) {
            return "ch0_18ef01dc";
        }
    }
    return "unknown";
}

static void storage_print_slot_fingerprint(const ChannelRuntime *rt,
                                           uint32_t slot,
                                           uint64_t file_offset,
                                           uint32_t actual_bytes)
{
    uint64_t ddr_offset;
    uint32_t sample_bytes;
    char prefix[129];
    const volatile uint8_t *sample;
    uint32_t status;

    if (!storage_env_flag_enabled("SRC_REAL_STORAGE_SLOT_FINGERPRINT") ||
        !rt || rt->gopt.dry_run || actual_bytes == 0u) {
        return;
    }
    ddr_offset = (uint64_t)slot * (uint64_t)rt->dma_desc_bytes;
    if (ddr_offset >= rt->cfg->ddr_cpu_size || ddr_offset >= rt->ddr.size) {
        printf("storage_slot_fingerprint channel=%d slot=%u file_offset=%" PRIu64
               " actual_bytes=%u sample_available=0 reason=outside_cpu_visible_ddr\n",
               rt->cfg->id,
               (unsigned)slot,
               file_offset,
               (unsigned)actual_bytes);
        fflush(stdout);
        return;
    }
    sample_bytes = actual_bytes < 64u ? actual_bytes : 64u;
    if (ddr_offset + sample_bytes > rt->ddr.size) {
        sample_bytes = (uint32_t)(rt->ddr.size - ddr_offset);
    }
    sample = rt->ddr.virt + ddr_offset;
    status = rt->dma_last_completed_status;
    format_hex_prefix(sample, sample_bytes, prefix, sizeof(prefix));
    printf("storage_slot_fingerprint channel=%d slot=%u file_offset=%" PRIu64
           " ddr_offset=%" PRIu64 " actual_bytes=%u rxsof=%u rxeof=%u"
           " status=0x%08x sample_bytes=%u magic=%s prefix=%s\n",
           rt->cfg->id,
           (unsigned)slot,
           file_offset,
           ddr_offset,
           (unsigned)actual_bytes,
           (status & STORAGE_DESC_STS_RXSOF) ? 1u : 0u,
           (status & STORAGE_DESC_STS_RXEOF) ? 1u : 0u,
           status,
           (unsigned)sample_bytes,
           storage_magic_hint(sample, sample_bytes),
           prefix);
    fflush(stdout);
}

static uint32_t *storage_local_count_for_state(StorageSlotCounts *c, StorageSlotState state)
{
    switch (state) {
    case STORAGE_SLOT_DMA_WRITABLE: return &c->dma_writable;
    case STORAGE_SLOT_DMA_COMPLETED_UNHARVESTED: return &c->completed_unharvested;
    case STORAGE_SLOT_READY_FOR_NVME: return &c->ready;
    case STORAGE_SLOT_NVME_BUSY: return &c->nvme_busy;
    case STORAGE_SLOT_REQUEUE_PENDING: return &c->requeue_pending;
    case STORAGE_SLOT_FREE: return &c->free_count;
    default: return NULL;
    }
}

static int storage_local_slot_transition_locked(StorageWriteQueue *q, uint32_t slot,
                                                StorageSlotState expected, StorageSlotState next)
{
    uint32_t *from, *to;
    StorageSlotCounts *c;
    if (!q || slot >= q->capacity || q->slot_state[slot] != expected) goto bad;
    c = &q->slot_counts;
    from = storage_local_count_for_state(c, expected); to = storage_local_count_for_state(c, next);
    if (!from || !to || *from == 0u) goto bad;
    --*from; ++*to; q->slot_state[slot] = (uint8_t)next;
    if (c->total != c->dma_writable + c->completed_unharvested + c->ready +
                    c->nvme_busy + c->requeue_pending + c->free_count) goto bad;
    return 0;
bad:
    if (q) q->error = true;
    return -1;
}

static int storage_queue_init(StorageWriteQueue *q,
                              ChannelRuntime *rt,
                              uint32_t file_index,
                              int metadata_slot,
                              const char *task_no,
                              bool cross_slot_qd,
                              uint32_t cross_slot_batch) {
    if (!q || !rt || rt->dma_desc_count == 0u) {
        return -1;
    }
    memset(q, 0, sizeof(*q));
    q->items = (PendingDdrSlot *)calloc(rt->dma_desc_count, sizeof(q->items[0]));
    q->slot_busy = (bool *)calloc(rt->dma_desc_count, sizeof(q->slot_busy[0]));
    q->slot_state = (uint8_t *)calloc(rt->dma_desc_count, sizeof(q->slot_state[0]));
    if (!q->items || !q->slot_busy || !q->slot_state) {
        free(q->items);
        free(q->slot_busy);
        free(q->slot_state);
        q->items = NULL;
        q->slot_busy = NULL;
        q->slot_state = NULL;
        return -1;
    }
    memset(q->slot_state, STORAGE_SLOT_DMA_WRITABLE, rt->dma_desc_count);
    q->rt = rt;
    q->capacity = rt->dma_desc_count;
    q->slot_counts.total = q->capacity;
    q->slot_counts.dma_writable = q->capacity;
    q->file_index = file_index;
    q->metadata_slot = metadata_slot;
    q->task_no = task_no;
    q->cross_slot_qd = cross_slot_qd;
    q->cross_slot_batch = cross_slot_batch;
    q->backlog_mode = storage_env_flag_enabled("SRC_REAL_WRITER_BACKLOG_MODE") != 0;
    if (pthread_mutex_init(&q->lock, NULL) != 0 ||
        pthread_cond_init(&q->not_empty, NULL) != 0 ||
        pthread_cond_init(&q->not_full, NULL) != 0) {
        free(q->items);
        free(q->slot_busy);
        free(q->slot_state);
        q->items = NULL;
        q->slot_busy = NULL;
        q->slot_state = NULL;
        return -1;
    }
    return 0;
}

static void storage_queue_destroy(StorageWriteQueue *q) {
    if (!q) {
        return;
    }
    (void)pthread_cond_destroy(&q->not_full);
    (void)pthread_cond_destroy(&q->not_empty);
    (void)pthread_mutex_destroy(&q->lock);
    free(q->items);
    free(q->slot_busy);
    free(q->slot_state);
    q->items = NULL;
    q->slot_busy = NULL;
    q->slot_state = NULL;
}

static void storage_queue_record_depth_locked(StorageWriteQueue *q) {
    if (!q) {
        return;
    }
    q->ready_depth_sum += q->count;
    ++q->ready_depth_samples;
    if (q->count > q->ready_depth_max) {
        q->ready_depth_max = q->count;
    }
}

static int storage_queue_push(StorageWriteQueue *q,
                              uint32_t slot,
                              uint64_t bytes,
                              uint64_t chunk_index,
                              uint64_t file_offset,
                              uint64_t start_lba,
                              uint64_t sectors,
                              uint64_t hw_addr) {
    if (!q || bytes == 0u) {
        return 0;
    }
    pthread_mutex_lock(&q->lock);
    if (slot >= q->capacity || q->slot_busy[slot]) {
        fprintf(stderr,
                "Duplicate or invalid DDR slot enqueue: channel=%d slot=%u capacity=%u busy=%u\n",
                q->rt ? q->rt->cfg->id : -1,
                (unsigned)slot,
                (unsigned)q->capacity,
                (slot < q->capacity && q->slot_busy[slot]) ? 1u : 0u);
        q->error = true;
        pthread_cond_broadcast(&q->not_empty);
        pthread_cond_broadcast(&q->not_full);
        pthread_mutex_unlock(&q->lock);
        return -1;
    }
    if (q->error || q->count == q->capacity) {
        q->error = true;
        pthread_cond_broadcast(&q->not_empty);
        pthread_mutex_unlock(&q->lock);
        return -1;
    }
    q->slot_busy[slot] = true;
    ++q->busy_count;
    q->buffered_bytes += bytes;
    if (q->busy_count > q->max_busy_count) {
        q->max_busy_count = q->busy_count;
    }
    if (q->buffered_bytes > q->max_buffered_bytes) {
        q->max_buffered_bytes = q->buffered_bytes;
    }
    q->items[q->tail].slot = slot;
    q->items[q->tail].bytes = bytes;
    q->items[q->tail].chunk_index = chunk_index;
    q->items[q->tail].file_offset = file_offset;
    q->items[q->tail].start_lba = start_lba;
    q->items[q->tail].sectors = sectors;
    q->items[q->tail].hw_addr = hw_addr;
    if (storage_local_slot_transition_locked(q, slot, STORAGE_SLOT_DMA_WRITABLE,
                                             STORAGE_SLOT_DMA_COMPLETED_UNHARVESTED) != 0 ||
        storage_local_slot_transition_locked(q, slot, STORAGE_SLOT_DMA_COMPLETED_UNHARVESTED,
                                             STORAGE_SLOT_READY_FOR_NVME) != 0) goto bad;
    q->tail = (q->tail + 1u) % q->capacity;
    ++q->count;
    storage_queue_record_depth_locked(q);
    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->lock);
    return 0;
bad:
    q->error = true;
    pthread_cond_broadcast(&q->not_empty);
    pthread_mutex_unlock(&q->lock);
    return -1;
}

/* Producer never waits for writer capacity: validate the complete harvest first. */
static int storage_local_queue_push_batch(StorageWriteQueue *q, const PendingDdrSlot *items,
                                    uint32_t item_count)
{
    uint32_t i, j;
    if (!q || !items || item_count == 0u) return -1;
    pthread_mutex_lock(&q->lock);
    if (q->error || item_count > q->capacity - q->count) goto bad;
    for (i = 0u; i < item_count; ++i) {
        if (items[i].bytes == 0u || items[i].slot >= q->capacity || q->slot_busy[items[i].slot]) goto bad;
        for (j = 0u; j < i; ++j) if (items[j].slot == items[i].slot) goto bad;
    }
    for (i = 0u; i < item_count; ++i) {
        uint32_t slot = items[i].slot;
        if (storage_local_slot_transition_locked(q, slot, STORAGE_SLOT_DMA_WRITABLE,
                                                 STORAGE_SLOT_DMA_COMPLETED_UNHARVESTED) != 0) goto bad;
        q->items[q->tail] = items[i];
        q->tail = (q->tail + 1u) % q->capacity;
        ++q->count;
        q->slot_busy[slot] = true;
        ++q->busy_count;
        q->buffered_bytes += items[i].bytes;
        if (storage_local_slot_transition_locked(q, slot, STORAGE_SLOT_DMA_COMPLETED_UNHARVESTED,
                                                 STORAGE_SLOT_READY_FOR_NVME) != 0) goto bad;
    }
    if (q->busy_count > q->max_busy_count) q->max_busy_count = q->busy_count;
    if (q->buffered_bytes > q->max_buffered_bytes) q->max_buffered_bytes = q->buffered_bytes;
    storage_queue_record_depth_locked(q);
    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->lock);
    return 0;
bad:
    q->error = true;
    pthread_cond_broadcast(&q->not_empty);
    pthread_mutex_unlock(&q->lock);
    return -1;
}

static void storage_queue_finish(StorageWriteQueue *q) {
    if (!q) {
        return;
    }
    pthread_mutex_lock(&q->lock);
    q->producer_done = true;
    pthread_cond_broadcast(&q->not_empty);
    pthread_mutex_unlock(&q->lock);
}

static bool storage_queue_has_error(StorageWriteQueue *q) {
    bool error;
    if (!q) {
        return true;
    }
    pthread_mutex_lock(&q->lock);
    error = q->error;
    pthread_mutex_unlock(&q->lock);
    return error;
}

static uint32_t storage_queue_busy_count(StorageWriteQueue *q, uint32_t *max_busy_count) {
    uint32_t busy_count;

    if (!q) {
        return 0u;
    }
    pthread_mutex_lock(&q->lock);
    busy_count = q->busy_count;
    if (max_busy_count) {
        *max_busy_count = q->max_busy_count;
    }
    pthread_mutex_unlock(&q->lock);
    return busy_count;
}

static uint64_t storage_queue_buffered_bytes(StorageWriteQueue *q, uint64_t *max_buffered_bytes) {
    uint64_t buffered_bytes;

    if (!q) {
        return 0u;
    }
    pthread_mutex_lock(&q->lock);
    buffered_bytes = q->buffered_bytes;
    if (max_buffered_bytes) {
        *max_buffered_bytes = q->max_buffered_bytes;
    }
    pthread_mutex_unlock(&q->lock);
    return buffered_bytes;
}

static uint64_t storage_queue_written_bytes(const StorageWriteQueue *q) {
    return q ? __atomic_load_n(&q->bytes_written, __ATOMIC_ACQUIRE) : 0u;
}

static void storage_queue_snapshot(StorageWriteQueue *q, StorageQueueSnapshot *out) {
    uint32_t i;

    if (!out) {
        return;
    }
    memset(out, 0, sizeof(*out));
    if (!q) {
        return;
    }
    pthread_mutex_lock(&q->lock);
    out->ready_depth_current = q->count;
    out->ready_depth_avg = q->ready_depth_samples > 0u
                               ? (uint32_t)(q->ready_depth_sum / q->ready_depth_samples)
                               : q->count;
    out->ready_depth_max = q->ready_depth_max;
    out->busy_count = q->busy_count;
    out->buffered_bytes = q->buffered_bytes;
    out->writer_idle_us = q->writer_idle_us;
    out->writer_active_us = q->writer_active_us;
    out->ready_q_nonempty_us = q->ready_q_nonempty_us;
    out->writer_drain_loop_count = q->writer_drain_loop_count;
    out->writer_slots_drained = q->writer_slots_drained;
    out->writer_rt_enabled = __atomic_load_n(&q->writer_rt_enabled, __ATOMIC_ACQUIRE);
    out->writer_rt_policy = __atomic_load_n(&q->writer_rt_policy, __ATOMIC_ACQUIRE);
    out->writer_rt_prio = __atomic_load_n(&q->writer_rt_prio, __ATOMIC_ACQUIRE);
    for (i = 0u; i < q->capacity; ++i) {
        switch (q->slot_state ? q->slot_state[i] : STORAGE_SLOT_FREE) {
        case STORAGE_SLOT_FREE:
            ++out->free_slots;
            break;
        case STORAGE_SLOT_DMA_WRITABLE:
            ++out->dma_writable_slots;
            break;
        case STORAGE_SLOT_DMA_COMPLETED_UNHARVESTED:
            ++out->completed_unharvested_slots;
            break;
        case STORAGE_SLOT_READY_FOR_NVME:
            ++out->ready_for_nvme_slots;
            break;
        case STORAGE_SLOT_NVME_BUSY:
            ++out->nvme_busy_slots;
            break;
        case STORAGE_SLOT_REQUEUE_PENDING:
            ++out->requeue_pending_slots;
            break;
        default:
            break;
        }
    }
    pthread_mutex_unlock(&q->lock);
}

static void storage_stats_finish_harvest_batch(StorageProducerStats *stats) {
    if (!stats || stats->dma_harvest_batch_current == 0u) {
        return;
    }
    ++stats->dma_harvest_batches;
    stats->dma_harvest_batch_total += stats->dma_harvest_batch_current;
    if (stats->dma_harvest_batch_current > stats->dma_harvest_batch_max) {
        stats->dma_harvest_batch_max = stats->dma_harvest_batch_current;
    }
    stats->dma_harvest_batch_current = 0u;
}

static void storage_stats_record_dma_desc(StorageProducerStats *stats, uint64_t now_us) {
    if (!stats) {
        return;
    }
    ++stats->dma_desc_completed_count;
    ++stats->dma_harvest_batch_current;
    if (stats->first_dma_desc_us == 0u) {
        stats->first_dma_desc_us = now_us;
    }
    if (stats->last_dma_desc_us != 0u && now_us >= stats->last_dma_desc_us) {
        uint64_t interval_us = now_us - stats->last_dma_desc_us;
        ++stats->dma_harvest_interval_count;
        stats->dma_harvest_interval_total_us += interval_us;
        if (stats->dma_harvest_interval_min_us == 0u || interval_us < stats->dma_harvest_interval_min_us) {
            stats->dma_harvest_interval_min_us = interval_us;
        }
        if (interval_us > stats->dma_harvest_interval_max_us) {
            stats->dma_harvest_interval_max_us = interval_us;
        }
    }
    stats->last_dma_desc_us = now_us;
}

static int storage_capture_bd_snapshot(StorageProducerStats *stats,
                                       StorageWriteQueue *q,
                                       const ChannelRuntime *rt,
                                       const char *task_no,
                                       uint32_t file_index,
                                       uint64_t received_bytes,
                                       DmaBdSnapshot *out)
{
    uint32_t state_total;
    uint64_t now_us;
    uint32_t interval_ms;
    uint32_t low_scan_threshold;
    int rc;

    if (!stats || !q || !rt || !out) return -1;
    now_us = storage_wall_time_us();
    interval_ms = storage_env_u32_limit("SRC_REAL_BD_SNAPSHOT_INTERVAL_MS", 100u, 60000u);
    low_scan_threshold = storage_env_u32_limit("SRC_REAL_BD_LOW_SCAN_THRESHOLD", 16u,
                                               rt->dma_desc_count);
    if (stats->last_bd_snapshot_us != 0u &&
        now_us - stats->last_bd_snapshot_us < (uint64_t)interval_ms * 1000ull &&
        __atomic_load_n(&rt->dma_hw_desc_count, __ATOMIC_ACQUIRE) > low_scan_threshold) {
        *out = stats->last_bd_snapshot;
        return 0;
    }
    pthread_mutex_lock(&q->lock);
    rc = dma_get_bd_snapshot((ChannelRuntime *)rt, q->slot_state, out);
    pthread_mutex_unlock(&q->lock);
    if (rc != 0) {
        snprintf(stats->receive_integrity_risk,
                 sizeof(stats->receive_integrity_risk),
                 "slot_ownership_invariant_failed");
        stats->receive_integrity_ok = false;
        return -1;
    }
    stats->last_bd_snapshot = *out;
    stats->last_bd_snapshot_us = now_us;
    state_total = out->dma_writable + out->completed_unharvested + out->ready_slots +
                  out->nvme_busy_slots + out->requeue_pending + out->free_slots;
    if (state_total != out->total_slots) {
        snprintf(stats->receive_integrity_risk,
                 sizeof(stats->receive_integrity_risk),
                 "slot_ownership_invariant_failed");
        stats->receive_integrity_ok = false;
        storage_emit_line("storage_receive_failed channel=%d task=%s file_index=%u"
                          " reason=slot_ownership_invariant_failed received_bytes=%" PRIu64,
                          rt->cfg->id, task_no, (unsigned)file_index, received_bytes);
        return -1;
    }
    if (out->completed_unharvested > stats->max_completed_unharvested) {
        stats->max_completed_unharvested = out->completed_unharvested;
    }
    {
        uint32_t low_threshold = storage_env_u32_limit("SRC_REAL_DMA_BD_LOW_WATERMARK",
                                                       4u, out->total_slots);
        if (out->dma_writable <= low_threshold && out->dma_writable > 0u &&
            !stats->dma_bd_low_active) {
            stats->dma_bd_low_active = true;
            storage_emit_line("dma_bd_low channel=%d task=%s file_index=%u"
                              " dma_writable=%u threshold=%u completed_unharvested=%u"
                              " occupied_bytes_est=%" PRIu64,
                              rt->cfg->id, task_no, (unsigned)file_index,
                              out->dma_writable, low_threshold,
                              out->completed_unharvested, out->occupied_bytes_est);
        } else if (out->dma_writable > low_threshold) {
            stats->dma_bd_low_active = false;
        }
    }
    if (stats->min_dma_writable == UINT32_MAX || out->dma_writable < stats->min_dma_writable) {
        stats->min_dma_writable = out->dma_writable;
    }
    if (out->occupied_bytes_est > stats->max_occupied_bytes_est) {
        stats->max_occupied_bytes_est = out->occupied_bytes_est;
    }
    if ((out->s2mm_dmasr & 0x00004770u) != 0u ||
        (!rt->gopt.dry_run && (out->s2mm_dmasr & 1u) != 0u)) {
        ++stats->dma_error_count;
        snprintf(stats->receive_integrity_risk,
                 sizeof(stats->receive_integrity_risk),
                 "dma_error_or_halted");
        stats->receive_integrity_ok = false;
    }
    if (out->dma_writable == 0u) {
        now_us = storage_wall_time_us();
        if (!stats->ring_full_active) {
            ++stats->ring_full_count;
            stats->ring_full_active = true;
            stats->ring_full_start_us = now_us;
            if (stats->first_receive_failure_us == 0u) {
                stats->first_receive_failure_us = now_us;
                stats->first_receive_failure_bytes = received_bytes;
                stats->first_failure_snapshot = *out;
                storage_emit_line("dma_bd_exhausted channel=%d task=%s file_index=%u"
                                  " received_bytes=%" PRIu64 " completed_unharvested=%u"
                                  " ready_slots=%u nvme_busy_slots=%u requeue_pending=%u"
                                  " curdesc=%u taildesc=%u s2mm_dmasr=0x%08x",
                                  rt->cfg->id, task_no, (unsigned)file_index,
                                  received_bytes, out->completed_unharvested,
                                  out->ready_slots, out->nvme_busy_slots,
                                  out->requeue_pending, out->curdesc_index,
                                  out->taildesc_index, out->s2mm_dmasr);
                storage_emit_line("storage_receive_failed channel=%d task=%s file_index=%u"
                                  " reason=dma_bd_exhausted_no_upstream_backpressure"
                                  " received_bytes=%" PRIu64 " dma_writable=0"
                                  " completed_unharvested=%u s2mm_dmasr=0x%08x",
                                  rt->cfg->id, task_no, (unsigned)file_index,
                                  received_bytes, out->completed_unharvested,
                                  out->s2mm_dmasr);
            }
        }
        stats->integrity_risk_ring_full = true;
        stats->receive_integrity_ok = false;
        snprintf(stats->receive_integrity_risk,
                 sizeof(stats->receive_integrity_risk),
                 "dma_bd_exhausted_no_upstream_backpressure");
        return -1;
    }
    if (!stats->receive_integrity_ok) {
        if (stats->first_receive_failure_us == 0u) {
            stats->first_receive_failure_us = storage_wall_time_us();
            stats->first_receive_failure_bytes = received_bytes;
            stats->first_failure_snapshot = *out;
            storage_emit_line("storage_receive_failed channel=%d task=%s file_index=%u"
                              " reason=%s received_bytes=%" PRIu64
                              " dma_writable=%u completed_unharvested=%u"
                              " s2mm_dmasr=0x%08x",
                              rt->cfg->id, task_no, (unsigned)file_index,
                              stats->receive_integrity_risk, received_bytes,
                              out->dma_writable, out->completed_unharvested,
                              out->s2mm_dmasr);
        }
        return -1;
    }
    if (stats->ring_full_active) {
        stats->ring_full_total_us += storage_wall_time_us() - stats->ring_full_start_us;
        stats->ring_full_active = false;
    }
    return 0;
}

static void storage_stats_update_ring_full(StorageProducerStats *stats,
                                           bool is_full,
                                           uint64_t now_us,
                                           uint64_t received_bytes) {
    if (!stats) {
        return;
    }
    if (is_full) {
        stats->ring_full_last_at_bytes = received_bytes;
        if (!stats->ring_full_active) {
            stats->ring_full_active = true;
            stats->integrity_risk_ring_full = true;
            stats->ring_full_start_us = now_us;
            ++stats->ring_full_count;
            if (stats->ring_full_first_at_bytes == 0u) {
                stats->ring_full_first_at_bytes = received_bytes;
            }
        }
    } else if (stats->ring_full_active) {
        stats->ring_full_total_us += now_us - stats->ring_full_start_us;
        stats->ring_full_active = false;
        stats->ring_full_start_us = 0u;
    }
}

static uint32_t storage_watermark_level(uint32_t busy_slots, uint32_t total_slots)
{
    if (total_slots == 0u) {
        return 0u;
    }
    if (busy_slots >= total_slots) {
        return 4u;
    }
    if (total_slots == 32u) {
        if (busy_slots >= 28u) {
            return 3u;
        }
        if (busy_slots >= 24u) {
            return 2u;
        }
        if (busy_slots >= 16u) {
            return 1u;
        }
    } else {
        if (busy_slots * 100u >= total_slots * 88u) {
            return 3u;
        }
        if (busy_slots * 100u >= total_slots * 75u) {
            return 2u;
        }
        if (busy_slots * 100u >= total_slots * 50u) {
            return 1u;
        }
    }
    return 0u;
}

static const char *storage_watermark_name(uint32_t level)
{
    switch (level) {
    case 1u:
        return "half";
    case 2u:
        return "high";
    case 3u:
        return "critical";
    case 4u:
        return "full";
    default:
        return "normal";
    }
}

static void storage_maybe_log_watermark(StorageProducerStats *stats,
                                        StorageWriteQueue *q,
                                        const ChannelRuntime *rt,
                                        const char *task_no,
                                        uint32_t file_index,
                                        uint64_t dma_received_bytes)
{
    uint32_t busy_slots;
    uint32_t level;
    uint64_t buffered_bytes;

    if (!stats || !q || !rt) {
        return;
    }
    busy_slots = storage_queue_busy_count(q, NULL);
    buffered_bytes = storage_queue_buffered_bytes(q, NULL);
    level = storage_watermark_level(busy_slots, rt->dma_desc_count);
    if (level < stats->watermark_level) {
        stats->watermark_level = level;
        return;
    }
    if (level == 0u || level == 4u || level <= stats->watermark_level) {
        return;
    }
    stats->watermark_level = level;
    if (storage_event_logs_enabled()) {
        printf("storage_ddr_watermark channel=%d task=%s file_index=%u level=%s"
               " busy_slots=%u total_slots=%u buffered_bytes=%" PRIu64
               " dma_received_bytes=%" PRIu64 " written_bytes=%" PRIu64
               " ring_full_count=%" PRIu64 "\n",
               rt->cfg->id,
               task_no,
               (unsigned)file_index,
               storage_watermark_name(level),
               (unsigned)busy_slots,
               (unsigned)rt->dma_desc_count,
               buffered_bytes,
               dma_received_bytes,
               storage_queue_written_bytes(q),
               stats->ring_full_count);
        fflush(stdout);
    }
}

static void storage_stats_print_periodic(StorageProducerStats *stats,
                                         StorageWriteQueue *q,
                                         const ChannelRuntime *rt,
                                         const char *task_no,
                                         uint32_t file_index,
                                         uint64_t received_bytes,
                                         uint64_t now_us) {
    uint64_t nvme_bytes;
    uint64_t elapsed_us;
    uint64_t received_delta;
    uint64_t nvme_delta;
    uint64_t nvme_cmd_count;
    uint64_t nvme_cq_completed;
    uint64_t nvme_cmd_delta;
    uint64_t nvme_cq_delta;
    uint64_t harvest_batch_avg;
    uint64_t active_qd_integral_us;
    uint64_t active_qd_observed_us;
    uint64_t submit_stall_count;
    uint64_t submit_stall_max_us;
    uint32_t busy_slots;
    bool print_zero_stats;
    StorageQueueSnapshot snapshot;
    DmaBdSnapshot bd_snapshot;

    if (!stats || stats->interval_ms == 0u || now_us < stats->next_log_us ||
        !storage_should_print_periodic_stats((ChannelRuntime *)rt)) {
        return;
    }
    storage_stats_finish_harvest_batch(stats);
    storage_queue_snapshot(q, &snapshot);
    pthread_mutex_lock(&q->lock);
    if (dma_get_bd_snapshot((ChannelRuntime *)rt, q->slot_state, &bd_snapshot) != 0) {
        memset(&bd_snapshot, 0, sizeof(bd_snapshot));
    }
    pthread_mutex_unlock(&q->lock);
    nvme_bytes = __atomic_load_n(&rt->nvme_write_bytes_done, __ATOMIC_ACQUIRE);
    nvme_cmd_count = __atomic_load_n(&rt->nvme_cmd_count, __ATOMIC_ACQUIRE);
    nvme_cq_completed = __atomic_load_n(&rt->nvme_cq_completed, __ATOMIC_ACQUIRE);
    elapsed_us = now_us - stats->window_start_us;
    received_delta = received_bytes - stats->window_received_bytes;
    nvme_delta = nvme_bytes - stats->window_nvme_bytes;
    nvme_cmd_delta = nvme_cmd_count - stats->window_nvme_cmd_count;
    nvme_cq_delta = nvme_cq_completed - stats->window_nvme_cq_completed;
    active_qd_integral_us = __atomic_load_n(&rt->nvme_active_qd_integral_us, __ATOMIC_ACQUIRE);
    active_qd_observed_us = __atomic_load_n(&rt->nvme_active_qd_observed_us, __ATOMIC_ACQUIRE);
    submit_stall_count = __atomic_load_n(&rt->nvme_submit_stall_count, __ATOMIC_ACQUIRE);
    submit_stall_max_us = __atomic_load_n(&rt->nvme_submit_stall_max_us, __ATOMIC_ACQUIRE);
    harvest_batch_avg = stats->dma_harvest_batches > 0u
                            ? stats->dma_harvest_batch_total / stats->dma_harvest_batches
                            : 0u;
    busy_slots = storage_queue_busy_count(q, NULL);
    print_zero_stats = storage_env_flag_enabled("SRC_REAL_PRINT_ZERO_STATS") != 0;

    if (!print_zero_stats && received_delta == 0u && nvme_delta == 0u) {
        if (!stats->idle_printed && received_bytes == 0u && nvme_bytes == 0u) {
            storage_emit_line("storage_pipeline_idle channel=%d dma_writable_slots=%u",
                   rt->cfg->id,
                   (unsigned)snapshot.dma_writable_slots);
            stats->idle_printed = true;
        }
        stats->window_start_us = now_us;
        stats->window_received_bytes = received_bytes;
        stats->window_nvme_bytes = nvme_bytes;
        stats->window_nvme_cmd_count = nvme_cmd_count;
        stats->window_nvme_cq_completed = nvme_cq_completed;
        stats->next_log_us = now_us + (uint64_t)stats->interval_ms * 1000ull;
        return;
    }

    if (storage_env_flag_enabled("SRC_REAL_ENABLE_STORAGE_STATS")) {
        printf("storage_stats channel=%d task=%s file_index=%u"
               " received_bytes=%" PRIu64 " written_bytes=%" PRIu64
               " rx_mib_s=%.3f nvme_complete_mib_s=%.3f busy_slots=%u software_free_slots=%u"
               " buffered_bytes=%" PRIu64 " ring_full_count=%" PRIu64
               " nvme_cmd_count=%" PRIu64 " nvme_cmd_size_bytes=%u"
               " nvme_qd_effective=%u nvme_active_qd_current=%u"
               " nvme_active_qd_max=%u\n",
               rt->cfg->id,
               task_no,
               (unsigned)file_index,
               received_bytes,
               storage_queue_written_bytes(q),
               elapsed_us > 0u ? ((double)received_delta * 1000000.0 / (double)elapsed_us / 1048576.0) : 0.0,
               elapsed_us > 0u ? ((double)nvme_delta * 1000000.0 / (double)elapsed_us / 1048576.0) : 0.0,
               (unsigned)busy_slots,
               (unsigned)(rt->dma_desc_count - busy_slots),
               storage_queue_buffered_bytes(q, NULL),
               stats->ring_full_count,
               nvme_cmd_count,
               (unsigned)rt->nvme_cmd_size_bytes,
               (unsigned)rt->nvme_qd_effective,
               (unsigned)__atomic_load_n(&rt->nvme_active_qd_current, __ATOMIC_ACQUIRE),
               (unsigned)__atomic_load_n(&rt->nvme_active_qd_max, __ATOMIC_ACQUIRE));
    }
    storage_emit_line("storage_pipeline channel=%d window_ms=%" PRIu64 " task=%s file_index=%u"
           " rx_mib_s=%.3f nvme_complete_mib_s=%.3f"
           " ready_q=%u ready_q_avg=%u ready_q_max=%u"
           " dma_writable_slots=%u software_free_slots=%u ready_slots=%u nvme_busy_slots=%u"
           " busy_count=%u backlog_bytes=%" PRIu64 " backlog_delta_bytes=%" PRId64
           " ring_full=%" PRIu64 " no_free=%" PRIu64
           " writer_idle_us=%" PRIu64 " writer_active_us=%" PRIu64
           " dma_harvest_count=%" PRIu64 " harvest_batch_avg=%" PRIu64
           " harvest_batch_max=%u nvme_cmd_submitted_window=%" PRIu64
           " nvme_cmd_completed_window=%" PRIu64
           " active_qd_avg=%.3f active_qd_max=%u"
           " writer_rt_enabled=%u writer_rt_prio=%u"
           " submit_stall_count=%" PRIu64 " submit_stall_max_us=%" PRIu64
           " writer_empty_wait_us=%" PRIu64 " writer_drain_active_us=%" PRIu64
           " ready_q_nonempty_us=%" PRIu64
           " writer_drain_loop_count=%" PRIu64
           " writer_slots_per_drain_loop=%" PRIu64,
           rt->cfg->id,
           elapsed_us / 1000u,
           task_no,
           (unsigned)file_index,
           elapsed_us > 0u ? ((double)received_delta * 1000000.0 / (double)elapsed_us / 1048576.0) : 0.0,
           elapsed_us > 0u ? ((double)nvme_delta * 1000000.0 / (double)elapsed_us / 1048576.0) : 0.0,
           (unsigned)snapshot.ready_depth_current,
           (unsigned)snapshot.ready_depth_avg,
           (unsigned)snapshot.ready_depth_max,
           (unsigned)snapshot.dma_writable_slots,
           (unsigned)snapshot.free_slots,
           (unsigned)snapshot.ready_for_nvme_slots,
           (unsigned)snapshot.nvme_busy_slots,
           (unsigned)snapshot.busy_count,
           snapshot.buffered_bytes,
           (int64_t)snapshot.buffered_bytes - (int64_t)stats->window_backlog_bytes,
           stats->ring_full_count,
           stats->dma_no_free_slot_count,
           snapshot.writer_idle_us,
           snapshot.writer_active_us,
           stats->dma_desc_completed_count,
           harvest_batch_avg,
           (unsigned)stats->dma_harvest_batch_max,
           nvme_cmd_delta,
           nvme_cq_delta,
           active_qd_observed_us > 0u
               ? (double)active_qd_integral_us / (double)active_qd_observed_us
               : 0.0,
           (unsigned)__atomic_load_n(&rt->nvme_active_qd_max, __ATOMIC_ACQUIRE),
           snapshot.writer_rt_enabled ? 1u : 0u,
           (unsigned)snapshot.writer_rt_prio,
           submit_stall_count,
           submit_stall_max_us,
           snapshot.writer_idle_us,
           snapshot.writer_active_us,
           snapshot.ready_q_nonempty_us,
           snapshot.writer_drain_loop_count,
           snapshot.writer_drain_loop_count > 0u
               ? snapshot.writer_slots_drained / snapshot.writer_drain_loop_count
               : 0u);
    storage_emit_line("storage_receive channel=%d task=%s file_index=%u window_ms=%" PRIu64
                      " dma_bytes=%" PRIu64 " dma_mib_s=%.3f dma_bd_total=%u"
                      " dma_bd_writable=%u dma_bd_completed_unharvested=%u ready_slots=%u"
                      " nvme_busy_slots=%u requeue_pending=%u occupied_bytes_est=%" PRIu64
                      " curdesc=%u taildesc=%u s2mm_dmasr=0x%08x"
                      " bd_exhaustion_count=%" PRIu64 " dma_error_count=%" PRIu64
                      " receive_integrity_ok=%u",
                      rt->cfg->id, task_no, (unsigned)file_index, elapsed_us / 1000u,
                      received_delta,
                      elapsed_us > 0u ? ((double)received_delta * 1000000.0 /
                                           (double)elapsed_us / 1048576.0) : 0.0,
                      bd_snapshot.total_slots, bd_snapshot.dma_writable,
                      bd_snapshot.completed_unharvested, bd_snapshot.ready_slots,
                      bd_snapshot.nvme_busy_slots, bd_snapshot.requeue_pending,
                      bd_snapshot.occupied_bytes_est, bd_snapshot.curdesc_index,
                      bd_snapshot.taildesc_index, bd_snapshot.s2mm_dmasr,
                      stats->ring_full_count, stats->dma_error_count,
                      stats->receive_integrity_ok ? 1u : 0u);

    stats->window_start_us = now_us;
    stats->window_received_bytes = received_bytes;
    stats->window_nvme_bytes = nvme_bytes;
    stats->window_backlog_bytes = snapshot.buffered_bytes;
    stats->window_nvme_cmd_count = nvme_cmd_count;
    stats->window_nvme_cq_completed = nvme_cq_completed;
    stats->next_log_us = now_us + (uint64_t)stats->interval_ms * 1000ull;
}

static int storage_complete_slot(StorageWriteQueue *q, const PendingDdrSlot *item) {
    bool should_requeue;

    if (!q || !item || item->slot >= q->capacity) {
        return -1;
    }
    pthread_mutex_lock(&q->lock);
    if (!q->slot_busy[item->slot]) {
        q->error = true;
        pthread_cond_broadcast(&q->not_full);
        pthread_cond_broadcast(&q->not_empty);
        pthread_mutex_unlock(&q->lock);
        fprintf(stderr,
                "DDR slot completion invariant failed: channel=%d slot=%u was not busy\n",
                q->rt ? q->rt->cfg->id : -1,
                (unsigned)item->slot);
        return -1;
    }
    (void)__atomic_add_fetch(&q->bytes_written, item->bytes, __ATOMIC_RELEASE);
    ++q->chunks;
    q->slot_busy[item->slot] = false;
    if (q->busy_count > 0u) {
        --q->busy_count;
    }
    if (q->buffered_bytes >= item->bytes) {
        q->buffered_bytes -= item->bytes;
    } else {
        q->buffered_bytes = 0u;
    }
    should_requeue = !q->producer_done;
    if (storage_local_slot_transition_locked(q, item->slot, STORAGE_SLOT_NVME_BUSY,
                                             should_requeue ? STORAGE_SLOT_REQUEUE_PENDING : STORAGE_SLOT_FREE) != 0) {
        pthread_mutex_unlock(&q->lock); return -1;
    }
    pthread_mutex_unlock(&q->lock);

    if (should_requeue) {
        if (dma_requeue_one(q->rt, item->slot) != 0) {
            storage_mark_writer_error(q);
            return -1;
        }
        pthread_mutex_lock(&q->lock);
        if (storage_local_slot_transition_locked(q, item->slot, STORAGE_SLOT_REQUEUE_PENDING,
                                                 STORAGE_SLOT_DMA_WRITABLE) != 0) {
            pthread_mutex_unlock(&q->lock); return -1;
        }
        pthread_mutex_unlock(&q->lock);
    }
    return 0;
}

static void storage_mark_writer_error(StorageWriteQueue *q) {
    if (!q) {
        return;
    }
    pthread_mutex_lock(&q->lock);
    q->error = true;
    q->producer_done = true;
    pthread_cond_broadcast(&q->not_full);
    pthread_cond_broadcast(&q->not_empty);
    pthread_mutex_unlock(&q->lock);
    storage_write_request_stop();
}

static int storage_queue_pop(StorageWriteQueue *q, PendingDdrSlot *out, bool wait_for_item) {
    if (!q || !out) {
        return -1;
    }
    pthread_mutex_lock(&q->lock);
    while (wait_for_item && !q->producer_done && !q->error && q->count == 0u) {
        pthread_cond_wait(&q->not_empty, &q->lock);
    }
    if ((q->producer_done || q->error || !wait_for_item) && q->count == 0u) {
        pthread_mutex_unlock(&q->lock);
        return 0;
    }
    *out = q->items[q->head];
    q->head = (q->head + 1u) % q->capacity;
    --q->count;
    if (storage_local_slot_transition_locked(q, out->slot, STORAGE_SLOT_READY_FOR_NVME,
                                             STORAGE_SLOT_NVME_BUSY) != 0) {
        pthread_mutex_unlock(&q->lock); return -1;
    }
    storage_queue_record_depth_locked(q);
    pthread_cond_signal(&q->not_full);
    pthread_mutex_unlock(&q->lock);
    return 1;
}

static int storage_queue_wait_run(StorageWriteQueue *q)
{
    int rc = 0;
    if (!q) return -1;
    pthread_mutex_lock(&q->lock);
    while (!q->run_enabled && !q->error && !q->producer_done) {
        pthread_cond_wait(&q->not_empty, &q->lock);
    }
    if (q->error) rc = -1;
    pthread_mutex_unlock(&q->lock);
    return rc;
}

static void storage_queue_enable_run(StorageWriteQueue *q)
{
    if (!q) return;
    pthread_mutex_lock(&q->lock);
    q->run_enabled = true;
    pthread_cond_broadcast(&q->not_empty);
    pthread_mutex_unlock(&q->lock);
}

static uint32_t storage_queue_ready_count(StorageWriteQueue *q)
{
    uint32_t count;

    if (!q) {
        return 0u;
    }
    pthread_mutex_lock(&q->lock);
    count = q->count;
    pthread_mutex_unlock(&q->lock);
    return count;
}

static int storage_cross_slot_done_cb(void *opaque, const NvmeWriteSlotReq *req) {
    StorageWriteQueue *q = (StorageWriteQueue *)opaque;
    PendingDdrSlot item;

    if (!q || !req) {
        return -1;
    }
    memset(&item, 0, sizeof(item));
    item.slot = req->slot;
    item.bytes = req->bytes;
    item.chunk_index = req->chunk_index;
    item.file_offset = req->file_offset;
    item.start_lba = req->start_lba;
    item.sectors = req->sectors;
    item.hw_addr = req->hw_addr;
    storage_trace_flush_done(q->rt, &item, q->file_index, q->metadata_slot, q->task_no);
    return storage_complete_slot(q, &item);
}

static void *storage_nvme_writer_thread(void *arg) {
    StorageWriteQueue *q = (StorageWriteQueue *)arg;

    if (storage_queue_wait_run(q) != 0) return NULL;
    storage_apply_writer_rt(q);
    while (1) {
        PendingDdrSlot item;
        uint64_t wait_start_us;
        uint64_t write_start_us;
        uint64_t write_us;
        bool backlog_after_pop = false;
        int pop_rc;

        wait_start_us = storage_wall_time_us();
        pop_rc = storage_queue_pop(q, &item, true);
        if (pop_rc > 0) {
            (void)__atomic_add_fetch(&q->writer_idle_us,
                                     storage_elapsed_us(wait_start_us),
                                     __ATOMIC_RELEASE);
        }
        if (pop_rc < 0) {
            storage_mark_writer_error(q);
            break;
        }
        if (pop_rc == 0) {
            break;
        }
        backlog_after_pop = storage_queue_ready_count(q) > 0u;

        write_start_us = storage_wall_time_us();
        if (flush_slot_to_nvme(q->rt,
                               &item,
                               q->file_index,
                               q->metadata_slot,
                               q->task_no) != 0) {
            storage_mark_writer_error(q);
            break;
        }
        write_us = storage_elapsed_us(write_start_us);
        (void)__atomic_add_fetch(&q->nvme_write_us, write_us, __ATOMIC_RELEASE);
        (void)__atomic_add_fetch(&q->writer_active_us, write_us, __ATOMIC_RELEASE);
        (void)__atomic_add_fetch(&q->writer_drain_loop_count, 1u, __ATOMIC_RELEASE);
        (void)__atomic_add_fetch(&q->writer_slots_drained, 1u, __ATOMIC_RELEASE);
        if (backlog_after_pop || q->backlog_mode) {
            (void)__atomic_add_fetch(&q->ready_q_nonempty_us, write_us, __ATOMIC_RELEASE);
        }
        if (storage_complete_slot(q, &item) != 0) {
            storage_mark_writer_error(q);
            break;
        }
    }
    return NULL;
}

static void *storage_nvme_cross_slot_writer_thread(void *arg) {
    StorageWriteQueue *q = (StorageWriteQueue *)arg;
    NvmeCrossSlotEngine *engine;

    if (!q) {
        return NULL;
    }
    if (storage_queue_wait_run(q) != 0) return NULL;
    storage_apply_writer_rt(q);
    engine = nvme_cross_slot_engine_create(q->rt);
    if (!engine) {
        storage_mark_writer_error(q);
        return NULL;
    }

    while (1) {
        PendingDdrSlot item;
        NvmeWriteSlotReq req;
        int pop_rc;

        pop_rc = storage_queue_pop(q, &item,
                                   nvme_cross_slot_engine_active(engine) == 0u);
        if (pop_rc < 0) {
            storage_mark_writer_error(q);
            break;
        }
        if (pop_rc > 0) {
            memset(&req, 0, sizeof(req));
            req.slot = item.slot; req.start_lba = item.start_lba; req.sectors = item.sectors;
            req.hw_addr = item.hw_addr; req.bytes = item.bytes; req.chunk_index = item.chunk_index;
            req.file_offset = item.file_offset;
            storage_trace_flush_start(q->rt, &item);
            if (nvme_cross_slot_engine_add(engine, &req) != 0) {
                storage_mark_writer_error(q); break;
            }
        }
        if (nvme_cross_slot_engine_step(engine, 300u, storage_cross_slot_done_cb, q) != 0) {
            storage_mark_writer_error(q); break;
        }
        if (pop_rc == 0 && nvme_cross_slot_engine_active(engine) == 0u) {
            break;
        }
    }
    nvme_cross_slot_engine_destroy(engine);
    return NULL;
}

void storage_write_reset_stop(void) {
    g_storage_stop_requested = 0;
    g_storage_control_stop_latched = false;
}

void storage_write_request_stop(void) {
    g_storage_stop_requested = 1;
}

static int storage_write_stop_requested(void) {
    return g_storage_stop_requested != 0;
}

/* Standardized per-chunk write report for automation/log parsing. */
static void print_report_write(const ChannelRuntime *rt,
                               uint32_t slot,
                               uint64_t ddr_offset,
                               uint64_t cpu_addr,
                               uint32_t cpu_visible,
                               uint64_t hw_addr,
                               uint64_t lba,
                               uint64_t sectors,
                               uint64_t bytes,
                               uint32_t file_index,
                               int metadata_slot,
                               const char *task_no) {
    if (!dbg_verbose_enabled()) {
        return;
    }
    printf("transfer_report channel=%d source=%s ddr_slot=%u ddr_offset=0x%08" PRIx64
           " ddr_cpu_addr=0x%08" PRIx64 " ddr_cpu_visible=%u"
           " ddr_hw_addr=0x%08" PRIx64 " ssd_lba=0x%08" PRIx64
           " sector_count=%" PRIu64 " byte_count=%" PRIu64 " task_no=%s file_index=%u metadata_slot=%d\n",
           rt->cfg->id,
           "write",
           slot,
           ddr_offset,
           cpu_addr,
           (unsigned)cpu_visible,
           hw_addr,
           lba,
           sectors,
           bytes,
           task_no,
           file_index,
           metadata_slot);
}

static int storage_slot_addresses(const ChannelRuntime *rt,
                                  const PendingDdrSlot *item,
                                  uint64_t *buffer_offset,
                                  uint64_t *cpu_addr) {
    uint64_t offset;

    if (!rt || !item || item->slot >= rt->dma_desc_count || item->bytes > rt->dma_desc_bytes) {
        return -1;
    }
    offset = (uint64_t)item->slot * (uint64_t)rt->dma_desc_bytes;
    if ((offset + item->bytes) > rt->dma_ring_bytes) {
        return -1;
    }
    if (buffer_offset) {
        *buffer_offset = offset;
    }
    if (cpu_addr) {
        *cpu_addr = (offset + item->bytes) <= rt->cfg->ddr_cpu_size
                        ? rt->cfg->ddr_cpu_base + offset
                        : 0u;
    }
    return 0;
}

static void storage_trace_flush_start(const ChannelRuntime *rt, const PendingDdrSlot *item) {
    uint64_t buffer_offset = 0u;

    if (!rt || !item || storage_slot_addresses(rt, item, &buffer_offset, NULL) != 0) {
        return;
    }
    if (storage_trace_chunk_enabled(item->chunk_index, item->slot)) {
        printf("storage_dma_chunk_trace phase=flush_start channel=%d chunk=%" PRIu64
               " chunk_ordinal=%" PRIu64 " slot=%u file_offset=%" PRIu64
               " bytes=%" PRIu64 " sectors=%" PRIu64
               " lba=0x%08" PRIx64 " ddr_offset=%" PRIu64
               " hw=0x%08" PRIx64 "\n",
               rt->cfg->id,
               item->chunk_index,
               item->chunk_index + 1u,
               (unsigned)item->slot,
               item->file_offset,
               item->bytes,
               item->sectors,
               item->start_lba,
               buffer_offset,
               item->hw_addr);
        fflush(stdout);
    }
    dbg_verbose_printf("[DBG][WRITE] flush start ch=%d slot=%u bytes=%" PRIu64
                       " lba=0x%08" PRIx64 " sectors=%" PRIu64 " hw=0x%08" PRIx64 "\n",
                       rt->cfg->id,
                       (unsigned)item->slot,
                       item->bytes,
                       item->start_lba,
                       item->sectors,
                       item->hw_addr);
}

static void storage_trace_flush_done(const ChannelRuntime *rt,
                                     const PendingDdrSlot *item,
                                     uint32_t file_index,
                                     int metadata_slot,
                                     const char *task_no) {
    uint64_t buffer_offset = 0u;
    uint64_t cpu_addr = 0u;

    if (!rt || !item || storage_slot_addresses(rt, item, &buffer_offset, &cpu_addr) != 0) {
        return;
    }
    dbg_verbose_printf("[DBG][WRITE] flush done ch=%d slot=%u bytes=%" PRIu64
                       " lba=0x%08" PRIx64 " sectors=%" PRIu64 "\n",
                       rt->cfg->id,
                       (unsigned)item->slot,
                       item->bytes,
                       item->start_lba,
                       item->sectors);
    if (storage_trace_chunk_enabled(item->chunk_index, item->slot)) {
        printf("storage_dma_chunk_trace phase=flush_done channel=%d chunk=%" PRIu64
               " chunk_ordinal=%" PRIu64 " slot=%u file_offset=%" PRIu64
               " bytes=%" PRIu64 " sectors=%" PRIu64
               " lba=0x%08" PRIx64 " next_lba=0x%08" PRIx64 "\n",
               rt->cfg->id,
               item->chunk_index,
               item->chunk_index + 1u,
               (unsigned)item->slot,
               item->file_offset,
               item->bytes,
               item->sectors,
               item->start_lba,
               item->start_lba + item->sectors);
        fflush(stdout);
    }
    print_report_write(rt,
                       item->slot,
                       buffer_offset,
                       cpu_addr,
                       (cpu_addr != 0u) ? 1u : 0u,
                       item->hw_addr,
                       item->start_lba,
                       item->sectors,
                       item->bytes,
                       file_index,
                       metadata_slot,
                       task_no);
}

static int flush_slot_to_nvme(ChannelRuntime *rt,
                              const PendingDdrSlot *item,
                              uint32_t file_index,
                              int metadata_slot,
                              const char *task_no) {
    static bool param_logged = false;
    uint64_t buffer_offset = 0u;
    uint64_t sectors;
    FILE *perf_log = NULL;
    bool do_perf_log = false;
    uint64_t perf_expected_cmds = 0u;
    uint64_t write_start_us = 0u;
    uint64_t write_us = 0u;
    StorageNvmeTimingSnapshot timing_before;
    StorageNvmeTimingSnapshot timing_after;

    if (!rt || !item || item->bytes == 0u) {
        return 0;
    }
    if (item->slot >= rt->dma_desc_count || item->bytes > rt->dma_desc_bytes) {
        fprintf(stderr,
                "Invalid DMA write slot: channel=%d slot=%u bytes=%" PRIu64 " desc_bytes=%u count=%u\n",
                rt->cfg->id,
                (unsigned)item->slot,
                item->bytes,
                (unsigned)rt->dma_desc_bytes,
                (unsigned)rt->dma_desc_count);
        return -1;
    }
    if (storage_slot_addresses(rt, item, &buffer_offset, NULL) != 0) {
        fprintf(stderr,
                "DMA write range exceeds ring window: channel=%d slot=%u bytes=%" PRIu64
                " ring_size=%" PRIu64 "\n",
                rt->cfg->id,
                (unsigned)item->slot,
                item->bytes,
                rt->dma_ring_bytes);
        return -1;
    }
    sectors = bytes_to_sectors(item->bytes);
    if (item->sectors != sectors || item->hw_addr != rt->cfg->ddr_hw_base + buffer_offset) {
        fprintf(stderr,
                "DMA queued slot invariant failed: channel=%d slot=%u bytes=%" PRIu64
                " queued_sectors=%" PRIu64 " expected_sectors=%" PRIu64
                " queued_hw=0x%08" PRIx64 " expected_hw=0x%08" PRIx64 "\n",
                rt->cfg->id,
                (unsigned)item->slot,
                item->bytes,
                item->sectors,
                sectors,
                item->hw_addr,
                rt->cfg->ddr_hw_base + buffer_offset);
        return -1;
    }
    if (storage_should_print_slot_perf(rt, item->chunk_index)) {
        perf_log = storage_slot_perf_log_open();
        do_perf_log = perf_log != NULL;
    }
    if (do_perf_log && !param_logged) {
        fprintf(perf_log,
                "storage_nvme_param_compare channel=%d path=storage-write"
                " cmd_size=%u qd=%u feed_mode=%s busy_poll_us=%u poll_sleep_us=%u"
                " max_dts=%u raw_path_uses_same_runtime_params=1\n",
                rt->cfg->id,
                (unsigned)rt->nvme_cmd_size_bytes,
                (unsigned)rt->nvme_qd_effective,
                rt->nvme_feed_mode == NVME_FEED_MODE_TIGHT ? "tight" : "legacy",
                (unsigned)rt->nvme_busy_poll_us,
                (unsigned)rt->nvme_poll_sleep_us,
                (unsigned)rt->nvme_max_dts_bytes);
        param_logged = true;
    }
    storage_trace_flush_start(rt, item);
    if (rt->nvme_max_lba > 0u && (item->start_lba + item->sectors) > rt->nvme_max_lba) {
        fprintf(stderr, "Requested SSD range exceeds max LBA: start=0x%08" PRIx64 " sectors=%" PRIu64
                " max=0x%08" PRIx64 "\n",
                item->start_lba,
                item->sectors,
                rt->nvme_max_lba);
        dbg_printf("[DBG][WRITE] lba exceeds max ch=%d lba=0x%08" PRIx64 " sectors=%" PRIu64
                   " max=0x%08" PRIx64 "\n",
                   rt->cfg->id,
                   item->start_lba,
                   item->sectors,
                   rt->nvme_max_lba);
        return -1;
    }
    if (do_perf_log) {
        storage_nvme_timing_snapshot(rt, &timing_before);
        perf_expected_cmds = nvme_perf_calc_begin(rt, item->bytes);
        write_start_us = storage_wall_time_us();
    }
    if (rt->nvme_feed_mode == NVME_FEED_MODE_TIGHT) {
        if (nvme_write_contiguous_tight_qd(rt,
                                           item->hw_addr,
                                           item->start_lba,
                                           item->bytes,
                                           rt->nvme_qd_effective) != 0) {
            dbg_printf("[DBG][WRITE] nvme tight write failed ch=%d lba=0x%08" PRIx64
                       " bytes=%" PRIu64 "\n",
                       rt->cfg->id,
                       item->start_lba,
                       item->bytes);
            return -1;
        }
    } else if (nvme_write_slot_qd(rt, item->slot, item->start_lba, item->sectors, item->hw_addr) != 0) {
        dbg_printf("[DBG][WRITE] nvme write failed ch=%d lba=0x%08" PRIx64 " sectors=%" PRIu64 "\n",
                   rt->cfg->id,
                   item->start_lba,
                   item->sectors);
        return -1;
    }
    if (do_perf_log) {
        uint64_t cmd_delta;
        double active_qd_avg;
        double slot_mib_s;

        write_us = storage_elapsed_us(write_start_us);
        storage_nvme_timing_snapshot(rt, &timing_after);
        cmd_delta = timing_after.cmd_count - timing_before.cmd_count;
        active_qd_avg = storage_nvme_active_qd_avg_delta(&timing_before, &timing_after);
        slot_mib_s = write_us > 0u
                         ? ((double)item->bytes * 1000000.0) /
                               ((double)write_us * 1048576.0)
                         : 0.0;
        fprintf(perf_log,
                "slot_write_perf channel=%d task=%s file_index=%u chunk=%" PRIu64
                " slot=%u bytes=%" PRIu64 " nvme_write_ms=%.3f mib_s=%.3f"
                " cmd_count=%" PRIu64 " active_qd_avg=%.3f"
                " cmd_size=%u qd=%u feed_mode=%s max_dts=%u\n",
                rt->cfg->id,
                task_no,
                (unsigned)file_index,
                item->chunk_index,
                (unsigned)item->slot,
                item->bytes,
                (double)write_us / 1000.0,
                slot_mib_s,
                cmd_delta,
                active_qd_avg,
                (unsigned)rt->nvme_cmd_size_bytes,
                (unsigned)rt->nvme_qd_effective,
                rt->nvme_feed_mode == NVME_FEED_MODE_TIGHT ? "tight" : "legacy",
                (unsigned)rt->nvme_max_dts_bytes);
        nvme_perf_calc_fprint(perf_log, rt, item->bytes, perf_expected_cmds, write_us);
        storage_print_slot_sw_timing(perf_log, rt, &timing_before, &timing_after);
    }
    storage_trace_flush_done(rt, item, file_index, metadata_slot, task_no);
    return 0;
}

int execute_write_with_result(const ParsedArgs *args, GlobalOptions gopt, WriteResult *result) {
    const ChannelConfig *cfg = find_channel(args->channel_id);
    ChannelRuntime rt;
    FileEntry table[MAX_FILES_TOTAL];
    int rc = -1;
    int metadata_slot = -1;
    uint64_t auto_lba = 0u;
    uint32_t valid_count = 0u;
    uint64_t start_lba = 0u;
    uint64_t total_sectors = 0u;
    uint64_t dma_received_bytes = 0u;
    uint64_t bytes_captured = 0u;
    uint64_t bytes_written = 0u;
    uint64_t next_queue_lba = 0u;
    uint64_t capture_start_us = 0u;
    uint64_t elapsed_us = 0u;
    uint64_t nvme_write_us = 0u;
    uint32_t chunks = 0u;
    uint32_t max_busy_slots = 0u;
    uint64_t max_buffered_bytes = 0u;
    StorageProducerStats producer_stats;
    StorageWriteQueue write_queue;
    StorageQueueSnapshot final_queue_snapshot;
    DmaStopReport dma_stop_report;
    DmaStopResult dma_stop_result = DMA_STOP_OK;
    pthread_t writer_thread;
    bool write_queue_ready = false;
    bool writer_started = false;
    bool dma_started = false;
    bool dma_stop_attempted = false;
    bool manual_stop_seen = false;
    bool tail_incomplete = false;
    bool dma_stop_failed = false;
    bool data_persisted = false;
    bool final_integrity_ok = false;
    bool auto_idle_done = false;
    bool bounded;
    bool cross_slot_qd;
    uint64_t requested_size;
    uint32_t dma_desc_bytes = DMA_DESC_BYTES_DEFAULT;
    uint32_t effective_file_index = args->file_index;
    uint32_t storage_poll_sleep_us;
    uint32_t storage_high_poll_sleep_us;
    uint32_t storage_critical_poll_sleep_us;
    uint32_t cross_slot_batch;
    uint32_t ready_queue_depth_cfg;
    uint32_t harvest_batch_max_cfg;
    uint32_t dma_idle_done_ms;
    int producer_rt_policy = SCHED_OTHER;
    uint32_t producer_rt_prio = 0u;
    uint64_t requested_ring_bytes;
    bool pipeline_threaded_mode;
    bool fast_pipeline_enabled;
    uint64_t start_skew_us = 0u;
    const char *start_gate_mode = "standalone_immediate";

    storage_write_reset_stop();
    memset(&producer_stats, 0, sizeof(producer_stats));
    producer_stats.receive_integrity_ok = true;
    producer_stats.min_dma_writable = UINT32_MAX;
    snprintf(producer_stats.receive_integrity_risk,
             sizeof(producer_stats.receive_integrity_risk), "none");
    memset(&final_queue_snapshot, 0, sizeof(final_queue_snapshot));
    memset(&dma_stop_report, 0, sizeof(dma_stop_report));
    if (result) {
        memset(result, 0, sizeof(*result));
    }
    if (!cfg) {
        dbg_printf("[DBG][WRITE] invalid channel=%d\n", args->channel_id);
        return -1;
    }
    dma_desc_bytes = args->has_dma_desc_bytes ? args->dma_desc_bytes : storage_default_dma_desc_bytes(cfg);

    bounded = args->has_size;
    requested_size = bounded ? args->size_bytes : 0u;
    dma_idle_done_ms = bounded ? 0u : storage_dma_idle_done_ms();
    storage_poll_sleep_us = storage_channel_env_u32(cfg,
                                                    "PRODUCER_IDLE_SLEEP_US",
                                                    "SRC_REAL_STORAGE_POLL_SLEEP_US",
                                                    cfg->id == 2 ? 20u : 3u,
                                                    1000000u);
    storage_high_poll_sleep_us = storage_env_u32_limit("SRC_REAL_STORAGE_HIGH_WATERMARK_POLL_US",
                                                       STORAGE_HIGH_WATERMARK_POLL_US_DEFAULT,
                                                       1000000u);
    storage_critical_poll_sleep_us = storage_env_u32_limit("SRC_REAL_STORAGE_CRITICAL_WATERMARK_POLL_US",
                                                           STORAGE_CRITICAL_WATERMARK_POLL_US_DEFAULT,
                                                           1000000u);
    pipeline_threaded_mode = storage_env_string_is("SRC_REAL_PIPELINE_MODE", "threaded");
    if (cfg && cfg->id == 0) {
        fast_pipeline_enabled = storage_env_flag_enabled("SRC_REAL_CH0_FAST_PIPELINE") != 0;
    } else if (cfg && cfg->id == 1) {
        fast_pipeline_enabled = storage_env_flag_enabled("SRC_REAL_CH1_FAST_PIPELINE") != 0;
    } else if (cfg && cfg->id == 2) {
        fast_pipeline_enabled = storage_env_flag_enabled("SRC_REAL_CH2_FAST_PIPELINE") != 0;
    } else {
        fast_pipeline_enabled = false;
    }
    ready_queue_depth_cfg = storage_env_u32_limit("SRC_REAL_READY_QUEUE_DEPTH",
                                                  STORAGE_READY_QUEUE_DEPTH_DEFAULT,
                                                  4096u);
    harvest_batch_max_cfg = storage_channel_env_u32(cfg,
                                                    "HARVEST_BATCH_MAX",
                                                    "SRC_REAL_HARVEST_BATCH_MAX",
                                                    cfg->id == 2 ? 4u : 16u,
                                                    1024u);
    {
        char name[64];
        snprintf(name, sizeof(name), "SRC_REAL_NVME_CROSS_SLOT_QD_CH%d", cfg->id);
        cross_slot_qd = getenv(name) ? storage_env_flag_enabled(name) != 0 :
                        (getenv("SRC_REAL_NVME_CROSS_SLOT_QD") ?
                         storage_env_flag_enabled("SRC_REAL_NVME_CROSS_SLOT_QD") != 0 : cfg->id != 2);
        snprintf(name, sizeof(name), "SRC_REAL_NVME_CROSS_SLOT_BATCH_CH%d", cfg->id);
        cross_slot_batch = getenv(name) ? storage_env_u32_limit(name, 4u, 32u) :
                           storage_env_u32_limit("SRC_REAL_NVME_CROSS_SLOT_BATCH",
                                                 cfg->id == 2 ? 1u : 4u, 32u);
    }
    dbg_printf("[DBG][WRITE] start ch=%d mode=%s size=%" PRIu64 " task=%s idx=%u lba_auto=%u dry=%u\n",
               cfg->id,
               bounded ? "bounded" : "continuous",
               requested_size,
               args->task_no,
               (unsigned)args->file_index,
               args->lba_auto ? 1u : 0u,
               gopt.dry_run ? 1u : 0u);
    if (channel_runtime_open(&rt, cfg, gopt) != 0) {
        dbg_printf("[DBG][WRITE] channel_runtime_open failed ch=%d\n", cfg->id);
        return -1;
    }
    requested_ring_bytes = storage_requested_ring_bytes(&rt);
    if (requested_ring_bytes != rt.dma_ring_bytes) {
        storage_emit_line("storage_ring_config_error channel=%d requested_ring_bytes=%" PRIu64
                          " effective_ring_bytes=%" PRIu64
                          " reason=%s",
                          cfg->id,
                          requested_ring_bytes,
                          rt.dma_ring_bytes,
                          storage_ring_clamp_reason(&rt, requested_ring_bytes));
        goto out;
    }
    {
        uint32_t planned_desc_count = 0u;
        uint32_t desc_capacity = 0u;
        const char *layout_error = storage_ring_layout_error_reason(&rt,
                                                                    dma_desc_bytes,
                                                                    &planned_desc_count,
                                                                    &desc_capacity);
        if (layout_error) {
            storage_emit_line("storage_ring_config_error channel=%d requested_ring_bytes=%" PRIu64
                              " effective_ring_bytes=%" PRIu64
                              " slot_bytes=%u total_slots=%u desc_capacity=%u reason=%s",
                              cfg->id,
                              requested_ring_bytes,
                              rt.dma_ring_bytes,
                              (unsigned)dma_desc_bytes,
                              (unsigned)planned_desc_count,
                              (unsigned)desc_capacity,
                              layout_error);
            goto out;
        }
    }
    dbg_verbose_printf("[DBG][WRITE] runtime open ok ch=%d\n", cfg->id);
    if (nvme_probe(&rt) != 0) {
        dbg_printf("[DBG][WRITE] nvme_probe failed ch=%d\n", cfg->id);
        goto out;
    }
    dbg_verbose_printf("[DBG][WRITE] nvme_probe ok ch=%d max_lba=0x%08" PRIx64 " max_dts=%u\n",
                       cfg->id,
                       rt.nvme_max_lba,
                       rt.nvme_max_dts_bytes);
    if (rt.nvme_max_lba == 0u) {
        fprintf(stderr, "NVMe capacity unavailable on channel %d\n", cfg->id);
        dbg_printf("[DBG][WRITE] nvme max_lba unavailable ch=%d, refuse write\n",
                   cfg->id);
        goto out;
    }
    if (metadata_read(&rt, table) != 0) {
        dbg_printf("[DBG][WRITE] metadata_read failed ch=%d\n", cfg->id);
        goto out;
    }
    dbg_verbose_printf("[DBG][WRITE] metadata_read ok ch=%d\n", cfg->id);
    if (metadata_resolve_file_index(table,
                                    args->task_no,
                                    args->file_index,
                                    &effective_file_index) != 0) {
        fprintf(stderr, "Unable to allocate next file index: task=%s requested=%u\n",
                args->task_no, (unsigned)args->file_index);
        goto out;
    }
    if (effective_file_index != args->file_index) {
        printf("storage_file_index_advanced task=%s requested_file_index=%u effective_file_index=%u source=metadata\n",
               args->task_no,
               (unsigned)args->file_index,
               (unsigned)effective_file_index);
        fflush(stdout);
    }
    if (metadata_alloc_slot_and_lba(table, &metadata_slot, &auto_lba, &valid_count) != 0) {
        fprintf(stderr, "No free metadata slot on channel %d\n", cfg->id);
        dbg_printf("[DBG][WRITE] metadata alloc failed ch=%d\n", cfg->id);
        goto out;
    }
    dbg_verbose_printf("[DBG][WRITE] metadata alloc ok ch=%d slot=%d auto_lba=0x%08" PRIx64 " valid=%u\n",
                       cfg->id, metadata_slot, auto_lba, (unsigned)valid_count);

    start_lba = args->lba_auto ? auto_lba : args->lba;
    if (bounded) {
        uint64_t requested_sectors = bytes_to_sectors(requested_size);
        if (!args->lba_auto && metadata_check_lba_overlap(table, start_lba, requested_sectors) != 0) {
            dbg_printf("[DBG][WRITE] lba overlap ch=%d lba=0x%08" PRIx64 " sectors=%" PRIu64 "\n",
                       cfg->id, start_lba, requested_sectors);
            goto out;
        }
        if (rt.nvme_max_lba > 0u && (start_lba + requested_sectors) > rt.nvme_max_lba) {
            fprintf(stderr,
                    "Requested SSD range exceeds max LBA: start=0x%08" PRIx64 " sectors=%" PRIu64
                    " max=0x%08" PRIx64 "\n",
                    start_lba, requested_sectors, rt.nvme_max_lba);
            dbg_printf("[DBG][WRITE] lba exceeds max ch=%d lba=0x%08" PRIx64 " sectors=%" PRIu64
                       " max=0x%08" PRIx64 "\n",
                       cfg->id, start_lba, requested_sectors, rt.nvme_max_lba);
            goto out;
        }
    } else if (!args->lba_auto) {
        fprintf(stderr, "continuous storage-write requires --ssd-lba auto\n");
        goto out;
    }

    axis_switch_select(&rt, args->source);
    dbg_verbose_printf("[DBG][WRITE] axis switch selected ch=%d source=%d\n", cfg->id, (int)args->source);
    if (dma_prepare_s2mm_ring(&rt, dma_desc_bytes) != 0) {
        dbg_printf("[DBG][WRITE] dma_init_s2mm_ring failed ch=%d desc_bytes=%u\n",
                   cfg->id, (unsigned)dma_desc_bytes);
        goto out;
    }
    dma_started = false;
    dbg_verbose_printf("[DBG][WRITE] dma ready ch=%d desc_bytes=%u desc_count=%u desc_cpu=0x%08" PRIx64
                       " desc_dma=0x%08" PRIx64 " desc_size=0x%08" PRIx64
                       " ddr_cpu=0x%08" PRIx64 " ddr_hw=0x%08" PRIx64
                       " ring_bytes=%" PRIu64 " continuous=%u\n",
                       cfg->id,
                       (unsigned)rt.dma_desc_bytes,
                       (unsigned)rt.dma_desc_count,
                       cfg->desc_cpu_base,
                       cfg->desc_dma_base,
                       cfg->desc_cpu_size,
                       cfg->ddr_cpu_base,
                       cfg->ddr_hw_base,
                       rt.dma_ring_bytes,
                       bounded ? 0u : 1u);
    printf("storage_prepared channel=%d task=%s file_index=%u size=%" PRIu64 " continuous=%u\n",
           cfg->id,
           args->task_no,
           (unsigned)effective_file_index,
           requested_size,
           bounded ? 0u : 1u);
    fflush(stdout);

    next_queue_lba = start_lba;
    if (storage_queue_init(&write_queue,
                           &rt,
                           effective_file_index,
                           metadata_slot,
                           args->task_no,
                           cross_slot_qd,
                           cross_slot_batch) != 0) {
        fprintf(stderr, "Failed to initialize storage write queue on channel %d\n", cfg->id);
        goto out;
    }
    write_queue_ready = true;
    printf("nvme_scheduler_config channel=%d mode=%s qd=%u cmd_size=%u max_dts=%u batch=%u\n",
           cfg->id,
           cross_slot_qd ? "cross_slot" : "single_slot",
           (unsigned)rt.nvme_qd_effective,
           (unsigned)rt.nvme_cmd_size_bytes,
           (unsigned)rt.nvme_max_dts_bytes,
           cross_slot_qd ? (unsigned)cross_slot_batch : 1u);
    storage_emit_line("storage_pipeline_config channel=%d mode=%s fast_pipeline=%u"
                      " requested_ring_bytes=%" PRIu64
                      " effective_ring_bytes=%" PRIu64
                      " slot_bytes=%u total_slots=%u ring_clamp_reason=%s"
                      " hw_ring_base=0x%08" PRIx64 " hw_ring_end=0x%08" PRIx64
                      " hw_ddr_span_bytes=%" PRIu64 " dma_bd_count=%u"
                      " qd=%u cmd_size=%u log_level=%u writer_rt_policy=%s writer_rt_prio=%u"
                      " producer_rt_policy=%s producer_rt_prio=%u"
                      " backlog_mode=%u dma_idle_done_ms=%u"
                      " ready_queue_depth=%u harvest_batch_max=%u poll_sleep_us=%u"
                      " high_watermark_poll_us=%u critical_watermark_poll_us=%u"
                      " pipeline_stats_sec=%u enable_storage_stats=%u"
                      " slot_write_perf=%u slot_write_perf_sample=%u"
                      " defer_db_until_stop=1 legacy_fallback=1",
                      cfg->id,
                      pipeline_threaded_mode ? "threaded" : "legacy",
                      fast_pipeline_enabled ? 1u : 0u,
                      requested_ring_bytes,
                      rt.dma_ring_bytes,
                      (unsigned)rt.dma_desc_bytes,
                      (unsigned)rt.dma_desc_count,
                      storage_ring_clamp_reason(&rt, requested_ring_bytes),
                      cfg->ddr_hw_base,
                      cfg->ddr_hw_base + rt.dma_ring_bytes,
                      cfg->dma_ring_bytes,
                      (unsigned)rt.dma_desc_count,
                      (unsigned)rt.nvme_qd_effective,
                      (unsigned)rt.nvme_cmd_size_bytes,
                      (unsigned)storage_log_level(),
                      storage_rt_policy_name(storage_rt_policy("SRC_REAL_WRITER_RT_POLICY", SCHED_RR)),
                      (unsigned)storage_writer_rt_prio(&rt),
                      storage_rt_policy_name(storage_rt_policy("SRC_REAL_PRODUCER_RT_POLICY", SCHED_RR)),
                      (unsigned)storage_producer_rt_prio(&rt),
                      storage_env_flag_enabled("SRC_REAL_WRITER_BACKLOG_MODE") ? 1u : 0u,
                      (unsigned)dma_idle_done_ms,
                      (unsigned)(ready_queue_depth_cfg != 0u ? ready_queue_depth_cfg : rt.dma_desc_count),
                      (unsigned)harvest_batch_max_cfg,
                      (unsigned)storage_poll_sleep_us,
                      (unsigned)storage_high_poll_sleep_us,
                      (unsigned)storage_critical_poll_sleep_us,
                      (unsigned)(storage_pipeline_stats_ms() / 1000u),
                      storage_env_flag_enabled("SRC_REAL_ENABLE_STORAGE_STATS") ? 1u : 0u,
                      storage_env_flag_enabled("SRC_REAL_SLOT_WRITE_PERF") ? 1u : 0u,
                      (unsigned)storage_slot_perf_interval());
    if (pthread_create(&writer_thread,
                       NULL,
                       cross_slot_qd ? storage_nvme_cross_slot_writer_thread
                                     : storage_nvme_writer_thread,
                       &write_queue) != 0) {
        fprintf(stderr, "Failed to start NVMe writer thread on channel %d\n", cfg->id);
        goto out;
    }
    writer_started = true;
    producer_stats.interval_ms = storage_pipeline_stats_ms();
    storage_emit_line("storage_ready channel=%d task=%s file_index=%u start_gate_mode=%s",
                      cfg->id, args->task_no, (unsigned)effective_file_index,
                      getenv("SRC_REAL_START_FD") ? "software_barrier" : "standalone_immediate");
    if (storage_wait_start_gate(&rt, &start_skew_us, &start_gate_mode) != 0) {
        producer_stats.receive_integrity_ok = false;
        snprintf(producer_stats.receive_integrity_risk,
                 sizeof(producer_stats.receive_integrity_risk), "start_gate_failed");
        goto out;
    }
    dma_started = true;
    storage_queue_enable_run(&write_queue);
    storage_apply_producer_rt(&rt, &producer_rt_policy, &producer_rt_prio);
    capture_start_us = storage_wall_time_us();
    producer_stats.window_start_us = capture_start_us;
    producer_stats.next_log_us = capture_start_us +
                                 (uint64_t)producer_stats.interval_ms * 1000ull;
    storage_emit_line("storage_started channel=%d task=%s file_index=%u"
                      " start_gate_mode=%s start_skew_us=%" PRIu64,
                      cfg->id, args->task_no, (unsigned)effective_file_index,
                      start_gate_mode, start_skew_us);
    {
        uint32_t stop_idle_polls = 0u;
        uint32_t idle_notice_ms = storage_idle_notice_ms();
        uint64_t last_dma_us = storage_wall_time_us();
        bool saw_dma_data = false;
        bool idle_notice_logged = false;
        bool stop_logged = false;
        bool ring_full_logged = false;

        while (!bounded || bytes_captured < requested_size) {
            uint32_t slot = 0u;
            uint32_t actual = 0u;
            DmaHarvestItem harvest_items[16];
            uint32_t harvest_count = 0u;
            uint32_t harvest_limit = harvest_batch_max_cfg == 0u ? 1u :
                                     (harvest_batch_max_cfg > 16u ? 16u : harvest_batch_max_cfg);
            int harvest_rc;
            bool harvest_fatal;
            int h;
            uint64_t chunk_bytes;
            bool stop_requested = storage_write_stop_requested() != 0 || storage_control_stop_requested();

            if (storage_queue_has_error(&write_queue)) {
                storage_emit_event(STORAGE_WORKER_FATAL, &rt, -1, dma_received_bytes,
                                   "storage_queue_error");
                dbg_printf("[DBG][WRITE] writer thread error observed ch=%d captured=%" PRIu64 "\n",
                           cfg->id, bytes_captured);
                goto out;
            }
            if (stop_requested && !stop_logged) {
                dbg_printf("[DBG][WRITE] stop signal observed ch=%d captured=%" PRIu64 "\n",
                           cfg->id,
                           bytes_captured);
                stop_logged = true;
            }
            if (stop_requested) {
                manual_stop_seen = true;
            }
            if (stop_requested && rt.gopt.dry_run) {
                dbg_printf("[DBG][WRITE] dry-run stop requested ch=%d captured=%" PRIu64 "\n",
                           cfg->id, bytes_captured);
                break;
            }
            if (stop_requested && stop_idle_polls >= STORAGE_STOP_DRAIN_POLLS_DEFAULT) {
                dbg_printf("[DBG][WRITE] stop drain done ch=%d captured=%" PRIu64 "\n",
                           cfg->id, bytes_captured);
                break;
            }

            harvest_rc = dma_harvest_batch(&rt, harvest_items, harvest_limit, 100u, &harvest_count);
            harvest_fatal = harvest_rc != 0;
            h = harvest_count != 0u ? 1 : (harvest_fatal ? -1 : 0);
            if (h < 0) {
                if ((rt.dma_last_completed_status & 0x70000000u) != 0u) {
                    ++producer_stats.descriptor_error_count;
                } else {
                    ++producer_stats.dma_error_count;
                }
                producer_stats.receive_integrity_ok = false;
                if (strcmp(producer_stats.receive_integrity_risk, "none") == 0) {
                    snprintf(producer_stats.receive_integrity_risk,
                             sizeof(producer_stats.receive_integrity_risk),
                             "dma_harvest_failed");
                }
                storage_emit_line("storage_receive_failed channel=%d task=%s file_index=%u"
                                  " reason=%s received_bytes=%" PRIu64
                                  " descriptor_status=0x%08x",
                                  cfg->id, args->task_no, (unsigned)effective_file_index,
                                  producer_stats.receive_integrity_risk,
                                  dma_received_bytes, rt.dma_last_completed_status);
                storage_emit_event(STORAGE_WORKER_FATAL, &rt, -1, dma_received_bytes,
                                   producer_stats.receive_integrity_risk);
                dbg_printf("[DBG][WRITE] dma harvest error ch=%d written=%" PRIu64 " captured=%" PRIu64 "\n",
                           cfg->id, bytes_written, bytes_captured);
                goto out;
            }
            if (h == 0) {
                uint32_t busy_slots = storage_queue_busy_count(&write_queue, &max_busy_slots);
                uint64_t now_us = storage_wall_time_us();
                uint64_t buffered_bytes = storage_queue_buffered_bytes(&write_queue, &max_buffered_bytes);
                DmaBdSnapshot bd_snapshot;
                storage_stats_finish_harvest_batch(&producer_stats);
                if (storage_capture_bd_snapshot(&producer_stats,
                                                &write_queue,
                                                &rt,
                                                args->task_no,
                                                effective_file_index,
                                                dma_received_bytes,
                                                &bd_snapshot) != 0) {
                    goto out;
                }
                storage_maybe_log_watermark(&producer_stats,
                                            &write_queue,
                                            &rt,
                                            args->task_no,
                                            effective_file_index,
                                            dma_received_bytes);
                if (bd_snapshot.dma_writable == 0u) {
                    ++producer_stats.dma_no_free_slot_count;
                    storage_emit_event(STORAGE_WORKER_FATAL, &rt, -1, dma_received_bytes,
                                       "dma_bd_exhausted");
                    producer_stats.receive_integrity_ok = false;
                    snprintf(producer_stats.receive_integrity_risk,
                             sizeof(producer_stats.receive_integrity_risk), "dma_bd_exhausted");
                    goto out;
                    if (!ring_full_logged) {
                        /*
                         * Aurora RX-only Simplex has no TREADY/NFC return path.
                         * Once every DDR slot is busy, software cannot guarantee
                         * that data upstream of AXI DMA is retained.
                         */
                        dbg_printf("[DBG][WRITE] DDR ring full ch=%d busy_slots=%u/%u"
                                   " buffered_bytes=%" PRIu64 " captured=%" PRIu64
                                   " manual_stop=%u\n",
                                   cfg->id,
                                   (unsigned)busy_slots,
                                   (unsigned)rt.dma_desc_count,
                                   buffered_bytes,
                                   bytes_captured,
                                   stop_requested ? 1u : 0u);
                        ring_full_logged = true;
                    }
                    if (!producer_stats.ring_warning_emitted ||
                        now_us - producer_stats.last_ring_warning_us >= 5000000ull) {
                        storage_emit_line("storage_ring_warning channel=%d ring_full_count=%" PRIu64
                                          " no_free=%" PRIu64 " buffered=%" PRIu64
                                          " busy_slots=%u total_slots=%u captured_bytes=%" PRIu64
                                          " writer_written_bytes=%" PRIu64
                                          " integrity_risk=dma_bd_exhausted_no_upstream_backpressure",
                                          cfg->id,
                                          producer_stats.ring_full_count,
                                          producer_stats.dma_no_free_slot_count,
                                          buffered_bytes,
                                          (unsigned)busy_slots,
                                          (unsigned)rt.dma_desc_count,
                                          bytes_captured,
                                          storage_queue_written_bytes(&write_queue));
                        producer_stats.ring_warning_emitted = true;
                        producer_stats.last_ring_warning_us = now_us;
                    }
                } else {
                    ring_full_logged = false;
                }
                if (!bounded && !stop_requested && dma_idle_done_ms > 0u &&
                    saw_dma_data && producer_stats.dma_desc_completed_count > 0u &&
                    !rt.dma_rx_packet_open) {
                    uint64_t idle_us = storage_elapsed_us(last_dma_us);
                    if (idle_us >= (uint64_t)dma_idle_done_ms * 1000ull) {
                        StorageQueueSnapshot idle_snapshot;

                        storage_queue_snapshot(&write_queue, &idle_snapshot);
                        storage_emit_line("storage_dma_idle_done channel=%d idle_ms=%" PRIu64
                                          " received_bytes=%" PRIu64
                                          " written_bytes=%" PRIu64
                                          " buffered=%" PRIu64
                                          " ready_slots=%u action=auto_drain",
                                          cfg->id,
                                          idle_us / 1000ull,
                                          dma_received_bytes,
                                          storage_queue_written_bytes(&write_queue),
                                          idle_snapshot.buffered_bytes,
                                          (unsigned)idle_snapshot.ready_for_nvme_slots);
                        auto_idle_done = true;
                        break;
                    }
                }
                if (stop_requested) {
                    ++stop_idle_polls;
                    usleep(STORAGE_STOP_DRAIN_SLEEP_US);
                } else {
                    uint32_t sleep_us = storage_poll_sleep_us;
                    if (idle_notice_ms > 0u && saw_dma_data && !idle_notice_logged) {
                        uint64_t idle_us = storage_elapsed_us(last_dma_us);
                        if (idle_us >= ((uint64_t)idle_notice_ms * 1000ull)) {
                            uint64_t idle_ms = idle_us / 1000ull;
                            dbg_printf("[DBG][WRITE] idle detected ch=%d task=%s idx=%u idle_ms=%" PRIu64
                                       " dma_received=%" PRIu64 " queued_file_bytes=%" PRIu64
                                       " manual_stop_required=1\n",
                                       cfg->id,
                                       args->task_no,
                                       (unsigned)effective_file_index,
                                       idle_ms,
                                       dma_received_bytes,
                                       bytes_captured);
                            printf("storage_idle_detected channel=%d task=%s file_index=%u idle_ms=%" PRIu64
                                   " dma_received_bytes=%" PRIu64 " queued_file_bytes=%" PRIu64
                                   " manual_stop_required=1\n",
                                   cfg->id,
                                   args->task_no,
                                   (unsigned)effective_file_index,
                                   idle_ms,
                                   dma_received_bytes,
                                   bytes_captured);
                            fflush(stdout);
                            idle_notice_logged = true;
                        }
                    }
                    if (bd_snapshot.dma_writable <= 1u) {
                        sleep_us = storage_critical_poll_sleep_us;
                    } else if (bd_snapshot.dma_writable * 4u <= bd_snapshot.total_slots) {
                        sleep_us = storage_high_poll_sleep_us;
                    }
                    if (sleep_us == 0u) {
                        if (producer_rt_policy != SCHED_OTHER &&
                            producer_rt_prio > storage_writer_rt_prio(&rt)) {
                            struct timespec ts = {0, 1000L};
                            (void)nanosleep(&ts, NULL);
                        } else {
                            sched_yield();
                        }
                    } else {
                        usleep(sleep_us);
                    }
                }
                storage_stats_print_periodic(&producer_stats,
                                             &write_queue,
                                             &rt,
                                             args->task_no,
                                             effective_file_index,
                                             dma_received_bytes,
                                             now_us);
                continue;
            }
            if (harvest_count > 1u) {
                PendingDdrSlot pending[16];
                uint32_t i;

                for (i = 0u; i < harvest_count; ++i) {
                    uint64_t queued_bytes = harvest_items[i].actual_bytes;
                    uint64_t remaining = bounded ? requested_size - bytes_captured : queued_bytes;
                    if (queued_bytes == 0u || queued_bytes > rt.dma_desc_bytes ||
                        (bounded && remaining == 0u)) {
                        producer_stats.receive_integrity_ok = false;
                        snprintf(producer_stats.receive_integrity_risk,
                                 sizeof(producer_stats.receive_integrity_risk), "invalid_dma_harvest_bytes");
                        goto out;
                    }
                    if (bounded && queued_bytes > remaining) queued_bytes = remaining;
                    memset(&pending[i], 0, sizeof(pending[i]));
                    pending[i].slot = harvest_items[i].slot;
                    pending[i].bytes = queued_bytes;
                    pending[i].chunk_index = producer_stats.dma_desc_completed_count + i;
                    pending[i].file_offset = bytes_captured;
                    pending[i].start_lba = next_queue_lba;
                    pending[i].sectors = bytes_to_sectors(queued_bytes);
                    pending[i].hw_addr = rt.cfg->ddr_hw_base +
                                         (uint64_t)pending[i].slot * rt.dma_desc_bytes;
                    bytes_captured += queued_bytes;
                    dma_received_bytes += harvest_items[i].actual_bytes;
                    next_queue_lba += pending[i].sectors;
                    storage_stats_record_dma_desc(&producer_stats, storage_wall_time_us());
                }
                if (storage_local_queue_push_batch(&write_queue, pending, harvest_count) != 0) {
                    storage_emit_event(STORAGE_WORKER_FATAL, &rt, -1, dma_received_bytes,
                                       "storage_queue_full_or_state_error");
                    goto out;
                }
                if (harvest_fatal) {
                    producer_stats.receive_integrity_ok = false;
                    snprintf(producer_stats.receive_integrity_risk,
                             sizeof(producer_stats.receive_integrity_risk), "dma_harvest_failed");
                    storage_emit_event(STORAGE_WORKER_FATAL, &rt, -1, dma_received_bytes,
                                       producer_stats.receive_integrity_risk);
                    goto out;
                }
                saw_dma_data = true;
                last_dma_us = storage_wall_time_us();
                storage_stats_finish_harvest_batch(&producer_stats);
                continue;
            }
            slot = harvest_items[0].slot;
            actual = harvest_items[0].actual_bytes;
            stop_idle_polls = 0u;
            ring_full_logged = false;
            saw_dma_data = true;
            idle_notice_logged = false;
            last_dma_us = storage_wall_time_us();
            storage_stats_record_dma_desc(&producer_stats, last_dma_us);

            chunk_bytes = actual;
            if (chunk_bytes == 0u || chunk_bytes > rt.dma_desc_bytes) {
                fprintf(stderr, "Invalid DMA harvest bytes=%" PRIu64 " on channel %d\n", chunk_bytes, cfg->id);
                goto out;
            }
            {
                uint64_t chunk_index = producer_stats.dma_desc_completed_count - 1u;
                uint64_t file_offset = bytes_captured;
                uint32_t status = rt.dma_last_completed_status;
                if (storage_trace_chunk_enabled(chunk_index, slot)) {
                    printf("storage_dma_chunk_trace phase=harvest channel=%d chunk=%" PRIu64
                           " chunk_ordinal=%" PRIu64 " slot=%u file_offset=%" PRIu64
                           " status=0x%08x actual_bytes=%u desc_bytes=%u"
                           " rxsof=%u rxeof=%u hw_owned=%u\n",
                           cfg->id,
                           chunk_index,
                           chunk_index + 1u,
                           (unsigned)slot,
                           file_offset,
                           status,
                           (unsigned)actual,
                           (unsigned)rt.dma_desc_bytes,
                           (status & STORAGE_DESC_STS_RXSOF) ? 1u : 0u,
                           (status & STORAGE_DESC_STS_RXEOF) ? 1u : 0u,
                           (unsigned)__atomic_load_n(&rt.dma_hw_desc_count, __ATOMIC_ACQUIRE));
                    fflush(stdout);
                }
            }
            storage_print_slot_fingerprint(&rt, slot, bytes_captured, actual);
            dma_received_bytes += actual;
            if (bounded) {
                uint64_t remaining_write = requested_size - bytes_captured;
                if (chunk_bytes > remaining_write) {
                    chunk_bytes = remaining_write;
                }
            }

            {
                uint64_t chunk_index = producer_stats.dma_desc_completed_count - 1u;
                uint64_t file_offset = bytes_captured;
                uint64_t chunk_sectors;
                uint64_t chunk_hw_addr;
                if (storage_trace_chunk_enabled(chunk_index, slot) &&
                    chunk_bytes != (uint64_t)actual) {
                    printf("storage_dma_chunk_trace phase=bounded_trim channel=%d chunk=%" PRIu64
                           " chunk_ordinal=%" PRIu64 " slot=%u actual_bytes=%u queued_bytes=%" PRIu64
                           " file_offset=%" PRIu64 "\n",
                           cfg->id,
                           chunk_index,
                           chunk_index + 1u,
                           (unsigned)slot,
                           (unsigned)actual,
                        chunk_bytes,
                        file_offset);
                    fflush(stdout);
                }
                chunk_sectors = bytes_to_sectors(chunk_bytes);
                chunk_hw_addr = rt.cfg->ddr_hw_base +
                                (uint64_t)slot * (uint64_t)rt.dma_desc_bytes;
                bytes_captured += chunk_bytes;
                if (storage_queue_push(&write_queue,
                                       slot,
                                       chunk_bytes,
                                       chunk_index,
                                       file_offset,
                                       next_queue_lba,
                                       chunk_sectors,
                                       chunk_hw_addr) != 0) {
                    dbg_printf("[DBG][WRITE] queue push failed ch=%d slot=%u bytes=%" PRIu64 "\n",
                               cfg->id, (unsigned)slot, chunk_bytes);
                    goto out;
                }
                next_queue_lba += chunk_sectors;
                if (harvest_fatal) {
                    producer_stats.receive_integrity_ok = false;
                    snprintf(producer_stats.receive_integrity_risk,
                             sizeof(producer_stats.receive_integrity_risk), "dma_harvest_failed");
                    storage_emit_event(STORAGE_WORKER_FATAL, &rt, -1, dma_received_bytes,
                                       producer_stats.receive_integrity_risk);
                    goto out;
                }
            }
            {
                uint64_t now_us = storage_wall_time_us();
                DmaBdSnapshot bd_snapshot;
                (void)storage_queue_busy_count(&write_queue, &max_busy_slots);
                (void)storage_queue_buffered_bytes(&write_queue, &max_buffered_bytes);
                if (storage_capture_bd_snapshot(&producer_stats,
                                                &write_queue,
                                                &rt,
                                                args->task_no,
                                                effective_file_index,
                                                dma_received_bytes,
                                                &bd_snapshot) != 0) {
                    goto out;
                }
                storage_maybe_log_watermark(&producer_stats,
                                            &write_queue,
                                            &rt,
                                            args->task_no,
                                            effective_file_index,
                                            dma_received_bytes);
                storage_stats_print_periodic(&producer_stats,
                                             &write_queue,
                                             &rt,
                                             args->task_no,
                                             effective_file_index,
                                             dma_received_bytes,
                                             now_us);
            }
            if (producer_stats.dma_harvest_batch_current >= harvest_batch_max_cfg) {
                storage_stats_finish_harvest_batch(&producer_stats);
                sched_yield();
            }
        }
    }
    storage_stats_update_ring_full(&producer_stats,
                                   false,
                                   storage_wall_time_us(),
                                   dma_received_bytes);
    tail_incomplete = manual_stop_seen && dma_s2mm_tail_incomplete(&rt);
    storage_queue_finish(&write_queue);
    dma_stop_attempted = true;
    dma_stop_result = dma_stop_s2mm(&rt, &dma_stop_report);
    if (dma_stop_result == DMA_STOP_FAILED) {
        dma_stop_failed = true;
        dbg_printf("[DBG][WRITE] dma S2MM halt failed ch=%d task=%s idx=%u\n",
                   cfg->id, args->task_no, (unsigned)effective_file_index);
    }
    pthread_join(writer_thread, NULL);
    writer_started = false;
    if (write_queue.error) {
        dbg_printf("[DBG][WRITE] writer thread failed ch=%d captured=%" PRIu64 "\n",
                   cfg->id, bytes_captured);
        goto out;
    }
    storage_stats_finish_harvest_batch(&producer_stats);
    storage_queue_snapshot(&write_queue, &final_queue_snapshot);
    bytes_written = write_queue.bytes_written;
    chunks = write_queue.chunks;
    nvme_write_us = write_queue.nvme_write_us;
    (void)storage_queue_busy_count(&write_queue, &max_busy_slots);
    (void)storage_queue_buffered_bytes(&write_queue, &max_buffered_bytes);
    storage_queue_destroy(&write_queue);
    write_queue_ready = false;

    total_sectors = next_queue_lba - start_lba;
    elapsed_us = storage_elapsed_us(capture_start_us);
    if (total_sectors > UINT32_MAX) {
        fprintf(stderr,
                "Captured file exceeds legacy metadata sector limit: channel=%d sectors=%" PRIu64 "\n",
                cfg->id,
                total_sectors);
        goto out;
    }

    {
        FileEntry e;
        memset(&e, 0, sizeof(e));
        memcpy(e.task_no, args->task_no, strlen(args->task_no));
        e.file_cnt = 1u;
        e.file_type = (uint8_t)(args->has_proto_file_type ? args->proto_file_type : cfg->file_type);
        e.file_index = (uint16_t)effective_file_index;
        e.file_size_bytes = bytes_written > UINT32_MAX ? UINT32_MAX : (uint32_t)bytes_written;
        e.start_lba = start_lba;
        e.sector_count = (uint32_t)total_sectors;
        e.valid = 1u;
        table[metadata_slot] = e;
    }

    if (metadata_write(&rt, table) != 0) {
        dbg_printf("[DBG][WRITE] metadata_write failed ch=%d task=%s idx=%u\n",
                   cfg->id, args->task_no, (unsigned)effective_file_index);
        goto out;
    }
    data_persisted = true;

    printf("metadata_write_done backend=ramfs channel=%d name=%s slot=%d task=%s file_index=%u size=%" PRIu64
           " metadata_size_saturated=%u start_lba=0x%08" PRIx64
           " sectors=%" PRIu64 " file_type=%u chunks=%u continuous=%u\n",
           cfg->id,
           cfg->name,
           metadata_slot,
           args->task_no,
           (unsigned)effective_file_index,
           bytes_written,
           bytes_written > UINT32_MAX ? 1u : 0u,
           start_lba,
           total_sectors,
           (unsigned)(args->has_proto_file_type ? args->proto_file_type : cfg->file_type),
           chunks,
           bounded ? 0u : 1u);
    {
        uint64_t nvme_cmd_count = __atomic_load_n(&rt.nvme_cmd_count, __ATOMIC_ACQUIRE);
        uint64_t nvme_cmd_bytes_total = __atomic_load_n(&rt.nvme_cmd_bytes_total, __ATOMIC_ACQUIRE);
        uint64_t nvme_write_bytes_done = __atomic_load_n(&rt.nvme_write_bytes_done, __ATOMIC_ACQUIRE);
        uint64_t latency_total_us = __atomic_load_n(&rt.nvme_cmd_latency_total_us, __ATOMIC_ACQUIRE);
        uint64_t latency_sample_count = __atomic_load_n(&rt.nvme_latency_sample_count, __ATOMIC_ACQUIRE);
        uint64_t nvme_first_submit_us = __atomic_load_n(&rt.nvme_first_submit_us, __ATOMIC_ACQUIRE);
        uint64_t nvme_last_completion_us = __atomic_load_n(&rt.nvme_last_completion_us, __ATOMIC_ACQUIRE);
        uint64_t nvme_wall_us = nvme_last_completion_us >= nvme_first_submit_us
                                    ? nvme_last_completion_us - nvme_first_submit_us
                                    : 0u;
        uint64_t dma_observed_us = producer_stats.last_dma_desc_us >= producer_stats.first_dma_desc_us
                                       ? producer_stats.last_dma_desc_us - producer_stats.first_dma_desc_us
                                       : 0u;
        char expected_name[64];
        uint64_t expected_bytes = 0u;
        bool strict_end_to_end = storage_env_flag_enabled("SRC_REAL_STRICT_END_TO_END") != 0;
        bool expected_available;
        bool end_to_end_ok;
        uint64_t harvest_batch_avg = producer_stats.dma_harvest_batches > 0u
                                         ? producer_stats.dma_harvest_batch_total /
                                               producer_stats.dma_harvest_batches
                                         : 0u;
        snprintf(expected_name, sizeof(expected_name),
                 "SRC_REAL_EXPECTED_BYTES_CH%d", cfg->id);
        expected_available = storage_env_u64(expected_name, &expected_bytes) != 0;
        end_to_end_ok = expected_available
                            ? expected_bytes == dma_received_bytes &&
                                  expected_bytes == nvme_write_bytes_done
                            : !strict_end_to_end;
        bool storage_integrity_ok = !tail_incomplete && !dma_stop_failed &&
                                    dma_received_bytes == bytes_written;
        bool integrity_ok = producer_stats.receive_integrity_ok && end_to_end_ok &&
                            !producer_stats.integrity_risk_ring_full && storage_integrity_ok;
        final_integrity_ok = integrity_ok;
        const char *integrity_risk = "none";

        if (producer_stats.integrity_risk_ring_full) {
            integrity_risk = "dma_bd_exhausted_no_upstream_backpressure";
        } else if (!producer_stats.receive_integrity_ok) {
            integrity_risk = producer_stats.receive_integrity_risk;
        } else if (!end_to_end_ok) {
            integrity_risk = expected_available ? "expected_byte_mismatch"
                                                : "source_count_unavailable";
        } else if (tail_incomplete) {
            integrity_risk = "tail_descriptor_incomplete";
        } else if (dma_received_bytes != bytes_written) {
            integrity_risk = "dma_file_byte_mismatch";
        } else if (dma_stop_failed) {
            integrity_risk = "dma_stop_recovery_failed";
        }

        if (storage_env_flag_enabled("SRC_REAL_LEGACY_RESULT_LINE")) {
            printf("storage_transfer_done channel=%d task=%s file_index=%u effective_file_index=%u"
               " dma_received_bytes=%" PRIu64 " file_bytes=%" PRIu64
               " ssd_sector_bytes=%" PRIu64 " sectors=%" PRIu64
               " chunks=%u elapsed_ms=%" PRIu64 " nvme_write_ms=%" PRIu64
               " dma_mib_s=%.3f file_mib_s=%.3f nvme_active_mib_s=%.3f"
               " dma_desc_completed_count=%" PRIu64
               " first_dma_desc_us=%" PRIu64 " last_dma_desc_us=%" PRIu64
               " dma_observed_mib_s=%.3f"
               " dma_harvest_interval_min_us=%" PRIu64
               " dma_harvest_interval_max_us=%" PRIu64
               " dma_harvest_interval_avg_us=%" PRIu64
               " nvme_cmd_size_bytes=%u nvme_max_dts_bytes=%u nvme_cmd_count=%" PRIu64
               " nvme_cmd_bytes_total=%" PRIu64
               " nvme_write_bytes_done=%" PRIu64
               " nvme_cmd_latency_min_us=%" PRIu64
               " nvme_cmd_latency_max_us=%" PRIu64
               " nvme_cmd_latency_avg_us=%" PRIu64
               " nvme_latency_sample_count=%" PRIu64
               " nvme_wall_ms=%" PRIu64 " nvme_wall_mib_s=%.3f"
               " nvme_qd_requested=%u nvme_qd_effective=%u"
               " nvme_active_qd_max=%u nvme_active_qd_avg=%.3f"
               " ring_full_count=%" PRIu64 " ring_full_total_ms=%" PRIu64
               " ring_full_first_at_bytes=%" PRIu64
               " ring_full_last_at_bytes=%" PRIu64
               " max_ddr_busy_slots=%u max_ddr_buffered_bytes=%" PRIu64
               " final_ready_q_max=%u final_ready_q_avg=%u"
               " final_writer_idle_ms=%" PRIu64 " final_writer_active_ms=%" PRIu64
               " final_dma_no_free_slot_count=%" PRIu64
               " final_dma_harvest_batch_avg=%" PRIu64 " final_dma_harvest_batch_max=%u"
               " submit_stall_count=%" PRIu64 " submit_stall_max_us=%" PRIu64
               " writer_rt_enabled=%u writer_rt_prio=%u"
               " writer_empty_wait_us=%" PRIu64 " writer_drain_active_us=%" PRIu64
               " ready_q_nonempty_us=%" PRIu64
               " writer_drain_loop_count=%" PRIu64
               " writer_slots_per_drain_loop=%" PRIu64
               " auto_idle_done=%u"
               " tail_incomplete=%u integrity_ok=%u integrity_risk=%s"
               " dma_stop_result=%d dma_stop_recovered=%u"
               " dma_rxsof_count=%" PRIu64 " dma_rxeof_count=%" PRIu64
               " dma_rx_packet_open=%u continuous=%u\n",
               cfg->id,
               args->task_no,
               (unsigned)effective_file_index,
               (unsigned)effective_file_index,
               dma_received_bytes,
               bytes_written,
               total_sectors * (uint64_t)SECTOR_SIZE,
               total_sectors,
               (unsigned)chunks,
               elapsed_us / 1000u,
               nvme_write_us / 1000u,
               elapsed_us > 0u ? ((double)dma_received_bytes * 1000000.0 / (double)elapsed_us / 1048576.0) : 0.0,
               elapsed_us > 0u ? ((double)bytes_written * 1000000.0 / (double)elapsed_us / 1048576.0) : 0.0,
               nvme_write_us > 0u ? ((double)nvme_write_bytes_done * 1000000.0 / (double)nvme_write_us / 1048576.0) : 0.0,
               producer_stats.dma_desc_completed_count,
               producer_stats.first_dma_desc_us,
               producer_stats.last_dma_desc_us,
               dma_observed_us > 0u ? ((double)dma_received_bytes * 1000000.0 / (double)dma_observed_us / 1048576.0) : 0.0,
               producer_stats.dma_harvest_interval_min_us,
               producer_stats.dma_harvest_interval_max_us,
               producer_stats.dma_harvest_interval_count > 0u
                   ? producer_stats.dma_harvest_interval_total_us / producer_stats.dma_harvest_interval_count
                   : 0u,
               (unsigned)rt.nvme_cmd_size_bytes,
               (unsigned)rt.nvme_max_dts_bytes,
               nvme_cmd_count,
               nvme_cmd_bytes_total,
               nvme_write_bytes_done,
               __atomic_load_n(&rt.nvme_cmd_latency_min_us, __ATOMIC_ACQUIRE),
               __atomic_load_n(&rt.nvme_cmd_latency_max_us, __ATOMIC_ACQUIRE),
               latency_sample_count > 0u ? latency_total_us / latency_sample_count : 0u,
               latency_sample_count,
               nvme_wall_us / 1000u,
               nvme_wall_us > 0u ? ((double)nvme_write_bytes_done * 1000000.0 / (double)nvme_wall_us / 1048576.0) : 0.0,
               (unsigned)rt.nvme_qd_requested,
               (unsigned)rt.nvme_qd_effective,
               (unsigned)__atomic_load_n(&rt.nvme_active_qd_max, __ATOMIC_ACQUIRE),
               rt.nvme_active_qd_observed_us > 0u
                   ? (double)rt.nvme_active_qd_integral_us /
                         (double)rt.nvme_active_qd_observed_us
                   : 0.0,
               producer_stats.ring_full_count,
               producer_stats.ring_full_total_us / 1000u,
               producer_stats.ring_full_first_at_bytes,
               producer_stats.ring_full_last_at_bytes,
               (unsigned)max_busy_slots,
               max_buffered_bytes,
               (unsigned)final_queue_snapshot.ready_depth_max,
               (unsigned)final_queue_snapshot.ready_depth_avg,
               final_queue_snapshot.writer_idle_us / 1000u,
               final_queue_snapshot.writer_active_us / 1000u,
               producer_stats.dma_no_free_slot_count,
               harvest_batch_avg,
               (unsigned)producer_stats.dma_harvest_batch_max,
               rt.nvme_submit_stall_count,
               rt.nvme_submit_stall_max_us,
               final_queue_snapshot.writer_rt_enabled ? 1u : 0u,
               (unsigned)final_queue_snapshot.writer_rt_prio,
               final_queue_snapshot.writer_idle_us,
               final_queue_snapshot.writer_active_us,
               final_queue_snapshot.ready_q_nonempty_us,
               final_queue_snapshot.writer_drain_loop_count,
               final_queue_snapshot.writer_drain_loop_count > 0u
                   ? final_queue_snapshot.writer_slots_drained /
                         final_queue_snapshot.writer_drain_loop_count
                   : 0u,
               auto_idle_done ? 1u : 0u,
               tail_incomplete ? 1u : 0u,
               integrity_ok ? 1u : 0u,
               integrity_risk,
               (int)dma_stop_result,
               dma_stop_result == DMA_STOP_RESET_RECOVERED ? 1u : 0u,
               rt.dma_rxsof_count,
               rt.dma_rxeof_count,
               rt.dma_rx_packet_open ? 1u : 0u,
                   bounded ? 0u : 1u);
        }

        storage_emit_line("storage_result channel=%d task=%s file_index=%u status=%s"
                          " file_bytes=%" PRIu64 " data_persisted=%u"
                          " receive_integrity_ok=%u storage_integrity_ok=%u integrity_ok=%u"
                          " integrity_risk=%s ring_full_count=%" PRIu64,
                          cfg->id,
                          args->task_no,
                          (unsigned)effective_file_index,
                          integrity_ok ? "success" : "failed",
                          bytes_written,
                          data_persisted ? 1u : 0u,
                          producer_stats.receive_integrity_ok ? 1u : 0u,
                          storage_integrity_ok ? 1u : 0u,
                          integrity_ok ? 1u : 0u,
                          integrity_risk,
                          producer_stats.ring_full_count);
        storage_emit_line("storage_result_perf channel=%d task=%s file_index=%u elapsed_ms=%" PRIu64
                          " nvme_active_ms=%" PRIu64 " nvme_active_mib_s=%.3f"
                          " nvme_wall_ms=%" PRIu64 " nvme_wall_mib_s=%.3f"
                          " dma_observed_mib_s=%.3f nvme_qd_effective=%u"
                          " nvme_active_qd_avg=%.3f nvme_active_qd_max=%u",
                          cfg->id, args->task_no, (unsigned)effective_file_index,
                          elapsed_us / 1000u,
                          nvme_write_us / 1000u,
                          nvme_write_us > 0u ? ((double)nvme_write_bytes_done * 1000000.0 /
                                                   (double)nvme_write_us / 1048576.0) : 0.0,
                          nvme_wall_us / 1000u,
                          nvme_wall_us > 0u ? ((double)nvme_write_bytes_done * 1000000.0 /
                                                 (double)nvme_wall_us / 1048576.0) : 0.0,
                          dma_observed_us > 0u ? ((double)dma_received_bytes * 1000000.0 /
                                                   (double)dma_observed_us / 1048576.0) : 0.0,
                          (unsigned)rt.nvme_qd_effective,
                          rt.nvme_active_qd_observed_us > 0u
                              ? (double)rt.nvme_active_qd_integral_us /
                                    (double)rt.nvme_active_qd_observed_us : 0.0,
                          (unsigned)__atomic_load_n(&rt.nvme_active_qd_max, __ATOMIC_ACQUIRE));
        storage_emit_line("storage_result_diag channel=%d task=%s file_index=%u"
                          " ready_q_max=%u ready_q_avg=%u"
                          " max_ddr_busy_slots=%u max_ddr_buffered_bytes=%" PRIu64
                          " submit_stall_count=%" PRIu64 " submit_stall_max_us=%" PRIu64
                          " dma_harvest_interval_min_us=%" PRIu64
                          " dma_harvest_interval_avg_us=%" PRIu64
                          " dma_harvest_interval_max_us=%" PRIu64,
                          cfg->id, args->task_no, (unsigned)effective_file_index,
                          (unsigned)final_queue_snapshot.ready_depth_max,
                          (unsigned)final_queue_snapshot.ready_depth_avg,
                          (unsigned)max_busy_slots,
                          max_buffered_bytes,
                          rt.nvme_submit_stall_count,
                          rt.nvme_submit_stall_max_us,
                          producer_stats.dma_harvest_interval_min_us,
                          producer_stats.dma_harvest_interval_count > 0u
                              ? producer_stats.dma_harvest_interval_total_us /
                                    producer_stats.dma_harvest_interval_count : 0u,
                          producer_stats.dma_harvest_interval_max_us);
        storage_emit_line("storage_result_receive channel=%d task=%s file_index=%u"
                          " dma_received_bytes=%" PRIu64
                          " dma_bd_exhaustion_count=%" PRIu64
                          " dma_error_count=%" PRIu64 " descriptor_error_count=%" PRIu64
                          " max_completed_unharvested=%u min_dma_writable=%u"
                          " max_occupied_bytes_est=%" PRIu64
                          " receive_integrity_ok=%u receive_integrity_risk=%s"
                          " source_counter_available=0 expected_bytes_available=%u"
                          " expected_bytes=%" PRIu64
                          " first_failure_us=%" PRIu64 " first_failure_bytes=%" PRIu64,
                          cfg->id, args->task_no, (unsigned)effective_file_index,
                          dma_received_bytes, producer_stats.ring_full_count,
                          producer_stats.dma_error_count,
                          producer_stats.descriptor_error_count,
                          producer_stats.max_completed_unharvested,
                          producer_stats.min_dma_writable == UINT32_MAX
                              ? rt.dma_desc_count : producer_stats.min_dma_writable,
                          producer_stats.max_occupied_bytes_est,
                          producer_stats.receive_integrity_ok ? 1u : 0u,
                          producer_stats.receive_integrity_risk,
                          expected_available ? 1u : 0u,
                          expected_bytes,
                          producer_stats.first_receive_failure_us,
                          producer_stats.first_receive_failure_bytes);

        if (result) {
            result->receive_integrity_ok = producer_stats.receive_integrity_ok &&
                                           !producer_stats.integrity_risk_ring_full;
            result->storage_integrity_ok = storage_integrity_ok;
            result->integrity_ok = integrity_ok;
            result->dma_stop_recovered = dma_stop_result == DMA_STOP_RESET_RECOVERED;
            result->dma_received_bytes = dma_received_bytes;
            result->nvme_completed_bytes = nvme_write_bytes_done;
            result->file_bytes = bytes_written;
            result->expected_bytes = expected_bytes;
            result->expected_available = expected_available;
            result->dma_bd_exhaustion_count = producer_stats.dma_no_free_slot_count;
            result->dma_error_count = producer_stats.dma_error_count;
            result->descriptor_error_count = producer_stats.descriptor_error_count;
            result->min_dma_writable = producer_stats.min_dma_writable == UINT32_MAX ?
                                       rt.dma_desc_count : producer_stats.min_dma_writable;
            result->max_completed_unharvested = producer_stats.max_completed_unharvested;
            result->max_occupied_bytes_est = producer_stats.max_occupied_bytes_est;
            result->submit_stall_max_us = rt.nvme_submit_stall_max_us;
            snprintf(result->integrity_risk,
                     sizeof(result->integrity_risk),
                     "%s",
                     integrity_risk);
        }
    }
    fflush(stdout);
    if (result) {
        result->channel_id = cfg->id;
        result->metadata_slot = (uint32_t)metadata_slot;
        result->start_lba = start_lba;
        result->sector_count = total_sectors;
        result->size_bytes = bytes_written;
        result->file_index = effective_file_index;
        result->data_persisted = data_persisted;
        if (!result->receive_integrity_ok) result->integrity_ok = false;
        strncpy(result->task_no, args->task_no, sizeof(result->task_no) - 1u);
    }
    rc = (dma_stop_failed || !final_integrity_ok || !producer_stats.receive_integrity_ok ||
          producer_stats.integrity_risk_ring_full) ? -1 : 0;
    dbg_printf("[DBG][WRITE] %s ch=%d task=%s idx=%u bytes=%" PRIu64 " lba=0x%08" PRIx64
               " sectors=%" PRIu64 " chunks=%u dma_stop_result=%d\n",
               dma_stop_failed ? "persisted with DMA stop failure" : "success",
               cfg->id,
               args->task_no,
               (unsigned)effective_file_index,
               bytes_written,
               start_lba,
               total_sectors,
               (unsigned)chunks,
               (int)dma_stop_result);

out:
    storage_stats_update_ring_full(&producer_stats,
                                   false,
                                   storage_wall_time_us(),
                                   dma_received_bytes);
    if (manual_stop_seen && !tail_incomplete) {
        tail_incomplete = dma_s2mm_tail_incomplete(&rt);
    }
    if (writer_started) {
        storage_queue_finish(&write_queue);
    }
    if (rc != 0 && dma_started && !dma_stop_attempted) {
        dma_stop_attempted = true;
        dma_stop_result = dma_stop_s2mm(&rt, &dma_stop_report);
        if (dma_stop_result == DMA_STOP_FAILED) {
            dma_stop_failed = true;
            dbg_printf("[DBG][WRITE] dma S2MM cleanup halt failed ch=%d task=%s idx=%u\n",
                       cfg->id, args->task_no, (unsigned)effective_file_index);
        }
    }
    if (writer_started) {
        pthread_join(writer_thread, NULL);
        writer_started = false;
        bytes_written = write_queue.bytes_written;
        chunks = write_queue.chunks;
        nvme_write_us = write_queue.nvme_write_us;
    }
    if (write_queue_ready) {
        storage_queue_destroy(&write_queue);
        write_queue_ready = false;
    }
    if (rc != 0) {
        if (result) {
            result->channel_id = cfg ? cfg->id : args->channel_id;
            result->metadata_slot = metadata_slot >= 0 ? (uint32_t)metadata_slot : 0u;
            result->start_lba = start_lba;
            result->sector_count = total_sectors;
            result->size_bytes = bytes_written;
            result->file_index = effective_file_index;
            result->data_persisted = data_persisted;
            result->receive_integrity_ok = false;
            result->storage_integrity_ok = false;
            result->integrity_ok = false;
            result->dma_received_bytes = dma_received_bytes;
            result->nvme_completed_bytes = __atomic_load_n(&rt.nvme_write_bytes_done, __ATOMIC_ACQUIRE);
            result->file_bytes = bytes_written;
            result->dma_bd_exhaustion_count = producer_stats.dma_no_free_slot_count;
            result->dma_error_count = producer_stats.dma_error_count;
            result->descriptor_error_count = producer_stats.descriptor_error_count;
            result->min_dma_writable = producer_stats.min_dma_writable == UINT32_MAX ?
                                       rt.dma_desc_count : producer_stats.min_dma_writable;
            result->max_completed_unharvested = producer_stats.max_completed_unharvested;
            result->max_occupied_bytes_est = producer_stats.max_occupied_bytes_est;
            result->submit_stall_max_us = rt.nvme_submit_stall_max_us;
            snprintf(result->integrity_risk, sizeof(result->integrity_risk), "%s",
                     producer_stats.receive_integrity_risk[0] ?
                     producer_stats.receive_integrity_risk : "storage_error");
            strncpy(result->task_no, args->task_no, sizeof(result->task_no) - 1u);
        }
        if (capture_start_us != 0u) {
            elapsed_us = storage_elapsed_us(capture_start_us);
        }
        if (chunks > 0u && next_queue_lba >= start_lba) {
            total_sectors = next_queue_lba - start_lba;
        }
        printf("storage_transfer_failed channel=%d task=%s file_index=%u dma_received_bytes=%" PRIu64
               " file_bytes=%" PRIu64 " ssd_sector_bytes=%" PRIu64 " sectors=%" PRIu64
               " chunks=%u elapsed_ms=%" PRIu64 " nvme_write_ms=%" PRIu64
               " nvme_cmd_size_bytes=%u nvme_max_dts_bytes=%u nvme_cmd_count=%" PRIu64
               " ring_full_count=%" PRIu64 " ring_full_total_ms=%" PRIu64
               " tail_incomplete=%u integrity_ok=0 integrity_risk=%s writer_error=%s"
               " data_persisted=%u dma_stop_result=%d dma_stop_recovered=%u"
               " dma_stop_s2mm_cr=0x%08x dma_stop_s2mm_sr=0x%08x"
               " dma_rxsof_count=%" PRIu64 " dma_rxeof_count=%" PRIu64
               " dma_rx_packet_open=%u\n",
               cfg ? cfg->id : args->channel_id,
               args->task_no,
               (unsigned)effective_file_index,
               dma_received_bytes,
               bytes_written,
               total_sectors * (uint64_t)SECTOR_SIZE,
               total_sectors,
               (unsigned)chunks,
               elapsed_us / 1000u,
               nvme_write_us / 1000u,
               (unsigned)rt.nvme_cmd_size_bytes,
               (unsigned)rt.nvme_max_dts_bytes,
               __atomic_load_n(&rt.nvme_cmd_count, __ATOMIC_ACQUIRE),
               producer_stats.ring_full_count,
               producer_stats.ring_full_total_us / 1000u,
               tail_incomplete ? 1u : 0u,
               producer_stats.integrity_risk_ring_full
                   ? "dma_bd_exhausted_no_upstream_backpressure"
                   : (tail_incomplete
                          ? "tail_descriptor_incomplete"
                          : (dma_stop_failed ? "dma_stop_recovery_failed" : "storage_error")),
               rt.nvme_last_error[0] != '\0' ? rt.nvme_last_error : "none",
               data_persisted ? 1u : 0u,
               (int)dma_stop_result,
               dma_stop_result == DMA_STOP_RESET_RECOVERED ? 1u : 0u,
               dma_stop_report.s2mm_cr_after,
               dma_stop_report.s2mm_sr_after,
               rt.dma_rxsof_count,
               rt.dma_rxeof_count,
               rt.dma_rx_packet_open ? 1u : 0u);
        storage_emit_line("storage_result channel=%d task=%s file_index=%u status=failed"
                          " file_bytes=%" PRIu64 " data_persisted=%u"
                          " receive_integrity_ok=0 storage_integrity_ok=0 integrity_ok=0"
                          " integrity_risk=%s ring_full_count=%" PRIu64,
                          cfg ? cfg->id : args->channel_id,
                          args->task_no,
                          (unsigned)effective_file_index,
                          bytes_written,
                          data_persisted ? 1u : 0u,
                          producer_stats.integrity_risk_ring_full
                              ? "dma_bd_exhausted_no_upstream_backpressure"
                              : (tail_incomplete ? "tail_descriptor_incomplete"
                                                 : (dma_stop_failed ? "dma_stop_recovery_failed"
                                                                    : "storage_error")),
                          producer_stats.ring_full_count);
        storage_emit_line("storage_result_perf channel=%d task=%s file_index=%u elapsed_ms=%" PRIu64
                          " nvme_active_ms=%" PRIu64 " nvme_active_mib_s=%.3f"
                          " nvme_wall_ms=0 nvme_wall_mib_s=0.000 dma_observed_mib_s=0.000"
                          " nvme_qd_effective=%u nvme_active_qd_avg=0.000 nvme_active_qd_max=%u",
                          cfg ? cfg->id : args->channel_id,
                          args->task_no, (unsigned)effective_file_index,
                          elapsed_us / 1000u,
                          nvme_write_us / 1000u,
                          nvme_write_us > 0u ? ((double)bytes_written * 1000000.0 /
                                                   (double)nvme_write_us / 1048576.0) : 0.0,
                          (unsigned)rt.nvme_qd_effective,
                          (unsigned)__atomic_load_n(&rt.nvme_active_qd_max, __ATOMIC_ACQUIRE));
        storage_emit_line("storage_result_diag channel=%d task=%s file_index=%u"
                          " ready_q_max=0 ready_q_avg=0"
                          " max_ddr_busy_slots=%u max_ddr_buffered_bytes=%" PRIu64
                          " submit_stall_count=%" PRIu64 " submit_stall_max_us=%" PRIu64
                          " dma_harvest_interval_min_us=%" PRIu64
                          " dma_harvest_interval_avg_us=%" PRIu64
                          " dma_harvest_interval_max_us=%" PRIu64,
                          cfg ? cfg->id : args->channel_id,
                          args->task_no, (unsigned)effective_file_index,
                          (unsigned)max_busy_slots,
                          max_buffered_bytes,
                          rt.nvme_submit_stall_count,
                          rt.nvme_submit_stall_max_us,
                          producer_stats.dma_harvest_interval_min_us,
                          producer_stats.dma_harvest_interval_count > 0u
                              ? producer_stats.dma_harvest_interval_total_us /
                                    producer_stats.dma_harvest_interval_count : 0u,
                          producer_stats.dma_harvest_interval_max_us);
        storage_emit_line("storage_result_receive channel=%d task=%s file_index=%u"
                          " dma_received_bytes=%" PRIu64
                          " dma_bd_exhaustion_count=%" PRIu64
                          " dma_error_count=%" PRIu64 " descriptor_error_count=%" PRIu64
                          " max_completed_unharvested=%u min_dma_writable=%u"
                          " max_occupied_bytes_est=%" PRIu64
                          " receive_integrity_ok=0 receive_integrity_risk=%s"
                          " first_failure_us=%" PRIu64 " first_failure_bytes=%" PRIu64,
                          cfg ? cfg->id : args->channel_id,
                          args->task_no, (unsigned)effective_file_index,
                          dma_received_bytes, producer_stats.ring_full_count,
                          producer_stats.dma_error_count,
                          producer_stats.descriptor_error_count,
                          producer_stats.max_completed_unharvested,
                          producer_stats.min_dma_writable == UINT32_MAX
                              ? rt.dma_desc_count : producer_stats.min_dma_writable,
                          producer_stats.max_occupied_bytes_est,
                          producer_stats.receive_integrity_risk,
                          producer_stats.first_receive_failure_us,
                          producer_stats.first_receive_failure_bytes);
        fflush(stdout);
        dbg_printf("[DBG][WRITE] exit fail ch=%d task=%s idx=%u dma_received=%" PRIu64
                   " file_bytes=%" PRIu64 " sectors=%" PRIu64 "\n",
                   cfg ? cfg->id : args->channel_id,
                   args->task_no,
                   (unsigned)effective_file_index,
                   dma_received_bytes,
                   bytes_written,
                   total_sectors);
    }
    channel_runtime_close(&rt);
    return rc;
}

int execute_write(const ParsedArgs *args, GlobalOptions gopt) {
    return execute_write_with_result(args, gopt, NULL);
}

static void fill_dual16_increment_pattern(volatile uint8_t *base, uint64_t bytes)
{
    uint64_t words = bytes / sizeof(uint32_t);
    volatile uint32_t *p32 = (volatile uint32_t *)(void *)base;
    uint64_t i;

    for (i = 0u; i < words; ++i) {
        uint32_t v = (uint32_t)(i & 0xffffu);
        p32[i] = (v << 16u) | v;
    }
    __sync_synchronize();
}

static int verify_dual16_increment_pattern(const volatile uint8_t *base,
                                           uint64_t bytes,
                                           const char *stage)
{
    const volatile uint32_t *p32 = (const volatile uint32_t *)(const void *)base;
    uint64_t words = bytes / sizeof(uint32_t);
    uint64_t i;

    __sync_synchronize();
    for (i = 0u; i < words; ++i) {
        uint32_t v = (uint32_t)(i & 0xffffu);
        uint32_t expected = (v << 16u) | v;
        uint32_t actual = p32[i];

        if (actual != expected) {
            printf("ddr_pattern_verify stage=%s result=failed word=%" PRIu64
                   " byte_offset=%" PRIu64 " expected=0x%08x actual=0x%08x\n",
                   stage,
                   i,
                   i * sizeof(uint32_t),
                   expected,
                   actual);
            fflush(stdout);
            return -1;
        }
    }
    printf("ddr_pattern_verify stage=%s result=ok words=%" PRIu64 " bytes=%" PRIu64 "\n",
           stage,
           words,
           bytes);
    fflush(stdout);
    return 0;
}

static uint32_t wrap_test_word(uint32_t seed, uint64_t word_index)
{
    uint32_t lo = (uint32_t)((word_index + seed) & 0xffffu);
    uint32_t hi = (uint32_t)(((word_index >> 3u) ^ (uint64_t)(seed >> 16u)) & 0xffffu);
    return (hi << 16u) | lo;
}

static void fill_wrap_test_pattern_at(volatile uint8_t *base,
                                      uint64_t bytes,
                                      uint32_t seed,
                                      uint64_t base_word)
{
    uint64_t words = bytes / sizeof(uint32_t);
    volatile uint32_t *p32 = (volatile uint32_t *)(void *)base;
    uint64_t i;

    for (i = 0u; i < words; ++i) {
        p32[i] = wrap_test_word(seed, base_word + i);
    }
    __sync_synchronize();
}

static void fill_wrap_test_pattern(volatile uint8_t *base, uint64_t bytes, uint32_t seed)
{
    fill_wrap_test_pattern_at(base, bytes, seed, 0u);
}

static int verify_wrap_test_pattern_at(const volatile uint8_t *base,
                                       uint64_t bytes,
                                       uint32_t seed,
                                       uint64_t base_word,
                                       uint64_t base_byte_offset,
                                       const char *stage)
{
    const volatile uint32_t *p32 = (const volatile uint32_t *)(const void *)base;
    uint64_t words = bytes / sizeof(uint32_t);
    uint64_t i;

    __sync_synchronize();
    for (i = 0u; i < words; ++i) {
        uint32_t expected = wrap_test_word(seed, base_word + i);
        uint32_t actual = p32[i];

        if (actual != expected) {
            printf("ssd_pattern_verify stage=%s result=failed word=%" PRIu64
                   " byte_offset=%" PRIu64 " expected=0x%08x actual=0x%08x seed=0x%08x\n",
                   stage,
                   base_word + i,
                   base_byte_offset + i * sizeof(uint32_t),
                   expected,
                   actual,
                   seed);
            fflush(stdout);
            return -1;
        }
    }
    printf("ssd_pattern_verify stage=%s result=ok words=%" PRIu64
           " bytes=%" PRIu64 " byte_offset=%" PRIu64 " seed=0x%08x\n",
           stage,
           words,
           bytes,
           base_byte_offset,
           seed);
    fflush(stdout);
    return 0;
}

static int verify_wrap_test_pattern(const volatile uint8_t *base,
                                    uint64_t bytes,
                                    uint32_t seed,
                                    const char *stage)
{
    return verify_wrap_test_pattern_at(base, bytes, seed, 0u, 0u, stage);
}

int execute_ddr_pattern_store_with_result(const ParsedArgs *args, GlobalOptions gopt, WriteResult *result)
{
    int channel_id = args && args->has_channel ? args->channel_id : LOW_SPEED_CHANNEL_ID;
    const ChannelConfig *cfg = find_channel(channel_id);
    ChannelRuntime rt;
    FileEntry table[MAX_FILES_TOTAL];
    int rc = -1;
    int metadata_slot = -1;
    uint64_t auto_lba = 0u;
    uint32_t valid_count = 0u;
    uint64_t start_lba = 0u;
    uint64_t size_bytes = DDR_PATTERN_STORE_DEFAULT_BYTES;
    uint64_t sectors = 0u;
    uint64_t ddr_offset = 0u;
    uint64_t ddr_hw_addr = 0u;
    uint64_t start_us = 0u;
    uint64_t fill_us = 0u;
    uint64_t write_us = 0u;
    uint64_t perf_expected_cmds = 0u;
    uint32_t effective_file_index;
    DmaStopReport dma_stop_report;
    bool raw_store;
    bool raw_verify;

    if (result) {
        memset(result, 0, sizeof(*result));
    }
    memset(&dma_stop_report, 0, sizeof(dma_stop_report));
    if (!args || !cfg) {
        return -1;
    }
    raw_store = storage_env_flag_enabled("SRC_REAL_DDR_RAW_STORE") != 0;
    raw_verify = raw_store && storage_env_flag_enabled("SRC_REAL_DDR_RAW_VERIFY") != 0;
    effective_file_index = args->file_index;
    if (args->has_size) {
        size_bytes = args->size_bytes;
    }
    if (args->has_ddr_offset) {
        ddr_offset = args->ddr_offset;
    }
    ddr_hw_addr = cfg->ddr_hw_base + ddr_offset;
    sectors = bytes_to_sectors(size_bytes);
    dbg_printf("[DBG][PATTERN] start ch=%d mode=%s size=%" PRIu64
               " ddr_offset=0x%08" PRIx64 " task=%s idx=%u lba_auto=%u dry=%u\n",
               cfg->id,
               raw_store ? "raw" : "pattern",
               size_bytes,
               ddr_offset,
               args->task_no,
               (unsigned)args->file_index,
               (!args->has_lba || args->lba_auto) ? 1u : 0u,
               gopt.dry_run ? 1u : 0u);

    if (channel_runtime_open(&rt, cfg, gopt) != 0) {
        dbg_printf("[DBG][PATTERN] channel_runtime_open failed ch=%d\n", cfg->id);
        return -1;
    }
    if (nvme_probe(&rt) != 0) {
        dbg_printf("[DBG][PATTERN] nvme_probe failed ch=%d\n", cfg->id);
        goto out;
    }
    if (rt.nvme_max_lba == 0u) {
        fprintf(stderr, "NVMe capacity unavailable on channel %d\n", cfg->id);
        goto out;
    }
    if (dma_stop_s2mm(&rt, &dma_stop_report) == DMA_STOP_FAILED) {
        dbg_printf("[DBG][PATTERN] failed to quiesce DMA before CPU DDR fill ch=%d\n", cfg->id);
        goto out;
    }
    printf("ddr_pattern_dma_quiesce channel=%d result=full_reset"
           " s2mm_cr=0x%08x s2mm_sr=0x%08x\n",
           cfg->id,
           dma_stop_report.s2mm_cr_after,
           dma_stop_report.s2mm_sr_after);
    fflush(stdout);
    if (ddr_offset > rt.dma_ring_bytes || size_bytes > (rt.dma_ring_bytes - ddr_offset)) {
        fprintf(stderr,
                "Pattern DDR range exceeds channel %d ring: offset=0x%08" PRIx64
                " size=%" PRIu64 " ring=%" PRIu64 "\n",
                cfg->id,
                ddr_offset,
                size_bytes,
                rt.dma_ring_bytes);
        goto out;
    }
    if (raw_store) {
        printf("ddr_raw_store_config channel=%d bytes=%" PRIu64
               " ring_bytes=%" PRIu64
               " cpu_mapped_bytes=%" PRIu64
               " raw_no_cpu_access=1 ddr_hw_start=0x%08" PRIx64
               " ddr_hw_end=0x%08" PRIx64 "\n",
               cfg->id,
               size_bytes,
               rt.dma_ring_bytes,
               (uint64_t)rt.ddr.size,
               ddr_hw_addr,
               ddr_hw_addr + size_bytes);
        fflush(stdout);
    }
    if (!raw_store && ddr_offset != 0u) {
        fprintf(stderr, "--ddr-offset is only supported with SRC_REAL_DDR_RAW_STORE=1\n");
        goto out;
    }
    if (!raw_store && size_bytes > cfg->ddr_cpu_size) {
        fprintf(stderr,
                "Pattern size exceeds channel %d CPU DDR window: size=%" PRIu64 " window=%" PRIu64 "\n",
                cfg->id,
                size_bytes,
                cfg->ddr_cpu_size);
        goto out;
    }
    if (metadata_read(&rt, table) != 0) {
        dbg_printf("[DBG][PATTERN] metadata_read failed ch=%d\n", cfg->id);
        goto out;
    }
    if (metadata_resolve_file_index(table,
                                    args->task_no,
                                    args->file_index,
                                    &effective_file_index) != 0) {
        fprintf(stderr, "Unable to allocate next pattern file index: task=%s requested=%u\n",
                args->task_no, (unsigned)args->file_index);
        goto out;
    }
    if (effective_file_index != args->file_index) {
        printf("storage_file_index_advanced task=%s requested_file_index=%u effective_file_index=%u source=metadata\n",
               args->task_no,
               (unsigned)args->file_index,
               (unsigned)effective_file_index);
    }
    if (metadata_alloc_slot_and_lba(table, &metadata_slot, &auto_lba, &valid_count) != 0) {
        fprintf(stderr, "No free metadata slot on channel %d\n", cfg->id);
        goto out;
    }
    (void)valid_count;
    start_lba = (!args->has_lba || args->lba_auto) ? auto_lba : args->lba;
    if (metadata_check_lba_overlap(table, start_lba, sectors) != 0) {
        dbg_printf("[DBG][PATTERN] lba overlap ch=%d lba=0x%08" PRIx64 " sectors=%" PRIu64 "\n",
                   cfg->id,
                   start_lba,
                   sectors);
        goto out;
    }
    if ((start_lba + sectors) > rt.nvme_max_lba) {
        fprintf(stderr,
                "Requested SSD range exceeds max LBA: start=0x%08" PRIx64
                " sectors=%" PRIu64 " max=0x%08" PRIx64 "\n",
                start_lba,
                sectors,
                rt.nvme_max_lba);
        goto out;
    }

    if (!gopt.dry_run) {
        bool source_cpu_visible = (ddr_offset <= cfg->ddr_cpu_size &&
                                   size_bytes <= (cfg->ddr_cpu_size - ddr_offset) &&
                                   ddr_offset <= rt.ddr.size &&
                                   size_bytes <= ((uint64_t)rt.ddr.size - ddr_offset));
        uint64_t readback_offset = (ddr_offset + size_bytes + 4095ull) & ~4095ull;
        volatile uint8_t *source = source_cpu_visible ? rt.ddr.virt + ddr_offset : NULL;

        if (!raw_store && !source_cpu_visible) {
            fprintf(stderr,
                    "Pattern source is outside CPU DDR window: offset=0x%08" PRIx64
                    " size=%" PRIu64 " window=%" PRIu64 "\n",
                    ddr_offset,
                    size_bytes,
                    cfg->ddr_cpu_size);
            goto out;
        }
        if (raw_verify && !source_cpu_visible) {
            fprintf(stderr,
                    "Raw DDR verify requires source inside CPU DDR window:"
                    " offset=0x%08" PRIx64 " size=%" PRIu64
                    " window=%" PRIu64 "\n",
                    ddr_offset,
                    size_bytes,
                    cfg->ddr_cpu_size);
            goto out;
        }
        if (raw_verify && readback_offset + size_bytes > cfg->ddr_cpu_size) {
            fprintf(stderr,
                    "Raw DDR store requires a second DDR buffer for readback compare:"
                    " size=%" PRIu64 " aligned=%" PRIu64 " window=%" PRIu64 "\n",
                    size_bytes,
                    readback_offset,
                    cfg->ddr_cpu_size);
            goto out;
        }
        if (!raw_store) {
            start_us = storage_wall_time_us();
            fill_dual16_increment_pattern(source, size_bytes);
            fill_us = storage_elapsed_us(start_us);
            if (verify_dual16_increment_pattern(source, size_bytes, "cpu_fill") != 0) {
                dbg_printf("[DBG][PATTERN] CPU fill verify failed ch=%d\n", cfg->id);
                goto out;
            }
        }
        __sync_synchronize();
        if (raw_store) {
            nvme_reset_sw_timing(&rt);
            perf_expected_cmds = nvme_perf_calc_begin(&rt, size_bytes);
        }
        start_us = storage_wall_time_us();
        if (raw_store && rt.nvme_feed_mode == NVME_FEED_MODE_TIGHT) {
            if (nvme_write_contiguous_tight_qd(&rt,
                                               ddr_hw_addr,
                                               start_lba,
                                               size_bytes,
                                               rt.nvme_qd_effective) != 0) {
                dbg_printf("[DBG][PATTERN] nvme tight raw write failed ch=%d lba=0x%08" PRIx64
                           " bytes=%" PRIu64 " hw=0x%08" PRIx64 "\n",
                           cfg->id,
                           start_lba,
                           size_bytes,
                           ddr_hw_addr);
                goto out;
            }
        } else if (nvme_rw(&rt, true, start_lba, sectors, ddr_hw_addr) != 0) {
            dbg_printf("[DBG][PATTERN] nvme write failed ch=%d lba=0x%08" PRIx64
                       " sectors=%" PRIu64 " hw=0x%08" PRIx64 "\n",
                       cfg->id,
                       start_lba,
                       sectors,
                       ddr_hw_addr);
            goto out;
        }
        write_us = storage_elapsed_us(start_us);
        if (raw_store) {
            nvme_perf_calc_print(&rt, size_bytes, perf_expected_cmds, write_us);
            nvme_print_sw_timing(&rt);
        }
        if (raw_store && !raw_verify) {
            printf("ddr_raw_store_verify channel=%d result=skipped"
                   " reason=raw_write_only bytes=%" PRIu64
                   " ddr_offset=0x%08" PRIx64 "\n",
                   cfg->id,
                   size_bytes,
                   ddr_offset);
            fflush(stdout);
        } else if (readback_offset + size_bytes <= cfg->ddr_cpu_size) {
            volatile uint8_t *readback = rt.ddr.virt + readback_offset;

            memset((void *)readback, 0xa5, (size_t)size_bytes);
            __sync_synchronize();
            if (nvme_rw(&rt,
                        false,
                        start_lba,
                        sectors,
                        cfg->ddr_hw_base + readback_offset) != 0) {
                dbg_printf("[DBG][PATTERN] SSD readback failed ch=%d lba=0x%08" PRIx64
                           " sectors=%" PRIu64 " hw=0x%08" PRIx64 "\n",
                           cfg->id,
                           start_lba,
                           sectors,
                           cfg->ddr_hw_base + readback_offset);
                goto out;
            }
            if (raw_store) {
                if (memcmp((const void *)source, (const void *)readback, (size_t)size_bytes) != 0) {
                    uint64_t mismatch = 0u;

                    while (mismatch < size_bytes &&
                           source[mismatch] == readback[mismatch]) {
                        ++mismatch;
                    }
                    printf("ddr_raw_store_verify channel=%d result=mismatch"
                           " offset=%" PRIu64 " expected=0x%02x actual=0x%02x\n",
                           cfg->id,
                           mismatch,
                           (unsigned)source[mismatch],
                           (unsigned)readback[mismatch]);
                    fflush(stdout);
                    dbg_printf("[DBG][PATTERN] raw SSD readback verify failed ch=%d offset=%" PRIu64 "\n",
                               cfg->id,
                               mismatch);
                    goto out;
                }
                printf("ddr_raw_store_verify channel=%d result=ok bytes=%" PRIu64 "\n",
                       cfg->id,
                       size_bytes);
                fflush(stdout);
            } else {
                if (verify_dual16_increment_pattern(readback, size_bytes, "ssd_readback") != 0) {
                    dbg_printf("[DBG][PATTERN] SSD readback verify failed ch=%d\n", cfg->id);
                    goto out;
                }
            }
        } else {
            printf("ddr_pattern_verify stage=ssd_readback result=skipped reason=insufficient_second_buffer"
                   " bytes=%" PRIu64 " ddr_window=%" PRIu64 "\n",
                   size_bytes,
                   cfg->ddr_cpu_size);
            fflush(stdout);
        }
    }

    {
        FileEntry e;
        memset(&e, 0, sizeof(e));
        memcpy(e.task_no, args->task_no, strlen(args->task_no));
        e.file_cnt = 1u;
        e.file_type = (uint8_t)args->proto_file_type;
        e.file_index = (uint16_t)effective_file_index;
        e.file_size_bytes = size_bytes > UINT32_MAX ? UINT32_MAX : (uint32_t)size_bytes;
        e.start_lba = start_lba;
        e.sector_count = (uint32_t)sectors;
        e.valid = 1u;
        table[metadata_slot] = e;
    }
    if (metadata_write(&rt, table) != 0) {
        dbg_printf("[DBG][PATTERN] metadata_write failed ch=%d task=%s idx=%u\n",
                   cfg->id,
                   args->task_no,
                   (unsigned)effective_file_index);
        goto out;
    }

    printf("ddr_pattern_store_done channel=%d mode=%s task=%s file_index=%u bytes=%" PRIu64
           " metadata_size_saturated=%u"
           " ddr_cpu=0x%08" PRIx64 " ddr_offset=0x%08" PRIx64
           " ddr_dma=0x%08" PRIx64
           " start_lba=0x%08" PRIx64 " sectors=%" PRIu64
           " proto_file_type=%u metadata_slot=%d fill_ms=%" PRIu64
           " nvme_write_ms=%" PRIu64
           " raw_large_ddr_store=%u\n",
           cfg->id,
           raw_store ? "raw" : "pattern",
           args->task_no,
           (unsigned)effective_file_index,
           size_bytes,
           size_bytes > UINT32_MAX ? 1u : 0u,
           cfg->ddr_cpu_base,
           ddr_offset,
           ddr_hw_addr,
           start_lba,
           sectors,
           (unsigned)args->proto_file_type,
           metadata_slot,
           fill_us / 1000u,
           write_us / 1000u,
           (unsigned)(raw_store && size_bytes > cfg->ddr_cpu_size));
    fflush(stdout);
    if (result) {
        result->channel_id = cfg->id;
        result->metadata_slot = (uint32_t)metadata_slot;
        result->start_lba = start_lba;
        result->sector_count = sectors;
        result->size_bytes = size_bytes;
        result->file_index = effective_file_index;
        result->data_persisted = true;
        result->integrity_ok = true;
        result->dma_stop_recovered = false;
        snprintf(result->integrity_risk, sizeof(result->integrity_risk), "none");
        strncpy(result->task_no, args->task_no, sizeof(result->task_no) - 1u);
    }
    rc = 0;

out:
    channel_runtime_close(&rt);
    return rc;
}

int execute_ssd_lba_wrap_test(const ParsedArgs *args, GlobalOptions gopt)
{
    const ChannelConfig *cfg = find_channel(LOW_SPEED_CHANNEL_ID);
    ChannelRuntime rt;
    DmaStopReport dma_stop_report;
    int rc = -1;
    uint64_t size_bytes = args && args->has_size ? args->size_bytes : (4ull * 1024ull * 1024ull);
    uint64_t aligned_bytes = (size_bytes + 4095ull) & ~4095ull;
    uint64_t sectors = bytes_to_sectors(size_bytes);
    uint64_t lba_a;
    uint64_t lba_b;
    uint64_t off_a = 0u;
    uint64_t off_b = aligned_bytes;
    uint64_t off_read_a = aligned_bytes * 2u;
    uint64_t off_read_b = aligned_bytes * 3u;
    const uint32_t seed_a = 0x13572468u;
    const uint32_t seed_b = 0x24681357u;

    memset(&rt, 0, sizeof(rt));
    memset(&dma_stop_report, 0, sizeof(dma_stop_report));
    if (!args || !cfg || !args->has_lba || args->lba_auto || sectors == 0u) {
        return -1;
    }
    lba_a = args->lba;
    lba_b = lba_a + 0x100000ull;
    if (off_read_b + aligned_bytes > cfg->ddr_cpu_size) {
        fprintf(stderr,
                "ssd-lba-wrap-test buffers exceed ch2 CPU DDR window: size=%" PRIu64
                " aligned=%" PRIu64 " window=%" PRIu64 "\n",
                size_bytes,
                aligned_bytes,
                cfg->ddr_cpu_size);
        return -1;
    }

    dbg_printf("[DBG][WRAP] start ch=2 lba_a=0x%08" PRIx64
               " lba_b=0x%08" PRIx64 " delta_sectors=0x100000"
               " bytes=%" PRIu64 " dry=%u\n",
               lba_a,
               lba_b,
               size_bytes,
               gopt.dry_run ? 1u : 0u);

    if (channel_runtime_open(&rt, cfg, gopt) != 0) {
        return -1;
    }
    if (nvme_probe(&rt) != 0) {
        goto out;
    }
    if (rt.nvme_max_lba == 0u) {
        fprintf(stderr, "NVMe capacity unavailable on channel 2\n");
        goto out;
    }
    if (lba_b + sectors > rt.nvme_max_lba) {
        fprintf(stderr,
                "ssd-lba-wrap-test range exceeds max LBA: lba_b=0x%08" PRIx64
                " sectors=%" PRIu64 " max=0x%08" PRIx64 "\n",
                lba_b,
                sectors,
                rt.nvme_max_lba);
        goto out;
    }
    if (!gopt.dry_run && dma_stop_s2mm(&rt, &dma_stop_report) == DMA_STOP_FAILED) {
        dbg_printf("[DBG][WRAP] failed to quiesce DMA before SSD LBA wrap test ch=2\n");
        goto out;
    }

    if (!gopt.dry_run) {
        fill_wrap_test_pattern(rt.ddr.virt + off_a, size_bytes, seed_a);
        fill_wrap_test_pattern(rt.ddr.virt + off_b, size_bytes, seed_b);
        if (verify_wrap_test_pattern(rt.ddr.virt + off_a, size_bytes, seed_a, "cpu_fill_a") != 0 ||
            verify_wrap_test_pattern(rt.ddr.virt + off_b, size_bytes, seed_b, "cpu_fill_b") != 0) {
            goto out;
        }

        if (nvme_rw(&rt, true, lba_a, sectors, cfg->ddr_hw_base + off_a) != 0) {
            dbg_printf("[DBG][WRAP] write A failed lba=0x%08" PRIx64 " sectors=%" PRIu64 "\n",
                       lba_a,
                       sectors);
            goto out;
        }
        if (nvme_rw(&rt, true, lba_b, sectors, cfg->ddr_hw_base + off_b) != 0) {
            dbg_printf("[DBG][WRAP] write B failed lba=0x%08" PRIx64 " sectors=%" PRIu64 "\n",
                       lba_b,
                       sectors);
            goto out;
        }

        memset((void *)(rt.ddr.virt + off_read_a), 0xa5, (size_t)size_bytes);
        memset((void *)(rt.ddr.virt + off_read_b), 0x5a, (size_t)size_bytes);
        __sync_synchronize();
        if (nvme_rw(&rt, false, lba_a, sectors, cfg->ddr_hw_base + off_read_a) != 0) {
            dbg_printf("[DBG][WRAP] read A failed lba=0x%08" PRIx64 " sectors=%" PRIu64 "\n",
                       lba_a,
                       sectors);
            goto out;
        }
        if (nvme_rw(&rt, false, lba_b, sectors, cfg->ddr_hw_base + off_read_b) != 0) {
            dbg_printf("[DBG][WRAP] read B failed lba=0x%08" PRIx64 " sectors=%" PRIu64 "\n",
                       lba_b,
                       sectors);
            goto out;
        }

        if (verify_wrap_test_pattern(rt.ddr.virt + off_read_a, size_bytes, seed_a, "readback_a") != 0 ||
            verify_wrap_test_pattern(rt.ddr.virt + off_read_b, size_bytes, seed_b, "readback_b") != 0) {
            printf("ssd_lba_wrap_test result=failed lba_a=0x%08" PRIx64
                   " lba_b=0x%08" PRIx64 " delta_sectors=0x100000"
                   " bytes=%" PRIu64 "\n",
                   lba_a,
                   lba_b,
                   size_bytes);
            fflush(stdout);
            goto out;
        }
    }

    printf("ssd_lba_wrap_test result=ok channel=2 lba_a=0x%08" PRIx64
           " lba_b=0x%08" PRIx64 " delta_sectors=0x100000"
           " bytes=%" PRIu64 " sectors=%" PRIu64
           " ddr_cpu=0x%08" PRIx64 "\n",
           lba_a,
           lba_b,
           size_bytes,
           sectors,
           cfg->ddr_cpu_base);
    fflush(stdout);
    rc = 0;

out:
    channel_runtime_close(&rt);
    return rc;
}

int execute_ssd_continuous_pattern_test(const ParsedArgs *args, GlobalOptions gopt)
{
    const ChannelConfig *cfg = find_channel(LOW_SPEED_CHANNEL_ID);
    ChannelRuntime rt;
    DmaStopReport dma_stop_report;
    int rc = -1;
    uint64_t size_bytes = args && args->has_size ? args->size_bytes : SSD_CONTINUOUS_PATTERN_TEST_DEFAULT_BYTES;
    uint64_t total_sectors = bytes_to_sectors(size_bytes);
    uint64_t start_lba;
    uint64_t chunk_bytes;
    uint64_t read_offset;
    uint64_t remaining;
    uint64_t file_offset;
    uint64_t cur_lba;
    uint64_t chunk_index;
    uint64_t write_start_us = 0u;
    uint64_t read_start_us = 0u;
    uint64_t write_us = 0u;
    uint64_t read_us = 0u;
    const uint32_t seed = 0x5a17c0deu;

    memset(&rt, 0, sizeof(rt));
    memset(&dma_stop_report, 0, sizeof(dma_stop_report));
    if (!args || !cfg || !args->has_lba || args->lba_auto || total_sectors == 0u) {
        return -1;
    }
    start_lba = args->lba;
    chunk_bytes = cfg->dma_desc_bytes_default;
    if (chunk_bytes == 0u) {
        chunk_bytes = 16ull * 1024ull * 1024ull;
    }
    if (chunk_bytes > (cfg->ddr_cpu_size / 2u)) {
        chunk_bytes = cfg->ddr_cpu_size / 2u;
    }
    chunk_bytes &= ~(uint64_t)(SECTOR_SIZE - 1u);
    if (chunk_bytes == 0u) {
        fprintf(stderr,
                "ssd-continuous-pattern-test has no usable DDR buffers: ddr_window=%" PRIu64 "\n",
                cfg->ddr_cpu_size);
        return -1;
    }
    read_offset = chunk_bytes;

    dbg_printf("[DBG][CONT] start ch=2 lba=0x%08" PRIx64
               " bytes=%" PRIu64 " sectors=%" PRIu64
               " chunk_bytes=%" PRIu64 " dry=%u\n",
               start_lba,
               size_bytes,
               total_sectors,
               chunk_bytes,
               gopt.dry_run ? 1u : 0u);

    if (channel_runtime_open(&rt, cfg, gopt) != 0) {
        return -1;
    }
    if (nvme_probe(&rt) != 0) {
        goto out;
    }
    if (rt.nvme_max_lba == 0u) {
        fprintf(stderr, "NVMe capacity unavailable on channel 2\n");
        goto out;
    }
    if (start_lba + total_sectors < start_lba || start_lba + total_sectors > rt.nvme_max_lba) {
        fprintf(stderr,
                "ssd-continuous-pattern-test range exceeds max LBA: start=0x%08" PRIx64
                " sectors=%" PRIu64 " max=0x%08" PRIx64 "\n",
                start_lba,
                total_sectors,
                rt.nvme_max_lba);
        goto out;
    }
    if (!gopt.dry_run && dma_stop_s2mm(&rt, &dma_stop_report) == DMA_STOP_FAILED) {
        dbg_printf("[DBG][CONT] failed to quiesce DMA before SSD continuous pattern test ch=2\n");
        goto out;
    }

    if (!gopt.dry_run) {
        remaining = size_bytes;
        file_offset = 0u;
        cur_lba = start_lba;
        chunk_index = 0u;
        write_start_us = storage_wall_time_us();
        while (remaining > 0u) {
            uint64_t this_bytes = remaining > chunk_bytes ? chunk_bytes : remaining;
            uint64_t this_sectors = bytes_to_sectors(this_bytes);
            uint64_t base_word = file_offset / sizeof(uint32_t);

            fill_wrap_test_pattern_at(rt.ddr.virt, this_bytes, seed, base_word);
            if (verify_wrap_test_pattern_at(rt.ddr.virt,
                                            this_bytes,
                                            seed,
                                            base_word,
                                            file_offset,
                                            "continuous_cpu_fill") != 0) {
                goto out;
            }
            if ((file_offset % (512ull * 1024ull * 1024ull)) == 0u) {
                printf("ssd_continuous_boundary phase=write chunk=%" PRIu64
                       " file_offset=%" PRIu64 " lba=0x%08" PRIx64 "\n",
                       chunk_index,
                       file_offset,
                       cur_lba);
                fflush(stdout);
            }
            if (nvme_rw(&rt, true, cur_lba, this_sectors, cfg->ddr_hw_base) != 0) {
                dbg_printf("[DBG][CONT] write failed chunk=%" PRIu64
                           " file_offset=%" PRIu64 " lba=0x%08" PRIx64
                           " sectors=%" PRIu64 "\n",
                           chunk_index,
                           file_offset,
                           cur_lba,
                           this_sectors);
                goto out;
            }
            printf("ssd_continuous_write chunk=%" PRIu64 " file_offset=%" PRIu64
                   " lba=0x%08" PRIx64 " bytes=%" PRIu64
                   " sectors=%" PRIu64 "\n",
                   chunk_index,
                   file_offset,
                   cur_lba,
                   this_bytes,
                   this_sectors);
            fflush(stdout);
            remaining -= this_bytes;
            file_offset += this_bytes;
            cur_lba += this_sectors;
            ++chunk_index;
        }
        write_us = storage_elapsed_us(write_start_us);

        remaining = size_bytes;
        file_offset = 0u;
        cur_lba = start_lba;
        chunk_index = 0u;
        read_start_us = storage_wall_time_us();
        while (remaining > 0u) {
            uint64_t this_bytes = remaining > chunk_bytes ? chunk_bytes : remaining;
            uint64_t this_sectors = bytes_to_sectors(this_bytes);
            uint64_t base_word = file_offset / sizeof(uint32_t);
            volatile uint8_t *read_buf = rt.ddr.virt + read_offset;

            memset((void *)read_buf, 0xa5, (size_t)this_bytes);
            __sync_synchronize();
            if ((file_offset % (512ull * 1024ull * 1024ull)) == 0u) {
                printf("ssd_continuous_boundary phase=read chunk=%" PRIu64
                       " file_offset=%" PRIu64 " lba=0x%08" PRIx64 "\n",
                       chunk_index,
                       file_offset,
                       cur_lba);
                fflush(stdout);
            }
            if (nvme_rw(&rt, false, cur_lba, this_sectors, cfg->ddr_hw_base + read_offset) != 0) {
                dbg_printf("[DBG][CONT] read failed chunk=%" PRIu64
                           " file_offset=%" PRIu64 " lba=0x%08" PRIx64
                           " sectors=%" PRIu64 "\n",
                           chunk_index,
                           file_offset,
                           cur_lba,
                           this_sectors);
                goto out;
            }
            if (verify_wrap_test_pattern_at(read_buf,
                                            this_bytes,
                                            seed,
                                            base_word,
                                            file_offset,
                                            "continuous_readback") != 0) {
                printf("ssd_continuous_pattern_test result=failed chunk=%" PRIu64
                       " file_offset=%" PRIu64 " lba=0x%08" PRIx64
                       " bytes=%" PRIu64 "\n",
                       chunk_index,
                       file_offset,
                       cur_lba,
                       this_bytes);
                fflush(stdout);
                goto out;
            }
            printf("ssd_continuous_read chunk=%" PRIu64 " file_offset=%" PRIu64
                   " lba=0x%08" PRIx64 " bytes=%" PRIu64
                   " sectors=%" PRIu64 "\n",
                   chunk_index,
                   file_offset,
                   cur_lba,
                   this_bytes,
                   this_sectors);
            fflush(stdout);
            remaining -= this_bytes;
            file_offset += this_bytes;
            cur_lba += this_sectors;
            ++chunk_index;
        }
        read_us = storage_elapsed_us(read_start_us);
    }

    printf("ssd_continuous_pattern_test result=ok channel=2 lba=0x%08" PRIx64
           " bytes=%" PRIu64 " sectors=%" PRIu64
           " chunk_bytes=%" PRIu64 " chunks=%" PRIu64
           " write_ms=%" PRIu64 " read_ms=%" PRIu64
           " ddr_cpu=0x%08" PRIx64 " ddr_dma=0x%08" PRIx64 "\n",
           start_lba,
           size_bytes,
           total_sectors,
           chunk_bytes,
           (size_bytes + chunk_bytes - 1u) / chunk_bytes,
           write_us / 1000u,
           read_us / 1000u,
           cfg->ddr_cpu_base,
           cfg->ddr_hw_base);
    fflush(stdout);
    rc = 0;

out:
    channel_runtime_close(&rt);
    return rc;
}

int execute_dma_rx_benchmark(const ParsedArgs *args, GlobalOptions gopt)
{
    const ChannelConfig *cfg;
    ChannelRuntime rt;
    uint8_t *slot_state = NULL;
    uint64_t start_us;
    uint64_t end_us;
    uint64_t next_log_us;
    uint64_t received_bytes = 0u;
    uint64_t first_desc_us = 0u;
    uint64_t last_desc_us = 0u;
    uint64_t exhaustion_count = 0u;
    uint64_t descriptor_error_count = 0u;
    uint32_t min_writable = UINT32_MAX;
    uint32_t max_unharvested = 0u;
    bool integrity_ok = true;
    int rc = -1;

    if (!args || !args->has_channel || args->channel_all ||
        !args->has_duration_sec || !args->has_source) return -1;
    cfg = find_channel(args->channel_id);
    if (!cfg) return -1;
    if (channel_runtime_open(&rt, cfg, gopt) != 0) return -1;
    axis_switch_select(&rt, args->source);
    if (dma_prepare_s2mm_ring(&rt,
                              args->has_dma_desc_bytes ? args->dma_desc_bytes
                                                       : storage_default_dma_desc_bytes(cfg)) != 0) {
        goto out;
    }
    slot_state = calloc(rt.dma_desc_count, sizeof(*slot_state));
    if (!slot_state) goto out;
    memset(slot_state, STORAGE_SLOT_DMA_WRITABLE, rt.dma_desc_count);
    if (dma_start_s2mm_ring(&rt) != 0) goto out;
    start_us = storage_wall_time_us();
    end_us = start_us + (uint64_t)args->duration_sec * 1000000ull;
    next_log_us = start_us + 1000000ull;
    while (storage_wall_time_us() < end_us && !storage_write_stop_requested()) {
        DmaBdSnapshot snapshot;
        uint32_t slot = 0u;
        uint32_t actual = 0u;
        uint64_t now_us;
        int h;

        if (dma_get_bd_snapshot(&rt, slot_state, &snapshot) != 0) {
            integrity_ok = false;
            break;
        }
        if (snapshot.dma_writable < min_writable) min_writable = snapshot.dma_writable;
        if (snapshot.completed_unharvested > max_unharvested) {
            max_unharvested = snapshot.completed_unharvested;
        }
        if (snapshot.dma_writable == 0u) {
            ++exhaustion_count;
            integrity_ok = false;
            storage_emit_line("dma_bd_exhausted channel=%d mode=benchmark received_bytes=%" PRIu64,
                              cfg->id, received_bytes);
            break;
        }
        h = dma_harvest_one(&rt, &slot, &actual);
        if (h < 0) {
            if ((rt.dma_last_completed_status & 0x70000000u) != 0u) {
                ++descriptor_error_count;
            }
            integrity_ok = false;
            break;
        }
        now_us = storage_wall_time_us();
        if (h == 0) {
            if (now_us >= next_log_us) {
                storage_emit_line("dma_rx_benchmark_snapshot channel=%d dma_received_bytes=%" PRIu64
                                  " dma_bd_writable=%u completed_unharvested=%u"
                                  " occupied_bytes_est=%" PRIu64 " curdesc=%u taildesc=%u"
                                  " s2mm_dmasr=0x%08x",
                                  cfg->id, received_bytes, snapshot.dma_writable,
                                  snapshot.completed_unharvested, snapshot.occupied_bytes_est,
                                  snapshot.curdesc_index, snapshot.taildesc_index,
                                  snapshot.s2mm_dmasr);
                next_log_us = now_us + 1000000ull;
            }
            if (gopt.dry_run) usleep(1000u); else sched_yield();
            continue;
        }
        if (first_desc_us == 0u) first_desc_us = now_us;
        last_desc_us = now_us;
        received_bytes += actual;
        slot_state[slot] = STORAGE_SLOT_REQUEUE_PENDING;
        if (dma_requeue_one(&rt, slot) != 0) {
            integrity_ok = false;
            break;
        }
        slot_state[slot] = STORAGE_SLOT_DMA_WRITABLE;
    }
    {
        uint64_t observed_us = last_desc_us >= first_desc_us
                                   ? last_desc_us - first_desc_us : 0u;
        storage_emit_line("dma_rx_benchmark_result channel=%d dma_received_bytes=%" PRIu64
                          " dma_observed_mib_s=%.3f min_dma_writable=%u"
                          " max_completed_unharvested=%u bd_exhaustion_count=%" PRIu64
                          " descriptor_error_count=%" PRIu64 " rxsof_count=%" PRIu64
                          " rxeof_count=%" PRIu64 " receive_integrity_ok=%u",
                          cfg->id, received_bytes,
                          observed_us > 0u ? ((double)received_bytes * 1000000.0 /
                                                (double)observed_us / 1048576.0) : 0.0,
                          min_writable == UINT32_MAX ? rt.dma_desc_count : min_writable,
                          max_unharvested, exhaustion_count, descriptor_error_count,
                          rt.dma_rxsof_count, rt.dma_rxeof_count,
                          integrity_ok ? 1u : 0u);
    }
    rc = integrity_ok ? 0 : -1;
out:
    free(slot_state);
    (void)dma_stop_s2mm(&rt, NULL);
    channel_runtime_close(&rt);
    return rc;
}

int execute_read(const ParsedArgs *args, GlobalOptions gopt) {
    const ChannelConfig *cfg = find_channel(args->channel_id);
    ChannelRuntime rt;
    int rc = -1;
    uint64_t ddr_cpu_addr;
    uint64_t lba = 0u;
    uint64_t sectors = 0u;

    if (!cfg) {
        return -1;
    }
    if (channel_runtime_open(&rt, cfg, gopt) != 0) {
        return -1;
    }
    if (nvme_probe(&rt) != 0) {
        goto out;
    }
    if (rt.nvme_max_lba == 0u) {
        fprintf(stderr, "NVMe capacity unavailable on channel %d\n", cfg->id);
        goto out;
    }

    ddr_cpu_addr = cfg->ddr_cpu_base;

    if (args->has_task_no && args->has_file_index) {
        FileEntry table[MAX_FILES_TOTAL];
        FileEntry e;
        int slot = -1;
        if (metadata_read(&rt, table) != 0) {
            goto out;
        }
        if (metadata_find_by_task(table, args->task_no, args->file_index, &slot, &e) != 0) {
            fprintf(stderr, "File not found on channel %d: task=%s index=%u\n",
                    cfg->id, args->task_no, args->file_index);
            goto out;
        }
        lba = e.start_lba;
        sectors = e.sector_count;
        printf("read_target channel=%d metadata_slot=%d task=%s file_index=%u lba=0x%08" PRIx64 " sectors=%" PRIu64 "\n",
               cfg->id, slot, args->task_no, args->file_index, lba, sectors);
    } else {
        /* Direct mode: read explicit LBA + size from CLI. */
        lba = args->lba;
        sectors = bytes_to_sectors(args->size_bytes);
    }

    if (nvme_rw(&rt, false, lba, sectors, cpu_to_hw_addr(cfg, ddr_cpu_addr)) != 0) {
        goto out;
    }

    printf("read_done channel=%d ddr_cpu_addr=0x%08" PRIx64 " ddr_hw_addr=0x%08" PRIx64 " lba=0x%08" PRIx64 " sectors=%" PRIu64 "\n",
           cfg->id, ddr_cpu_addr, cpu_to_hw_addr(cfg, ddr_cpu_addr), lba, sectors);
    rc = 0;

out:
    channel_runtime_close(&rt);
    return rc;
}

static int execute_list_single(int ch, GlobalOptions gopt) {
    const ChannelConfig *cfg = find_channel(ch);
    ChannelRuntime rt;
    FileEntry table[MAX_FILES_TOTAL];
    int rc = -1;
    uint32_t i;
    uint32_t count = 0u;

    if (!cfg) {
        return -1;
    }
    if (channel_runtime_open(&rt, cfg, gopt) != 0) {
        return -1;
    }
    if (nvme_probe(&rt) != 0) {
        goto out;
    }
    if (metadata_read(&rt, table) != 0) {
        goto out;
    }

    printf("=== channel %d (%s) file list ===\n", cfg->id, cfg->name);
    for (i = 0; i < MAX_FILES_TOTAL; ++i) {
        if (table[i].valid == 1u) {
            print_entry(cfg->id, (int)i, &table[i]);
            ++count;
        }
    }
    if (count == 0u) {
        printf("(empty)\n");
    }
    rc = 0;

out:
    channel_runtime_close(&rt);
    return rc;
}

int execute_list(const ParsedArgs *args, GlobalOptions gopt) {
    if (args->channel_all) {
        int rc = 0;
        size_t i;
        /* Best-effort: continue listing supported channels even if one fails. */
        for (i = 0; i < NUM_CHANNELS; ++i) {
            if (execute_list_single(kChannels[i].id, gopt) != 0) {
                rc = -1;
            }
        }
        return rc;
    }
    return execute_list_single(args->channel_id, gopt);
}

static int execute_init_meta_single(int channel_id, GlobalOptions gopt) {
    const ChannelConfig *cfg = find_channel(channel_id);
    ChannelRuntime rt;
    FileEntry table[MAX_FILES_TOTAL];
    int rc = -1;

    if (!cfg) {
        return -1;
    }
    if (channel_runtime_open(&rt, cfg, gopt) != 0) {
        return -1;
    }
    if (nvme_probe(&rt) != 0) {
        goto out;
    }
    memset(table, 0, sizeof(table));
    if (metadata_write(&rt, table) != 0) {
        goto out;
    }

    printf("metadata_init_done backend=ramfs channel=%d entries=%u\n",
           cfg->id,
           (unsigned)MAX_FILES_TOTAL);
    rc = 0;

out:
    channel_runtime_close(&rt);
    return rc;
}

int execute_init_meta(const ParsedArgs *args, GlobalOptions gopt) {
    if (args->channel_all) {
        int rc = 0;
        size_t i;
        for (i = 0; i < NUM_CHANNELS; ++i) {
            if (execute_init_meta_single(kChannels[i].id, gopt) != 0) {
                rc = -1;
            }
        }
        return rc;
    }
    return execute_init_meta_single(args->channel_id, gopt);
}
