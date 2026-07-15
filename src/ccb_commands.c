#define _GNU_SOURCE
#include "ccb_commands.h"

#include "ccb_config.h"
#include "ccb_hw.h"
#include "ccb_metadata.h"
#include "ccb_storage_ipc.h"
#include "ccb_storage_pipeline.h"
#include "ccb_storage_diag.h"
#include "ccb_storage_log.h"
#include "debug_uart.h"
#include "storage_config.h"
#include "storage_worker.h"

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
#define STORAGE_EVENT_DEADLINE_US 1000000ull
#define STORAGE_STOP_STABLE_EMPTY_SCANS 3u
#define STORAGE_STOP_STABLE_EMPTY_US 100ull
static volatile sig_atomic_t g_storage_stop_requested = 0;
static bool g_storage_control_drain_latched = false;
static bool g_storage_control_stop_latched = false;
static uint64_t g_storage_stop_epoch;
static _Atomic uint64_t g_storage_dropped_perf_samples;
static _Atomic uint64_t g_storage_dropped_diag_events;
/* A worker has one primary FATAL.  Cleanup failures are retained separately
 * and must not overwrite the first cause. */
static bool g_storage_fatal_event_sent;
static StorageEventRecord *g_storage_deferred_diag_records;
static uint32_t g_storage_deferred_diag_count;
static uint32_t g_storage_deferred_diag_capacity;

typedef struct {
    uint32_t slot;
    uint64_t bytes;
    uint64_t media_bytes;
    uint64_t chunk_index;
    uint64_t file_offset;
    uint64_t start_lba;
    uint64_t sectors;
    uint64_t hw_addr;
} PendingDdrSlot;

typedef enum {
    STORAGE_QUEUE_POP_ERROR = -1,
    STORAGE_QUEUE_POP_TEMP_EMPTY = 0,
    STORAGE_QUEUE_POP_ITEM = 1,
    STORAGE_QUEUE_POP_PRODUCER_DRAINED = 2,
    STORAGE_QUEUE_POP_NOT_ATTEMPTED = 3
} StorageQueuePopResult;

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
    uint64_t queue_empty_wait_us;
    uint64_t writer_active_us;
    uint64_t ready_q_nonempty_us;
    uint64_t writer_drain_loop_count;
    uint64_t writer_slots_drained;
    uint64_t queued_slot_count;
    uint64_t completed_slot_count;
    uint64_t recycled_slot_count;
    uint64_t writer_schedule_gap_count;
    uint64_t writer_schedule_gap_max_us;
    uint64_t writer_no_progress_sleep_count;
    bool writer_rt_enabled;
    int writer_rt_policy;
    uint32_t writer_rt_prio;
} StorageQueueSnapshot;

typedef struct {
    ChannelRuntime *rt;
    PendingDdrSlot *items;
    StorageSlotTable slots;
    StorageEventRing producer_event_ring;
    StorageEventRing writer_event_ring;
    uint32_t capacity;
    uint32_t head;
    uint32_t tail;
    uint32_t count;
    uint64_t ready_depth_sum;
    uint64_t ready_depth_samples;
    uint32_t ready_depth_max;
    uint32_t max_busy_count;
    uint64_t buffered_bytes;
    uint64_t max_buffered_bytes;
    uint64_t bytes_written;
    uint64_t nvme_write_us;
    uint64_t writer_idle_us;
    uint64_t queue_empty_wait_us;
    uint64_t writer_active_us;
    uint64_t ready_q_nonempty_us;
    uint64_t writer_drain_loop_count;
    uint64_t writer_slots_drained;
    uint64_t queued_slot_count;
    uint64_t completed_slot_count;
    uint64_t recycled_slot_count;
    uint64_t writer_schedule_gap_count;
    uint64_t writer_schedule_gap_max_us;
    uint64_t cross_sq_full_wait_count;
    uint64_t cross_sq_full_wait_max_us;
    uint64_t cross_cq_empty_wait_count;
    uint64_t cross_cq_empty_wait_max_us;
    uint64_t cross_submit_mmio_count;
    uint64_t cross_submit_mmio_max_us;
    uint64_t cross_completion_process_count;
    uint64_t cross_completion_process_max_us;
    uint64_t cross_no_progress_sleep_count;
    uint64_t writer_no_progress_sleep_count;
    uint32_t writer_budget_override_us;
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
    NvmeCrossSlotConfig cross_slot_config;
    uint64_t nvme_abort_timeout_us;
    StorageWorkerState worker_state;
    bool writer_ready;
    bool producer_ready;
    bool requeue_enabled;
    bool error;
    char error_reason[64];
    StorageErrorCode deferred_stop_error;
    char deferred_stop_reason[64];
    bool nvme_engine_quiesced;
    /* Published while the cross-slot writer owns the engine.  Stop/error
     * paths may use it to request an abort without waiting for the writer's
     * next queue wake-up.  It is never cleared until the engine is quiesced
     * and destroyed. */
    NvmeCrossSlotEngine *nvme_engine;
    pthread_mutex_t lock;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} StorageWriteQueue;

typedef struct {
    ChannelRuntime *rt;
    uint64_t deadline_us;
    const uint8_t *slot_states;
    DmaStopReport *report;
} StorageDmaQuiesceCall;

static int storage_dma_quiesce_invoke(void *opaque)
{
    StorageDmaQuiesceCall *call = opaque;

    return (int)dma_quiesce_s2mm_with_state(call->rt, call->deadline_us,
                                             call->slot_states, call->report);
}

static DmaStopResult storage_dma_quiesce_epoch(StorageStopState *stop,
                                                uint64_t drain_epoch,
                                                ChannelRuntime *rt,
                                                uint64_t deadline_us,
                                                const uint8_t *slot_states,
                                                DmaStopReport *report)
{
    StorageDmaQuiesceCall call = {
        .rt = rt,
        .deadline_us = deadline_us,
        .slot_states = slot_states,
        .report = report,
    };

    return (DmaStopResult)storage_dma_quiesce_once(
        stop ? &stop->quiesce : NULL, drain_epoch,
        storage_dma_quiesce_invoke, &call);
}

static bool storage_queue_slot_counts_valid_locked(const StorageWriteQueue *q)
{
    if (!q) return false;
    return q->count <= q->capacity && q->capacity == q->slots.capacity &&
           storage_slot_table_valid(&q->slots);
}

static int storage_queue_transition_locked(StorageWriteQueue *q,
                                           StorageWorkerState expected,
                                           StorageWorkerState next)
{
    if (!q || storage_worker_transition(&q->worker_state, expected, next) != 0) {
        if (q) q->error = true;
        return -1;
    }
    return 0;
}

static void storage_queue_fail_locked(StorageWriteQueue *q)
{
    StorageWorkerState current;

    if (!q) return;
    current = q->worker_state;
    if (current != WORKER_DONE && current != WORKER_FAILED)
        (void)storage_worker_transition(&q->worker_state, current,
                                        WORKER_FAILED);
    q->error = true;
}

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
    StorageErrorCode primary_error;
    StorageErrorCode secondary_error;
    char secondary_reason[64];
    uint64_t window_backlog_bytes;
    uint64_t ring_full_count;
    uint64_t ring_full_total_us;
    uint64_t ring_full_start_us;
    uint64_t ring_full_first_at_bytes;
    uint64_t ring_full_last_at_bytes;
    bool ring_full_active;
    bool integrity_risk_ring_full;
    uint32_t watermark_level;
    uint32_t ring_warning_percent;
    uint32_t ring_critical_percent;
    uint64_t ring_critical_duration_us;
    uint64_t ring_critical_since_us;
    bool ring_critical_stop_enabled;
    bool ring_critical_stop_triggered;
    uint64_t dma_no_free_slot_count;
    uint64_t dma_harvest_batches;
    uint64_t dma_harvest_batch_total;
    uint32_t dma_harvest_batch_max;
    uint32_t dma_harvest_batch_current;
    uint64_t window_nvme_cmd_count;
    uint64_t window_nvme_cq_completed;
    uint64_t perf_next_us;
    uint64_t perf_window_start_us;
    uint64_t perf_window_received_bytes;
    uint64_t perf_window_nvme_bytes;
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

static void storage_stats_observe_bd_snapshot(StorageProducerStats *stats,
                                              const DmaBdSnapshot *snapshot)
{
    if (!stats || !snapshot) return;
    if (snapshot->completed_unharvested > stats->max_completed_unharvested)
        stats->max_completed_unharvested = snapshot->completed_unharvested;
    if (stats->min_dma_writable == UINT32_MAX ||
        snapshot->dma_writable < stats->min_dma_writable)
        stats->min_dma_writable = snapshot->dma_writable;
    if (snapshot->occupied_bytes_est > stats->max_occupied_bytes_est)
        stats->max_occupied_bytes_est = snapshot->occupied_bytes_est;
}

static void storage_record_error(StorageProducerStats *stats,
                                 StorageErrorCode code, const char *reason)
{
    StorageErrorCode previous_primary;
    StorageErrorCode previous_secondary;

    if (!stats || !reason || reason[0] == '\0') return;
    stats->receive_integrity_ok = false;
    previous_primary = stats->primary_error;
    previous_secondary = stats->secondary_error;
    storage_error_record(&stats->primary_error, &stats->secondary_error, code);
    if (previous_primary == STORAGE_ERR_NONE &&
        stats->primary_error != STORAGE_ERR_NONE) {
        (void)snprintf(stats->receive_integrity_risk,
                       sizeof(stats->receive_integrity_risk), "%s", reason);
        return;
    }
    if (previous_secondary == STORAGE_ERR_NONE &&
        stats->secondary_error != STORAGE_ERR_NONE) {
        (void)snprintf(stats->secondary_reason,
                       sizeof(stats->secondary_reason), "%s", reason);
    }
}

#define storage_record_failure(stats_, reason_) \
    storage_record_error((stats_), STORAGE_ERR_INTERNAL, (reason_))

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
static int storage_slot_addresses(const ChannelRuntime *rt,
                                  const PendingDdrSlot *item,
                                  uint64_t *buffer_offset,
                                  uint64_t *cpu_addr);
static int storage_zero_tail_padding(ChannelRuntime *rt,
                                     const PendingDdrSlot *item,
                                     uint64_t buffer_offset);
static void storage_fail_fatal(StorageWriteQueue *q);
static void storage_record_deferred_stop_error(StorageWriteQueue *q,
                                               StorageErrorCode code,
                                               const char *reason);
static void storage_set_writer_error_reason(StorageWriteQueue *q, const char *reason);
static void storage_copy_writer_error_reason(StorageWriteQueue *q, char *out, size_t out_size);
static uint64_t storage_wall_time_us(void);
static void storage_ring_event(StorageEventRing *ring, uint32_t id, const ChannelRuntime *rt,
                               uint64_t arg0, uint64_t arg1, bool fatal)
{
    StorageEventRecord e;
    memset(&e, 0, sizeof(e)); e.timestamp_us = storage_wall_time_us(); e.event_id = id;
    e.channel = rt && rt->cfg ? (uint16_t)rt->cfg->id : UINT16_MAX; e.arg0 = arg0; e.arg1 = arg1;
    storage_event_ring_push(ring, &e, fatal ? 1 : 0);
}
static void storage_trace_flush_start(const ChannelRuntime *rt, const PendingDdrSlot *item);
static void storage_trace_flush_done(const ChannelRuntime *rt,
                                     const PendingDdrSlot *item,
                                     uint32_t file_index,
                                     int metadata_slot,
                                     const char *task_no);
void storage_write_request_stop(void);
static int storage_env_flag_enabled(const char *name);
static bool storage_text_output_enabled(void);
static uint64_t storage_timeout_us(const char *us_name, const char *ms_name,
                                   uint64_t fallback_us);

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

static void storage_format_rate(char *out, size_t out_size, uint64_t bytes,
                                uint64_t elapsed_us)
{
    double rate;

    if (!out || out_size == 0u) return;
    if (!storage_rate_mib_s(bytes, elapsed_us, &rate)) {
        (void)snprintf(out, out_size, "N/A");
        return;
    }
    (void)snprintf(out, out_size, "%.3f", rate);
}

static int storage_env_fd(const char *name)
{
    const char *value = storage_config_compat_getenv(name);
    char *end = NULL;
    long fd;
    if (!value || value[0] == '\0') return -1;
    fd = strtol(value, &end, 10);
    return end != value && *end == '\0' && fd >= 0 && fd <= INT_MAX ? (int)fd : -1;
}

static int storage_emit_event(StorageWorkerEventType type, const ChannelRuntime *rt,
                              StorageErrorCode error_code, uint64_t bytes,
                              const char *reason)
{
    StorageWorkerEvent event;
    int fd = storage_env_fd(CCB_INTERNAL_STORAGE_EVENT_FD);
    if (fd < 0) return 0;
    if (type == STORAGE_WORKER_FATAL) {
        if (g_storage_fatal_event_sent) return 0;
    }
    storage_ipc_make_event(&event, type, rt && rt->cfg ? (uint32_t)rt->cfg->id : UINT32_MAX,
                           error_code, bytes, reason);
    {
        int rc = storage_ipc_write_event_deadline(
            fd, &event, storage_ipc_monotonic_us() + STORAGE_EVENT_DEADLINE_US);
        if (type == STORAGE_WORKER_FATAL && rc == 0) g_storage_fatal_event_sent = true;
        return rc;
    }
}

static int storage_emit_stop_phase(const ChannelRuntime *rt,
                                   StorageWorkerStopPhase phase,
                                   StorageErrorCode error_code,
                                   uint64_t bytes,
                                   const char *reason)
{
    StorageWorkerEvent event;
    int fd = storage_env_fd(CCB_INTERNAL_STORAGE_EVENT_FD);

    if (fd < 0) return 0;
    storage_ipc_make_event(&event, STORAGE_WORKER_STOP_PHASE,
                           rt && rt->cfg ? (uint32_t)rt->cfg->id : UINT32_MAX,
                           error_code,
                           bytes, reason);
    event.payload.phase.stop_epoch = g_storage_stop_epoch;
    event.payload.phase.stop_phase = (uint32_t)phase;
    return storage_ipc_write_event_deadline(
        fd, &event, storage_ipc_monotonic_us() + STORAGE_EVENT_DEADLINE_US);
}

static void storage_emit_perf_event(const ChannelRuntime *rt, const StorageProducerStats *stats,
                                    const StorageWriteQueue *q,
                                    const DmaBdSnapshot *bd, uint64_t start_us, uint64_t end_us,
                                    uint64_t dma_delta, uint64_t nvme_delta)
{
    StorageWorkerEvent event;
    int fd = storage_env_fd(CCB_INTERNAL_STORAGE_EVENT_FD);
    if (fd < 0 || !rt || !stats || !q || !bd) return;
    storage_ipc_make_event(&event, STORAGE_WORKER_PERF_SAMPLE, (uint32_t)rt->cfg->id,
                           STORAGE_ERR_NONE, dma_delta, "perf_sample");
    event.payload.perf.window_start_us = start_us; event.payload.perf.window_end_us = end_us;
    event.payload.perf.dma_bytes_delta = dma_delta; event.payload.perf.nvme_bytes_delta = nvme_delta;
    event.payload.perf.dma_writable = bd->dma_writable;
    event.payload.perf.completed_unharvested = bd->completed_unharvested;
    event.payload.perf.ready_slots = bd->ready_slots;
    event.payload.perf.nvme_busy_slots = bd->nvme_busy_slots;
    event.payload.perf.requeue_pending = bd->requeue_pending;
    event.payload.perf.free_slots = bd->free_slots;
    event.payload.perf.active_qd = __atomic_load_n(&rt->nvme_active_qd_current, __ATOMIC_ACQUIRE);
    event.payload.perf.active_qd_max = __atomic_load_n(&rt->nvme_active_qd_max, __ATOMIC_ACQUIRE);
    event.payload.perf.submit_stall_count = __atomic_load_n(&rt->nvme_submit_stall_count, __ATOMIC_ACQUIRE);
    event.payload.perf.submit_stall_max_us = __atomic_load_n(&rt->nvme_submit_stall_max_us, __ATOMIC_ACQUIRE);
    event.payload.perf.writer_schedule_gap_count = __atomic_load_n(&q->writer_schedule_gap_count, __ATOMIC_ACQUIRE);
    event.payload.perf.writer_schedule_gap_max_us = __atomic_load_n(&q->writer_schedule_gap_max_us, __ATOMIC_ACQUIRE);
    event.payload.perf.sq_full_wait_count = __atomic_load_n(&q->cross_sq_full_wait_count, __ATOMIC_ACQUIRE);
    event.payload.perf.sq_full_wait_max_us = __atomic_load_n(&q->cross_sq_full_wait_max_us, __ATOMIC_ACQUIRE);
    event.payload.perf.cq_empty_wait_count = __atomic_load_n(&q->cross_cq_empty_wait_count, __ATOMIC_ACQUIRE);
    event.payload.perf.cq_empty_wait_max_us = __atomic_load_n(&q->cross_cq_empty_wait_max_us, __ATOMIC_ACQUIRE);
    event.payload.perf.submit_mmio_count = __atomic_load_n(&q->cross_submit_mmio_count, __ATOMIC_ACQUIRE);
    event.payload.perf.submit_mmio_max_us = __atomic_load_n(&q->cross_submit_mmio_max_us, __ATOMIC_ACQUIRE);
    event.payload.perf.completion_process_count = __atomic_load_n(&q->cross_completion_process_count, __ATOMIC_ACQUIRE);
    event.payload.perf.completion_process_max_us = __atomic_load_n(&q->cross_completion_process_max_us, __ATOMIC_ACQUIRE);
    event.payload.perf.queue_empty_wait_us = __atomic_load_n(&q->queue_empty_wait_us, __ATOMIC_ACQUIRE);
    event.payload.perf.writer_active_us = __atomic_load_n(&q->writer_active_us, __ATOMIC_ACQUIRE);
    event.payload.perf.no_progress_sleep_count = __atomic_load_n(&q->cross_no_progress_sleep_count, __ATOMIC_ACQUIRE);
    event.payload.perf.writer_no_progress_sleep_count =
        __atomic_load_n(&q->writer_no_progress_sleep_count, __ATOMIC_ACQUIRE);
    event.payload.perf.dropped_perf_samples = atomic_load_explicit(
        &g_storage_dropped_perf_samples, memory_order_relaxed);
    event.payload.perf.dropped_diag_events = atomic_load_explicit(
        &g_storage_dropped_diag_events, memory_order_relaxed);
    event.payload.perf.receive_integrity_ok = stats->receive_integrity_ok ? 1u : 0u;
    event.payload.perf.storage_integrity_ok =
        (!q->error && !stats->integrity_risk_ring_full) ? 1u : 0u;
    (void)storage_ipc_try_write_perf(fd, &event, &g_storage_dropped_perf_samples);
}

static void storage_emit_final_perf_window(ChannelRuntime *rt,
                                           StorageProducerStats *stats,
                                           StorageWriteQueue *q,
                                           uint64_t received_bytes,
                                           uint64_t now_us)
{
    DmaBdSnapshot bd_snapshot;
    uint64_t nvme_bytes;
    uint64_t dma_delta;
    uint64_t nvme_delta;

    if (!rt || !stats || !q || stats->perf_window_start_us == 0u) return;
    memset(&bd_snapshot, 0, sizeof(bd_snapshot));
    pthread_mutex_lock(&q->lock);
    (void)dma_get_bd_snapshot_o1(rt, &q->slots.counts, &bd_snapshot);
    pthread_mutex_unlock(&q->lock);
    storage_stats_observe_bd_snapshot(stats, &bd_snapshot);
    nvme_bytes = __atomic_load_n(&rt->nvme_write_bytes_done, __ATOMIC_ACQUIRE);
    dma_delta = received_bytes >= stats->perf_window_received_bytes
                    ? received_bytes - stats->perf_window_received_bytes : 0u;
    nvme_delta = nvme_bytes >= stats->perf_window_nvme_bytes
                     ? nvme_bytes - stats->perf_window_nvme_bytes : 0u;
    storage_emit_perf_event(rt, stats, q, &bd_snapshot,
                            stats->perf_window_start_us, now_us,
                            dma_delta, nvme_delta);
    stats->perf_window_start_us = now_us;
    stats->perf_window_received_bytes = received_bytes;
    stats->perf_window_nvme_bytes = nvme_bytes;
}

static void storage_emit_diag_event(const ChannelRuntime *rt, const StorageEventRecord *record)
{
    StorageWorkerEvent event;
    int fd = storage_env_fd(CCB_INTERNAL_STORAGE_EVENT_FD);

    if (fd < 0 || !record) return;
    storage_ipc_make_event(&event, STORAGE_WORKER_DIAG_EVENT,
                           rt && rt->cfg ? (uint32_t)rt->cfg->id : record->channel,
                           STORAGE_ERR_NONE, 0u, "diag_event");
    event.payload.diag = *record;
    (void)storage_ipc_try_write_diag(fd, &event, &g_storage_dropped_diag_events);
}

static void storage_defer_event_ring_dump(const StorageEventRing *ring)
{
    StorageEventRecord *records;
    StorageEventRecord *resized;
    uint32_t count;

    if (!ring || ring->capacity == 0u) return;
    records = calloc((size_t)ring->capacity + 1u, sizeof(*records));
    if (!records) return;
    count = storage_event_ring_copy((StorageEventRing *)ring, records, ring->capacity + 1u);
    if (count > 0u && count <= UINT32_MAX - g_storage_deferred_diag_count) {
        uint32_t needed = g_storage_deferred_diag_count + count;
        if (needed > g_storage_deferred_diag_capacity) {
            resized = realloc(g_storage_deferred_diag_records,
                              (size_t)needed * sizeof(*g_storage_deferred_diag_records));
            if (resized) {
                g_storage_deferred_diag_records = resized;
                g_storage_deferred_diag_capacity = needed;
            }
        }
        if (g_storage_deferred_diag_capacity >= needed) {
            memcpy(g_storage_deferred_diag_records + g_storage_deferred_diag_count,
                   records, (size_t)count * sizeof(*records));
            g_storage_deferred_diag_count = needed;
        }
    }
    free(records);
}

void storage_write_flush_deferred_diag(void)
{
    uint32_t i;

    for (i = 0u; i < g_storage_deferred_diag_count; ++i)
        storage_emit_diag_event(NULL, &g_storage_deferred_diag_records[i]);
    free(g_storage_deferred_diag_records);
    g_storage_deferred_diag_records = NULL;
    g_storage_deferred_diag_count = 0u;
    g_storage_deferred_diag_capacity = 0u;
}

static void storage_maybe_dump_event_rings(const StorageWriteQueue *q,
                                           const ChannelRuntime *rt,
                                           bool is_error, bool stopped)
{
    const AppConfig *config = storage_config_get();
    bool dump_error = config && config->dump_diag_on_error;

    if (!q || !storage_event_ring_should_dump(is_error, stopped, dump_error, false)) return;
    (void)rt;
    storage_defer_event_ring_dump(&q->producer_event_ring);
    storage_defer_event_ring_dump(&q->writer_event_ring);
}

static int storage_wait_start_gate(ChannelRuntime *rt, uint64_t *start_skew_us,
                                   const char **gate_mode, const char **failure_reason)
{
    const char *value = storage_config_compat_getenv(CCB_INTERNAL_START_FD);
    char *end = NULL;
    long fd;
    uint64_t parent_start_us = 0u;
    size_t used = 0u;

    int control_fd = storage_env_fd(CCB_INTERNAL_STORAGE_CONTROL_FD);
    StorageControlMessage msg;
    StorageControlReader reader;
    if (start_skew_us) *start_skew_us = 0u;
    if (gate_mode) *gate_mode = "standalone_immediate";
    if (failure_reason) *failure_reason = "start_gate_failed";
    if (control_fd >= 0) {
        uint64_t timeout_us = storage_timeout_us("SRC_REAL_STORAGE_ARM_TIMEOUT_US", NULL,
            storage_timeout_us("SRC_REAL_STORAGE_START_TIMEOUT_US", NULL,
                               rt->gopt.timeout_us ? rt->gopt.timeout_us : DEFAULT_TIMEOUT_US));
        uint64_t deadline_us;
        int flags = fcntl(control_fd, F_GETFL, 0);
        storage_ipc_control_reader_init(&reader);
        if (flags < 0 || fcntl(control_fd, F_SETFL, flags | O_NONBLOCK) != 0) {
            if (failure_reason) *failure_reason = "control_fd_nonblocking_failed";
            return -1;
        }
        if (storage_emit_event(STORAGE_WORKER_READY, rt, STORAGE_ERR_NONE,
                               0u, "ready") != 0) {
            if (failure_reason) *failure_reason = "ready_event_send_failed";
            return -1;
        }
        deadline_us = storage_ipc_monotonic_us() + timeout_us;
        if (storage_ipc_read_control_deadline(control_fd, &reader, &msg, deadline_us) != 0) {
            if (failure_reason) *failure_reason = errno == ETIMEDOUT ? "arm_wait_timeout" :
                                                errno == EPROTO ? "arm_control_protocol_error" :
                                                "arm_control_eof_or_io_error";
            return -1;
        }
        if (msg.type == STORAGE_CTRL_STOP) {
            g_storage_control_stop_latched = true;
            g_storage_stop_epoch = msg.stop_epoch;
            if (failure_reason) *failure_reason = "stop_before_arm";
            return -1;
        }
        if (msg.type != STORAGE_CTRL_ARM) {
            if (failure_reason) *failure_reason = "invalid_arm_sequence";
            return -1;
        }
        if (dma_start_s2mm_ring(rt) != 0) { if (failure_reason) *failure_reason = "dma_start_failed"; return -1; }
        if (storage_emit_event(STORAGE_WORKER_ARMED, rt, STORAGE_ERR_NONE,
                               0u, "armed") != 0) {
            if (failure_reason) *failure_reason = "armed_event_send_failed";
            return -1;
        }
        timeout_us = storage_timeout_us("SRC_REAL_STORAGE_RUN_TIMEOUT_US", NULL,
            storage_timeout_us("SRC_REAL_STORAGE_START_TIMEOUT_US", NULL,
                               rt->gopt.timeout_us ? rt->gopt.timeout_us : DEFAULT_TIMEOUT_US));
        deadline_us = storage_ipc_monotonic_us() + timeout_us;
        if (storage_ipc_read_control_deadline(control_fd, &reader, &msg, deadline_us) != 0) {
            if (failure_reason) *failure_reason = errno == ETIMEDOUT ? "run_wait_timeout" :
                                                errno == EPROTO ? "run_control_protocol_error" :
                                                "run_control_eof_or_io_error";
            return -1;
        }
        if (msg.type == STORAGE_CTRL_STOP) {
            g_storage_control_stop_latched = true;
            g_storage_stop_epoch = msg.stop_epoch;
            if (failure_reason) *failure_reason = "stop_before_run";
            return -1;
        }
        if (msg.type != STORAGE_CTRL_RUN) { if (failure_reason) *failure_reason = "invalid_run_sequence"; return -1; }
        if (gate_mode) *gate_mode = "software_two_phase_barrier";
        if (start_skew_us) {
            uint64_t now_us = storage_wall_time_us();
            *start_skew_us = now_us >= msg.timestamp_us ? now_us - msg.timestamp_us : 0u;
        }
        return 0;
    }
    if (!value || value[0] == '\0') return 0;
    fd = strtol(value, &end, 10);
    if (end == value || *end != '\0' || fd < 0 || fd > INT_MAX) { if (failure_reason) *failure_reason = "invalid_start_fd"; return -1; }
    while (used < sizeof(parent_start_us)) {
        ssize_t n = read((int)fd, (uint8_t *)&parent_start_us + used,
                         sizeof(parent_start_us) - used);
        if (n > 0) {
            used += (size_t)n;
        } else if (n < 0 && errno == EINTR) {
            continue;
        } else {
            if (failure_reason) *failure_reason = "start_barrier_io_error";
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

static bool storage_control_drain_requested(void)
{
    StorageControlMessage msg;
    int fd = storage_env_fd(CCB_INTERNAL_STORAGE_CONTROL_FD);
    if (g_storage_control_drain_latched) return true;
    if (fd >= 0 && storage_ipc_read_control(fd, &msg) == 0) {
        if (msg.type == STORAGE_CTRL_AUTO_DRAIN) {
            g_storage_control_drain_latched = true;
            g_storage_stop_epoch = msg.stop_epoch;
        } else if (msg.type == STORAGE_CTRL_STOP) {
            g_storage_control_drain_latched = true;
            g_storage_control_stop_latched = true;
            g_storage_stop_epoch = msg.stop_epoch;
        }
    }
    return g_storage_control_drain_latched;
}

static void storage_emit_line_impl(const char *fmt, ...)
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

#define storage_emit_line(severity, fmt, ...) \
    do { \
        if (storage_log_enabled((severity))) \
            storage_emit_line_impl((fmt), ##__VA_ARGS__); \
    } while (0)

static uint32_t storage_idle_notice_ms(void) {
    const char *env = storage_config_compat_getenv("SRC_REAL_STORAGE_IDLE_NOTICE_MS");
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

static bool storage_log_enabled(StorageLogSeverity need)
{
    return storage_log_severity_enabled(need);
}

static bool storage_text_output_enabled(void)
{
    return storage_log_enabled(STORAGE_LOG_DEBUG);
}

static uint32_t storage_env_u32_limit(const char *name, uint32_t fallback, uint32_t max_value);
static int storage_env_flag_enabled(const char *name);

static uint32_t storage_pipeline_stats_ms(void)
{
    const char *env = storage_config_compat_getenv("SRC_REAL_PIPELINE_STATS_SEC");
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

static uint32_t storage_perf_log_interval_ms(void)
{
    return storage_env_u32_limit("SRC_REAL_PERF_LOG_INTERVAL_SEC", 5u,
                                 UINT32_MAX / 1000u) * 1000u;
}

static uint32_t storage_env_u32_limit(const char *name, uint32_t fallback, uint32_t max_value)
{
    const char *value = storage_config_compat_getenv(name);
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
    if (storage_config_compat_getenv(name))
        return storage_env_u32_limit(name, fallback, max_value);
    if (global_fallback && storage_config_compat_getenv(global_fallback)) {
        return storage_env_u32_limit(global_fallback, fallback, max_value);
    }
    return fallback;
}

typedef struct {
    uint64_t compat_us;
    uint64_t dma_quiesce_us;
    uint64_t stop_harvest_us;
    uint64_t writer_drain_us;
    uint64_t nvme_abort_us;
} StorageStopTimeouts;

static uint64_t storage_timeout_us(const char *us_name, const char *ms_name,
                                   uint64_t fallback_us)
{
    const char *value;
    char *end = NULL;
    unsigned long long parsed;
    if (us_name && (value = storage_config_compat_getenv(us_name)) && value[0] != '\0') {
        errno = 0; parsed = strtoull(value, &end, 0);
        if (errno == 0 && end != value && *end == '\0' && parsed > 0u)
            return (uint64_t)parsed;
    }
    if (ms_name && (value = storage_config_compat_getenv(ms_name)) && value[0] != '\0') {
        errno = 0; parsed = strtoull(value, &end, 0);
        if (errno == 0 && end != value && *end == '\0' && parsed > 0u &&
            parsed <= UINT64_MAX / 1000ull)
            return (uint64_t)parsed * 1000ull;
    }
    return fallback_us;
}

static StorageStopTimeouts storage_stop_timeouts(const ChannelRuntime *rt)
{
    uint64_t compat = rt && rt->gopt.timeout_us ? rt->gopt.timeout_us : DEFAULT_TIMEOUT_US;
    StorageStopTimeouts t;
    compat = storage_timeout_us("SRC_REAL_STORAGE_STOP_TIMEOUT_US",
                               "SRC_REAL_STORAGE_STOP_TIMEOUT_MS", compat);
    t.compat_us = compat;
    t.dma_quiesce_us = storage_timeout_us("SRC_REAL_DMA_QUIESCE_TIMEOUT_US", NULL, compat);
    t.stop_harvest_us = storage_timeout_us("SRC_REAL_STOP_HARVEST_TIMEOUT_US", NULL, compat);
    t.writer_drain_us = storage_timeout_us("SRC_REAL_WRITER_DRAIN_TIMEOUT_US", NULL, compat);
    t.nvme_abort_us = storage_timeout_us("SRC_REAL_NVME_ABORT_TIMEOUT_US", NULL,
                                         t.writer_drain_us < compat ? compat : t.writer_drain_us);
    return t;
}

static uint64_t storage_first_dma_timeout_us(const ChannelConfig *cfg)
{
    char name[80];
    uint32_t fallback = storage_env_u32_limit("SRC_REAL_FIRST_DMA_TIMEOUT_US", 0u,
                                              UINT32_MAX);
    if (!cfg) return fallback;
    snprintf(name, sizeof(name), "SRC_REAL_FIRST_DMA_TIMEOUT_US_CH%d", cfg->id);
    return (uint64_t)storage_env_u32_limit(name, fallback, UINT32_MAX);
}

uint32_t storage_cross_slot_default_target_qd(int channel_id)
{
    const ChannelStorageConfig *profile = channel_id >= 0
        ? storage_config_channel(storage_config_get(), (uint32_t)channel_id)
        : NULL;

    return profile ? profile->nvme_qd : (channel_id == 2 ? 4u : 8u);
}

typedef struct {
    const char *name;
    StorageCrossSlotSourceKind kind;
} StorageCrossSlotCandidate;

static void storage_cross_slot_resolution_default(StorageCrossSlotResolution *out,
                                                  uint32_t fallback,
                                                  bool profile_source)
{
    memset(out, 0, sizeof(*out));
    out->value = fallback;
    out->source_kind = STORAGE_CROSS_SLOT_SOURCE_DEFAULT;
    (void)snprintf(out->source_name, sizeof(out->source_name), "%s",
                   profile_source ? "profile" : "default");
    (void)snprintf(out->fallback_source_name,
                   sizeof(out->fallback_source_name), "%s",
                   profile_source ? "profile" : "default");
}

const char *storage_cross_slot_source_kind_name(StorageCrossSlotSourceKind kind)
{
    switch (kind) {
    case STORAGE_CROSS_SLOT_SOURCE_CHANNEL_NEW: return "channel_new";
    case STORAGE_CROSS_SLOT_SOURCE_CHANNEL_LEGACY: return "channel_legacy";
    case STORAGE_CROSS_SLOT_SOURCE_GLOBAL_NEW: return "global_new";
    case STORAGE_CROSS_SLOT_SOURCE_GLOBAL_LEGACY: return "global_legacy";
    case STORAGE_CROSS_SLOT_SOURCE_DEFAULT: return "default";
    default: return "default";
    }
}

static bool storage_cross_slot_parse_u32(const char *value, uint32_t max_value,
                                         uint32_t *out)
{
    char *end = NULL;
    unsigned long parsed;

    if (!value || value[0] == '\0' || !out) return false;
    errno = 0;
    parsed = strtoul(value, &end, 0);
    if (errno != 0 || end == value || *end != '\0' || parsed > max_value) return false;
    *out = (uint32_t)parsed;
    return true;
}

static bool storage_cross_slot_parse_enabled(const char *value, uint32_t *out)
{
    uint32_t numeric;

    if (!value || !out) return false;
    if (strcmp(value, "true") == 0 || strcmp(value, "TRUE") == 0 ||
        strcmp(value, "on") == 0 || strcmp(value, "ON") == 0 ||
        strcmp(value, "yes") == 0 || strcmp(value, "YES") == 0) {
        *out = 1u;
        return true;
    }
    if (strcmp(value, "false") == 0 || strcmp(value, "FALSE") == 0 ||
        strcmp(value, "off") == 0 || strcmp(value, "OFF") == 0 ||
        strcmp(value, "no") == 0 || strcmp(value, "NO") == 0) {
        *out = 0u;
        return true;
    }
    if (!storage_cross_slot_parse_u32(value, UINT32_MAX, &numeric)) return false;
    *out = numeric != 0u ? 1u : 0u;
    return true;
}

static StorageCrossSlotResolution storage_cross_slot_resolve_candidates(
    uint32_t fallback, uint32_t max_value, bool enabled, bool profile_source,
    const StorageCrossSlotCandidate *candidates, size_t count)
{
    StorageCrossSlotResolution out;
    size_t i;

    storage_cross_slot_resolution_default(&out, fallback, profile_source);
    for (i = 0u; candidates && i < count; ++i) {
        const char *value = storage_config_compat_getenv(candidates[i].name);
        uint32_t parsed;

        if (!value) continue;
        if ((enabled ? storage_cross_slot_parse_enabled(value, &parsed) :
                       storage_cross_slot_parse_u32(value, max_value, &parsed))) {
            out.value = parsed;
            out.source_kind = candidates[i].kind;
            (void)snprintf(out.source_name, sizeof(out.source_name), "%s",
                           candidates[i].name);
            out.invalid_source_name[0] = '\0';
            out.fallback_source_name[0] = '\0';
            return out;
        }
        (void)snprintf(out.invalid_source_name, sizeof(out.invalid_source_name), "%s",
                       candidates[i].name);
        return out;
    }
    return out;
}

StorageCrossSlotResolution storage_cross_slot_resolve_config(
    int channel_id, StorageCrossSlotConfigParam param)
{
    char names[12][96];
    StorageCrossSlotCandidate candidates[12];
    const char *short_name = NULL;
    uint32_t fallback = 0u;
    uint32_t max_value = UINT32_MAX;
    size_t count = 0u;
    const ChannelStorageConfig *profile = NULL;
    bool profile_source = false;

#define CROSS_SLOT_ADD(kind_, format_, ...)                                    \
    do {                                                                        \
        (void)snprintf(names[count], sizeof(names[count]), format_, __VA_ARGS__); \
        candidates[count].name = names[count];                                 \
        candidates[count].kind = (kind_);                                      \
        ++count;                                                                \
    } while (0)

    if (channel_id < 0) {
        StorageCrossSlotResolution out;
        storage_cross_slot_resolution_default(&out, 0u, false);
        return out;
    }
    profile = storage_config_channel(storage_config_get(),
                                     (uint32_t)channel_id);
    switch (param) {
    case STORAGE_CROSS_SLOT_CONFIG_ENABLED:
        fallback = profile
                       ? (profile->writer_mode == STORAGE_WRITER_CROSS_SLOT
                              ? 1u : 0u)
                       : (channel_id == 2 ? 0u : 1u);
        profile_source = profile != NULL;
        CROSS_SLOT_ADD(STORAGE_CROSS_SLOT_SOURCE_CHANNEL_NEW,
                       "SRC_REAL_CROSS_SLOT_CH%d", channel_id);
        CROSS_SLOT_ADD(STORAGE_CROSS_SLOT_SOURCE_CHANNEL_LEGACY,
                       "SRC_REAL_CROSS_SLOT_ENABLED_CH%d", channel_id);
        CROSS_SLOT_ADD(STORAGE_CROSS_SLOT_SOURCE_CHANNEL_LEGACY,
                       "SRC_REAL_NVME_CROSS_SLOT_QD_CH%d", channel_id);
        CROSS_SLOT_ADD(STORAGE_CROSS_SLOT_SOURCE_GLOBAL_NEW, "%s", "SRC_REAL_CROSS_SLOT");
        CROSS_SLOT_ADD(STORAGE_CROSS_SLOT_SOURCE_GLOBAL_LEGACY,
                       "%s", "SRC_REAL_CROSS_SLOT_ENABLED");
        CROSS_SLOT_ADD(STORAGE_CROSS_SLOT_SOURCE_GLOBAL_LEGACY,
                       "%s", "SRC_REAL_NVME_CROSS_SLOT_QD");
        return storage_cross_slot_resolve_candidates(fallback, 1u, true,
                                                     profile_source,
                                                     candidates, count);
    case STORAGE_CROSS_SLOT_CONFIG_MAX_ACTIVE:
        fallback = profile ? profile->max_active_slots
                           : (channel_id == 2 ? 1u : 4u);
        profile_source = profile != NULL;
        max_value = 64u;
        CROSS_SLOT_ADD(STORAGE_CROSS_SLOT_SOURCE_CHANNEL_NEW,
                       "SRC_REAL_MAX_ACTIVE_CH%d", channel_id);
        CROSS_SLOT_ADD(STORAGE_CROSS_SLOT_SOURCE_CHANNEL_LEGACY,
                       "SRC_REAL_CROSS_SLOT_BATCH_CH%d", channel_id);
        CROSS_SLOT_ADD(STORAGE_CROSS_SLOT_SOURCE_CHANNEL_LEGACY,
                       "SRC_REAL_NVME_CROSS_SLOT_MAX_ACTIVE_CH%d", channel_id);
        CROSS_SLOT_ADD(STORAGE_CROSS_SLOT_SOURCE_CHANNEL_LEGACY,
                       "SRC_REAL_NVME_CROSS_SLOT_BATCH_CH%d", channel_id);
        CROSS_SLOT_ADD(STORAGE_CROSS_SLOT_SOURCE_CHANNEL_LEGACY,
                       "SRC_REAL_CH%d_NVME_CROSS_SLOT_MAX_ACTIVE", channel_id);
        CROSS_SLOT_ADD(STORAGE_CROSS_SLOT_SOURCE_CHANNEL_LEGACY,
                       "SRC_REAL_CH%d_CROSS_SLOT_BATCH", channel_id);
        CROSS_SLOT_ADD(STORAGE_CROSS_SLOT_SOURCE_GLOBAL_NEW, "%s", "SRC_REAL_MAX_ACTIVE");
        CROSS_SLOT_ADD(STORAGE_CROSS_SLOT_SOURCE_GLOBAL_LEGACY,
                       "%s", "SRC_REAL_CROSS_SLOT_BATCH");
        CROSS_SLOT_ADD(STORAGE_CROSS_SLOT_SOURCE_GLOBAL_LEGACY,
                       "%s", "SRC_REAL_CROSS_SLOT_MAX_ACTIVE");
        CROSS_SLOT_ADD(STORAGE_CROSS_SLOT_SOURCE_GLOBAL_LEGACY,
                       "%s", "SRC_REAL_NVME_CROSS_SLOT_MAX_ACTIVE");
        CROSS_SLOT_ADD(STORAGE_CROSS_SLOT_SOURCE_GLOBAL_LEGACY,
                       "%s", "SRC_REAL_NVME_CROSS_SLOT_BATCH");
        return storage_cross_slot_resolve_candidates(fallback, max_value, false,
                                                     profile_source,
                                                     candidates, count);
    case STORAGE_CROSS_SLOT_CONFIG_TARGET_QD:
        short_name = "TARGET_QD";
        fallback = profile ? profile->nvme_qd
                           : storage_cross_slot_default_target_qd(channel_id);
        profile_source = profile != NULL;
        max_value = 64u;
        break;
    case STORAGE_CROSS_SLOT_CONFIG_CQ_BATCH:
        short_name = "CQ_BATCH";
        fallback = profile ? profile->cq_batch : 32u;
        profile_source = profile != NULL;
        max_value = 64u;
        break;
    case STORAGE_CROSS_SLOT_CONFIG_WRITER_BUDGET_US:
        short_name = "WRITER_BUDGET_US";
        fallback = 300u;
        max_value = 1000000u;
        break;
    case STORAGE_CROSS_SLOT_CONFIG_BUSY_POLL_US:
        short_name = "BUSY_POLL_US";
        fallback = 20u;
        max_value = 1000000u;
        break;
    case STORAGE_CROSS_SLOT_CONFIG_EMPTY_SLEEP_US:
        short_name = "EMPTY_SLEEP_US";
        fallback = 1u;
        max_value = 1000000u;
        break;
    case STORAGE_CROSS_SLOT_CONFIG_NO_PROGRESS_TIMEOUT_US:
        short_name = "NO_PROGRESS_TIMEOUT_US";
        fallback = 5000000u;
        break;
    default: {
        StorageCrossSlotResolution out;
        storage_cross_slot_resolution_default(&out, 0u, false);
        return out;
    }
    }

    CROSS_SLOT_ADD(STORAGE_CROSS_SLOT_SOURCE_CHANNEL_NEW,
                   "SRC_REAL_%s_CH%d", short_name, channel_id);
    CROSS_SLOT_ADD(STORAGE_CROSS_SLOT_SOURCE_CHANNEL_LEGACY,
                   "SRC_REAL_CH%d_%s", channel_id, short_name);
    CROSS_SLOT_ADD(STORAGE_CROSS_SLOT_SOURCE_CHANNEL_LEGACY,
                   "SRC_REAL_NVME_CROSS_SLOT_%s_CH%d", short_name, channel_id);
    CROSS_SLOT_ADD(STORAGE_CROSS_SLOT_SOURCE_GLOBAL_NEW, "SRC_REAL_%s", short_name);
    CROSS_SLOT_ADD(STORAGE_CROSS_SLOT_SOURCE_GLOBAL_LEGACY,
                   "SRC_REAL_NVME_CROSS_SLOT_%s", short_name);
    return storage_cross_slot_resolve_candidates(fallback, max_value, false,
                                                 profile_source,
                                                 candidates, count);

#undef CROSS_SLOT_ADD
}

uint32_t storage_cross_slot_active_slots_for_channel(int channel_id)
{
    return storage_cross_slot_resolve_config(
        channel_id, STORAGE_CROSS_SLOT_CONFIG_MAX_ACTIVE).value;
}

bool storage_cross_slot_enabled_for_channel(int channel_id)
{
    return storage_cross_slot_resolve_config(
        channel_id, STORAGE_CROSS_SLOT_CONFIG_ENABLED).value != 0u;
}

static int storage_env_flag_enabled(const char *name)
{
    const char *value = storage_config_compat_getenv(name);

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

static FILE *storage_slot_perf_log_open(void)
{
    static bool initialized = false;
    static FILE *log = NULL;
    const char *path;

    if (initialized) {
        return log;
    }
    initialized = true;
    path = storage_config_compat_getenv("SRC_REAL_SLOT_WRITE_PERF_LOG");
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
    const ChannelStorageConfig *profile;

    if (!rt) {
        return 0u;
    }
    snprintf(name, sizeof(name), "SRC_REAL_CH%d_WRITER_RT_PRIO", rt->cfg->id);
    if (storage_config_compat_getenv(name)) {
        return storage_env_u32_limit(name, 0u, 99u);
    }
    profile = storage_config_channel(storage_config_get(),
                                     (uint32_t)rt->cfg->id);
    return profile ? profile->writer_priority : 0u;
}

static uint32_t storage_producer_rt_prio(const ChannelRuntime *rt)
{
    char name[64];
    const ChannelStorageConfig *profile;

    if (!rt) {
        return 0u;
    }
    snprintf(name, sizeof(name), "SRC_REAL_CH%d_PRODUCER_RT_PRIO", rt->cfg->id);
    if (storage_config_compat_getenv(name)) {
        return storage_env_u32_limit(name, 0u, 99u);
    }
    profile = storage_config_channel(storage_config_get(),
                                     (uint32_t)rt->cfg->id);
    return profile ? profile->producer_priority : 0u;
}

static int storage_profile_rt_policy(const ChannelRuntime *rt, bool writer)
{
    const ChannelStorageConfig *profile;

    if (!rt || !rt->cfg) return SCHED_OTHER;
    profile = storage_config_channel(storage_config_get(),
                                     (uint32_t)rt->cfg->id);
    if (!profile) return SCHED_OTHER;
    return (writer ? profile->writer_realtime : profile->producer_realtime)
               ? SCHED_RR : SCHED_OTHER;
}

static int storage_rt_policy(const char *name, int fallback)
{
    const char *value = storage_config_compat_getenv(name);

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
    fprintf(stderr, "warning: invalid %s=%s; fallback=%s\n", name, value,
            fallback == SCHED_RR ? "rr" :
            (fallback == SCHED_FIFO ? "fifo" : "other"));
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
    env = storage_config_compat_getenv(name);
    if (!env || env[0] == '\0') {
        env = storage_config_compat_getenv("SRC_REAL_STORAGE_RING_BYTES");
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

static bool storage_require_rt(void)
{
    return storage_env_flag_enabled("SRC_REAL_REQUIRE_RT") != 0;
}

static int storage_apply_writer_rt(StorageWriteQueue *q)
{
    struct sched_param sp;
    uint32_t prio;
    int policy;
    int rc;

    if (!q || !q->rt) {
        return -1;
    }
    prio = storage_writer_rt_prio(q->rt);
    __atomic_store_n(&q->writer_rt_prio, prio, __ATOMIC_RELEASE);
    if (prio == 0u) {
        __atomic_store_n(&q->writer_rt_policy, SCHED_OTHER, __ATOMIC_RELEASE);
        __atomic_store_n(&q->writer_rt_enabled, false, __ATOMIC_RELEASE);
        storage_emit_line(STORAGE_LOG_SUMMARY, "storage_writer_scheduler channel=%d effective_policy=other"
                          " effective_prio=0 rt_enabled=0",
                          q->rt->cfg->id);
        return 0;
    }
    memset(&sp, 0, sizeof(sp));
    policy = storage_rt_policy("SRC_REAL_WRITER_RT_POLICY",
                               storage_profile_rt_policy(q->rt, true));
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
        __atomic_store_n(&q->writer_rt_enabled, false, __ATOMIC_RELEASE);
        storage_emit_line(STORAGE_LOG_SUMMARY, "storage_rt_fallback channel=%d role=writer requested_policy=%s"
                          " requested_prio=%u effective_policy=other effective_prio=0 reason=%s",
                          q->rt->cfg->id, storage_rt_policy_name(policy), (unsigned)prio,
                          storage_require_rt() ? "required" : "setschedparam_failed");
        return storage_require_rt() ? -1 : 0;
    }
    storage_emit_line(STORAGE_LOG_SUMMARY, "storage_writer_scheduler channel=%d effective_policy=%s"
                      " effective_prio=%u rt_enabled=%u",
                      q->rt->cfg->id,
                      storage_rt_policy_name(__atomic_load_n(&q->writer_rt_policy,
                                                              __ATOMIC_ACQUIRE)),
                      (unsigned)__atomic_load_n(&q->writer_rt_prio, __ATOMIC_ACQUIRE),
                      __atomic_load_n(&q->writer_rt_enabled, __ATOMIC_ACQUIRE) ? 1u : 0u);
    return 0;
}

static int storage_apply_producer_rt(const ChannelRuntime *rt,
                                     int *effective_policy,
                                     uint32_t *effective_prio)
{
    struct sched_param sp;
    uint32_t producer_prio;
    uint32_t requested_prio;
    int policy;
    int requested_policy;
    int rc;

    if (!rt) return -1;
    producer_prio = storage_producer_rt_prio(rt);
    policy = storage_rt_policy("SRC_REAL_PRODUCER_RT_POLICY",
                               storage_profile_rt_policy(rt, false));
    requested_prio = producer_prio;
    requested_policy = policy;
    memset(&sp, 0, sizeof(sp));
    sp.sched_priority = policy == SCHED_OTHER ? 0 : (int)producer_prio;
    rc = pthread_setschedparam(pthread_self(), policy, &sp);
    if (rc != 0) {
        fprintf(stderr,
                "warning: producer RT setup failed channel=%d policy=%s prio=%u errno=%d\n",
                rt->cfg->id, storage_rt_policy_name(policy), producer_prio, rc);
        policy = SCHED_OTHER;
        producer_prio = 0u;
        if (effective_policy) *effective_policy = policy;
        if (effective_prio) *effective_prio = producer_prio;
        storage_emit_line(STORAGE_LOG_SUMMARY, "storage_rt_fallback channel=%d role=producer requested_policy=%s"
                          " requested_prio=%u effective_policy=other effective_prio=0 reason=%s",
                          rt->cfg->id, storage_rt_policy_name(requested_policy),
                          (unsigned)requested_prio,
                          storage_require_rt() ? "required" : "setschedparam_failed");
        return storage_require_rt() ? -1 : 0;
    }
    if (effective_policy) *effective_policy = policy;
    if (effective_prio) *effective_prio = producer_prio;
    storage_emit_line(STORAGE_LOG_SUMMARY, "storage_producer_scheduler channel=%d effective_policy=%s"
                      " effective_prio=%u rt_enabled=%u",
                      rt->cfg->id, storage_rt_policy_name(policy),
                      (unsigned)producer_prio, policy != SCHED_OTHER ? 1u : 0u);
    return 0;
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
    const char *value = storage_config_compat_getenv(name);
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

static int storage_local_slot_transition_locked(StorageWriteQueue *q, uint32_t slot,
                                                StorageSlotState expected, StorageSlotState next)
{
    if (!q || storage_slot_transition(&q->slots, slot, expected, next) != 0) {
        if (q) q->error = true;
        return -1;
    }
    return 0;
}

/* A harvester can consume a descriptor before discovering a malformed length
 * or range.  Keep that slot explicitly accounted as unharvested rather than
 * silently returning it to the writable pool; the later stop audit will then
 * refuse an unsafe reset. */
static void storage_mark_harvest_slot_failed(StorageWriteQueue *q, uint32_t slot)
{
    if (!q || slot >= q->capacity) return;
    pthread_mutex_lock(&q->lock);
    if (storage_slot_state(&q->slots, slot) == STORAGE_SLOT_DMA_WRITABLE)
        (void)storage_local_slot_transition_locked(
            q, slot, STORAGE_SLOT_DMA_WRITABLE, STORAGE_SLOT_DMA_COMPLETED_UNHARVESTED);
    q->error = true;
    pthread_cond_broadcast(&q->not_empty);
    pthread_cond_broadcast(&q->not_full);
    pthread_mutex_unlock(&q->lock);
}

/* A completed STOP tail is software-owned after harvest.  When it cannot be
 * padded safely it must not enter NVMe, but it also must not remain counted as
 * hardware-writable or be requeued after the STOP latch. */
static int storage_release_harvested_slot(StorageWriteQueue *q, uint32_t slot)
{
    int rc;

    if (!q || slot >= q->capacity) return -1;
    pthread_mutex_lock(&q->lock);
    rc = storage_local_slot_transition_locked(q, slot,
                                              STORAGE_SLOT_DMA_WRITABLE,
                                              STORAGE_SLOT_FREE);
    pthread_mutex_unlock(&q->lock);
    return rc;
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
    if (!q->items || storage_slot_table_init(&q->slots, rt->dma_desc_count) != 0) {
        free(q->items);
        storage_slot_table_destroy(&q->slots);
        q->items = NULL;
        return -1;
    }
    q->rt = rt;
    q->capacity = rt->dma_desc_count;
    if (storage_event_ring_init(&q->producer_event_ring,
                                storage_env_u32_limit("SRC_REAL_EVENT_RING_SIZE", 1024u, 65536u)) != 0 ||
        storage_event_ring_init(&q->writer_event_ring,
                                storage_env_u32_limit("SRC_REAL_EVENT_RING_SIZE", 1024u, 65536u)) != 0) {
        storage_event_ring_destroy(&q->producer_event_ring);
        storage_event_ring_destroy(&q->writer_event_ring);
        free(q->items);
        storage_slot_table_destroy(&q->slots);
        return -1;
    }
    q->file_index = file_index;
    q->metadata_slot = metadata_slot;
    q->task_no = task_no;
    q->cross_slot_qd = cross_slot_qd;
    q->cross_slot_batch = cross_slot_batch;
    q->nvme_engine_quiesced = true;
    q->requeue_enabled = true;
    q->worker_state = WORKER_INIT;
    q->backlog_mode = storage_env_flag_enabled("SRC_REAL_WRITER_BACKLOG_MODE") != 0;
    if (pthread_mutex_init(&q->lock, NULL) != 0 ||
        pthread_cond_init(&q->not_empty, NULL) != 0 ||
        pthread_cond_init(&q->not_full, NULL) != 0) {
        free(q->items);
        storage_slot_table_destroy(&q->slots);
        q->items = NULL;
        return -1;
    }
    return 0;
}

static void storage_queue_destroy(StorageWriteQueue *q) {
    if (!q) {
        return;
    }
    if (!q->nvme_engine_quiesced) return;
    (void)pthread_cond_destroy(&q->not_full);
    (void)pthread_cond_destroy(&q->not_empty);
    (void)pthread_mutex_destroy(&q->lock);
    free(q->items);
    storage_slot_table_destroy(&q->slots);
    storage_event_ring_destroy(&q->producer_event_ring);
    storage_event_ring_destroy(&q->writer_event_ring);
    q->items = NULL;
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


/* Producer never waits for writer capacity: validate the complete harvest first. */
static int storage_local_queue_push_batch(StorageWriteQueue *q, const PendingDdrSlot *items,
                                    uint32_t item_count)
{
    uint32_t i, j, tail;
    uint64_t batch_bytes = 0u;
    if (!q || !items || item_count == 0u) return -1;
    pthread_mutex_lock(&q->lock);
    if (q->error || q->rt->dma_desc_bytes == 0u ||
        !storage_queue_slot_counts_valid_locked(q) ||
        item_count > q->capacity - q->count ||
        item_count > q->slots.counts.dma_writable ||
        item_count == 0u) goto bad;
    for (i = 0u; i < item_count; ++i) {
        uint64_t expected_sectors;
        uint64_t ddr_end;
        uint64_t expected_hw_addr;
        if (items[i].bytes == 0u || items[i].bytes > q->rt->dma_desc_bytes ||
            items[i].sectors == 0u || items[i].slot >= q->capacity ||
            storage_slot_state(&q->slots, items[i].slot) !=
                STORAGE_SLOT_DMA_WRITABLE) goto bad;
        expected_sectors = bytes_to_sectors(items[i].bytes);
        if ((uint64_t)items[i].slot >
            (UINT64_MAX - q->rt->cfg->ddr_hw_base) / q->rt->dma_desc_bytes) goto bad;
        expected_hw_addr = q->rt->cfg->ddr_hw_base +
                           (uint64_t)items[i].slot * q->rt->dma_desc_bytes;
        if (items[i].media_bytes == 0u ||
            items[i].media_bytes > q->rt->dma_desc_bytes ||
            items[i].media_bytes != expected_sectors * (uint64_t)SECTOR_SIZE ||
            items[i].sectors != expected_sectors ||
            items[i].start_lba > UINT64_MAX - items[i].sectors ||
            (q->rt->nvme_max_lba != 0u &&
             !nvme_lba_range_valid(q->rt, items[i].start_lba, items[i].sectors)) ||
            items[i].hw_addr != expected_hw_addr ||
            items[i].hw_addr > UINT64_MAX - items[i].media_bytes) goto bad;
        ddr_end = items[i].hw_addr + items[i].media_bytes;
        if (q->rt->cfg->ddr_hw_base > UINT64_MAX - q->rt->dma_ring_bytes ||
            ddr_end > q->rt->cfg->ddr_hw_base + q->rt->dma_ring_bytes ||
            batch_bytes > UINT64_MAX - items[i].bytes) goto bad;
        batch_bytes += items[i].bytes;
        for (j = 0u; j < i; ++j) if (items[j].slot == items[i].slot) goto bad;
    }
    if (q->buffered_bytes > UINT64_MAX - batch_bytes ||
        q->queued_slot_count > UINT64_MAX - item_count) goto bad;
    /* All validation is complete. No operation below may fail. */
    tail = q->tail;
    for (i = 0u; i < item_count; ++i) {
        uint32_t slot = items[i].slot;
        q->items[tail] = items[i];
        tail = (tail + 1u) % q->capacity;
        if (storage_local_slot_transition_locked(
                q, slot, STORAGE_SLOT_DMA_WRITABLE,
                STORAGE_SLOT_DMA_COMPLETED_UNHARVESTED) != 0 ||
            storage_local_slot_transition_locked(
                q, slot, STORAGE_SLOT_DMA_COMPLETED_UNHARVESTED,
                STORAGE_SLOT_READY_FOR_NVME) != 0)
            goto bad;
    }
    q->tail = tail;
    q->count += item_count;
    q->buffered_bytes += batch_bytes;
    q->queued_slot_count += item_count;
    {
        uint32_t busy_count = storage_slot_busy_count(&q->slots);
        if (busy_count > q->max_busy_count) q->max_busy_count = busy_count;
    }
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
    int rc = 0;

    if (!q) {
        return;
    }
    pthread_mutex_lock(&q->lock);
    if (q->worker_state == WORKER_RUNNING) {
        rc = storage_queue_transition_locked(q, WORKER_RUNNING,
                                             WORKER_DRAINING);
    } else if (q->worker_state == WORKER_HARVESTING) {
        rc = storage_queue_transition_locked(q, WORKER_HARVESTING,
                                             WORKER_DRAINING);
    } else if (q->worker_state != WORKER_DRAINING &&
               q->worker_state != WORKER_FINALIZING &&
               q->worker_state != WORKER_DONE &&
               q->worker_state != WORKER_FAILED) {
        storage_queue_fail_locked(q);
        rc = -1;
    }
    pthread_cond_broadcast(&q->not_empty);
    pthread_mutex_unlock(&q->lock);
    if (rc != 0) storage_write_request_stop();
}

/* STOP first moves RUNNING to WAIT_BOUNDARY.  A final completion may still
 * drain, but storage_worker_can_requeue() becomes false at this point. */
static int storage_queue_request_stop_state(StorageWriteQueue *q)
{
    int rc = 0;

    if (!q) return -1;
    pthread_mutex_lock(&q->lock);
    if (q->worker_state == WORKER_RUNNING) {
        rc = storage_queue_transition_locked(q, WORKER_RUNNING,
                                             WORKER_WAIT_BOUNDARY);
    } else if (!storage_worker_stop_latched(q->worker_state)) {
        storage_queue_fail_locked(q);
        rc = -1;
    }
    pthread_cond_broadcast(&q->not_empty);
    pthread_mutex_unlock(&q->lock);
    return rc;
}

static bool storage_queue_latch_stop(StorageWriteQueue *q)
{
    bool first = false;
    if (!q) return false;
    pthread_mutex_lock(&q->lock);
    if (q->worker_state == WORKER_RUNNING)
        (void)storage_queue_transition_locked(q, WORKER_RUNNING,
                                              WORKER_WAIT_BOUNDARY);
    if (q->worker_state == WORKER_WAIT_BOUNDARY) {
        if (storage_queue_transition_locked(q, WORKER_WAIT_BOUNDARY,
                                            WORKER_DMA_QUIESCING) == 0)
            first = true;
    }
    q->requeue_enabled = false;
    /* Serialize the software latch with a completion's requeue.  The
     * DMA-side latch is lock-free, so it is safe to publish it while the
     * queue lock is held and gives stop a single linearization point. */
    if (q->rt) dma_latch_stop(q->rt);
    pthread_cond_broadcast(&q->not_empty);
    pthread_cond_broadcast(&q->not_full);
    pthread_mutex_unlock(&q->lock);
    return first;
}

static int storage_queue_mark_harvesting(StorageWriteQueue *q)
{
    int rc;

    if (!q) return -1;
    pthread_mutex_lock(&q->lock);
    rc = q->worker_state == WORKER_FAILED
             ? 0
             : storage_queue_transition_locked(q, WORKER_DMA_QUIESCING,
                                                WORKER_HARVESTING);
    pthread_cond_broadcast(&q->not_empty);
    pthread_mutex_unlock(&q->lock);
    return rc;
}

static int storage_queue_mark_finalizing(StorageWriteQueue *q)
{
    int rc;

    if (!q) return -1;
    pthread_mutex_lock(&q->lock);
    rc = storage_queue_transition_locked(q, WORKER_DRAINING,
                                         WORKER_DRAINED_WAIT_STOP);
    if (rc == 0)
        rc = storage_queue_transition_locked(q, WORKER_DRAINED_WAIT_STOP,
                                             WORKER_FINALIZING);
    pthread_mutex_unlock(&q->lock);
    return rc;
}

static int storage_queue_mark_done(StorageWriteQueue *q)
{
    int rc;

    if (!q) return -1;
    pthread_mutex_lock(&q->lock);
    rc = storage_queue_transition_locked(q, WORKER_FINALIZING, WORKER_DONE);
    pthread_mutex_unlock(&q->lock);
    return rc;
}

static void storage_queue_set_engine(StorageWriteQueue *q,
                                     NvmeCrossSlotEngine *engine)
{
    bool abort_requested;

    if (!q) return;
    pthread_mutex_lock(&q->lock);
    q->nvme_engine = engine;
    abort_requested = q->worker_state == WORKER_FAILED;
    pthread_mutex_unlock(&q->lock);
    if (engine && abort_requested) {
        nvme_cross_slot_engine_request_abort(engine, "writer_abort_requested");
    }
}

/* Clear the published pointer before destroying the engine.  The producer
 * may be requesting an abort concurrently; it must never retain a pointer
 * after the writer has released the engine. */
static void storage_queue_destroy_engine(StorageWriteQueue *q,
                                         NvmeCrossSlotEngine *engine)
{
    bool owned = false;

    if (!q || !engine) return;
    pthread_mutex_lock(&q->lock);
    if (q->nvme_engine == engine) {
        q->nvme_engine = NULL;
        owned = true;
    }
    pthread_mutex_unlock(&q->lock);
    if (owned) nvme_cross_slot_engine_destroy(engine);
}

static void storage_queue_request_abort(StorageWriteQueue *q)
{
    NvmeCrossSlotEngine *engine;
    if (!q) return;
    pthread_mutex_lock(&q->lock);
    storage_queue_fail_locked(q);
    engine = q->nvme_engine;
    /* Keep the engine published while the abort state is latched.  The
     * writer cannot clear/destroy it until this lock is released, avoiding a
     * use-after-free between the two lifecycle paths. */
    if (engine)
        nvme_cross_slot_engine_request_abort(engine, "writer_abort_requested");
    pthread_cond_broadcast(&q->not_empty);
    pthread_cond_broadcast(&q->not_full);
    pthread_mutex_unlock(&q->lock);
}

static bool storage_queue_abort_requested(StorageWriteQueue *q)
{
    bool value = false;
    if (!q) return true;
    pthread_mutex_lock(&q->lock);
    value = q->worker_state == WORKER_FAILED;
    pthread_mutex_unlock(&q->lock);
    return value;
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
    busy_count = storage_slot_busy_count(&q->slots);
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
    out->busy_count = storage_slot_busy_count(&q->slots);
    out->buffered_bytes = q->buffered_bytes;
    out->writer_idle_us = q->writer_idle_us;
    out->queue_empty_wait_us = q->queue_empty_wait_us;
    out->writer_active_us = q->writer_active_us;
    out->ready_q_nonempty_us = q->ready_q_nonempty_us;
    out->writer_drain_loop_count = q->writer_drain_loop_count;
    out->writer_slots_drained = q->writer_slots_drained;
    out->queued_slot_count = q->queued_slot_count;
    out->completed_slot_count = q->completed_slot_count;
    out->recycled_slot_count = q->recycled_slot_count;
    out->writer_schedule_gap_count = q->writer_schedule_gap_count;
    out->writer_schedule_gap_max_us = q->writer_schedule_gap_max_us;
    out->writer_no_progress_sleep_count = q->writer_no_progress_sleep_count;
    out->writer_rt_enabled = __atomic_load_n(&q->writer_rt_enabled, __ATOMIC_ACQUIRE);
    out->writer_rt_policy = __atomic_load_n(&q->writer_rt_policy, __ATOMIC_ACQUIRE);
    out->writer_rt_prio = __atomic_load_n(&q->writer_rt_prio, __ATOMIC_ACQUIRE);
    out->free_slots = q->slots.counts.free_count;
    out->dma_writable_slots = q->slots.counts.dma_writable;
    out->completed_unharvested_slots = q->slots.counts.completed_unharvested;
    out->ready_for_nvme_slots = q->slots.counts.ready;
    out->nvme_busy_slots = q->slots.counts.nvme_busy;
    out->requeue_pending_slots = q->slots.counts.requeue_pending;
    if (!storage_queue_slot_counts_valid_locked(q)) q->error = true;
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
    rc = dma_get_bd_snapshot_o1((ChannelRuntime *)rt, &q->slots.counts, out);
    pthread_mutex_unlock(&q->lock);
    if (rc != 0) {
        storage_record_failure(stats, "slot_ownership_invariant_failed");
        storage_ring_event(&q->producer_event_ring, STORAGE_EVENT_SLOT_STATE_ERROR,
                           rt, 0u, 0u, true);
        return -1;
    }
    stats->last_bd_snapshot = *out;
    stats->last_bd_snapshot_us = now_us;
    state_total = out->dma_writable + out->completed_unharvested + out->ready_slots +
                  out->nvme_busy_slots + out->requeue_pending + out->free_slots;
    if (state_total != out->total_slots) {
        storage_record_failure(stats, "slot_ownership_invariant_failed");
        storage_emit_line(STORAGE_LOG_ALWAYS_CRITICAL, "storage_receive_failed channel=%d task=%s file_index=%u"
                          " reason=slot_ownership_invariant_failed received_bytes=%" PRIu64,
                          rt->cfg->id, task_no, (unsigned)file_index, received_bytes);
        return -1;
    }
    storage_stats_observe_bd_snapshot(stats, out);
    {
        uint32_t low_threshold = storage_env_u32_limit("SRC_REAL_DMA_BD_LOW_WATERMARK",
                                                       4u, out->total_slots);
        if (out->dma_writable <= low_threshold && out->dma_writable > 0u &&
            !stats->dma_bd_low_active) {
            stats->dma_bd_low_active = true;
            storage_emit_line(STORAGE_LOG_DEBUG, "dma_bd_low channel=%d task=%s file_index=%u"
                              " dma_writable=%u threshold=%u completed_unharvested=%u"
                              " occupied_bytes_est=%" PRIu64,
                              rt->cfg->id, task_no, (unsigned)file_index,
                              out->dma_writable, low_threshold,
                              out->completed_unharvested, out->occupied_bytes_est);
            storage_ring_event(&q->producer_event_ring, STORAGE_EVENT_DMA_BD_LOW, rt,
                               out->dma_writable, out->completed_unharvested, false);
        } else if (out->dma_writable > low_threshold) {
            stats->dma_bd_low_active = false;
        }
    }
    if ((out->s2mm_dmasr & 0x00004770u) != 0u ||
        (!rt->gopt.dry_run && (out->s2mm_dmasr & 1u) != 0u)) {
        ++stats->dma_error_count;
        storage_record_failure(stats, "dma_error_or_halted");
        storage_ring_event(&q->producer_event_ring, STORAGE_EVENT_DMA_ERROR, rt,
                           out->s2mm_dmasr, 0u, true);
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
                storage_emit_line(STORAGE_LOG_ALWAYS_CRITICAL, "storage_ddr_full channel=%d task=%s file_index=%u"
                                  " received_bytes=%" PRIu64 " completed_unharvested=%u"
                                  " ready_slots=%u nvme_busy_slots=%u requeue_pending=%u"
                                  " curdesc=%u taildesc=%u s2mm_dmasr=0x%08x",
                                  rt->cfg->id, task_no, (unsigned)file_index,
                                  received_bytes, out->completed_unharvested,
                                  out->ready_slots, out->nvme_busy_slots,
                                  out->requeue_pending, out->curdesc_index,
                                  out->taildesc_index, out->s2mm_dmasr);
                storage_emit_line(STORAGE_LOG_ALWAYS_CRITICAL, "storage_receive_failed channel=%d task=%s file_index=%u"
                                  " reason=dma_bd_exhausted_no_upstream_backpressure"
                                  " received_bytes=%" PRIu64 " dma_writable=0"
                                  " completed_unharvested=%u s2mm_dmasr=0x%08x",
                                  rt->cfg->id, task_no, (unsigned)file_index,
                                  received_bytes, out->completed_unharvested,
                                  out->s2mm_dmasr);
            }
        }
        stats->integrity_risk_ring_full = true;
        storage_ring_event(&q->producer_event_ring, STORAGE_EVENT_DMA_BD_EXHAUSTED, rt,
                           out->completed_unharvested, out->ready_slots, true);
        storage_record_failure(stats, "dma_bd_exhausted_no_upstream_backpressure");
        return -1;
    }
    if (!stats->receive_integrity_ok) {
        if (stats->first_receive_failure_us == 0u) {
            stats->first_receive_failure_us = storage_wall_time_us();
            stats->first_receive_failure_bytes = received_bytes;
            stats->first_failure_snapshot = *out;
            storage_emit_line(STORAGE_LOG_ALWAYS_CRITICAL, "storage_receive_failed channel=%d task=%s file_index=%u"
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

static const char *storage_watermark_name(uint32_t level)
{
    switch (level) {
    case 1u:
        return "warning";
    case 2u:
        return "critical";
    case 3u:
        return "full";
    default:
        return "normal";
    }
}

static bool storage_update_ring_pressure(StorageProducerStats *stats,
                                         StorageWriteQueue *q,
                                         const ChannelRuntime *rt,
                                         const char *task_no,
                                         uint32_t file_index,
                                         uint64_t dma_received_bytes)
{
    uint32_t busy_slots;
    uint32_t level;
    uint32_t previous_level;
    uint64_t buffered_bytes;
    uint64_t now_us;
    bool should_stop;

    if (!stats || !q || !rt) {
        return false;
    }
    busy_slots = storage_queue_busy_count(q, NULL);
    buffered_bytes = storage_queue_buffered_bytes(q, NULL);
    level = storage_ring_pressure_level(busy_slots, rt->dma_desc_count,
                                        stats->ring_warning_percent,
                                        stats->ring_critical_percent);
    now_us = storage_wall_time_us();
    previous_level = stats->watermark_level;
    stats->watermark_level = level;
    __atomic_store_n(&q->writer_budget_override_us,
                     level == 0u
                         ? 0u
                         : storage_writer_budget_for_pressure(
                               q->cross_slot_config.writer_budget_us, level),
                     __ATOMIC_RELEASE);
    if (level >= 2u) {
        if (stats->ring_critical_since_us == 0u)
            stats->ring_critical_since_us = now_us;
    } else {
        stats->ring_critical_since_us = 0u;
    }
    should_stop = !stats->ring_critical_stop_triggered &&
                  storage_ring_pressure_should_stop(
                      level, stats->ring_critical_since_us, now_us,
                      stats->ring_critical_duration_us,
                      stats->ring_critical_stop_enabled);
    if (should_stop) {
        stats->ring_critical_stop_triggered = true;
        storage_emit_line(
            STORAGE_LOG_SUMMARY,
            "storage_ring_pressure_stop channel=%d task=%s file_index=%u"
            " level=%s busy_slots=%u total_slots=%u"
            " critical_duration_us=%" PRIu64,
            rt->cfg->id, task_no, (unsigned)file_index,
            storage_watermark_name(level), (unsigned)busy_slots,
            (unsigned)rt->dma_desc_count, stats->ring_critical_duration_us);
    }

    if (level == 0u || level == 3u || level <= previous_level) {
        return should_stop;
    }
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
    return should_stop;
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
    bool perf_enabled;
    bool periodic_due;
    bool perf_due;
    StorageQueueSnapshot snapshot;
    DmaBdSnapshot bd_snapshot;

    perf_enabled = storage_config_compat_getenv("SRC_REAL_PERF_LOG_ENABLE") == NULL ||
                   storage_env_flag_enabled("SRC_REAL_PERF_LOG_ENABLE") != 0;
    if (!stats) {
        return;
    }
    periodic_due = stats->interval_ms != 0u && now_us >= stats->next_log_us &&
                   storage_should_print_periodic_stats((ChannelRuntime *)rt);
    perf_due = perf_enabled && now_us >= stats->perf_next_us;
    if (!periodic_due && !perf_due) return;
    storage_stats_finish_harvest_batch(stats);
    storage_queue_snapshot(q, &snapshot);
    pthread_mutex_lock(&q->lock);
    if (dma_get_bd_snapshot_o1((ChannelRuntime *)rt, &q->slots.counts,
                               &bd_snapshot) != 0) {
        memset(&bd_snapshot, 0, sizeof(bd_snapshot));
    }
    pthread_mutex_unlock(&q->lock);
    storage_stats_observe_bd_snapshot(stats, &bd_snapshot);
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

    if (perf_due) {
        storage_emit_perf_event(rt, stats, q, &bd_snapshot, stats->perf_window_start_us,
                                now_us, received_bytes - stats->perf_window_received_bytes,
                                nvme_bytes - stats->perf_window_nvme_bytes);
        stats->perf_window_start_us = now_us;
        stats->perf_window_received_bytes = received_bytes;
        stats->perf_window_nvme_bytes = nvme_bytes;
        stats->perf_next_us = now_us + (uint64_t)storage_perf_log_interval_ms() * 1000ull;
    }
    if (!periodic_due) return;

    if (!print_zero_stats && received_delta == 0u && nvme_delta == 0u) {
        if (!stats->idle_printed && received_bytes == 0u && nvme_bytes == 0u) {
            storage_emit_line(STORAGE_LOG_DEBUG, "storage_pipeline_idle channel=%d dma_writable_slots=%u",
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

    if (!storage_should_print_periodic_stats((ChannelRuntime *)rt)) {
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
    storage_emit_line(STORAGE_LOG_SUMMARY, "storage_pipeline channel=%d window_ms=%" PRIu64 " task=%s file_index=%u"
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
    storage_emit_line(STORAGE_LOG_SUMMARY, "storage_receive channel=%d task=%s file_index=%u window_ms=%" PRIu64
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
    if (storage_slot_state(&q->slots, item->slot) != STORAGE_SLOT_NVME_BUSY) {
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
    if (__atomic_load_n(&q->bytes_written, __ATOMIC_RELAXED) >
            UINT64_MAX - item->bytes ||
        q->completed_slot_count == UINT64_MAX) {
        q->error = true;
        pthread_mutex_unlock(&q->lock);
        return -1;
    }
    (void)__atomic_add_fetch(&q->bytes_written, item->bytes, __ATOMIC_RELEASE);
    ++q->chunks;
    ++q->completed_slot_count;
    if (q->buffered_bytes >= item->bytes) {
        q->buffered_bytes -= item->bytes;
    } else {
        q->buffered_bytes = 0u;
    }
    should_requeue = q->requeue_enabled &&
                     storage_worker_can_requeue(q->worker_state);
    if (storage_local_slot_transition_locked(q, item->slot, STORAGE_SLOT_NVME_BUSY,
                                             should_requeue ? STORAGE_SLOT_REQUEUE_PENDING : STORAGE_SLOT_FREE) != 0) {
        pthread_mutex_unlock(&q->lock); return -1;
    }
    if (should_requeue) {
        if (q->recycled_slot_count == UINT64_MAX) {
            q->error = true;
            pthread_mutex_unlock(&q->lock);
            return -1;
        }
        /* Keep the queue lock through the DMA requeue.  storage_queue_latch_stop
         * takes the same lock, so a stop either linearizes before this call or
         * after it; it can never observe a post-latch requeue race. */
        if (dma_requeue_one(q->rt, item->slot) != 0) {
            storage_queue_fail_locked(q);
            if (q->error_reason[0] == '\0')
                (void)snprintf(q->error_reason, sizeof(q->error_reason),
                               "%s", "dma_requeue_failed");
            pthread_cond_broadcast(&q->not_full);
            pthread_cond_broadcast(&q->not_empty);
            pthread_mutex_unlock(&q->lock);
            storage_fail_fatal(q);
            return -1;
        }
        if (storage_local_slot_transition_locked(q, item->slot, STORAGE_SLOT_REQUEUE_PENDING,
                                                 STORAGE_SLOT_DMA_WRITABLE) != 0) {
            pthread_mutex_unlock(&q->lock);
            return -1;
        }
        ++q->recycled_slot_count;
    }
    pthread_mutex_unlock(&q->lock);
    if (!should_requeue && q->rt && dma_requeue_after_stop_count(q->rt) != 0u) {
        storage_set_writer_error_reason(q, "requeue_after_stop_latched");
        storage_fail_fatal(q);
        return -1;
    }
    return 0;
}

static void storage_set_writer_error_reason(StorageWriteQueue *q, const char *reason)
{
    if (!q || !reason || reason[0] == '\0') return;
    pthread_mutex_lock(&q->lock);
    if (q->error_reason[0] == '\0')
        (void)snprintf(q->error_reason, sizeof(q->error_reason), "%s", reason);
    pthread_mutex_unlock(&q->lock);
}

static void storage_copy_writer_error_reason(StorageWriteQueue *q,
                                             char *out, size_t out_size)
{
    if (!out || out_size == 0u) return;
    out[0] = '\0';
    if (!q) return;
    pthread_mutex_lock(&q->lock);
    (void)snprintf(out, out_size, "%s", q->error_reason);
    pthread_mutex_unlock(&q->lock);
}

static void storage_fail_fatal(StorageWriteQueue *q) {
    if (!q) {
        return;
    }
    storage_set_writer_error_reason(q, "storage_writer_error");
    pthread_mutex_lock(&q->lock);
    if (q->rt && q->rt->nvme_ownership_unresolved) {
        /* The legacy pending array is intentionally retained only until this
         * worker exits.  Do not destroy the queue or unmap DDR while an
         * unconfirmed Host Core command may still hold its PRP. */
        q->nvme_engine_quiesced = false;
    }
    storage_queue_fail_locked(q);
    pthread_cond_broadcast(&q->not_full);
    pthread_cond_broadcast(&q->not_empty);
    pthread_mutex_unlock(&q->lock);
    storage_ring_event(&q->writer_event_ring, STORAGE_EVENT_WORKER_FATAL, q->rt, 0u, 0u, true);
    storage_write_request_stop();
}

static void storage_record_deferred_stop_error(StorageWriteQueue *q,
                                               StorageErrorCode code,
                                               const char *reason)
{
    if (!q || storage_error_class(code) != STORAGE_ERROR_DEFERRED) return;
    pthread_mutex_lock(&q->lock);
    if (q->deferred_stop_error == STORAGE_ERR_NONE) {
        q->deferred_stop_error = code;
        (void)snprintf(q->deferred_stop_reason,
                       sizeof(q->deferred_stop_reason), "%s", reason);
    }
    pthread_mutex_unlock(&q->lock);
}

static StorageQueuePopResult storage_queue_pop(StorageWriteQueue *q, PendingDdrSlot *out,
                                               bool wait_for_item) {
    uint64_t empty_wait_start_us = 0u;
    if (!q || !out) {
        return STORAGE_QUEUE_POP_ERROR;
    }
    pthread_mutex_lock(&q->lock);
    if (wait_for_item && q->count == 0u &&
        !storage_worker_producer_done(q->worker_state) && !q->error) {
        empty_wait_start_us = storage_wall_time_us();
    }
    while (wait_for_item && !storage_worker_producer_done(q->worker_state) &&
           !q->error && q->count == 0u) {
        pthread_cond_wait(&q->not_empty, &q->lock);
    }
    if (empty_wait_start_us != 0u) {
        uint64_t wait_us = storage_elapsed_us(empty_wait_start_us);
        q->queue_empty_wait_us += wait_us;
        q->writer_idle_us += wait_us; /* legacy alias */
    }
    if (q->error) {
        pthread_mutex_unlock(&q->lock);
        return STORAGE_QUEUE_POP_ERROR;
    }
    if (q->count == 0u) {
        StorageQueuePopResult result = storage_worker_producer_done(q->worker_state)
                                           ? STORAGE_QUEUE_POP_PRODUCER_DRAINED
                                           : STORAGE_QUEUE_POP_TEMP_EMPTY;
        pthread_mutex_unlock(&q->lock);
        return result;
    }
    *out = q->items[q->head];
    q->head = (q->head + 1u) % q->capacity;
    --q->count;
    if (storage_local_slot_transition_locked(q, out->slot, STORAGE_SLOT_READY_FOR_NVME,
                                             STORAGE_SLOT_NVME_BUSY) != 0) {
        pthread_mutex_unlock(&q->lock); return STORAGE_QUEUE_POP_ERROR;
    }
    storage_queue_record_depth_locked(q);
    pthread_cond_signal(&q->not_full);
    pthread_mutex_unlock(&q->lock);
    return STORAGE_QUEUE_POP_ITEM;
}

static StorageCrossSlotWriterDecision storage_cross_slot_writer_decision_locked(
    StorageWriteQueue *q, const NvmeCrossSlotEngine *engine)
{
    StorageCrossSlotWriterDecision decision;

    if (!q || !engine) return STORAGE_CROSS_SLOT_WRITER_QUEUE_ERROR;
    pthread_mutex_lock(&q->lock);
    decision = storage_cross_slot_writer_decide(
        storage_worker_producer_done(q->worker_state), q->count, q->error,
        nvme_cross_slot_engine_active(engine), nvme_cross_slot_engine_inflight(engine));
    pthread_mutex_unlock(&q->lock);
    return decision;
}

static int storage_queue_wait_run(StorageWriteQueue *q)
{
    int rc = 0;
    if (!q) return -1;
    pthread_mutex_lock(&q->lock);
    while (q->worker_state == WORKER_INIT ||
           q->worker_state == WORKER_READY) {
        pthread_cond_wait(&q->not_empty, &q->lock);
    }
    if (q->error || q->worker_state != WORKER_ARMED) rc = -1;
    pthread_mutex_unlock(&q->lock);
    return rc;
}

static int storage_queue_mark_ready(StorageWriteQueue *q)
{
    int rc;

    if (!q) return -1;
    pthread_mutex_lock(&q->lock);
    rc = storage_queue_transition_locked(q, WORKER_INIT, WORKER_READY);
    pthread_cond_broadcast(&q->not_empty);
    pthread_mutex_unlock(&q->lock);
    return rc;
}

static int storage_queue_enable_run(StorageWriteQueue *q)
{
    int rc;

    if (!q) return -1;
    pthread_mutex_lock(&q->lock);
    rc = storage_queue_transition_locked(q, WORKER_READY, WORKER_ARMED);
    pthread_cond_broadcast(&q->not_empty);
    pthread_mutex_unlock(&q->lock);
    return rc;
}

static void storage_queue_set_writer_ready(StorageWriteQueue *q, bool success)
{
    if (!q) return;
    pthread_mutex_lock(&q->lock);
    if (success && q->worker_state == WORKER_ARMED && !q->writer_ready)
        q->writer_ready = true;
    else if (!success)
        storage_queue_fail_locked(q);
    else
        storage_queue_fail_locked(q);
    pthread_cond_broadcast(&q->not_empty);
    pthread_mutex_unlock(&q->lock);
}

static int storage_queue_wait_writer_ready(StorageWriteQueue *q, uint64_t deadline_us)
{
    struct timespec pause = {0, 100000L};

    if (!q) return -1;
    for (;;) {
        bool ready;
        bool failed;

        pthread_mutex_lock(&q->lock);
        ready = q->writer_ready;
        failed = q->worker_state == WORKER_FAILED || q->error;
        pthread_mutex_unlock(&q->lock);
        if (ready) return 0;
        if (failed) return -1;
        if (storage_wall_time_us() >= deadline_us) {
            errno = ETIMEDOUT;
            return -1;
        }
        (void)clock_nanosleep(CLOCK_MONOTONIC, 0, &pause, NULL);
    }
}

static int storage_queue_set_producer_ready(StorageWriteQueue *q, bool success)
{
    int rc;

    if (!q) return -1;
    pthread_mutex_lock(&q->lock);
    if (success && q->worker_state == WORKER_ARMED && q->writer_ready &&
        !q->producer_ready) {
        q->producer_ready = true;
        rc = 0;
    } else {
        storage_queue_fail_locked(q);
        rc = -1;
    }
    pthread_cond_broadcast(&q->not_empty);
    pthread_mutex_unlock(&q->lock);
    return rc;
}

static int storage_queue_mark_running(StorageWriteQueue *q)
{
    int rc;

    if (!q) return -1;
    pthread_mutex_lock(&q->lock);
    if (!q->writer_ready || !q->producer_ready) {
        storage_queue_fail_locked(q);
        rc = -1;
    } else {
        rc = storage_queue_transition_locked(q, WORKER_ARMED,
                                             WORKER_RUNNING);
    }
    pthread_mutex_unlock(&q->lock);
    return rc;
}

static int storage_join_writer_deadline(pthread_t thread, uint64_t deadline_us)
{
    struct timespec pause = {0, 100000L};

    for (;;) {
        int rc = pthread_tryjoin_np(thread, NULL);

        if (rc == 0) return 0;
        if (rc != EBUSY) {
            errno = rc;
            return -1;
        }
        if (storage_wall_time_us() >= deadline_us) {
            errno = ETIMEDOUT;
            return -1;
        }
        (void)clock_nanosleep(CLOCK_MONOTONIC, 0, &pause, NULL);
    }
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
    item.media_bytes = req->media_bytes != 0u ? req->media_bytes
                                              : req->sectors * (uint64_t)SECTOR_SIZE;
    item.chunk_index = req->chunk_index;
    item.file_offset = req->file_offset;
    item.start_lba = req->start_lba;
    item.sectors = req->sectors;
    item.hw_addr = req->hw_addr;
    storage_trace_flush_done(q->rt, &item, q->file_index, q->metadata_slot, q->task_no);
    return storage_complete_slot(q, &item);
}

static void storage_cross_slot_update_stats(StorageWriteQueue *q,
                                            const NvmeCrossSlotEngine *engine)
{
    NvmeCrossSlotStats stats;

    if (!q || !engine) return;
    nvme_cross_slot_engine_get_stats(engine, &stats);
    __atomic_store_n(&q->cross_sq_full_wait_count, stats.sq_full_wait_count, __ATOMIC_RELEASE);
    __atomic_store_n(&q->cross_sq_full_wait_max_us, stats.sq_full_wait_max_us, __ATOMIC_RELEASE);
    __atomic_store_n(&q->cross_cq_empty_wait_count, stats.cq_empty_wait_count, __ATOMIC_RELEASE);
    __atomic_store_n(&q->cross_cq_empty_wait_max_us, stats.cq_empty_wait_max_us, __ATOMIC_RELEASE);
    __atomic_store_n(&q->cross_submit_mmio_count, stats.submit_mmio_count, __ATOMIC_RELEASE);
    __atomic_store_n(&q->cross_submit_mmio_max_us, stats.submit_mmio_max_us, __ATOMIC_RELEASE);
    __atomic_store_n(&q->cross_completion_process_count, stats.completion_process_count,
                     __ATOMIC_RELEASE);
    __atomic_store_n(&q->cross_completion_process_max_us, stats.completion_process_max_us,
                     __ATOMIC_RELEASE);
    __atomic_store_n(&q->cross_no_progress_sleep_count, stats.no_progress_sleep_count,
                     __ATOMIC_RELEASE);
    __atomic_store_n(&q->writer_no_progress_sleep_count, stats.no_progress_sleep_count,
                     __ATOMIC_RELEASE);
}

static void storage_record_writer_schedule_gap(StorageWriteQueue *q,
                                               uint64_t expected_run_us,
                                               uint64_t actual_run_us)
{
    uint64_t gap_us;

    if (!q || expected_run_us == 0u || actual_run_us <= expected_run_us) return;
    gap_us = actual_run_us - expected_run_us;
    if (gap_us < 1000u) return;
    (void)__atomic_add_fetch(&q->writer_schedule_gap_count, 1u, __ATOMIC_RELAXED);
    if (gap_us > __atomic_load_n(&q->writer_schedule_gap_max_us, __ATOMIC_RELAXED))
        __atomic_store_n(&q->writer_schedule_gap_max_us, gap_us, __ATOMIC_RELAXED);
    storage_ring_event(&q->writer_event_ring, STORAGE_EVENT_WRITER_SCHEDULE_GAP,
                       q->rt, gap_us, 0u, false);
}

static void *storage_nvme_writer_thread(void *arg) {
    StorageWriteQueue *q = (StorageWriteQueue *)arg;
    uint64_t expected_run_us = 0u;

    if (storage_queue_wait_run(q) != 0) return NULL;
    if (storage_apply_writer_rt(q) != 0) {
        storage_set_writer_error_reason(q, "writer_schedule_failed");
        storage_queue_set_writer_ready(q, false);
        storage_ring_event(&q->writer_event_ring, STORAGE_EVENT_WORKER_FATAL,
                           q->rt, 0u, 0u, true);
        storage_write_request_stop();
        return NULL;
    }
    storage_queue_set_writer_ready(q, true);
    while (1) {
        PendingDdrSlot item;
        uint64_t write_start_us;
        uint64_t write_us;
        uint64_t submit_stalls_before;
        uint64_t cq_empty_before;
        bool backlog_after_pop = false;
        StorageQueuePopResult pop_result;

        if (storage_queue_abort_requested(q)) break;
        if (expected_run_us != 0u)
            storage_record_writer_schedule_gap(q, expected_run_us, storage_wall_time_us());
        pop_result = storage_queue_pop(q, &item, true);
        expected_run_us = 0u;
        if (pop_result == STORAGE_QUEUE_POP_ERROR) {
            storage_fail_fatal(q);
            break;
        }
        if (pop_result == STORAGE_QUEUE_POP_PRODUCER_DRAINED) {
            storage_ring_event(&q->writer_event_ring, STORAGE_EVENT_STOP_DRAINED,
                               q->rt, 0u, 0u, false);
            break;
        }
        if (pop_result != STORAGE_QUEUE_POP_ITEM) continue;
        backlog_after_pop = storage_queue_ready_count(q) > 0u;

        submit_stalls_before = __atomic_load_n(&q->rt->nvme_submit_stall_count, __ATOMIC_ACQUIRE);
        cq_empty_before = __atomic_load_n(&q->rt->nvme_cq_empty_polls, __ATOMIC_ACQUIRE);
        write_start_us = storage_wall_time_us();
        if (flush_slot_to_nvme(q->rt,
                               &item,
                               q->file_index,
                               q->metadata_slot,
                               q->task_no) != 0) {
            storage_fail_fatal(q);
            break;
        }
        write_us = storage_elapsed_us(write_start_us);
        if (__atomic_load_n(&q->rt->nvme_submit_stall_count, __ATOMIC_ACQUIRE) > submit_stalls_before)
            storage_ring_event(&q->writer_event_ring, STORAGE_EVENT_NVME_SUBMIT_STALL,
                               q->rt, write_us, 0u, false);
        if (__atomic_load_n(&q->rt->nvme_cq_empty_polls, __ATOMIC_ACQUIRE) > cq_empty_before)
            storage_ring_event(&q->writer_event_ring, STORAGE_EVENT_NVME_CQ_STALL,
                               q->rt, write_us, 0u, false);
        (void)__atomic_add_fetch(&q->nvme_write_us, write_us, __ATOMIC_RELEASE);
        (void)__atomic_add_fetch(&q->writer_active_us, write_us, __ATOMIC_RELEASE);
        (void)__atomic_add_fetch(&q->writer_drain_loop_count, 1u, __ATOMIC_RELEASE);
        (void)__atomic_add_fetch(&q->writer_slots_drained, 1u, __ATOMIC_RELEASE);
        if (backlog_after_pop || q->backlog_mode) {
            (void)__atomic_add_fetch(&q->ready_q_nonempty_us, write_us, __ATOMIC_RELEASE);
        }
        if (storage_complete_slot(q, &item) != 0) {
            storage_fail_fatal(q);
            break;
        }
        if (storage_queue_ready_count(q) > 0u) expected_run_us = storage_wall_time_us();
    }
    return NULL;
}

static void *storage_nvme_cross_slot_writer_thread(void *arg) {
    StorageWriteQueue *q = (StorageWriteQueue *)arg;
    NvmeCrossSlotEngine *engine;
    bool writer_failed = false;
    uint64_t expected_run_us = 0u;

    if (!q) {
        return NULL;
    }
    if (storage_queue_wait_run(q) != 0) return NULL;
    if (storage_apply_writer_rt(q) != 0) {
        storage_queue_set_writer_ready(q, false);
        storage_ring_event(&q->writer_event_ring, STORAGE_EVENT_WORKER_FATAL,
                           q->rt, 0u, 0u, true);
        storage_write_request_stop();
        return NULL;
    }
    engine = nvme_cross_slot_engine_create_with_config(q->rt, &q->cross_slot_config);
    if (!engine) {
        storage_set_writer_error_reason(q, "cross_slot_engine_create_failed");
        storage_queue_set_writer_ready(q, false);
        storage_ring_event(&q->writer_event_ring, STORAGE_EVENT_WORKER_FATAL,
                           q->rt, 0u, 0u, true);
        storage_write_request_stop();
        return NULL;
    }
    storage_queue_set_engine(q, engine);
    storage_queue_set_writer_ready(q, true);

    while (1) {
        PendingDdrSlot item;
        NvmeWriteSlotReq req;
        StorageQueuePopResult pop_result = STORAGE_QUEUE_POP_NOT_ATTEMPTED;
        StorageCrossSlotWriterDecision writer_decision;
        bool wait_for_item = nvme_cross_slot_engine_active(engine) == 0u;

        if (storage_queue_abort_requested(q)) {
            storage_set_writer_error_reason(q, "writer_abort_requested");
            writer_failed = true;
            break;
        }
        if (wait_for_item) {
            expected_run_us = 0u;
        } else {
            storage_record_writer_schedule_gap(q, expected_run_us, storage_wall_time_us());
        }

        if (nvme_cross_slot_engine_can_accept(engine)) {
            pop_result = storage_queue_pop(q, &item, wait_for_item);
        }
        if (pop_result == STORAGE_QUEUE_POP_ERROR) {
            storage_set_writer_error_reason(q, "storage_queue_pop_failed");
            writer_failed = true;
            break;
        }
        if (pop_result == STORAGE_QUEUE_POP_ITEM) {
            uint64_t buffer_offset = 0u;
            memset(&req, 0, sizeof(req));
            req.slot = item.slot; req.start_lba = item.start_lba; req.sectors = item.sectors;
            req.hw_addr = item.hw_addr; req.bytes = item.bytes; req.chunk_index = item.chunk_index;
            req.media_bytes = item.media_bytes;
            req.file_offset = item.file_offset;
            if (storage_slot_addresses(q->rt, &item, &buffer_offset, NULL) != 0 ||
                storage_zero_tail_padding(q->rt, &item, buffer_offset) != 0) {
                storage_set_writer_error_reason(
                    q, q->rt->nvme_last_error[0] != '\0'
                           ? q->rt->nvme_last_error : "slot_padding_failed");
                writer_failed = true;
                break;
            }
            storage_trace_flush_start(q->rt, &item);
            if (nvme_cross_slot_engine_add(engine, &req) != 0) {
                storage_set_writer_error_reason(q, nvme_cross_slot_engine_last_error(engine));
                writer_failed = true; break;
            }
        }
        {
            uint64_t step_start_us = storage_wall_time_us();
            uint32_t step_budget_us = __atomic_load_n(
                &q->writer_budget_override_us, __ATOMIC_ACQUIRE);
            if (nvme_cross_slot_engine_step(engine, step_budget_us,
                                            storage_cross_slot_done_cb, q) != 0) {
                (void)__atomic_add_fetch(&q->writer_active_us,
                                         storage_elapsed_us(step_start_us), __ATOMIC_RELEASE);
                storage_cross_slot_update_stats(q, engine);
                storage_set_writer_error_reason(q, nvme_cross_slot_engine_last_error(engine));
                writer_failed = true; break;
            }
            (void)__atomic_add_fetch(&q->writer_active_us,
                                     storage_elapsed_us(step_start_us), __ATOMIC_RELEASE);
        }
        storage_cross_slot_update_stats(q, engine);
        expected_run_us = storage_wall_time_us();
        writer_decision = storage_cross_slot_writer_decision_locked(q, engine);
        if (writer_decision == STORAGE_CROSS_SLOT_WRITER_QUEUE_ERROR) {
            storage_set_writer_error_reason(q, "storage_queue_error");
            writer_failed = true;
            break;
        }
        if (writer_decision == STORAGE_CROSS_SLOT_WRITER_DRAINED) {
            storage_ring_event(&q->writer_event_ring, STORAGE_EVENT_STOP_DRAINED,
                               q->rt, 0u, 0u, false);
            break;
        }
        /* CONTINUE covers queued work or live engine ownership.  WAIT_FOR_QUEUE
         * returns to the top where active==0 makes storage_queue_pop block on
         * not_empty until the producer supplies work or marks itself done. */
    }
    if (writer_failed) {
        uint64_t now_us = storage_wall_time_us();
        uint64_t drain_us = q->nvme_abort_timeout_us != 0u
                                ? q->nvme_abort_timeout_us
                                : (q->cross_slot_config.no_progress_timeout_us != 0u
                                ? q->cross_slot_config.no_progress_timeout_us
                                : (uint64_t)q->rt->gopt.timeout_us);
        nvme_cross_slot_engine_request_abort(engine,
                                             nvme_cross_slot_engine_last_error(engine));
        (void)nvme_cross_slot_engine_drain_abort(engine, now_us + drain_us);
        q->nvme_engine_quiesced = nvme_cross_slot_engine_is_quiesced(engine);
        storage_cross_slot_update_stats(q, engine);
        storage_fail_fatal(q);
    } else {
        q->nvme_engine_quiesced = nvme_cross_slot_engine_is_quiesced(engine);
    }
    if (q->nvme_engine_quiesced) {
        storage_queue_destroy_engine(q, engine);
    }
    return NULL;
}

void storage_write_reset_stop(void) {
    g_storage_stop_requested = 0;
    g_storage_control_drain_latched = false;
    g_storage_control_stop_latched = false;
    g_storage_fatal_event_sent = false;
    g_storage_stop_epoch = 0u;
}

void storage_write_request_stop(void) {
    g_storage_stop_requested = 1;
}

bool storage_write_fatal_event_sent(void)
{
    return g_storage_fatal_event_sent;
}

bool storage_write_finalize_requested(void)
{
    return g_storage_control_stop_latched || g_storage_stop_requested != 0;
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
    uint64_t media_bytes;

    media_bytes = item ? (item->media_bytes != 0u ? item->media_bytes : item->bytes) : 0u;
    if (!rt || !item || item->slot >= rt->dma_desc_count ||
        item->bytes == 0u || item->bytes > media_bytes || media_bytes > rt->dma_desc_bytes) {
        return -1;
    }
    offset = (uint64_t)item->slot * (uint64_t)rt->dma_desc_bytes;
    if (offset > rt->dma_ring_bytes || media_bytes > rt->dma_ring_bytes - offset) {
        return -1;
    }
    if (buffer_offset) {
        *buffer_offset = offset;
    }
    if (cpu_addr) {
        *cpu_addr = (offset + media_bytes) <= rt->cfg->ddr_cpu_size
                        ? rt->cfg->ddr_cpu_base + offset
                        : 0u;
    }
    return 0;
}

static int storage_zero_tail_padding(ChannelRuntime *rt, const PendingDdrSlot *item,
                                     uint64_t buffer_offset)
{
    uint64_t media_bytes;
    uint64_t pad;
    if (!rt || !item) return -1;
    media_bytes = item->media_bytes != 0u ? item->media_bytes : item->bytes;
    if (media_bytes < item->bytes) return -1;
    pad = media_bytes - item->bytes;
    if (pad == 0u || rt->gopt.dry_run) return 0;
    (void)buffer_offset;
    /* DDR coherence with the NVMe DMA master has not been proven. */
    (void)snprintf(rt->nvme_last_error, sizeof(rt->nvme_last_error),
                   "unaligned_payload_not_safely_paddable");
    return -1;
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
    uint64_t media_bytes;
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
    media_bytes = item->media_bytes != 0u ? item->media_bytes : item->bytes;
    if (item->slot >= rt->dma_desc_count || item->bytes > rt->dma_desc_bytes ||
        media_bytes < item->bytes || media_bytes > rt->dma_desc_bytes) {
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
    if (storage_zero_tail_padding(rt, item, buffer_offset) != 0) return -1;
    sectors = media_bytes / SECTOR_SIZE;
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
    if (item->start_lba > UINT64_MAX - item->sectors ||
        (rt->nvme_max_lba > 0u &&
         item->start_lba + item->sectors > rt->nvme_max_lba)) {
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
        perf_expected_cmds = nvme_perf_calc_begin(rt, media_bytes);
        write_start_us = storage_wall_time_us();
    }
    if (rt->nvme_feed_mode == NVME_FEED_MODE_TIGHT) {
        if (nvme_write_contiguous_tight_qd_payload(rt,
                                           item->hw_addr,
                                           item->start_lba,
                                           media_bytes,
                                           item->bytes,
                                           rt->nvme_qd_effective) != 0) {
            dbg_printf("[DBG][WRITE] nvme tight write failed ch=%d lba=0x%08" PRIx64
                       " bytes=%" PRIu64 "\n",
                       rt->cfg->id,
                       item->start_lba,
                       item->bytes);
            return -1;
        }
    } else if (nvme_write_slot_qd_payload(rt, item->slot, item->start_lba,
                                          item->sectors, item->bytes, item->hw_addr) != 0) {
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
                         ? ((double)media_bytes * 1000000.0) /
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

int execute_write_with_result_mode(const ParsedArgs *args, GlobalOptions gopt,
                                   WriteResult *result, StorageWriteMode mode) {
    const ChannelConfig *cfg = find_channel(args->channel_id);
    const AppConfig *app_config = storage_config_get();
    ChannelRuntime rt;
    FileEntry table[MAX_FILES_TOTAL];
    int rc = -1;
    int metadata_slot = -1;
    uint64_t auto_lba = 0u;
    uint32_t valid_count = 0u;
    uint64_t start_lba = 0u;
    uint64_t total_sectors = 0u;
    uint64_t dma_received_bytes = 0u;
    uint64_t dma_observed_bytes = 0u;
    uint64_t dma_harvested_payload_bytes = 0u;
    uint64_t queued_payload_bytes = 0u;
    uint64_t tail_unqueued_bytes = 0u;
    uint64_t completed_unharvested_bytes = 0u;
    uint64_t stop_request_us = 0u;
    uint64_t packet_boundary_us = 0u;
    uint64_t dma_quiesced_us = 0u;
    uint64_t producer_done_us = 0u;
    uint64_t writer_drained_us = 0u;
    uint64_t final_us = 0u;
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
    StorageStopState stop_state;
    StorageStopHarvestState stop_harvest_state;
    StorageInputIdleState input_idle_state;
    pthread_t writer_thread;
    bool write_queue_ready = false;
    bool writer_started = false;
    bool dma_started = false;
    bool dma_quiesced = false;
    bool dma_stop_attempted = false;
    bool manual_stop_seen = false;
    bool tail_incomplete = false;
    bool dma_stop_failed = false;
    bool data_persisted = false;
    bool final_integrity_ok = false;
    bool auto_idle_done = false;
    bool stop_harvest_stable = false;
    bool stop_tail_seen = false;
    bool stop_failed_phase_sent = false;
    bool final_perf_emitted = false;
    bool bounded;
    bool cross_slot_qd;
    uint64_t requested_size;
    uint32_t dma_desc_bytes = DMA_DESC_BYTES_DEFAULT;
    uint32_t effective_file_index = args->file_index;
    uint32_t storage_poll_sleep_us;
    uint32_t storage_high_poll_sleep_us;
    uint32_t storage_critical_poll_sleep_us;
    uint32_t cross_slot_batch;
    NvmeCrossSlotConfig cross_slot_config;
    StorageCrossSlotResolution cross_slot_enabled_resolution;
    StorageCrossSlotResolution cross_slot_max_active_resolution;
    StorageCrossSlotResolution cross_slot_target_qd_resolution;
    StorageCrossSlotResolution cross_slot_cq_batch_resolution;
    StorageCrossSlotResolution cross_slot_writer_budget_resolution;
    StorageCrossSlotResolution cross_slot_busy_poll_resolution;
    StorageCrossSlotResolution cross_slot_empty_sleep_resolution;
    StorageCrossSlotResolution cross_slot_no_progress_resolution;
    uint32_t ready_queue_depth_cfg;
    uint32_t harvest_batch_max_cfg;
    uint32_t dma_idle_done_ms;
    uint64_t first_dma_timeout_us;
    uint64_t next_input_idle_scan_us = 0u;
    uint32_t supervised_channel_count;
    StorageStopTimeouts stop_timeouts;
    int producer_rt_policy = SCHED_OTHER;
    uint32_t producer_rt_prio = 0u;
    uint64_t requested_ring_bytes;
    bool fast_pipeline_enabled;
    bool auto_idle_enabled;
    bool coordinated_idle_enabled;
    bool require_nonempty_payload;
    uint64_t start_skew_us = 0u;
    const char *start_gate_mode = "standalone_immediate";

    storage_write_reset_stop();
    storage_stop_state_init(&stop_state);
    storage_stop_harvest_state_init(&stop_harvest_state);
    storage_input_idle_init(&input_idle_state);
    atomic_store_explicit(&g_storage_dropped_perf_samples, 0u, memory_order_relaxed);
    atomic_store_explicit(&g_storage_dropped_diag_events, 0u, memory_order_relaxed);
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
    dma_idle_done_ms = bounded ? 0u
                               : (app_config
                                      ? app_config->idle_required_ms
                                      : storage_dma_idle_done_ms());
    stop_timeouts = storage_stop_timeouts(NULL);
    first_dma_timeout_us = storage_first_dma_timeout_us(cfg);
    supervised_channel_count = mode == STORAGE_WRITE_SUPERVISED
                                   ? storage_env_u32_limit(
                                         CCB_INTERNAL_SUPERVISED_CHANNEL_COUNT, 1u, 3u)
                                   : 1u;
    require_nonempty_payload = mode == STORAGE_WRITE_SUPERVISED &&
                               supervised_channel_count == NUM_CHANNELS;
    if (require_nonempty_payload && first_dma_timeout_us == 0u) {
        /* A production 0xAA capture must identify a missing input before
         * STOP.  Thirty seconds is deliberately independent of idle-done. */
        first_dma_timeout_us = 30000000ull;
    }
    /* A supervised task is ended by the shared STOP/barrier.  In particular
     * ch2 must not auto-complete and cascade a stop while ch0/ch1 are still
     * waiting for their first input.  The idle policy is retained only for a
     * standalone single-channel capture (and the bounded compatibility path). */
    coordinated_idle_enabled = !bounded && mode == STORAGE_WRITE_SUPERVISED &&
                               app_config && app_config->auto_input_complete;
    auto_idle_enabled = !bounded && !coordinated_idle_enabled &&
                        supervised_channel_count <= 1u;
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
    producer_stats.ring_warning_percent = storage_env_u32_limit(
        "SRC_REAL_RING_WARNING_PERCENT", 75u, 100u);
    producer_stats.ring_critical_percent = storage_env_u32_limit(
        "SRC_REAL_RING_CRITICAL_PERCENT", 90u, 100u);
    if (producer_stats.ring_warning_percent == 0u ||
        producer_stats.ring_warning_percent >= producer_stats.ring_critical_percent) {
        fprintf(stderr,
                "warning: invalid storage ring pressure thresholds warning=%u"
                " critical=%u; fallback=75/90\n",
                (unsigned)producer_stats.ring_warning_percent,
                (unsigned)producer_stats.ring_critical_percent);
        producer_stats.ring_warning_percent = 75u;
        producer_stats.ring_critical_percent = 90u;
    }
    producer_stats.ring_critical_duration_us = storage_timeout_us(
        "SRC_REAL_RING_CRITICAL_DURATION_US", NULL, 100000u);
    producer_stats.ring_critical_stop_enabled = false;
    {
        cross_slot_enabled_resolution = storage_cross_slot_resolve_config(
            cfg->id, STORAGE_CROSS_SLOT_CONFIG_ENABLED);
        cross_slot_max_active_resolution = storage_cross_slot_resolve_config(
            cfg->id, STORAGE_CROSS_SLOT_CONFIG_MAX_ACTIVE);
        cross_slot_target_qd_resolution = storage_cross_slot_resolve_config(
            cfg->id, STORAGE_CROSS_SLOT_CONFIG_TARGET_QD);
        cross_slot_cq_batch_resolution = storage_cross_slot_resolve_config(
            cfg->id, STORAGE_CROSS_SLOT_CONFIG_CQ_BATCH);
        cross_slot_writer_budget_resolution = storage_cross_slot_resolve_config(
            cfg->id, STORAGE_CROSS_SLOT_CONFIG_WRITER_BUDGET_US);
        cross_slot_busy_poll_resolution = storage_cross_slot_resolve_config(
            cfg->id, STORAGE_CROSS_SLOT_CONFIG_BUSY_POLL_US);
        cross_slot_empty_sleep_resolution = storage_cross_slot_resolve_config(
            cfg->id, STORAGE_CROSS_SLOT_CONFIG_EMPTY_SLEEP_US);
        cross_slot_no_progress_resolution = storage_cross_slot_resolve_config(
            cfg->id, STORAGE_CROSS_SLOT_CONFIG_NO_PROGRESS_TIMEOUT_US);
        cross_slot_qd = cross_slot_enabled_resolution.value != 0u;
        cross_slot_config.max_active_slots = cross_slot_max_active_resolution.value;
        cross_slot_batch = cross_slot_config.max_active_slots;
        cross_slot_config.target_qd = cross_slot_target_qd_resolution.value;
        cross_slot_config.cq_batch = cross_slot_cq_batch_resolution.value;
        cross_slot_config.writer_budget_us = cross_slot_writer_budget_resolution.value;
        cross_slot_config.busy_poll_us = cross_slot_busy_poll_resolution.value;
        cross_slot_config.empty_sleep_us = cross_slot_empty_sleep_resolution.value;
        cross_slot_config.no_progress_timeout_us = cross_slot_no_progress_resolution.value;
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
    stop_timeouts = storage_stop_timeouts(&rt);
    requested_ring_bytes = storage_requested_ring_bytes(&rt);
    if (requested_ring_bytes != rt.dma_ring_bytes) {
        storage_emit_line(STORAGE_LOG_ALWAYS_CRITICAL, "storage_ring_config_error channel=%d requested_ring_bytes=%" PRIu64
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
            storage_emit_line(STORAGE_LOG_ALWAYS_CRITICAL, "storage_ring_config_error channel=%d requested_ring_bytes=%" PRIu64
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
    if (effective_file_index != args->file_index && storage_text_output_enabled()) {
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
        if (requested_sectors > UINT64_MAX - start_lba) {
            storage_record_failure(&producer_stats, "lba_range_overflow");
            goto out;
        }
        if (!args->lba_auto && metadata_check_lba_overlap(table, start_lba, requested_sectors) != 0) {
            dbg_printf("[DBG][WRITE] lba overlap ch=%d lba=0x%08" PRIx64 " sectors=%" PRIu64 "\n",
                       cfg->id, start_lba, requested_sectors);
            goto out;
        }
        if (rt.nvme_max_lba > 0u &&
            !nvme_lba_range_valid(&rt, start_lba, requested_sectors)) {
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
    if (storage_text_output_enabled()) {
        printf("storage_prepared channel=%d task=%s file_index=%u size=%" PRIu64 " continuous=%u\n",
               cfg->id, args->task_no, (unsigned)effective_file_index, requested_size,
               bounded ? 0u : 1u);
        fflush(stdout);
    }

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
    write_queue.cross_slot_config = cross_slot_config;
    write_queue.nvme_abort_timeout_us = stop_timeouts.nvme_abort_us;
    write_queue_ready = true;
    if (storage_text_output_enabled()) {
        printf("nvme_scheduler_config channel=%d mode=%s qd=%u cmd_size=%u max_dts=%u batch=%u\n",
               cfg->id, cross_slot_qd ? "cross_slot" : "single_slot",
               (unsigned)rt.nvme_qd_effective, (unsigned)rt.nvme_cmd_size_bytes,
               (unsigned)rt.nvme_max_dts_bytes, cross_slot_qd ? (unsigned)cross_slot_batch : 1u);
    }
    storage_emit_line(STORAGE_LOG_SUMMARY, "storage_pipeline_config channel=%d pipeline_mode=%s"
                      " writer_mode=%s legacy_fallback_enabled=0 fast_pipeline=%u"
                      " requested_ring_bytes=%" PRIu64
                      " effective_ring_bytes=%" PRIu64
                      " slot_bytes=%u total_slots=%u ring_clamp_reason=%s"
                      " hw_ring_base=0x%08" PRIx64 " hw_ring_end=0x%08" PRIx64
                      " hw_ddr_span_bytes=%" PRIu64 " dma_bd_count=%u"
                      " qd=%u cmd_size=%u log_level=%u writer_rt_policy=%s writer_rt_prio=%u"
                      " producer_rt_policy=%s producer_rt_prio=%u"
                      " cross_slot_enabled=%u max_active_slots=%u target_qd=%u cq_batch=%u"
                      " writer_budget_us=%u busy_poll_us=%u empty_sleep_us=%u"
                      " no_progress_timeout_us=%u"
                      " backlog_mode=%u dma_idle_done_ms=%u"
                      " ready_queue_depth=%u harvest_batch_max=%u poll_sleep_us=%u"
                      " high_watermark_poll_us=%u critical_watermark_poll_us=%u"
                      " pipeline_stats_sec=%u enable_storage_stats=%u"
                      " slot_write_perf=%u slot_write_perf_sample=%u"
                      " defer_db_until_stop=1",
                      cfg->id,
                      cross_slot_qd ? "cross_slot" : "legacy",
                      cross_slot_qd ? "cross_slot" : "legacy",
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
                      (unsigned)storage_log_effective_level(),
                      storage_rt_policy_name(storage_rt_policy(
                          "SRC_REAL_WRITER_RT_POLICY",
                          storage_profile_rt_policy(&rt, true))),
                      (unsigned)storage_writer_rt_prio(&rt),
                      storage_rt_policy_name(storage_rt_policy(
                          "SRC_REAL_PRODUCER_RT_POLICY",
                          storage_profile_rt_policy(&rt, false))),
                      (unsigned)storage_producer_rt_prio(&rt),
                      cross_slot_qd ? 1u : 0u,
                      (unsigned)cross_slot_config.max_active_slots,
                      (unsigned)cross_slot_config.target_qd,
                      (unsigned)cross_slot_config.cq_batch,
                      (unsigned)cross_slot_config.writer_budget_us,
                      (unsigned)cross_slot_config.busy_poll_us,
                      (unsigned)cross_slot_config.empty_sleep_us,
                      (unsigned)cross_slot_config.no_progress_timeout_us,
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
    storage_emit_line(STORAGE_LOG_SUMMARY, "storage_cross_slot_effective_config channel=%d enabled=%u"
                      " max_active=%u target_qd=%u cq_batch=%u writer_budget_us=%u"
                      " busy_poll_us=%u empty_sleep_us=%u no_progress_timeout_us=%u"
                      " enabled_source_kind=%s enabled_source_name=%s"
                      " max_active_source_kind=%s max_active_source_name=%s"
                      " target_qd_source_kind=%s target_qd_source_name=%s"
                      " cq_batch_source_kind=%s cq_batch_source_name=%s"
                      " budget_source_kind=%s budget_source_name=%s"
                      " busy_poll_source_kind=%s busy_poll_source_name=%s"
                      " empty_sleep_source_kind=%s empty_sleep_source_name=%s"
                      " no_progress_source_kind=%s no_progress_source_name=%s",
                      cfg->id, cross_slot_qd ? 1u : 0u,
                      (unsigned)cross_slot_config.max_active_slots,
                      (unsigned)cross_slot_config.target_qd,
                      (unsigned)cross_slot_config.cq_batch,
                      (unsigned)cross_slot_config.writer_budget_us,
                      (unsigned)cross_slot_config.busy_poll_us,
                      (unsigned)cross_slot_config.empty_sleep_us,
                      (unsigned)cross_slot_config.no_progress_timeout_us,
                      storage_cross_slot_source_kind_name(cross_slot_enabled_resolution.source_kind),
                      cross_slot_enabled_resolution.source_name,
                      storage_cross_slot_source_kind_name(cross_slot_max_active_resolution.source_kind),
                      cross_slot_max_active_resolution.source_name,
                      storage_cross_slot_source_kind_name(cross_slot_target_qd_resolution.source_kind),
                      cross_slot_target_qd_resolution.source_name,
                      storage_cross_slot_source_kind_name(cross_slot_cq_batch_resolution.source_kind),
                      cross_slot_cq_batch_resolution.source_name,
                      storage_cross_slot_source_kind_name(cross_slot_writer_budget_resolution.source_kind),
                      cross_slot_writer_budget_resolution.source_name,
                      storage_cross_slot_source_kind_name(cross_slot_busy_poll_resolution.source_kind),
                      cross_slot_busy_poll_resolution.source_name,
                      storage_cross_slot_source_kind_name(cross_slot_empty_sleep_resolution.source_kind),
                      cross_slot_empty_sleep_resolution.source_name,
                      storage_cross_slot_source_kind_name(cross_slot_no_progress_resolution.source_kind),
                      cross_slot_no_progress_resolution.source_name);
    storage_emit_line(STORAGE_LOG_SUMMARY,
                      "storage_ring_pressure_config channel=%d warning_percent=%u"
                      " critical_percent=%u critical_duration_us=%" PRIu64
                      " graceful_stop=%u",
                      cfg->id,
                      (unsigned)producer_stats.ring_warning_percent,
                      (unsigned)producer_stats.ring_critical_percent,
                      producer_stats.ring_critical_duration_us,
                      producer_stats.ring_critical_stop_enabled ? 1u : 0u);
    {
        const StorageCrossSlotResolution *resolutions[] = {
            &cross_slot_enabled_resolution, &cross_slot_max_active_resolution,
            &cross_slot_target_qd_resolution, &cross_slot_cq_batch_resolution,
            &cross_slot_writer_budget_resolution, &cross_slot_busy_poll_resolution,
            &cross_slot_empty_sleep_resolution, &cross_slot_no_progress_resolution,
        };
        const char *labels[] = {
            "enabled", "max_active", "target_qd", "cq_batch", "writer_budget_us",
            "busy_poll_us", "empty_sleep_us", "no_progress_timeout_us",
        };
        size_t i;

        for (i = 0u; i < sizeof(resolutions) / sizeof(resolutions[0]); ++i) {
            if (resolutions[i]->invalid_source_name[0] != '\0') {
                storage_emit_line(STORAGE_LOG_SUMMARY, "storage_cross_slot_invalid_config channel=%d parameter=%s"
                                  " invalid_source=%s fallback_source=%s fallback_value=%u",
                                  cfg->id, labels[i], resolutions[i]->invalid_source_name,
                                  resolutions[i]->fallback_source_name,
                                  (unsigned)resolutions[i]->value);
            }
        }
    }
    storage_emit_line(STORAGE_LOG_SUMMARY, "storage_stop_config channel=%d compat_timeout_us=%" PRIu64
                      " dma_quiesce_timeout_us=%" PRIu64
                      " stop_harvest_timeout_us=%" PRIu64
                      " writer_drain_timeout_us=%" PRIu64
                      " nvme_abort_timeout_us=%" PRIu64,
                      cfg->id, stop_timeouts.compat_us, stop_timeouts.dma_quiesce_us,
                      stop_timeouts.stop_harvest_us, stop_timeouts.writer_drain_us,
                      stop_timeouts.nvme_abort_us);
    storage_emit_line(STORAGE_LOG_SUMMARY, "storage_idle_policy channel=%d supervised_channel_count=%u"
                      " auto_idle_enabled=%u coordinated_idle_enabled=%u"
                      " idle_scan_interval_ms=%u idle_required_ms=%u"
                      " idle_required_scans=%u reason=%s",
                      cfg->id, (unsigned)supervised_channel_count,
                      auto_idle_enabled ? 1u : 0u,
                      coordinated_idle_enabled ? 1u : 0u,
                      app_config ? app_config->idle_scan_interval_ms : 100u,
                      app_config ? app_config->idle_required_ms : 500u,
                      app_config ? app_config->idle_required_scans : 5u,
                      coordinated_idle_enabled ? "supervised_barrier" :
                          (auto_idle_enabled ? "standalone_single_channel"
                                             : "disabled"));
    storage_emit_line(STORAGE_LOG_SUMMARY, "storage_zero_payload_policy channel=%d require_nonempty=%u first_dma_timeout_us=%" PRIu64
                      " allow_zero_env=%u",
                      cfg->id, require_nonempty_payload ? 1u : 0u,
                      first_dma_timeout_us, 0u);
    if (first_dma_timeout_us != 0u)
        storage_emit_line(STORAGE_LOG_SUMMARY, "storage_first_dma_timeout channel=%d timeout_us=%" PRIu64,
                          cfg->id, first_dma_timeout_us);
    if (pthread_create(&writer_thread,
                       NULL,
                       cross_slot_qd ? storage_nvme_cross_slot_writer_thread
                                     : storage_nvme_writer_thread,
                       &write_queue) != 0) {
        fprintf(stderr, "Failed to start NVMe writer thread on channel %d\n", cfg->id);
        goto out;
    }
    writer_started = true;
    if (storage_queue_mark_ready(&write_queue) != 0) {
        storage_record_error(&producer_stats, STORAGE_ERR_QUEUE,
                             "worker_ready_transition_failed");
        goto out;
    }
    producer_stats.interval_ms = storage_pipeline_stats_ms();
    storage_emit_line(STORAGE_LOG_DEBUG, "storage_ready channel=%d task=%s file_index=%u start_gate_mode=%s",
                      cfg->id, args->task_no, (unsigned)effective_file_index,
                      storage_config_compat_getenv(CCB_INTERNAL_START_FD)
                          ? "software_barrier" : "standalone_immediate");
    dma_started = true;
    {
        const char *gate_failure_reason = "start_gate_failed";
        if (storage_wait_start_gate(&rt, &start_skew_us, &start_gate_mode,
                                    &gate_failure_reason) != 0) {
        storage_record_error(&producer_stats, STORAGE_ERR_IPC_SEQUENCE,
                             gate_failure_reason);
        (void)storage_emit_event(STORAGE_WORKER_FATAL, &rt, STORAGE_ERR_IPC_SEQUENCE,
                                 0u, gate_failure_reason);
        goto out;
        }
    }
    if (storage_queue_enable_run(&write_queue) != 0) {
        storage_record_error(&producer_stats, STORAGE_ERR_QUEUE,
                             "writer_enable_failed");
        goto out;
    }
    {
        uint32_t ready_timeout_us = storage_env_u32_limit(
            "SRC_REAL_STORAGE_RUN_READY_TIMEOUT_US",
            rt.gopt.timeout_us ? rt.gopt.timeout_us : DEFAULT_TIMEOUT_US,
            UINT32_MAX);
        uint64_t ready_deadline_us = storage_wall_time_us() + ready_timeout_us;

        if (storage_queue_wait_writer_ready(&write_queue, ready_deadline_us) != 0) {
            storage_record_error(&producer_stats, STORAGE_ERR_QUEUE,
                                 errno == ETIMEDOUT ? "writer_run_ready_timeout"
                                                   : "writer_schedule_failed");
            (void)storage_emit_event(STORAGE_WORKER_FATAL, &rt, STORAGE_ERR_QUEUE, 0u,
                                     producer_stats.receive_integrity_risk);
            goto out;
        }
    }
    if (storage_apply_producer_rt(&rt, &producer_rt_policy, &producer_rt_prio) != 0 ||
        storage_queue_set_producer_ready(&write_queue, true) != 0) {
        storage_record_error(&producer_stats, STORAGE_ERR_QUEUE,
                             "producer_schedule_failed");
        (void)storage_queue_set_producer_ready(&write_queue, false);
        storage_fail_fatal(&write_queue);
        (void)storage_emit_event(STORAGE_WORKER_FATAL, &rt, STORAGE_ERR_QUEUE, 0u,
                                 producer_stats.receive_integrity_risk);
        goto out;
    }
    nvme_reset_sw_timing(&rt);
    capture_start_us = storage_wall_time_us();
    next_input_idle_scan_us = capture_start_us +
        (uint64_t)(app_config ? app_config->idle_scan_interval_ms : 100u) * 1000u;
    producer_stats.window_start_us = capture_start_us;
    producer_stats.next_log_us = capture_start_us +
                                 (uint64_t)producer_stats.interval_ms * 1000ull;
    producer_stats.perf_window_start_us = capture_start_us;
    producer_stats.perf_next_us = capture_start_us +
                                  (uint64_t)storage_perf_log_interval_ms() * 1000ull;
    if (storage_queue_mark_running(&write_queue) != 0) {
        storage_record_error(&producer_stats, STORAGE_ERR_IPC_SEQUENCE,
                             "invalid_running_sequence");
        (void)storage_emit_event(STORAGE_WORKER_FATAL, &rt, STORAGE_ERR_IPC_SEQUENCE, 0u,
                                 "invalid_running_sequence");
        storage_fail_fatal(&write_queue);
        goto out;
    }
    if (storage_emit_event(STORAGE_WORKER_RUNNING, &rt, STORAGE_ERR_NONE,
                           0u, "running") != 0) {
        storage_record_error(&producer_stats, STORAGE_ERR_IPC,
                             "running_event_send_failed");
        storage_fail_fatal(&write_queue);
        goto out;
    }
    storage_emit_line(STORAGE_LOG_DEBUG, "storage_started channel=%d task=%s file_index=%u"
                      " start_gate_mode=%s start_skew_us=%" PRIu64,
                      cfg->id, args->task_no, (unsigned)effective_file_index,
                      start_gate_mode, start_skew_us);
    {
        uint32_t idle_notice_ms = storage_idle_notice_ms();
        uint64_t last_dma_us = storage_wall_time_us();
        uint64_t first_dma_deadline_us = first_dma_timeout_us != 0u
                                            ? last_dma_us + first_dma_timeout_us : 0u;
        bool saw_dma_data = false;
        bool idle_notice_logged = false;
        bool stop_logged = false;
        bool ring_full_logged = false;

        for (;;) {
            DmaHarvestItem harvest_items[16];
            uint32_t harvest_count = 0u;
            uint32_t harvest_limit = harvest_batch_max_cfg == 0u ? 1u :
                                     (harvest_batch_max_cfg > 16u ? 16u : harvest_batch_max_cfg);
            int harvest_rc;
            bool harvest_fatal;
            bool first_dma_deadline_due;
            int h;
            bool control_drain_requested = storage_control_drain_requested();
            bool external_stop_requested = storage_write_stop_requested() != 0 ||
                                           g_storage_control_stop_latched;
            bool stop_requested = external_stop_requested ||
                                  control_drain_requested || auto_idle_done ||
                                  (bounded && bytes_captured >= requested_size);

            if (control_drain_requested && !g_storage_control_stop_latched)
                auto_idle_done = true;

            first_dma_deadline_due = first_dma_deadline_us != 0u &&
                                     !saw_dma_data && !stop_requested &&
                                     storage_wall_time_us() >= first_dma_deadline_us;

            if (producer_stats.watermark_level >= 1u && harvest_limit < 16u)
                harvest_limit = 16u;

            if (bounded && stop_state.state == STORAGE_STOP_NONE) {
                harvest_limit = storage_harvest_limit_for_remaining(
                    requested_size - bytes_captured, rt.dma_desc_bytes, harvest_limit);
                if (harvest_limit == 0u) break;
            }

            if (storage_queue_has_error(&write_queue)) {
                char queue_reason[64];
                storage_copy_writer_error_reason(&write_queue, queue_reason,
                                                 sizeof(queue_reason));
                if (queue_reason[0] == '\0')
                    (void)snprintf(queue_reason, sizeof(queue_reason), "%s",
                                   "storage_queue_error");
                storage_record_error(&producer_stats, STORAGE_ERR_QUEUE,
                                     queue_reason);
                storage_emit_event(STORAGE_WORKER_FATAL, &rt, STORAGE_ERR_QUEUE,
                                   dma_received_bytes,
                                   queue_reason);
                dbg_printf("[DBG][WRITE] writer thread error observed ch=%d captured=%" PRIu64 "\n",
                           cfg->id, bytes_captured);
                goto out;
            }
            if (stop_requested && !stop_logged) {
                dbg_printf("[DBG][WRITE] stop signal observed ch=%d captured=%" PRIu64 "\n",
                           cfg->id,
                           bytes_captured);
                stop_logged = true;
                storage_ring_event(&write_queue.producer_event_ring, STORAGE_EVENT_STOP_LATCHED,
                                   &rt, dma_received_bytes, 0u, false);
            }
            if (stop_requested && stop_state.state == STORAGE_STOP_NONE) {
                manual_stop_seen = external_stop_requested;
                if (g_storage_stop_epoch == 0u) {
                    g_storage_stop_epoch = storage_wall_time_us();
                    if (g_storage_stop_epoch == 0u) g_storage_stop_epoch = 1u;
                }
                (void)storage_stop_state_latch(
                    &stop_state, storage_wall_time_us() + stop_timeouts.stop_harvest_us);
                stop_request_us = storage_wall_time_us();
                (void)storage_stop_state_advance(&stop_state,
                                                 STORAGE_STOP_WAIT_BOUNDARY);
                if (storage_queue_request_stop_state(&write_queue) != 0) {
                    storage_record_error(&producer_stats, STORAGE_ERR_QUEUE,
                                         "worker_stop_transition_failed");
                    goto out;
                }
                if (storage_emit_stop_phase(&rt, STORAGE_WORKER_STOP_REQUESTED,
                                            STORAGE_ERR_NONE,
                                            dma_received_bytes,
                                            auto_idle_done ? "auto_idle" : "stop_requested") != 0) {
                    storage_record_failure(&producer_stats,
                                           "stop_requested_event_send_failed");
                    goto out;
                }
            }
            if (stop_state.state == STORAGE_STOP_WAIT_BOUNDARY &&
                !rt.gopt.dry_run) {
                uint64_t boundary_now_us = storage_wall_time_us();

                if (storage_stop_boundary_should_quiesce(
                        &stop_state, rt.dma_rx_packet_open, boundary_now_us)) {
                    bool boundary_timed_out = storage_stop_state_expired(
                        &stop_state, boundary_now_us);

                    if (boundary_timed_out && rt.dma_rx_packet_open) {
                        storage_record_error(&producer_stats,
                                             STORAGE_ERR_STOP_BOUNDARY_TIMEOUT,
                                             "stop_packet_boundary_timeout");
                    }
                    if (!boundary_timed_out &&
                        storage_emit_stop_phase(
                            &rt, STORAGE_WORKER_PACKET_BOUNDARY_REACHED,
                            STORAGE_ERR_NONE,
                            dma_received_bytes, "packet_boundary_reached") != 0) {
                        storage_record_failure(
                            &producer_stats,
                            "packet_boundary_event_send_failed");
                        goto out;
                    }
                    if (!boundary_timed_out)
                        packet_boundary_us = boundary_now_us;
                    (void)storage_queue_latch_stop(&write_queue);
                    (void)storage_stop_state_advance(
                        &stop_state, STORAGE_STOP_DMA_QUIESCING);
                    stop_state.deadline_us = storage_wall_time_us() +
                                             stop_timeouts.dma_quiesce_us;
                    {
                        DmaStopResult quiesce_result =
                            storage_dma_quiesce_epoch(
                                &stop_state, g_storage_stop_epoch, &rt,
                                stop_state.deadline_us,
                                write_queue.slots.states, &dma_stop_report);
                        if (quiesce_result == DMA_STOP_FAILED) {
                            storage_record_failure(
                                &producer_stats,
                                dma_stop_report.reason[0] != '\0'
                                    ? dma_stop_report.reason
                                    : "dma_quiesce_failed");
                            (void)storage_emit_event(
                                STORAGE_WORKER_FATAL, &rt, STORAGE_ERR_DMA_STOP,
                                dma_received_bytes,
                                producer_stats.receive_integrity_risk);
                            storage_stop_state_fail(&stop_state);
                            goto out;
                        }
                        dma_stop_result = quiesce_result;
                    }
                    dma_quiesced = true;
                    dma_quiesced_us = storage_wall_time_us();
                    if (storage_queue_mark_harvesting(&write_queue) != 0) {
                        storage_record_error(&producer_stats, STORAGE_ERR_QUEUE,
                                             "worker_harvest_transition_failed");
                        goto out;
                    }
                    if (storage_emit_stop_phase(
                            &rt, STORAGE_WORKER_DMA_QUIESCED,
                            boundary_timed_out
                                ? STORAGE_ERR_STOP_BOUNDARY_TIMEOUT
                                : STORAGE_ERR_NONE,
                            dma_received_bytes,
                            boundary_timed_out ? "boundary_timeout_quiesced"
                                               : "dma_quiesced") != 0) {
                        storage_record_failure(
                            &producer_stats,
                            "dma_quiesced_event_send_failed");
                        goto out;
                    }
                    stop_state.deadline_us = storage_wall_time_us() +
                                             stop_timeouts.stop_harvest_us;
                    (void)storage_stop_state_advance(
                        &stop_state, STORAGE_STOP_HARVESTING);
                    storage_stop_harvest_state_init(&stop_harvest_state);
                    continue;
                }
            }
            if (stop_state.state == STORAGE_STOP_WAIT_BOUNDARY && rt.gopt.dry_run) {
                (void)storage_queue_latch_stop(&write_queue);
                (void)storage_stop_state_advance(&stop_state,
                                                 STORAGE_STOP_DMA_QUIESCING);
                stop_state.deadline_us = storage_wall_time_us() +
                                         stop_timeouts.dma_quiesce_us;
                {
                    DmaStopResult quiesce_result = storage_dma_quiesce_epoch(
                        &stop_state, g_storage_stop_epoch, &rt,
                        stop_state.deadline_us, write_queue.slots.states,
                        &dma_stop_report);
                    if (quiesce_result == DMA_STOP_FAILED) {
                    storage_record_failure(&producer_stats,
                                           dma_stop_report.reason[0] != '\0'
                                               ? dma_stop_report.reason : "dma_quiesce_failed");
                    (void)storage_emit_event(STORAGE_WORKER_FATAL, &rt,
                                             STORAGE_ERR_DMA_STOP,
                                             dma_received_bytes,
                                             producer_stats.receive_integrity_risk);
                    goto out;
                    }
                    dma_stop_result = quiesce_result;
                }
                dma_quiesced = true;
                if (storage_queue_mark_harvesting(&write_queue) != 0) {
                    storage_record_error(&producer_stats, STORAGE_ERR_QUEUE,
                                         "worker_harvest_transition_failed");
                    goto out;
                }
                stop_state.deadline_us = storage_wall_time_us() + stop_timeouts.stop_harvest_us;
                (void)storage_stop_state_advance(
                    &stop_state, STORAGE_STOP_HARVESTING);
            }
            if (stop_state.state == STORAGE_STOP_HARVESTING && rt.gopt.dry_run) {
                dbg_printf("[DBG][WRITE] dry-run stop requested ch=%d captured=%" PRIu64 "\n",
                           cfg->id, bytes_captured);
                stop_harvest_stable = true;
                break;
            }

            if (stop_state.state == STORAGE_STOP_HARVESTING) {
                harvest_rc = dma_harvest_completed_batch(&rt, harvest_items,
                                                         harvest_limit, &harvest_count);
            } else {
                harvest_rc = dma_harvest_batch(&rt, harvest_items, harvest_limit,
                                               100u, &harvest_count);
            }
            harvest_fatal = harvest_rc != 0;
            h = harvest_count != 0u ? 1 : (harvest_fatal ? -1 : 0);
            if (h < 0) {
                if ((rt.dma_last_completed_status & 0x70000000u) != 0u) {
                    ++producer_stats.descriptor_error_count;
                    storage_ring_event(&write_queue.producer_event_ring,
                                       STORAGE_EVENT_DESCRIPTOR_ERROR, &rt,
                                       rt.dma_last_completed_status, 0u, true);
                } else {
                    ++producer_stats.dma_error_count;
                    storage_ring_event(&write_queue.producer_event_ring,
                                       STORAGE_EVENT_DMA_ERROR, &rt,
                                       rt.dma_last_completed_status, 0u, true);
                }
                storage_record_error(&producer_stats, STORAGE_ERR_DMA_DESCRIPTOR,
                                     "dma_harvest_failed");
                storage_emit_line(STORAGE_LOG_ALWAYS_CRITICAL, "storage_receive_failed channel=%d task=%s file_index=%u"
                                  " reason=%s received_bytes=%" PRIu64
                                  " descriptor_status=0x%08x",
                                  cfg->id, args->task_no, (unsigned)effective_file_index,
                                  producer_stats.receive_integrity_risk,
                                  dma_received_bytes, rt.dma_last_completed_status);
                storage_emit_event(STORAGE_WORKER_FATAL, &rt,
                                   STORAGE_ERR_DMA_DESCRIPTOR, dma_received_bytes,
                                   producer_stats.receive_integrity_risk);
                dbg_printf("[DBG][WRITE] dma harvest error ch=%d written=%" PRIu64 " captured=%" PRIu64 "\n",
                           cfg->id, bytes_written, bytes_captured);
                goto out;
            }
            if (h == 0) {
                if (storage_first_dma_deadline_outcome(first_dma_deadline_due,
                                                       saw_dma_data, stop_requested,
                                                       harvest_rc, harvest_count) ==
                    STORAGE_FIRST_DMA_DEADLINE_EXPIRED) {
                    DmaBdSnapshot first_snapshot;

                    /* This O(1) snapshot is diagnostic only.  The normal
                     * harvest above is the sole ownership-changing test for
                     * a completed BD at the first-DMA deadline. */
                    memset(&first_snapshot, 0, sizeof(first_snapshot));
                    pthread_mutex_lock(&write_queue.lock);
                    (void)dma_get_bd_snapshot_o1(&rt, &write_queue.slots.counts,
                                                  &first_snapshot);
                    pthread_mutex_unlock(&write_queue.lock);
                    storage_stats_observe_bd_snapshot(&producer_stats,
                                                      &first_snapshot);
                    storage_record_error(&producer_stats, STORAGE_ERR_DMA_DESCRIPTOR,
                                         "first_dma_timeout");
                    storage_emit_line(STORAGE_LOG_ALWAYS_CRITICAL, "storage_first_dma_timeout channel=%d reason=first_dma_timeout"
                                      " axis_source=%d cr=0x%08x sr=0x%08x curdesc=0x%08" PRIx64
                                      " taildesc=0x%08" PRIx64
                                      " hw_owned=%u completed_unharvested=%u",
                                      cfg->id, (int)args->source, first_snapshot.s2mm_dmacr,
                                      first_snapshot.s2mm_dmasr, first_snapshot.curdesc_addr,
                                      first_snapshot.taildesc_addr,
                                      (unsigned)__atomic_load_n(&rt.dma_hw_desc_count, __ATOMIC_ACQUIRE),
                                      first_snapshot.completed_unharvested);
                    (void)storage_emit_event(STORAGE_WORKER_FATAL, &rt,
                                              STORAGE_ERR_DMA_DESCRIPTOR,
                                              dma_received_bytes, "first_dma_timeout");
                    goto out;
                }
                if (stop_state.state == STORAGE_STOP_HARVESTING) {
                    DmaBdSnapshot stop_snapshot;
                    uint64_t now_us = storage_wall_time_us();

                    memset(&stop_snapshot, 0, sizeof(stop_snapshot));
                    if (dma_get_bd_snapshot(&rt, write_queue.slots.states,
                                            &stop_snapshot) != 0) {
                        storage_record_error(&producer_stats, STORAGE_ERR_OWNERSHIP,
                                             "slot_ownership_invariant_failed");
                        (void)storage_emit_event(STORAGE_WORKER_FATAL, &rt,
                                                 STORAGE_ERR_OWNERSHIP,
                                                 dma_received_bytes,
                                                 producer_stats.receive_integrity_risk);
                        storage_stop_state_fail(&stop_state);
                        goto out;
                    }
                    storage_stats_observe_bd_snapshot(&producer_stats,
                                                      &stop_snapshot);
                    if (storage_stop_harvest_observe(
                            &stop_harvest_state, dma_quiesced, 0u,
                            stop_snapshot.completed_unharvested,
                            rt.dma_rx_packet_open, now_us,
                            STORAGE_STOP_STABLE_EMPTY_SCANS,
                            STORAGE_STOP_STABLE_EMPTY_US)) {
                        dbg_printf("[DBG][WRITE] stopped DMA stable-empty harvest done"
                                   " ch=%d captured=%" PRIu64 "\n",
                                   cfg->id, bytes_captured);
                        stop_harvest_stable = true;
                        if (storage_emit_stop_phase(
                                &rt, STORAGE_WORKER_HARVEST_STABLE_EMPTY,
                                STORAGE_ERR_NONE,
                                dma_received_bytes, "harvest_stable_empty") != 0) {
                            storage_record_failure(
                                &producer_stats,
                                "harvest_stable_event_send_failed");
                            goto out;
                        }
                        break;
                    }
                    if (storage_stop_state_expired(&stop_state, now_us)) {
                        storage_record_error(&producer_stats,
                                             STORAGE_ERR_STOP_HARVEST_TIMEOUT,
                                             "stop_harvest_timeout");
                        storage_stop_state_fail(&stop_state);
                        goto out;
                    }
                    usleep(50u);
                    continue;
                }
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
                {
                    bool ring_pressure_stop = storage_update_ring_pressure(
                        &producer_stats, &write_queue, &rt, args->task_no,
                        effective_file_index, dma_received_bytes);

                    if (bd_snapshot.dma_writable != 0u && ring_pressure_stop) {
                        storage_write_request_stop();
                        continue;
                    }
                }
                if (bd_snapshot.dma_writable == 0u) {
                    ++producer_stats.dma_no_free_slot_count;
                    storage_ring_event(&write_queue.producer_event_ring,
                                       STORAGE_EVENT_DMA_BD_EXHAUSTED, &rt,
                                       bd_snapshot.completed_unharvested,
                                       bd_snapshot.ready_slots, true);
                    storage_record_error(&producer_stats, STORAGE_ERR_DMA_DESCRIPTOR,
                                         "dma_bd_exhausted");
                    if (!ring_full_logged) {
                        storage_emit_line(STORAGE_LOG_ALWAYS_CRITICAL, "storage_ddr_full channel=%d busy_slots=%u total_slots=%u"
                                          " buffered_bytes=%" PRIu64 " captured_bytes=%" PRIu64,
                                          cfg->id, (unsigned)busy_slots,
                                          (unsigned)rt.dma_desc_count, buffered_bytes,
                                          bytes_captured);
                        ring_full_logged = true;
                    }
                    storage_emit_event(STORAGE_WORKER_FATAL, &rt,
                                       STORAGE_ERR_DMA_DESCRIPTOR, dma_received_bytes,
                                       "dma_bd_exhausted");
                    goto out;
                }
                if (coordinated_idle_enabled && !stop_requested &&
                    now_us >= next_input_idle_scan_us) {
                    StorageInputIdleEvent idle_event = storage_input_idle_observe(
                        &input_idle_state, now_us, dma_observed_bytes,
                        producer_stats.dma_desc_completed_count,
                        rt.dma_rx_packet_open,
                        producer_stats.dma_error_count != 0u,
                        (uint64_t)(app_config
                                       ? app_config->idle_required_ms : 500u) *
                            1000u,
                        app_config ? app_config->idle_required_scans : 5u);
                    next_input_idle_scan_us = now_us +
                        (uint64_t)(app_config
                                       ? app_config->idle_scan_interval_ms : 100u) *
                            1000u;
                    if (idle_event == STORAGE_INPUT_IDLE_CANDIDATE) {
                        uint64_t idle_ms = now_us >= input_idle_state.last_dma_activity_us
                                               ? (now_us - input_idle_state.last_dma_activity_us) /
                                                     1000u
                                               : 0u;
                        storage_emit_line(
                            STORAGE_LOG_SUMMARY,
                            "storage_input_idle_candidate task=%s channel=%d"
                            " idle_ms=%" PRIu64 " bytes=%" PRIu64,
                            args->task_no, cfg->id, idle_ms,
                            dma_observed_bytes);
                        if (storage_emit_event(
                                STORAGE_WORKER_INPUT_IDLE_CANDIDATE, &rt,
                                STORAGE_ERR_NONE, dma_observed_bytes,
                                "stable_input_idle") != 0) {
                            storage_record_error(&producer_stats,
                                                 STORAGE_ERR_IPC,
                                                 "input_idle_event_send_failed");
                            goto out;
                        }
                    } else if (idle_event == STORAGE_INPUT_ACTIVE) {
                        storage_emit_line(
                            STORAGE_LOG_SUMMARY,
                            "storage_input_active task=%s channel=%d"
                            " reason=new_dma_activity bytes=%" PRIu64,
                            args->task_no, cfg->id, dma_observed_bytes);
                        if (storage_emit_event(
                                STORAGE_WORKER_INPUT_ACTIVE, &rt,
                                STORAGE_ERR_NONE, dma_observed_bytes,
                                "new_dma_activity") != 0) {
                            storage_record_error(&producer_stats,
                                                 STORAGE_ERR_IPC,
                                                 "input_active_event_send_failed");
                            goto out;
                        }
                    }
                }
                if (auto_idle_enabled && !stop_requested && dma_idle_done_ms > 0u &&
                    saw_dma_data && producer_stats.dma_desc_completed_count > 0u &&
                    !rt.dma_rx_packet_open) {
                    uint64_t idle_us = storage_elapsed_us(last_dma_us);
                    if (idle_us >= (uint64_t)dma_idle_done_ms * 1000ull) {
                        StorageQueueSnapshot idle_snapshot;

                        storage_queue_snapshot(&write_queue, &idle_snapshot);
                        storage_emit_line(STORAGE_LOG_DEBUG, "storage_dma_idle_done channel=%d idle_ms=%" PRIu64
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
                        continue;
                    }
                }
                if (!stop_requested) {
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
                            if (storage_text_output_enabled()) {
                                printf("storage_idle_detected channel=%d task=%s file_index=%u idle_ms=%" PRIu64
                                       " dma_received_bytes=%" PRIu64 " queued_file_bytes=%" PRIu64
                                       " manual_stop_required=1\n",
                                       cfg->id, args->task_no, (unsigned)effective_file_index,
                                       idle_ms, dma_received_bytes, bytes_captured);
                                fflush(stdout);
                            }
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
            {
                PendingDdrSlot pending[16];
                uint64_t batch_base_file_offset = bytes_captured;
                uint64_t batch_base_lba = next_queue_lba;
                uint64_t batch_bytes = 0u;
                uint64_t batch_sectors = 0u;
                uint32_t i;
                uint32_t valid_count = 0u;
                bool batch_item_invalid = false;

                for (i = 0u; i < harvest_count; ++i) {
                    uint64_t observed_bytes = harvest_items[i].actual_bytes;
                    uint64_t queued_bytes = observed_bytes;
                    uint64_t remaining = observed_bytes;
                    uint64_t sectors;
                    uint64_t media_bytes;
                    bool stop_phase = stop_state.state != STORAGE_STOP_NONE;

                    if (observed_bytes == 0u || observed_bytes > rt.dma_desc_bytes) {
                        storage_record_failure(&producer_stats, "invalid_dma_harvest_bytes");
                        storage_mark_harvest_slot_failed(&write_queue, harvest_items[i].slot);
                        storage_ring_event(&write_queue.producer_event_ring,
                                           STORAGE_EVENT_DESCRIPTOR_ERROR, &rt,
                                           observed_bytes, rt.dma_desc_bytes, true);
                        batch_item_invalid = true;
                        break;
                    }
                    if (bounded) {
                        if (batch_base_file_offset > requested_size ||
                            batch_bytes > requested_size - batch_base_file_offset) {
                            storage_record_failure(&producer_stats,
                                                   "bounded_harvest_overflow");
                            storage_mark_harvest_slot_failed(
                                &write_queue, harvest_items[i].slot);
                            batch_item_invalid = true;
                            break;
                        }
                        remaining = requested_size - batch_base_file_offset - batch_bytes;
                        if (remaining == 0u) {
                            if (dma_observed_bytes > UINT64_MAX - observed_bytes ||
                                dma_harvested_payload_bytes > UINT64_MAX - observed_bytes ||
                                dma_received_bytes > UINT64_MAX - observed_bytes ||
                                tail_unqueued_bytes > UINT64_MAX - observed_bytes) {
                                storage_record_failure(&producer_stats,
                                                       "dma_byte_counter_overflow");
                                storage_mark_harvest_slot_failed(
                                    &write_queue, harvest_items[i].slot);
                                batch_item_invalid = true;
                                break;
                            }
                            storage_record_error(&producer_stats,
                                                 STORAGE_ERR_LATE_COMPLETION,
                                                 "late_completed_descriptor");
                            storage_record_deferred_stop_error(
                                &write_queue, STORAGE_ERR_LATE_COMPLETION,
                                "late_completed_descriptor");
                            if (storage_release_harvested_slot(
                                    &write_queue, harvest_items[i].slot) != 0) {
                                batch_item_invalid = true;
                                break;
                            }
                            dma_observed_bytes += observed_bytes;
                            dma_harvested_payload_bytes += observed_bytes;
                            dma_received_bytes += observed_bytes;
                            tail_unqueued_bytes += observed_bytes;
                            storage_stats_record_dma_desc(&producer_stats,
                                                          storage_wall_time_us());
                            stop_tail_seen = true;
                            continue;
                        }
                    }
                    if (bounded && queued_bytes > remaining) queued_bytes = remaining;
                    sectors = bytes_to_sectors(queued_bytes);
                    if (sectors == 0u ||
                        sectors > UINT64_MAX / (uint64_t)SECTOR_SIZE) {
                        storage_record_failure(&producer_stats, "media_byte_overflow");
                        storage_mark_harvest_slot_failed(&write_queue,
                                                         harvest_items[i].slot);
                        batch_item_invalid = true;
                        break;
                    }
                    media_bytes = sectors * (uint64_t)SECTOR_SIZE;
                    if (dma_observed_bytes > UINT64_MAX - observed_bytes ||
                        dma_harvested_payload_bytes > UINT64_MAX - queued_bytes ||
                        dma_received_bytes > UINT64_MAX - queued_bytes) {
                        storage_record_failure(&producer_stats, "dma_byte_counter_overflow");
                        storage_mark_harvest_slot_failed(&write_queue,
                                                         harvest_items[i].slot);
                        batch_item_invalid = true;
                        break;
                    }
                    dma_observed_bytes += observed_bytes;
                    dma_harvested_payload_bytes += queued_bytes;
                    dma_received_bytes += queued_bytes;
                    storage_stats_record_dma_desc(&producer_stats,
                                                  storage_wall_time_us());

                    {
                        StorageStopTailDisposition tail_disposition =
                            storage_stop_tail_disposition(
                                stop_phase, stop_tail_seen, queued_bytes, media_bytes,
                                false);
                    if (tail_disposition != STORAGE_STOP_TAIL_QUEUE) {
                        const char *deferred_reason =
                            tail_disposition == STORAGE_STOP_TAIL_DEFER_LATE
                                ? "late_completed_descriptor"
                                : "unaligned_payload_not_safely_paddable";
                        StorageErrorCode deferred_error =
                            tail_disposition == STORAGE_STOP_TAIL_DEFER_LATE
                                ? STORAGE_ERR_LATE_COMPLETION
                                : STORAGE_ERR_TAIL_UNALIGNED;

                        if (tail_unqueued_bytes > UINT64_MAX - queued_bytes ||
                            storage_release_harvested_slot(
                                &write_queue, harvest_items[i].slot) != 0) {
                            storage_record_failure(&producer_stats,
                                                   "slot_ownership_invariant_failed");
                            batch_item_invalid = true;
                            break;
                        }
                        tail_unqueued_bytes += queued_bytes;
                        stop_tail_seen = true;
                        storage_record_error(&producer_stats, deferred_error,
                                             deferred_reason);
                        storage_record_deferred_stop_error(&write_queue,
                                                           deferred_error,
                                                           deferred_reason);
                        continue;
                    }
                    }
                    memset(&pending[valid_count], 0, sizeof(pending[valid_count]));
                    pending[valid_count].slot = harvest_items[i].slot;
                    pending[valid_count].bytes = queued_bytes;
                    pending[valid_count].chunk_index =
                        harvest_items[i].submission_sequence;
                    pending[valid_count].file_offset = batch_base_file_offset + batch_bytes;
                    if (batch_sectors > UINT64_MAX - batch_base_lba) {
                        storage_record_failure(&producer_stats, "lba_range_overflow");
                        storage_mark_harvest_slot_failed(&write_queue, harvest_items[i].slot);
                        batch_item_invalid = true;
                        break;
                    }
                    pending[valid_count].start_lba = batch_base_lba + batch_sectors;
                    pending[valid_count].sectors = sectors;
                    pending[valid_count].media_bytes = media_bytes;
                    if ((uint64_t)pending[valid_count].slot >
                        (UINT64_MAX - rt.cfg->ddr_hw_base) / rt.dma_desc_bytes) {
                        storage_record_failure(&producer_stats, "ddr_address_overflow");
                        storage_mark_harvest_slot_failed(&write_queue, harvest_items[i].slot);
                        batch_item_invalid = true;
                        break;
                    }
                    pending[valid_count].hw_addr = rt.cfg->ddr_hw_base +
                                         (uint64_t)pending[valid_count].slot * rt.dma_desc_bytes;
                    batch_bytes += queued_bytes;
                    batch_sectors += pending[valid_count].sectors;
                    ++valid_count;
                }
                if (valid_count != 0u &&
                    storage_local_queue_push_batch(&write_queue, pending, valid_count) != 0) {
                    storage_ring_event(&write_queue.producer_event_ring,
                                       STORAGE_EVENT_QUEUE_FULL, &rt,
                                       valid_count, write_queue.capacity, true);
                    storage_record_error(&producer_stats, STORAGE_ERR_QUEUE,
                                         "storage_queue_full_or_state_error");
                    storage_emit_event(STORAGE_WORKER_FATAL, &rt, STORAGE_ERR_QUEUE,
                                       dma_received_bytes,
                                       "storage_queue_full_or_state_error");
                    goto out;
                }
                bytes_captured = batch_base_file_offset + batch_bytes;
                if (queued_payload_bytes > UINT64_MAX - batch_bytes) {
                    storage_record_failure(&producer_stats, "queued_byte_counter_overflow");
                    goto out;
                }
                queued_payload_bytes += batch_bytes;
                if (batch_sectors > UINT64_MAX - batch_base_lba) {
                    storage_record_failure(&producer_stats, "lba_range_overflow");
                    goto out;
                }
                next_queue_lba = batch_base_lba + batch_sectors;
                ring_full_logged = false;
                saw_dma_data = saw_dma_data || harvest_count != 0u;
                idle_notice_logged = false;
                last_dma_us = storage_wall_time_us();
                if (harvest_fatal || batch_item_invalid) {
                    if (batch_item_invalid && !harvest_fatal)
                        storage_record_failure(&producer_stats, "invalid_dma_harvest_bytes");
                    storage_record_error(&producer_stats, STORAGE_ERR_DMA_DESCRIPTOR,
                                         "dma_harvest_failed");
                    storage_emit_event(STORAGE_WORKER_FATAL, &rt,
                                       STORAGE_ERR_DMA_DESCRIPTOR, dma_received_bytes,
                                       producer_stats.receive_integrity_risk);
                    goto out;
                }
                if (stop_state.state == STORAGE_STOP_HARVESTING) {
                    (void)storage_stop_harvest_observe(
                        &stop_harvest_state, dma_quiesced, harvest_count, 0u,
                        rt.dma_rx_packet_open, storage_wall_time_us(),
                        STORAGE_STOP_STABLE_EMPTY_SCANS,
                        STORAGE_STOP_STABLE_EMPTY_US);
                }
                storage_stats_finish_harvest_batch(&producer_stats);
                if (storage_update_ring_pressure(
                        &producer_stats, &write_queue, &rt, args->task_no,
                        effective_file_index, dma_received_bytes)) {
                    storage_write_request_stop();
                }
                if (harvest_count >= harvest_batch_max_cfg) sched_yield();
                continue;
            }
        }
    }
    storage_stats_update_ring_full(&producer_stats,
                                   false,
                                   storage_wall_time_us(),
                                   dma_received_bytes);
    if (!stop_harvest_stable || stop_state.state != STORAGE_STOP_HARVESTING) {
        storage_record_failure(&producer_stats, "stop_harvest_not_stable");
        storage_stop_state_fail(&stop_state);
        goto out;
    }
    tail_incomplete = dma_s2mm_tail_incomplete(&rt);
    if (tail_incomplete) {
        storage_record_error(&producer_stats, STORAGE_ERR_TAIL_UNALIGNED,
                             "tail_descriptor_incomplete");
    }
    (void)storage_stop_state_advance(&stop_state, STORAGE_STOP_PRODUCER_DONE);
    producer_done_us = storage_wall_time_us();
    storage_queue_finish(&write_queue);
    (void)storage_stop_state_advance(&stop_state, STORAGE_STOP_WRITER_DRAINING);
    dma_stop_attempted = true;
    stop_state.deadline_us = storage_wall_time_us() + stop_timeouts.writer_drain_us;
    if (storage_join_writer_deadline(writer_thread, stop_state.deadline_us) != 0) {
        storage_record_error(&producer_stats, STORAGE_ERR_NVME_TIMEOUT,
                             "writer_drain_timeout");
        (void)storage_emit_event(STORAGE_WORKER_FATAL, &rt, STORAGE_ERR_NVME_TIMEOUT,
                                 dma_received_bytes,
                                 producer_stats.receive_integrity_risk);
        storage_queue_request_abort(&write_queue);
        stop_state.deadline_us = storage_wall_time_us() + stop_timeouts.nvme_abort_us;
        if (storage_join_writer_deadline(writer_thread, stop_state.deadline_us) != 0) {
            storage_record_failure(&producer_stats, "writer_abort_timeout");
        } else {
            writer_started = false;
        }
        goto out;
    }
    writer_started = false;
    writer_drained_us = storage_wall_time_us();
    if (storage_emit_stop_phase(&rt, STORAGE_WORKER_WRITER_DRAINED,
                                STORAGE_ERR_NONE,
                                dma_received_bytes, "writer_drained") != 0) {
        storage_record_failure(&producer_stats,
                               "writer_drained_event_send_failed");
        goto out;
    }
    /* The final DMA reset is deliberately last: all queued slots and all
     * NVMe completions have drained before hardware state is destroyed. */
    {
        DmaStopResult finalize_result = dma_finalize_stop_s2mm_with_state(
            &rt, write_queue.slots.states, &dma_stop_report);
        if (dma_quiesced && dma_stop_result != DMA_STOP_RESET_RECOVERED)
            dma_stop_result = finalize_result;
    }
    if (dma_stop_result == DMA_STOP_FAILED) {
        dma_stop_failed = true;
        storage_record_error(&producer_stats, STORAGE_ERR_DMA_STOP,
                             "dma_stop_recovery_failed");
        dbg_printf("[DBG][WRITE] dma S2MM finalization failed ch=%d task=%s idx=%u\n",
                   cfg->id, args->task_no, (unsigned)effective_file_index);
    }
    if (stop_state.state == STORAGE_STOP_WRITER_DRAINING) {
        (void)storage_stop_state_advance(&stop_state, STORAGE_STOP_FINALIZING);
    }
    if (storage_queue_mark_finalizing(&write_queue) != 0) {
        storage_record_error(&producer_stats, STORAGE_ERR_QUEUE,
                             "worker_finalize_transition_failed");
        goto out;
    }
    if (write_queue.error) {
        dbg_printf("[DBG][WRITE] writer thread failed ch=%d captured=%" PRIu64 "\n",
                   cfg->id, bytes_captured);
        goto out;
    }
    storage_maybe_dump_event_rings(&write_queue, &rt, false, manual_stop_seen);
    storage_stats_finish_harvest_batch(&producer_stats);
    storage_queue_snapshot(&write_queue, &final_queue_snapshot);
    storage_emit_final_perf_window(&rt, &producer_stats, &write_queue,
                                   dma_received_bytes, storage_wall_time_us());
    final_perf_emitted = true;
    if (final_queue_snapshot.completed_unharvested_slots >
        UINT64_MAX / (uint64_t)rt.dma_desc_bytes) {
        completed_unharvested_bytes = UINT64_MAX;
    } else {
        completed_unharvested_bytes =
            (uint64_t)final_queue_snapshot.completed_unharvested_slots *
            (uint64_t)rt.dma_desc_bytes;
    }
    bytes_written = write_queue.bytes_written;
    chunks = write_queue.chunks;
    nvme_write_us = write_queue.nvme_write_us;
    (void)storage_queue_busy_count(&write_queue, &max_busy_slots);
    (void)storage_queue_buffered_bytes(&write_queue, &max_buffered_bytes);
    if (storage_queue_mark_done(&write_queue) != 0) {
        storage_record_error(&producer_stats, STORAGE_ERR_QUEUE,
                             "worker_done_transition_failed");
        goto out;
    }
    storage_queue_destroy(&write_queue);
    write_queue_ready = false;
    final_us = storage_wall_time_us();
    (void)storage_stop_state_advance(&stop_state, STORAGE_STOP_FINISHED);

    total_sectors = next_queue_lba - start_lba;
    elapsed_us = storage_elapsed_us(capture_start_us);
    if (total_sectors > UINT32_MAX) {
        fprintf(stderr,
                "Captured file exceeds legacy metadata sector limit: channel=%d sectors=%" PRIu64 "\n",
                cfg->id,
                total_sectors);
        goto out;
    }

    if (storage_write_mode_commits_locally(mode) &&
        producer_stats.receive_integrity_ok && !tail_incomplete &&
        tail_unqueued_bytes == 0u && completed_unharvested_bytes == 0u &&
        queued_payload_bytes == write_queue.bytes_written &&
        queued_payload_bytes ==
            __atomic_load_n(&rt.nvme_payload_bytes_done, __ATOMIC_ACQUIRE)) {
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
        if (metadata_write(&rt, table) != 0) {
            dbg_printf("[DBG][WRITE] metadata_write failed ch=%d task=%s idx=%u\n",
                       cfg->id, args->task_no, (unsigned)effective_file_index);
            goto out;
        }
    }
    data_persisted = true;

    if (storage_write_mode_commits_locally(mode)) {
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
    }
    {
        uint64_t nvme_cmd_count = __atomic_load_n(&rt.nvme_cmd_count, __ATOMIC_ACQUIRE);
        uint64_t nvme_cmd_bytes_total = __atomic_load_n(&rt.nvme_cmd_bytes_total, __ATOMIC_ACQUIRE);
        uint64_t nvme_write_bytes_done = __atomic_load_n(&rt.nvme_write_bytes_done, __ATOMIC_ACQUIRE);
        uint64_t nvme_payload_bytes_done = __atomic_load_n(&rt.nvme_payload_bytes_done, __ATOMIC_ACQUIRE);
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
        StorageDrainInvariant drain_invariant;
        StorageDrainStableState drain_stable_state;
        bool drain_invariant_ok;
        char capture_rate[32];
        char nvme_active_rate[32];
        char nvme_wall_rate[32];
        char task_wall_rate[32];
        snprintf(expected_name, sizeof(expected_name),
                 "SRC_REAL_EXPECTED_BYTES_CH%d", cfg->id);
        expected_available = storage_env_u64(expected_name, &expected_bytes) != 0;
        end_to_end_ok = expected_available
                            ? expected_bytes == dma_received_bytes &&
                                  expected_bytes == nvme_payload_bytes_done
                            : !strict_end_to_end;
        memset(&drain_invariant, 0, sizeof(drain_invariant));
        drain_invariant.input_complete = stop_state.state >=
                                         STORAGE_STOP_PRODUCER_DONE;
        drain_invariant.dma_quiesced = dma_quiesced;
        drain_invariant.dma_harvested_payload_bytes =
            dma_harvested_payload_bytes;
        drain_invariant.queued_payload_bytes = queued_payload_bytes;
        drain_invariant.nvme_completed_payload_bytes = nvme_payload_bytes_done;
        drain_invariant.file_bytes = bytes_written;
        drain_invariant.tail_unqueued_bytes = tail_unqueued_bytes;
        drain_invariant.completed_unharvested =
            final_queue_snapshot.completed_unharvested_slots;
        drain_invariant.ready_count = final_queue_snapshot.ready_depth_current;
        drain_invariant.active_count = final_queue_snapshot.nvme_busy_slots;
        drain_invariant.global_inflight =
            __atomic_load_n(&rt.nvme_active_qd_current, __ATOMIC_ACQUIRE);
        drain_invariant.submit_count = nvme_cmd_count;
        drain_invariant.completion_count =
            __atomic_load_n(&rt.nvme_cq_completed, __ATOMIC_ACQUIRE);
        drain_invariant.ring_occupied_bytes = final_queue_snapshot.buffered_bytes;
        storage_drain_stable_init(&drain_stable_state);
        drain_invariant_ok = false;
        {
            uint32_t scan;
            uint32_t required_scans = app_config
                                          ? app_config->drain_stable_scans : 3u;
            uint64_t stable_us = app_config
                                     ? app_config->drain_stable_us : 100u;

            for (scan = 0u; scan < required_scans; ++scan) {
                if (storage_drain_stable_observe(
                        &drain_stable_state, &drain_invariant,
                        storage_wall_time_us(), required_scans, stable_us)) {
                    drain_invariant_ok = true;
                    break;
                }
                if (!storage_drain_invariant_ok(&drain_invariant)) break;
                usleep(50u);
            }
        }
        bool storage_integrity_ok = !tail_incomplete && !dma_stop_failed &&
                                    completed_unharvested_bytes == 0u &&
                                    drain_invariant_ok;
        if (require_nonempty_payload && dma_received_bytes == 0u) {
            storage_record_failure(&producer_stats, "zero_payload_not_allowed");
            storage_integrity_ok = false;
        }
        bool integrity_ok = producer_stats.receive_integrity_ok && end_to_end_ok &&
                            !producer_stats.integrity_risk_ring_full &&
                            tail_unqueued_bytes == 0u && storage_integrity_ok;
        final_integrity_ok = integrity_ok;
        const char *integrity_risk = "none";

        storage_emit_line(drain_invariant_ok ? STORAGE_LOG_SUMMARY
                                             : STORAGE_LOG_ALWAYS_CRITICAL,
                          "storage_drain_invariants channel=%d stop_epoch=%" PRIu64
                          " stop_request_us=%" PRIu64 " packet_boundary_us=%" PRIu64
                          " dma_quiesced_us=%" PRIu64
                          " last_bd_complete_us=%" PRIu64
                          " last_bd_harvest_us=%" PRIu64
                          " producer_done_us=%" PRIu64
                          " writer_drained_us=%" PRIu64 " final_us=%" PRIu64
                          " dma_observed_bytes=%" PRIu64
                          " dma_harvested_payload_bytes=%" PRIu64
                          " queued_payload_bytes=%" PRIu64
                          " nvme_completed_payload_bytes=%" PRIu64
                          " nvme_completed_media_bytes=%" PRIu64
                          " tail_unqueued_bytes=%" PRIu64
                          " padding_bytes=%" PRIu64
                          " harvested_bd_count=%" PRIu64
                          " queued_slot_count=%" PRIu64
                          " completed_slot_count=%" PRIu64
                          " recycled_slot_count=%" PRIu64
                          " ready_count=%u active_count=%u global_inflight=%u"
                          " submit_count=%" PRIu64 " completion_count=%" PRIu64
                          " completed_unharvested=%u free_dma_bd=%u"
                          " ring_occupied_bytes=%" PRIu64 " invariant_ok=%u",
                          cfg->id, g_storage_stop_epoch, stop_request_us,
                          packet_boundary_us, dma_quiesced_us,
                          producer_stats.last_dma_desc_us,
                          producer_stats.last_dma_desc_us,
                          producer_done_us, writer_drained_us, final_us,
                          dma_observed_bytes, dma_harvested_payload_bytes,
                          queued_payload_bytes, nvme_payload_bytes_done,
                          nvme_write_bytes_done, tail_unqueued_bytes,
                          nvme_write_bytes_done >= nvme_payload_bytes_done
                              ? nvme_write_bytes_done - nvme_payload_bytes_done : 0u,
                          producer_stats.dma_desc_completed_count,
                          final_queue_snapshot.queued_slot_count,
                          final_queue_snapshot.completed_slot_count,
                          final_queue_snapshot.recycled_slot_count,
                          drain_invariant.ready_count,
                          drain_invariant.active_count,
                          drain_invariant.global_inflight,
                          drain_invariant.submit_count,
                          drain_invariant.completion_count,
                          drain_invariant.completed_unharvested,
                          final_queue_snapshot.free_slots,
                          final_queue_snapshot.buffered_bytes,
                          drain_invariant_ok ? 1u : 0u);

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
        } else if (require_nonempty_payload && dma_received_bytes == 0u) {
            integrity_risk = "zero_payload_not_allowed";
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

        if (mode == STORAGE_WRITE_STANDALONE)
            storage_emit_line(integrity_ok ? STORAGE_LOG_SUMMARY : STORAGE_LOG_ALWAYS_CRITICAL,
                          "storage_result channel=%d task=%s file_index=%u status=%s"
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
        storage_format_rate(capture_rate, sizeof(capture_rate),
                            dma_observed_bytes, dma_observed_us);
        storage_format_rate(nvme_active_rate, sizeof(nvme_active_rate),
                            nvme_payload_bytes_done, nvme_write_us);
        storage_format_rate(nvme_wall_rate, sizeof(nvme_wall_rate),
                            nvme_payload_bytes_done, nvme_wall_us);
        storage_format_rate(task_wall_rate, sizeof(task_wall_rate),
                            nvme_payload_bytes_done, elapsed_us);
        storage_emit_line(STORAGE_LOG_SUMMARY, "storage_result_perf channel=%d task=%s file_index=%u elapsed_ms=%" PRIu64
                          " capture_window_mib_s=%s"
                          " nvme_active_ms=%" PRIu64 " nvme_active_mib_s=%s"
                          " nvme_wall_ms=%" PRIu64 " nvme_wall_mib_s=%s"
                          " task_wall_mib_s=%s nvme_qd_effective=%u"
                          " nvme_active_qd_avg=%.3f nvme_active_qd_max=%u",
                          cfg->id, args->task_no, (unsigned)effective_file_index,
                          elapsed_us / 1000u,
                          capture_rate,
                          nvme_write_us / 1000u,
                          nvme_active_rate,
                          nvme_wall_us / 1000u,
                          nvme_wall_rate,
                          task_wall_rate,
                          (unsigned)rt.nvme_qd_effective,
                          rt.nvme_active_qd_observed_us > 0u
                              ? (double)rt.nvme_active_qd_integral_us /
                                    (double)rt.nvme_active_qd_observed_us : 0.0,
                          (unsigned)__atomic_load_n(&rt.nvme_active_qd_max, __ATOMIC_ACQUIRE));
        storage_emit_line(STORAGE_LOG_DEBUG, "storage_result_diag channel=%d task=%s file_index=%u"
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
        storage_emit_line(STORAGE_LOG_SUMMARY, "storage_result_receive channel=%d task=%s file_index=%u"
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
            result->dma_observed_bytes = dma_observed_bytes;
            result->dma_harvested_payload_bytes = dma_harvested_payload_bytes;
            result->queued_payload_bytes = queued_payload_bytes;
            result->nvme_completed_bytes = nvme_payload_bytes_done;
            result->nvme_media_bytes = nvme_write_bytes_done;
            result->nvme_padding_bytes = nvme_write_bytes_done >= nvme_payload_bytes_done
                                             ? nvme_write_bytes_done - nvme_payload_bytes_done : 0u;
            result->tail_unqueued_bytes = tail_unqueued_bytes;
            result->completed_unharvested_bytes = completed_unharvested_bytes;
            result->stop_epoch = g_storage_stop_epoch;
            result->stop_request_us = stop_request_us;
            result->packet_boundary_us = packet_boundary_us;
            result->dma_quiesced_us = dma_quiesced_us;
            result->last_bd_complete_us = producer_stats.last_dma_desc_us;
            result->last_bd_harvest_us = producer_stats.last_dma_desc_us;
            result->producer_done_us = producer_done_us;
            result->writer_drained_us = writer_drained_us;
            result->final_us = final_us;
            result->harvested_bd_count = producer_stats.dma_desc_completed_count;
            result->queued_slot_count = final_queue_snapshot.queued_slot_count;
            result->completed_slot_count = final_queue_snapshot.completed_slot_count;
            result->recycled_slot_count = final_queue_snapshot.recycled_slot_count;
            result->final_ready_count = drain_invariant.ready_count;
            result->final_active_count = drain_invariant.active_count;
            result->final_global_inflight = drain_invariant.global_inflight;
            result->submit_count = drain_invariant.submit_count;
            result->completion_count = drain_invariant.completion_count;
            result->final_completed_unharvested =
                drain_invariant.completed_unharvested;
            result->final_free_dma_bd = final_queue_snapshot.free_slots;
            result->final_ring_occupied_bytes = final_queue_snapshot.buffered_bytes;
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
            result->primary_error = producer_stats.primary_error != STORAGE_ERR_NONE
                                        ? producer_stats.primary_error
                                        : rt.nvme_primary_error;
            result->secondary_error = producer_stats.secondary_error != STORAGE_ERR_NONE
                                          ? producer_stats.secondary_error
                                          : rt.nvme_secondary_error;
            snprintf(result->integrity_risk,
                     sizeof(result->integrity_risk),
                     "%s",
                     integrity_risk);
            snprintf(result->secondary_reason,
                     sizeof(result->secondary_reason),
                     "%s",
                     producer_stats.secondary_reason);
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
        snprintf(result->task_no, sizeof(result->task_no), "%s", args->task_no);
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
    if (dma_started && !dma_stop_attempted) {
        if (g_storage_stop_epoch == 0u) {
            g_storage_stop_epoch = storage_wall_time_us();
            if (g_storage_stop_epoch == 0u) g_storage_stop_epoch = 1u;
        }
        if (stop_state.state == STORAGE_STOP_NONE) {
            if (stop_request_us == 0u) stop_request_us = storage_wall_time_us();
            (void)storage_stop_state_latch(
                &stop_state, storage_wall_time_us() + stop_timeouts.stop_harvest_us);
            (void)storage_stop_state_advance(&stop_state, STORAGE_STOP_WAIT_BOUNDARY);
            (void)storage_queue_request_stop_state(&write_queue);
            (void)storage_queue_latch_stop(&write_queue);
            (void)storage_stop_state_advance(&stop_state,
                                             STORAGE_STOP_DMA_QUIESCING);
            stop_state.deadline_us = storage_wall_time_us() +
                                     stop_timeouts.dma_quiesce_us;
        } else if (stop_state.state == STORAGE_STOP_REQUESTED) {
            (void)storage_stop_state_advance(&stop_state, STORAGE_STOP_WAIT_BOUNDARY);
            (void)storage_queue_request_stop_state(&write_queue);
            (void)storage_queue_latch_stop(&write_queue);
            (void)storage_stop_state_advance(&stop_state, STORAGE_STOP_DMA_QUIESCING);
            stop_state.deadline_us = storage_wall_time_us() +
                                     stop_timeouts.dma_quiesce_us;
        } else if (stop_state.state == STORAGE_STOP_WAIT_BOUNDARY) {
            (void)storage_queue_request_stop_state(&write_queue);
            (void)storage_queue_latch_stop(&write_queue);
            (void)storage_stop_state_advance(&stop_state, STORAGE_STOP_DMA_QUIESCING);
            stop_state.deadline_us = storage_wall_time_us() +
                                     stop_timeouts.dma_quiesce_us;
        }
        if (!dma_quiesced) {
            DmaStopResult quiesce_result = storage_dma_quiesce_epoch(
                &stop_state, g_storage_stop_epoch, &rt,
                stop_state.deadline_us, write_queue.slots.states,
                &dma_stop_report);
            if (quiesce_result != DMA_STOP_FAILED) {
                dma_quiesced = true;
                dma_stop_result = quiesce_result;
                (void)storage_queue_mark_harvesting(&write_queue);
            } else {
                dma_stop_failed = true;
                storage_record_failure(&producer_stats,
                                       dma_stop_report.reason[0] != '\0'
                                           ? dma_stop_report.reason : "dma_quiesce_failed");
            }
        }
        stop_state.deadline_us = storage_wall_time_us() + stop_timeouts.stop_harvest_us;
        if (dma_quiesced) {
            StorageStopHarvestState cleanup_harvest_state;

            storage_stop_harvest_state_init(&cleanup_harvest_state);
            for (;;) {
                DmaHarvestItem stopped_items[16];
                uint32_t stopped_count = 0u;
                uint32_t i;

                if (storage_stop_state_expired(&stop_state, storage_wall_time_us())) {
                    storage_record_error(&producer_stats,
                                         STORAGE_ERR_STOP_HARVEST_TIMEOUT,
                                         "stop_harvest_timeout");
                    break;
                }
                if (dma_harvest_completed_batch(&rt, stopped_items, 16u,
                                                &stopped_count) != 0) {
                    storage_record_failure(&producer_stats, "stopped_dma_harvest_failed");
                    break;
                }
                if (stopped_count == 0u) {
                    DmaBdSnapshot stop_snapshot;
                    uint64_t now_us = storage_wall_time_us();

                    memset(&stop_snapshot, 0, sizeof(stop_snapshot));
                    if (dma_get_bd_snapshot(&rt, write_queue.slots.states,
                                            &stop_snapshot) != 0) {
                        storage_record_failure(&producer_stats,
                                               "slot_ownership_invariant_failed");
                        break;
                    }
                    storage_stats_observe_bd_snapshot(&producer_stats,
                                                      &stop_snapshot);
                    if (storage_stop_harvest_observe(
                            &cleanup_harvest_state, true, 0u,
                            stop_snapshot.completed_unharvested,
                            rt.dma_rx_packet_open, now_us,
                            STORAGE_STOP_STABLE_EMPTY_SCANS,
                            STORAGE_STOP_STABLE_EMPTY_US)) {
                        break;
                    }
                    if (now_us >= stop_state.deadline_us) {
                        storage_record_failure(&producer_stats,
                                               "stop_harvest_timeout");
                        break;
                    }
                    usleep(50u);
                    continue;
                }
                (void)storage_stop_harvest_observe(
                    &cleanup_harvest_state, true, stopped_count, 0u,
                    rt.dma_rx_packet_open, storage_wall_time_us(),
                    STORAGE_STOP_STABLE_EMPTY_SCANS,
                    STORAGE_STOP_STABLE_EMPTY_US);
                {
                    PendingDdrSlot stopped_pending[16];
                    uint64_t base_offset = bytes_captured;
                    uint64_t base_lba = next_queue_lba;
                    uint64_t batch_bytes = 0u;
                    uint64_t batch_sectors = 0u;
                    uint32_t valid_count = 0u;

                    memset(stopped_pending, 0, sizeof(stopped_pending));
                    for (i = 0u; i < stopped_count; ++i) {
                        uint64_t observed_bytes = stopped_items[i].actual_bytes;
                        uint64_t bytes = observed_bytes;
                        uint64_t remaining = bounded && base_offset + batch_bytes < requested_size
                                                 ? requested_size - (base_offset + batch_bytes)
                                                 : (bounded ? 0u : bytes);
                        if (bytes == 0u || bytes > rt.dma_desc_bytes ||
                            (bounded && remaining == 0u)) {
                            storage_mark_harvest_slot_failed(&write_queue, stopped_items[i].slot);
                            storage_record_failure(&producer_stats,
                                                   "fatal_stop_unqueued_dma_data");
                            continue;
                        }
                        if (bounded && bytes > remaining) bytes = remaining;
                        {
                            uint64_t sectors = bytes_to_sectors(bytes);
                            uint64_t media_bytes = sectors <= UINT64_MAX / SECTOR_SIZE
                                                       ? sectors * (uint64_t)SECTOR_SIZE : 0u;
                            StorageStopTailDisposition tail_disposition =
                                storage_stop_tail_disposition(
                                    true, stop_tail_seen, bytes, media_bytes,
                                    false);

                            if (sectors == 0u || media_bytes == 0u) {
                                storage_mark_harvest_slot_failed(
                                    &write_queue, stopped_items[i].slot);
                                storage_record_failure(&producer_stats,
                                                       "media_byte_overflow");
                                continue;
                            }
                            if (dma_observed_bytes > UINT64_MAX - observed_bytes ||
                                dma_harvested_payload_bytes > UINT64_MAX - bytes ||
                                dma_received_bytes > UINT64_MAX - bytes) {
                                storage_mark_harvest_slot_failed(
                                    &write_queue, stopped_items[i].slot);
                                storage_record_failure(&producer_stats,
                                                       "dma_byte_counter_overflow");
                                continue;
                            }
                            dma_observed_bytes += observed_bytes;
                            dma_harvested_payload_bytes += bytes;
                            dma_received_bytes += bytes;
                            storage_stats_record_dma_desc(&producer_stats,
                                                          storage_wall_time_us());
                            if (tail_disposition != STORAGE_STOP_TAIL_QUEUE) {
                                const char *reason =
                                    tail_disposition == STORAGE_STOP_TAIL_DEFER_LATE
                                    ? "late_completed_descriptor"
                                    : "unaligned_payload_not_safely_paddable";
                                StorageErrorCode deferred_error =
                                    tail_disposition == STORAGE_STOP_TAIL_DEFER_LATE
                                    ? STORAGE_ERR_LATE_COMPLETION
                                    : STORAGE_ERR_TAIL_UNALIGNED;
                                if (tail_unqueued_bytes > UINT64_MAX - bytes ||
                                    storage_release_harvested_slot(
                                        &write_queue, stopped_items[i].slot) != 0) {
                                    storage_record_failure(
                                        &producer_stats,
                                        "slot_ownership_invariant_failed");
                                    continue;
                                }
                                tail_unqueued_bytes += bytes;
                                stop_tail_seen = true;
                                storage_record_error(&producer_stats,
                                                     deferred_error, reason);
                                storage_record_deferred_stop_error(&write_queue,
                                                                   deferred_error,
                                                                   reason);
                                continue;
                            }
                            stopped_pending[valid_count].sectors = sectors;
                            stopped_pending[valid_count].media_bytes = media_bytes;
                        }
                        stopped_pending[valid_count].slot = stopped_items[i].slot;
                        stopped_pending[valid_count].bytes = bytes;
                        stopped_pending[valid_count].chunk_index =
                            stopped_items[i].submission_sequence;
                        stopped_pending[valid_count].file_offset = base_offset + batch_bytes;
                        if (batch_sectors > UINT64_MAX - base_lba) {
                            storage_record_failure(&producer_stats, "lba_range_overflow");
                            storage_mark_harvest_slot_failed(&write_queue, stopped_items[i].slot);
                            continue;
                        }
                        stopped_pending[valid_count].start_lba = base_lba + batch_sectors;
                        if ((uint64_t)stopped_pending[valid_count].slot >
                            (UINT64_MAX - rt.cfg->ddr_hw_base) / rt.dma_desc_bytes) {
                            storage_record_failure(&producer_stats, "ddr_address_overflow");
                            storage_mark_harvest_slot_failed(&write_queue, stopped_items[i].slot);
                            continue;
                        }
                        stopped_pending[valid_count].hw_addr = rt.cfg->ddr_hw_base +
                            (uint64_t)stopped_pending[valid_count].slot * rt.dma_desc_bytes;
                        batch_bytes += bytes;
                        batch_sectors += stopped_pending[valid_count].sectors;
                        ++valid_count;
                    }
                    if (valid_count != 0u &&
                        storage_local_queue_push_batch(&write_queue, stopped_pending,
                                                      valid_count) != 0) {
                        storage_record_failure(&producer_stats,
                                               "fatal_stop_unqueued_dma_data");
                        break;
                    }
                    bytes_captured += batch_bytes;
                    if (queued_payload_bytes <= UINT64_MAX - batch_bytes)
                        queued_payload_bytes += batch_bytes;
                    else
                        storage_record_failure(&producer_stats,
                                               "queued_byte_counter_overflow");
                    if (batch_sectors <= UINT64_MAX - next_queue_lba)
                        next_queue_lba += batch_sectors;
                    else
                        storage_record_failure(&producer_stats, "lba_range_overflow");
                }
            }
        }
        if (manual_stop_seen && !tail_incomplete && dma_quiesced) {
            tail_incomplete = dma_s2mm_tail_incomplete(&rt);
            if (tail_incomplete) {
                storage_record_error(&producer_stats, STORAGE_ERR_TAIL_UNALIGNED,
                                     "tail_descriptor_incomplete");
            }
        }
        if (stop_state.state == STORAGE_STOP_DMA_QUIESCING) {
            (void)storage_stop_state_advance(
                &stop_state, STORAGE_STOP_HARVESTING);
        }
        if (writer_started) {
            if (producer_done_us == 0u) producer_done_us = storage_wall_time_us();
            storage_queue_finish(&write_queue);
        }
        if (stop_state.state == STORAGE_STOP_HARVESTING) {
            (void)storage_stop_state_advance(
                &stop_state, STORAGE_STOP_PRODUCER_DONE);
            (void)storage_stop_state_advance(
                &stop_state, STORAGE_STOP_WRITER_DRAINING);
        }
        dma_stop_attempted = true;
    } else if (writer_started) {
        if (producer_done_us == 0u) producer_done_us = storage_wall_time_us();
        storage_queue_finish(&write_queue);
    }
    if (writer_started) {
        stop_state.deadline_us = storage_wall_time_us() + stop_timeouts.writer_drain_us;
        if (storage_join_writer_deadline(writer_thread, stop_state.deadline_us) != 0) {
            storage_record_error(&producer_stats, STORAGE_ERR_NVME_TIMEOUT,
                                 "writer_drain_timeout");
            storage_queue_request_abort(&write_queue);
            stop_state.deadline_us = storage_wall_time_us() + stop_timeouts.nvme_abort_us;
            if (storage_join_writer_deadline(writer_thread, stop_state.deadline_us) != 0) {
                storage_record_failure(&producer_stats, "writer_abort_timeout");
            } else {
                writer_started = false;
            }
        } else {
            writer_started = false;
        }
        if (!writer_started && writer_drained_us == 0u)
            writer_drained_us = storage_wall_time_us();
        bytes_written = write_queue.bytes_written;
        chunks = write_queue.chunks;
        nvme_write_us = write_queue.nvme_write_us;
    }
    if (!writer_started && dma_stop_attempted && dma_quiesced &&
        dma_stop_result != DMA_STOP_RESET_RECOVERED) {
        DmaStopResult finalize_result = dma_finalize_stop_s2mm_with_state(
            &rt, write_queue.slots.states, &dma_stop_report);
        dma_stop_result = finalize_result;
        if (finalize_result == DMA_STOP_FAILED) {
            dma_stop_failed = true;
            storage_record_error(&producer_stats, STORAGE_ERR_DMA_STOP,
                                 "dma_stop_recovery_failed");
        }
    }
    if (!stop_failed_phase_sent && capture_start_us != 0u &&
        (write_queue.error || dma_stop_failed || !write_queue.nvme_engine_quiesced)) {
        if (g_storage_stop_epoch == 0u) {
            g_storage_stop_epoch = storage_wall_time_us();
            if (g_storage_stop_epoch == 0u) g_storage_stop_epoch = 1u;
        }
        (void)storage_emit_stop_phase(
            &rt, STORAGE_WORKER_FAILED_FATAL,
            producer_stats.primary_error != STORAGE_ERR_NONE
                ? producer_stats.primary_error : STORAGE_ERR_INTERNAL,
            dma_received_bytes,
            producer_stats.receive_integrity_risk[0] != '\0'
                ? producer_stats.receive_integrity_risk : "storage_fatal");
        stop_failed_phase_sent = true;
    }
    if (stop_state.state == STORAGE_STOP_WRITER_DRAINING) {
        (void)storage_stop_state_advance(&stop_state, STORAGE_STOP_FINALIZING);
    }
    if (write_queue_ready) {
        storage_queue_snapshot(&write_queue, &final_queue_snapshot);
        if (final_queue_snapshot.completed_unharvested_slots >
            UINT64_MAX / (uint64_t)rt.dma_desc_bytes) {
            completed_unharvested_bytes = UINT64_MAX;
        } else {
            completed_unharvested_bytes =
                (uint64_t)final_queue_snapshot.completed_unharvested_slots *
                (uint64_t)rt.dma_desc_bytes;
        }
        if (!final_perf_emitted && capture_start_us != 0u) {
            storage_emit_final_perf_window(&rt, &producer_stats, &write_queue,
                                           dma_received_bytes,
                                           storage_wall_time_us());
            final_perf_emitted = true;
        }
        storage_maybe_dump_event_rings(&write_queue, &rt,
                                       rc != 0 || write_queue.error, manual_stop_seen);
        if (write_queue.nvme_engine_quiesced &&
            (!dma_started || dma_quiesced || dma_stop_result == DMA_STOP_RESET_RECOVERED)) {
            storage_queue_destroy(&write_queue);
            write_queue_ready = false;
        }
    }
    if (rc != 0) {
        if (final_us == 0u) final_us = storage_wall_time_us();
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
            result->dma_observed_bytes = dma_observed_bytes;
            result->dma_harvested_payload_bytes = dma_harvested_payload_bytes;
            result->queued_payload_bytes = queued_payload_bytes;
            result->nvme_completed_bytes = __atomic_load_n(&rt.nvme_payload_bytes_done, __ATOMIC_ACQUIRE);
            result->nvme_media_bytes = __atomic_load_n(&rt.nvme_write_bytes_done, __ATOMIC_ACQUIRE);
            result->nvme_padding_bytes = result->nvme_media_bytes >= result->nvme_completed_bytes
                                             ? result->nvme_media_bytes - result->nvme_completed_bytes : 0u;
            result->tail_unqueued_bytes = tail_unqueued_bytes;
            result->completed_unharvested_bytes = completed_unharvested_bytes;
            result->stop_epoch = g_storage_stop_epoch;
            result->stop_request_us = stop_request_us;
            result->packet_boundary_us = packet_boundary_us;
            result->dma_quiesced_us = dma_quiesced_us;
            result->last_bd_complete_us = producer_stats.last_dma_desc_us;
            result->last_bd_harvest_us = producer_stats.last_dma_desc_us;
            result->producer_done_us = producer_done_us;
            result->writer_drained_us = writer_drained_us;
            result->final_us = final_us;
            result->harvested_bd_count = producer_stats.dma_desc_completed_count;
            result->queued_slot_count = final_queue_snapshot.queued_slot_count;
            result->completed_slot_count = final_queue_snapshot.completed_slot_count;
            result->recycled_slot_count = final_queue_snapshot.recycled_slot_count;
            result->final_ready_count = final_queue_snapshot.ready_depth_current;
            result->final_active_count = final_queue_snapshot.nvme_busy_slots;
            result->final_global_inflight =
                __atomic_load_n(&rt.nvme_active_qd_current, __ATOMIC_ACQUIRE);
            result->submit_count =
                __atomic_load_n(&rt.nvme_cmd_count, __ATOMIC_ACQUIRE);
            result->completion_count =
                __atomic_load_n(&rt.nvme_cq_completed, __ATOMIC_ACQUIRE);
            result->final_completed_unharvested =
                final_queue_snapshot.completed_unharvested_slots;
            result->final_free_dma_bd = final_queue_snapshot.free_slots;
            result->final_ring_occupied_bytes = final_queue_snapshot.buffered_bytes;
            result->file_bytes = bytes_written;
            result->dma_bd_exhaustion_count = producer_stats.dma_no_free_slot_count;
            result->dma_error_count = producer_stats.dma_error_count;
            result->descriptor_error_count = producer_stats.descriptor_error_count;
            result->min_dma_writable = producer_stats.min_dma_writable == UINT32_MAX ?
                                       rt.dma_desc_count : producer_stats.min_dma_writable;
            result->max_completed_unharvested = producer_stats.max_completed_unharvested;
            result->max_occupied_bytes_est = producer_stats.max_occupied_bytes_est;
            result->submit_stall_max_us = rt.nvme_submit_stall_max_us;
            result->primary_error = producer_stats.primary_error != STORAGE_ERR_NONE
                                        ? producer_stats.primary_error
                                        : (rt.nvme_primary_error != STORAGE_ERR_NONE
                                               ? rt.nvme_primary_error
                                               : STORAGE_ERR_INTERNAL);
            result->secondary_error = producer_stats.secondary_error != STORAGE_ERR_NONE
                                          ? producer_stats.secondary_error
                                          : rt.nvme_secondary_error;
            snprintf(result->integrity_risk, sizeof(result->integrity_risk), "%s",
                     producer_stats.receive_integrity_risk[0] ?
                     producer_stats.receive_integrity_risk : "storage_error");
            snprintf(result->secondary_reason, sizeof(result->secondary_reason), "%s",
                     producer_stats.secondary_reason);
            snprintf(result->task_no, sizeof(result->task_no), "%s", args->task_no);
        }
        if (capture_start_us != 0u) {
            elapsed_us = storage_elapsed_us(capture_start_us);
        }
        if (chunks > 0u && next_queue_lba >= start_lba) {
            total_sectors = next_queue_lba - start_lba;
        }
        if (storage_text_output_enabled()) {
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
        }
        if (mode == STORAGE_WRITE_STANDALONE)
            storage_emit_line(STORAGE_LOG_ALWAYS_CRITICAL, "storage_result channel=%d task=%s file_index=%u status=failed"
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
        {
            uint64_t observed_us = producer_stats.last_dma_desc_us >=
                                       producer_stats.first_dma_desc_us
                                       ? producer_stats.last_dma_desc_us -
                                             producer_stats.first_dma_desc_us
                                       : 0u;
            char capture_rate[32];
            char nvme_active_rate[32];
            char task_wall_rate[32];

            storage_format_rate(capture_rate, sizeof(capture_rate),
                                dma_observed_bytes, observed_us);
            storage_format_rate(nvme_active_rate, sizeof(nvme_active_rate),
                                bytes_written, nvme_write_us);
            storage_format_rate(task_wall_rate, sizeof(task_wall_rate),
                                bytes_written, elapsed_us);
            storage_emit_line(STORAGE_LOG_SUMMARY,
                              "storage_result_perf channel=%d task=%s file_index=%u elapsed_ms=%" PRIu64
                              " capture_window_mib_s=%s"
                              " nvme_active_ms=%" PRIu64 " nvme_active_mib_s=%s"
                              " nvme_wall_ms=0 nvme_wall_mib_s=N/A"
                              " task_wall_mib_s=%s nvme_qd_effective=%u"
                              " nvme_active_qd_avg=0.000 nvme_active_qd_max=%u",
                              cfg ? cfg->id : args->channel_id,
                              args->task_no, (unsigned)effective_file_index,
                              elapsed_us / 1000u, capture_rate,
                              nvme_write_us / 1000u, nvme_active_rate,
                              task_wall_rate, (unsigned)rt.nvme_qd_effective,
                              (unsigned)__atomic_load_n(
                                  &rt.nvme_active_qd_max, __ATOMIC_ACQUIRE));
        }
        storage_emit_line(STORAGE_LOG_DEBUG, "storage_result_diag channel=%d task=%s file_index=%u"
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
        storage_emit_line(STORAGE_LOG_SUMMARY, "storage_result_receive channel=%d task=%s file_index=%u"
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
    /* Do not unmap the runtime while S2MM may still own DDR.  A failed
     * quiesce deliberately leaves the child to exit with the mappings alive;
     * process teardown is the only safe fallback until board reset confirms
     * that hardware no longer dereferences them. */
    if ((!write_queue_ready || write_queue.nvme_engine_quiesced) &&
        (!dma_started || dma_quiesced || dma_stop_result == DMA_STOP_RESET_RECOVERED))
        channel_runtime_close(&rt);
    return rc;
}

int execute_write_with_result(const ParsedArgs *args, GlobalOptions gopt, WriteResult *result) {
    return execute_write_with_result_mode(args, gopt, result, STORAGE_WRITE_STANDALONE);
}

int execute_write(const ParsedArgs *args, GlobalOptions gopt) {
    return execute_write_with_result(args, gopt, NULL);
}

#ifdef CCB_BUILD_DIAG
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
        snprintf(result->task_no, sizeof(result->task_no), "%s", args->task_no);
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
    StorageSlotTable slots;
    bool slots_ready = false;
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

    memset(&slots, 0, sizeof(slots));

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
    if (storage_slot_table_init(&slots, rt.dma_desc_count) != 0) goto out;
    slots_ready = true;
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

        if (dma_get_bd_snapshot(&rt, slots.states, &snapshot) != 0) {
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
            storage_emit_line(STORAGE_LOG_ALWAYS_CRITICAL, "dma_bd_exhausted channel=%d mode=benchmark received_bytes=%" PRIu64,
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
                storage_emit_line(STORAGE_LOG_DEBUG, "dma_rx_benchmark_snapshot channel=%d dma_received_bytes=%" PRIu64
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
        if (storage_slot_transition(&slots, slot, STORAGE_SLOT_DMA_WRITABLE,
                                    STORAGE_SLOT_DMA_COMPLETED_UNHARVESTED) != 0 ||
            storage_slot_transition(&slots, slot,
                                    STORAGE_SLOT_DMA_COMPLETED_UNHARVESTED,
                                    STORAGE_SLOT_REQUEUE_PENDING) != 0) {
            integrity_ok = false;
            break;
        }
        if (dma_requeue_one(&rt, slot) != 0) {
            integrity_ok = false;
            break;
        }
        if (storage_slot_transition(&slots, slot, STORAGE_SLOT_REQUEUE_PENDING,
                                    STORAGE_SLOT_DMA_WRITABLE) != 0) {
            integrity_ok = false;
            break;
        }
    }
    {
        uint64_t observed_us = last_desc_us >= first_desc_us
                                   ? last_desc_us - first_desc_us : 0u;
        storage_emit_line(STORAGE_LOG_SUMMARY, "dma_rx_benchmark_result channel=%d dma_received_bytes=%" PRIu64
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
    if (slots_ready) storage_slot_table_destroy(&slots);
    (void)dma_stop_s2mm(&rt, NULL);
    channel_runtime_close(&rt);
    return rc;
}
#endif

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
