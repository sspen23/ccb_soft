#include "ccb_storage_ipc.h"
#include "storage_config.h"

#include <errno.h>
#include <limits.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

uint64_t storage_ipc_monotonic_us(void)
{
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) return 0u;
    return (uint64_t)ts.tv_sec * 1000000ull + (uint64_t)ts.tv_nsec / 1000ull;
}

int storage_ipc_deadline_remaining_ms(uint64_t deadline_us, uint64_t now_us,
                                      int *timeout_ms)
{
    uint64_t remaining_us;
    uint64_t remaining_ms;

    if (!timeout_ms) {
        errno = EINVAL;
        return -1;
    }
    if (now_us >= deadline_us) {
        errno = ETIMEDOUT;
        return -1;
    }
    remaining_us = deadline_us - now_us;
    remaining_ms = remaining_us / 1000u;
    if ((remaining_us % 1000u) != 0u) ++remaining_ms;
    *timeout_ms = remaining_ms > (uint64_t)INT_MAX ? INT_MAX : (int)remaining_ms;
    return 0;
}

uint64_t storage_ipc_saturating_add_u64(uint64_t a, uint64_t b)
{
    return a > UINT64_MAX - b ? UINT64_MAX : a + b;
}

static uint64_t storage_ipc_saturating_mul_u64(uint64_t a, uint64_t b)
{
    if (a == 0u || b == 0u) return 0u;
    return a > UINT64_MAX / b ? UINT64_MAX : a * b;
}

static bool storage_ipc_parse_u64_env(const char *name, uint64_t *out)
{
    const char *value;
    char *end = NULL;
    unsigned long long parsed;

    if (!name || !out || !(value = storage_config_compat_getenv(name)) || value[0] == '\0') return false;
    errno = 0;
    parsed = strtoull(value, &end, 0);
    if (errno != 0 || end == value || *end != '\0' || parsed == 0u) return false;
    *out = (uint64_t)parsed;
    return true;
}

static uint64_t storage_ipc_timeout_env_us(const char *us_name, const char *ms_name,
                                           uint64_t fallback_us)
{
    uint64_t value;

    if (storage_ipc_parse_u64_env(us_name, &value)) return value;
    if (storage_ipc_parse_u64_env(ms_name, &value))
        return storage_ipc_saturating_mul_u64(value, 1000u);
    return fallback_us;
}

const char *storage_ipc_parent_stop_timeout_source(
    StorageParentStopTimeoutSource source)
{
    switch (source) {
    case STORAGE_PARENT_STOP_TIMEOUT_EXPLICIT_US: return "explicit_us";
    case STORAGE_PARENT_STOP_TIMEOUT_EXPLICIT_MS: return "explicit_ms";
    case STORAGE_PARENT_STOP_TIMEOUT_CALCULATED: return "calculated";
    default: return "calculated";
    }
}

void storage_ipc_parent_stop_timeout_config(StorageParentStopTimeoutConfig *out,
                                            uint64_t worker_compat_default_us)
{
    uint64_t compat_us;
    uint64_t explicit_value;

    if (!out) return;
    memset(out, 0, sizeof(*out));
    compat_us = storage_ipc_timeout_env_us("SRC_REAL_STORAGE_STOP_TIMEOUT_US",
                                           "SRC_REAL_STORAGE_STOP_TIMEOUT_MS",
                                           worker_compat_default_us);
    out->dma_quiesce_us = storage_ipc_timeout_env_us(
        "SRC_REAL_DMA_QUIESCE_TIMEOUT_US", NULL, compat_us);
    out->stop_harvest_us = storage_ipc_timeout_env_us(
        "SRC_REAL_STOP_HARVEST_TIMEOUT_US", NULL, compat_us);
    out->writer_drain_us = storage_ipc_timeout_env_us(
        "SRC_REAL_WRITER_DRAIN_TIMEOUT_US", NULL, compat_us);
    out->nvme_abort_us = storage_ipc_timeout_env_us(
        "SRC_REAL_NVME_ABORT_TIMEOUT_US", NULL,
        out->writer_drain_us < compat_us ? compat_us : out->writer_drain_us);
    out->stage_total_us = storage_ipc_saturating_add_u64(out->dma_quiesce_us,
                                                          out->stop_harvest_us);
    out->stage_total_us = storage_ipc_saturating_add_u64(out->stage_total_us,
                                                          out->writer_drain_us);
    out->stage_total_us = storage_ipc_saturating_add_u64(out->stage_total_us,
                                                          out->nvme_abort_us);
    out->margin_us = out->stage_total_us / 10u;
    if (out->margin_us < 5000000u) out->margin_us = 5000000u;

    if (storage_ipc_parse_u64_env("SRC_REAL_STORAGE_PARENT_STOP_TIMEOUT_US",
                                  &explicit_value)) {
        out->parent_timeout_us = explicit_value;
        out->source = STORAGE_PARENT_STOP_TIMEOUT_EXPLICIT_US;
    } else if (storage_ipc_parse_u64_env("SRC_REAL_STORAGE_PARENT_STOP_TIMEOUT_MS",
                                         &explicit_value)) {
        out->parent_timeout_us = storage_ipc_saturating_mul_u64(explicit_value, 1000u);
        out->source = STORAGE_PARENT_STOP_TIMEOUT_EXPLICIT_MS;
    } else {
        out->parent_timeout_us = storage_ipc_saturating_add_u64(out->stage_total_us,
                                                                 out->margin_us);
        out->source = STORAGE_PARENT_STOP_TIMEOUT_CALCULATED;
    }
}

bool storage_ipc_parent_stop_should_force_reap(bool worker_live, bool already_forced,
                                               uint64_t now_us, uint64_t deadline_us)
{
    return worker_live && !already_forced && deadline_us != 0u && now_us >= deadline_us;
}

static int storage_ipc_write_full(int fd, const void *buf, size_t size)
{
    const uint8_t *p = buf;
    size_t used = 0u;
    while (used < size) {
        ssize_t n = write(fd, p + used, size - used);
        if (n > 0) { used += (size_t)n; continue; }
        if (n < 0 && errno == EINTR) continue;
        return -1;
    }
    return 0;
}

static int storage_ipc_read_full(int fd, void *buf, size_t size)
{
    uint8_t *p = buf;
    size_t used = 0u;
    while (used < size) {
        ssize_t n = read(fd, p + used, size - used);
        if (n > 0) { used += (size_t)n; continue; }
        if (n == 0) return used == 0u ? 1 : -1;
        if (errno == EINTR) continue;
        return -1;
    }
    return 0;
}

void storage_ipc_make_control(StorageControlMessage *msg, StorageControlType type,
                              uint64_t timestamp_us)
{
    memset(msg, 0, sizeof(*msg));
    msg->magic = STORAGE_IPC_MAGIC;
    msg->version = STORAGE_IPC_VERSION;
    msg->size = sizeof(*msg);
    msg->type = (uint32_t)type;
    msg->timestamp_us = timestamp_us;
}

void storage_ipc_make_event(StorageWorkerEvent *event, StorageWorkerEventType type,
                            uint32_t channel, StorageErrorCode error_code,
                            uint64_t received_bytes, const char *reason)
{
    WorkerReadyPayload *ready;

    memset(event, 0, sizeof(*event));
    event->magic = STORAGE_IPC_MAGIC;
    event->version = STORAGE_IPC_VERSION;
    event->size = sizeof(*event);
    event->type = (uint32_t)type;
    event->channel = channel;
    event->payload_size = storage_ipc_event_payload_size(type);
    event->timestamp_us = storage_ipc_monotonic_us();

    switch (type) {
    case STORAGE_WORKER_READY:
    case STORAGE_WORKER_ARMED:
    case STORAGE_WORKER_RUNNING:
    case STORAGE_WORKER_DRAINED:
    case STORAGE_WORKER_INPUT_IDLE_CANDIDATE:
    case STORAGE_WORKER_INPUT_ACTIVE:
        ready = &event->payload.ready;
        ready->received_bytes = received_bytes;
        if (reason) (void)snprintf(ready->reason, sizeof(ready->reason), "%s", reason);
        break;
    case STORAGE_WORKER_FATAL:
        event->payload.fatal.error_code = error_code;
        event->payload.fatal.received_bytes = received_bytes;
        if (reason) {
            (void)snprintf(event->payload.fatal.reason,
                           sizeof(event->payload.fatal.reason), "%s", reason);
        }
        break;
    case STORAGE_WORKER_FINAL_RESULT:
        event->payload.final.error_code = error_code;
        event->payload.final.received_bytes = received_bytes;
        if (reason) {
            (void)snprintf(event->payload.final.reason,
                           sizeof(event->payload.final.reason), "%s", reason);
        }
        break;
    case STORAGE_WORKER_STOP_PHASE:
        event->payload.phase.error_code = error_code;
        event->payload.phase.received_bytes = received_bytes;
        if (reason) {
            (void)snprintf(event->payload.phase.reason,
                           sizeof(event->payload.phase.reason), "%s", reason);
        }
        break;
    case STORAGE_WORKER_PERF_SAMPLE:
    case STORAGE_WORKER_DIAG_EVENT:
        break;
    }
}

uint32_t storage_ipc_event_payload_size(StorageWorkerEventType type)
{
    switch (type) {
    case STORAGE_WORKER_READY:
    case STORAGE_WORKER_ARMED:
    case STORAGE_WORKER_RUNNING:
    case STORAGE_WORKER_DRAINED:
    case STORAGE_WORKER_INPUT_IDLE_CANDIDATE:
    case STORAGE_WORKER_INPUT_ACTIVE:
        return sizeof(WorkerReadyPayload);
    case STORAGE_WORKER_FATAL:
        return sizeof(WorkerFatalPayload);
    case STORAGE_WORKER_FINAL_RESULT:
        return sizeof(WorkerFinalPayload);
    case STORAGE_WORKER_PERF_SAMPLE:
        return sizeof(StoragePerfSample);
    case STORAGE_WORKER_DIAG_EVENT:
        return sizeof(StorageEventRecord);
    case STORAGE_WORKER_STOP_PHASE:
        return sizeof(WorkerPhasePayload);
    default:
        return 0u;
    }
}

static int storage_ipc_control_valid(const StorageControlMessage *msg)
{
    return msg && msg->magic == STORAGE_IPC_MAGIC &&
           msg->version == STORAGE_IPC_VERSION && msg->size == sizeof(*msg) &&
           msg->type >= STORAGE_CTRL_ARM && msg->type <= STORAGE_CTRL_AUTO_DRAIN;
}

int storage_ipc_validate_event(const StorageWorkerEvent *event)
{
    uint32_t expected_payload_size;

    if (!event || event->magic != STORAGE_IPC_MAGIC ||
        event->version != STORAGE_IPC_VERSION || event->size != sizeof(*event) ||
        event->type < STORAGE_WORKER_READY ||
        event->type > STORAGE_WORKER_INPUT_ACTIVE)
        return 0;
    expected_payload_size = storage_ipc_event_payload_size(
        (StorageWorkerEventType)event->type);
    if (event->payload_size != expected_payload_size) return 0;
    if (event->type == STORAGE_WORKER_FATAL) {
        return storage_error_code_valid(event->payload.fatal.error_code) &&
               storage_error_class(event->payload.fatal.error_code) == STORAGE_ERROR_FATAL;
    }
    if (event->type == STORAGE_WORKER_FINAL_RESULT) {
        return storage_error_code_valid(event->payload.final.error_code);
    }
    if (event->type == STORAGE_WORKER_STOP_PHASE) {
        const WorkerPhasePayload *phase = &event->payload.phase;

        if (!storage_error_code_valid(phase->error_code) ||
            phase->stop_phase < STORAGE_WORKER_STOP_REQUESTED ||
            phase->stop_phase > STORAGE_WORKER_FAILED_FATAL)
            return 0;
        if (phase->stop_phase == STORAGE_WORKER_FAILED_FATAL)
            return storage_error_class(phase->error_code) == STORAGE_ERROR_FATAL;
        return phase->error_code == STORAGE_ERR_NONE ||
               storage_error_class(phase->error_code) == STORAGE_ERROR_DEFERRED;
    }
    return true;
}

const char *storage_ipc_stop_phase_name(StorageWorkerStopPhase phase)
{
    switch (phase) {
    case STORAGE_WORKER_STOP_REQUESTED: return "stop_requested";
    case STORAGE_WORKER_PACKET_BOUNDARY_REACHED: return "packet_boundary_reached";
    case STORAGE_WORKER_DMA_QUIESCED: return "dma_quiesced";
    case STORAGE_WORKER_HARVEST_STABLE_EMPTY: return "harvest_stable_empty";
    case STORAGE_WORKER_WRITER_DRAINED: return "writer_drained";
    case STORAGE_WORKER_FINALIZED: return "finalized";
    case STORAGE_WORKER_FAILED_FATAL: return "failed_fatal";
    default: return "invalid";
    }
}

int storage_ipc_write_control(int fd, const StorageControlMessage *msg)
{ return storage_ipc_control_valid(msg) ? storage_ipc_write_full(fd, msg, sizeof(*msg)) : -1; }
int storage_ipc_write_control_deadline(int fd, const StorageControlMessage *msg,
                                       uint64_t deadline_us)
{
    struct pollfd pfd;
    if (!storage_ipc_control_valid(msg)) { errno = EINVAL; return -1; }
    for (;;) {
        ssize_t n = write(fd, msg, sizeof(*msg));
        if (n == (ssize_t)sizeof(*msg)) return 0;
        if (n >= 0) { errno = EIO; return -1; }
        if (errno == EINTR) continue;
        if (errno != EAGAIN && errno != EWOULDBLOCK) return -1;
        {
            uint64_t now_us = storage_ipc_monotonic_us();
            int timeout_ms;
            int rc;

            if (storage_ipc_deadline_remaining_ms(deadline_us, now_us,
                                                  &timeout_ms) != 0)
                return -1;
            pfd.fd = fd;
            pfd.events = POLLOUT;
            pfd.revents = 0;
            rc = poll(&pfd, 1u, timeout_ms);
            if (rc > 0) continue;
            if (rc == 0) { errno = ETIMEDOUT; return -1; }
            if (errno != EINTR) return -1;
        }
    }
}
int storage_ipc_read_control(int fd, StorageControlMessage *msg)
{ int rc = storage_ipc_read_full(fd, msg, sizeof(*msg)); return rc == 0 && !storage_ipc_control_valid(msg) ? -1 : rc; }

void storage_ipc_control_reader_init(StorageControlReader *reader)
{
    if (reader) memset(reader, 0, sizeof(*reader));
}

int storage_ipc_read_control_deadline(int fd, StorageControlReader *reader,
                                      StorageControlMessage *msg,
                                      uint64_t deadline_us)
{
    struct pollfd pfd;

    if (fd < 0 || !reader || !msg) { errno = EINVAL; return -1; }
    while (reader->used < sizeof(*msg)) {
        ssize_t n = read(fd, reader->bytes + reader->used,
                         sizeof(*msg) - reader->used);
        if (n > 0) {
            reader->used += (uint16_t)n;
            continue;
        }
        if (n == 0) {
            if (reader->used == 0u) return 1;
            errno = EPROTO;
            return -1;
        }
        if (errno == EINTR) continue;
        if (errno != EAGAIN && errno != EWOULDBLOCK) return -1;
        pfd.fd = fd;
        pfd.events = POLLIN | POLLHUP;
        pfd.revents = 0;
        {
            uint64_t now_us = storage_ipc_monotonic_us();
            int timeout_ms;
            int rc;

            if (storage_ipc_deadline_remaining_ms(deadline_us, now_us,
                                                  &timeout_ms) != 0)
                return -1;
            rc = poll(&pfd, 1u, timeout_ms);
            if (rc > 0) continue;
            if (rc == 0) { errno = ETIMEDOUT; return -1; }
            if (errno != EINTR) return -1;
        }
    }
    memcpy(msg, reader->bytes, sizeof(*msg));
    reader->used = 0u;
    if (!storage_ipc_control_valid(msg)) { errno = EPROTO; return -1; }
    return 0;
}
int storage_ipc_write_event(int fd, const StorageWorkerEvent *event)
{ return storage_ipc_validate_event(event) ? storage_ipc_write_full(fd, event, sizeof(*event)) : -1; }

static int storage_ipc_try_write_event(int fd, const StorageWorkerEvent *event,
                                       StorageWorkerEventType expected_type,
                                       _Atomic uint64_t *dropped_events)
{
    ssize_t written;

    if (!storage_ipc_validate_event(event) || event->type != expected_type) {
        errno = EINVAL;
        return -1;
    }
    written = write(fd, event, sizeof(*event));
    if (written == (ssize_t)sizeof(*event)) return 0;
    if (written < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        if (dropped_events) {
            (void)atomic_fetch_add_explicit(dropped_events, 1u, memory_order_relaxed);
        }
        return 1;
    }
    if (written >= 0) errno = EIO;
    return -1;
}

int storage_ipc_try_write_perf(int fd, const StorageWorkerEvent *event,
                               _Atomic uint64_t *dropped_perf_samples)
{
    return storage_ipc_try_write_event(fd, event, STORAGE_WORKER_PERF_SAMPLE,
                                       dropped_perf_samples);
}

int storage_ipc_try_write_diag(int fd, const StorageWorkerEvent *event,
                               _Atomic uint64_t *dropped_diag_events)
{
    return storage_ipc_try_write_event(fd, event, STORAGE_WORKER_DIAG_EVENT,
                                       dropped_diag_events);
}

int storage_ipc_write_event_deadline(int fd, const StorageWorkerEvent *event,
                                     uint64_t deadline_us)
{
    struct pollfd pfd;

    if (!storage_ipc_validate_event(event) || event->type == STORAGE_WORKER_PERF_SAMPLE) {
        errno = EINVAL;
        return -1;
    }
    for (;;) {
        ssize_t written = write(fd, event, sizeof(*event));
        if (written == (ssize_t)sizeof(*event)) return 0;
        if (written >= 0) {
            errno = EIO;
            return -1;
        }
        if (errno == EINTR) continue;
        if (errno != EAGAIN && errno != EWOULDBLOCK) return -1;
        {
            uint64_t now_us = storage_ipc_monotonic_us();
            int timeout_ms;
            int rc;

            if (storage_ipc_deadline_remaining_ms(deadline_us, now_us,
                                                  &timeout_ms) != 0)
                return -1;
            pfd.fd = fd;
            pfd.events = POLLOUT;
            pfd.revents = 0;
            rc = poll(&pfd, 1u, timeout_ms);
            if (rc > 0) continue;
            if (rc == 0) {
                errno = ETIMEDOUT;
                return -1;
            }
            if (errno != EINTR) return -1;
        }
    }
}
int storage_ipc_read_event(int fd, StorageWorkerEvent *event)
{
    int rc = storage_ipc_read_event_raw(fd, event);
    return rc == 0 && !storage_ipc_validate_event(event) ? -1 : rc;
}

int storage_ipc_read_event_raw(int fd, StorageWorkerEvent *event)
{ return storage_ipc_read_full(fd, event, sizeof(*event)); }
