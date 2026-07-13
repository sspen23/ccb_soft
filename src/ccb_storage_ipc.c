#include "ccb_storage_ipc.h"

#include <errno.h>
#include <poll.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

uint64_t storage_ipc_monotonic_us(void)
{
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) return 0u;
    return (uint64_t)ts.tv_sec * 1000000ull + (uint64_t)ts.tv_nsec / 1000ull;
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
                            uint32_t channel, int32_t error_code,
                            uint64_t received_bytes, const char *reason)
{
    memset(event, 0, sizeof(*event));
    event->magic = STORAGE_IPC_MAGIC;
    event->version = STORAGE_IPC_VERSION;
    event->size = sizeof(*event);
    event->type = (uint32_t)type;
    event->channel = channel;
    event->timestamp_us = storage_ipc_monotonic_us();
    event->error_code = error_code;
    event->received_bytes = received_bytes;
    if (reason) strncpy(event->reason, reason, sizeof(event->reason) - 1u);
}

static int storage_ipc_control_valid(const StorageControlMessage *msg)
{
    return msg && msg->magic == STORAGE_IPC_MAGIC &&
           msg->version == STORAGE_IPC_VERSION && msg->size == sizeof(*msg) &&
           msg->type >= STORAGE_CTRL_ARM && msg->type <= STORAGE_CTRL_STOP;
}

static int storage_ipc_event_valid(const StorageWorkerEvent *event)
{
    return event && event->magic == STORAGE_IPC_MAGIC &&
           event->version == STORAGE_IPC_VERSION && event->size == sizeof(*event) &&
           event->type >= STORAGE_WORKER_READY && event->type <= STORAGE_WORKER_DIAG_EVENT;
}

int storage_ipc_write_control(int fd, const StorageControlMessage *msg)
{ return storage_ipc_control_valid(msg) ? storage_ipc_write_full(fd, msg, sizeof(*msg)) : -1; }
int storage_ipc_read_control(int fd, StorageControlMessage *msg)
{ int rc = storage_ipc_read_full(fd, msg, sizeof(*msg)); return rc == 0 && !storage_ipc_control_valid(msg) ? -1 : rc; }
int storage_ipc_write_event(int fd, const StorageWorkerEvent *event)
{ return storage_ipc_event_valid(event) ? storage_ipc_write_full(fd, event, sizeof(*event)) : -1; }

int storage_ipc_try_write_perf(int fd, const StorageWorkerEvent *event,
                               _Atomic uint64_t *dropped_perf_samples)
{
    ssize_t written;

    if (!storage_ipc_event_valid(event) || event->type != STORAGE_WORKER_PERF_SAMPLE) {
        errno = EINVAL;
        return -1;
    }
    written = write(fd, event, sizeof(*event));
    if (written == (ssize_t)sizeof(*event)) return 0;
    if (written < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        if (dropped_perf_samples) {
            (void)atomic_fetch_add_explicit(dropped_perf_samples, 1u, memory_order_relaxed);
        }
        return 1;
    }
    if (written >= 0) errno = EIO;
    return -1;
}

int storage_ipc_write_event_deadline(int fd, const StorageWorkerEvent *event,
                                     uint64_t deadline_us)
{
    struct pollfd pfd;

    if (!storage_ipc_event_valid(event) || event->type == STORAGE_WORKER_PERF_SAMPLE) {
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
            uint64_t remaining_us;
            int timeout_ms;
            int rc;

            if (now_us >= deadline_us) {
                errno = ETIMEDOUT;
                return -1;
            }
            remaining_us = deadline_us - now_us;
            timeout_ms = (int)((remaining_us + 999u) / 1000u);
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
{ int rc = storage_ipc_read_full(fd, event, sizeof(*event)); return rc == 0 && !storage_ipc_event_valid(event) ? -1 : rc; }
