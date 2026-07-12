#include "ccb_storage_ipc.h"

#include <errno.h>
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
           event->type >= STORAGE_WORKER_READY && event->type <= STORAGE_WORKER_PERF_SAMPLE;
}

int storage_ipc_write_control(int fd, const StorageControlMessage *msg)
{ return storage_ipc_control_valid(msg) ? storage_ipc_write_full(fd, msg, sizeof(*msg)) : -1; }
int storage_ipc_read_control(int fd, StorageControlMessage *msg)
{ int rc = storage_ipc_read_full(fd, msg, sizeof(*msg)); return rc == 0 && !storage_ipc_control_valid(msg) ? -1 : rc; }
int storage_ipc_write_event(int fd, const StorageWorkerEvent *event)
{ return storage_ipc_event_valid(event) ? storage_ipc_write_full(fd, event, sizeof(*event)) : -1; }
int storage_ipc_read_event(int fd, StorageWorkerEvent *event)
{ int rc = storage_ipc_read_full(fd, event, sizeof(*event)); return rc == 0 && !storage_ipc_event_valid(event) ? -1 : rc; }
