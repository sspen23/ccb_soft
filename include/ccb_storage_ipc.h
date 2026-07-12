#ifndef CCB_STORAGE_IPC_H
#define CCB_STORAGE_IPC_H

#include "ccb_commands.h"
#include <stdint.h>

#define STORAGE_IPC_MAGIC 0x53544732u
#define STORAGE_IPC_VERSION 1u

typedef enum {
    STORAGE_CTRL_ARM = 1,
    STORAGE_CTRL_RUN = 2,
    STORAGE_CTRL_STOP = 3
} StorageControlType;

typedef struct {
    uint32_t magic;
    uint16_t version;
    uint16_t size;
    uint32_t type;
    uint32_t flags;
    uint64_t timestamp_us;
} StorageControlMessage;

typedef enum {
    STORAGE_WORKER_READY = 1,
    STORAGE_WORKER_ARMED = 2,
    STORAGE_WORKER_RUNNING = 3,
    STORAGE_WORKER_FATAL = 4,
    STORAGE_WORKER_DRAINED = 5,
    STORAGE_WORKER_FINAL_RESULT = 6,
    STORAGE_WORKER_PERF_SAMPLE = 7
} StorageWorkerEventType;

typedef struct {
    uint32_t magic;
    uint16_t version;
    uint16_t size;
    uint32_t type;
    uint32_t channel;
    uint64_t timestamp_us;
    int32_t error_code;
    uint32_t reserved;
    uint64_t received_bytes;
    char reason[64];
    WriteResult result;
} StorageWorkerEvent;

_Static_assert(sizeof(StorageControlMessage) <= 4096u, "control must fit PIPE_BUF");
_Static_assert(sizeof(StorageWorkerEvent) <= 4096u, "event must fit PIPE_BUF");

uint64_t storage_ipc_monotonic_us(void);
void storage_ipc_make_control(StorageControlMessage *msg, StorageControlType type,
                              uint64_t timestamp_us);
void storage_ipc_make_event(StorageWorkerEvent *event, StorageWorkerEventType type,
                            uint32_t channel, int32_t error_code,
                            uint64_t received_bytes, const char *reason);
int storage_ipc_write_control(int fd, const StorageControlMessage *msg);
int storage_ipc_read_control(int fd, StorageControlMessage *msg);
int storage_ipc_write_event(int fd, const StorageWorkerEvent *event);
int storage_ipc_read_event(int fd, StorageWorkerEvent *event);

#endif
