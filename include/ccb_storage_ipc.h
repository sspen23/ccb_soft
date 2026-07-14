#ifndef CCB_STORAGE_IPC_H
#define CCB_STORAGE_IPC_H

#include "ccb_commands.h"
#include "ccb_storage_diag.h"
#include <stdint.h>
#include <stdatomic.h>

#define STORAGE_IPC_MAGIC 0x53544732u
#define STORAGE_IPC_VERSION 2u

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

/* A control pipe is a byte stream.  Keep incomplete frames here so a
 * nonblocking ARM/RUN wait never consumes and loses a partial message. */
typedef struct {
    uint8_t bytes[sizeof(StorageControlMessage)];
    uint16_t used;
} StorageControlReader;

typedef enum {
    STORAGE_PARENT_STOP_TIMEOUT_EXPLICIT_US = 0,
    STORAGE_PARENT_STOP_TIMEOUT_EXPLICIT_MS,
    STORAGE_PARENT_STOP_TIMEOUT_CALCULATED
} StorageParentStopTimeoutSource;

typedef struct {
    uint64_t parent_timeout_us;
    uint64_t dma_quiesce_us;
    uint64_t stop_harvest_us;
    uint64_t writer_drain_us;
    uint64_t nvme_abort_us;
    uint64_t stage_total_us;
    uint64_t margin_us;
    StorageParentStopTimeoutSource source;
} StorageParentStopTimeoutConfig;

typedef enum {
    STORAGE_WORKER_READY = 1,
    STORAGE_WORKER_ARMED = 2,
    STORAGE_WORKER_RUNNING = 3,
    STORAGE_WORKER_FATAL = 4,
    STORAGE_WORKER_DRAINED = 5,
    STORAGE_WORKER_FINAL_RESULT = 6,
    STORAGE_WORKER_PERF_SAMPLE = 7,
    STORAGE_WORKER_DIAG_EVENT = 8
} StorageWorkerEventType;

typedef struct {
    uint64_t window_start_us, window_end_us, dma_bytes_delta, nvme_bytes_delta;
    uint32_t dma_writable, completed_unharvested, ready_slots, nvme_busy_slots, requeue_pending, free_slots;
    uint32_t active_qd, active_qd_max;
    uint64_t submit_stall_count, submit_stall_max_us;
    uint64_t writer_schedule_gap_count, writer_schedule_gap_max_us;
    uint64_t sq_full_wait_count, sq_full_wait_max_us, cq_empty_wait_count, cq_empty_wait_max_us;
    uint64_t submit_mmio_count, submit_mmio_max_us;
    uint64_t completion_process_count, completion_process_max_us;
    uint64_t queue_empty_wait_us;
    uint64_t writer_active_us;
    uint64_t no_progress_sleep_count;
    uint64_t writer_no_progress_sleep_count;
    uint64_t dropped_perf_samples;
    uint64_t dropped_diag_events;
    uint32_t receive_integrity_ok, storage_integrity_ok;
} StoragePerfSample;

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
    StoragePerfSample perf;
    StorageEventRecord diag;
    WriteResult result;
} StorageWorkerEvent;

_Static_assert(sizeof(StorageControlMessage) <= 4096u, "control must fit PIPE_BUF");
_Static_assert(sizeof(StorageWorkerEvent) <= 4096u, "event must fit PIPE_BUF");

uint64_t storage_ipc_monotonic_us(void);
/* Convert one absolute monotonic deadline using the caller's single sampled
 * now_us.  Returns -1/ETIMEDOUT once the deadline has passed. */
int storage_ipc_deadline_remaining_ms(uint64_t deadline_us, uint64_t now_us,
                                      int *timeout_ms);
uint64_t storage_ipc_saturating_add_u64(uint64_t a, uint64_t b);
void storage_ipc_parent_stop_timeout_config(StorageParentStopTimeoutConfig *out,
                                            uint64_t worker_compat_default_us);
const char *storage_ipc_parent_stop_timeout_source(
    StorageParentStopTimeoutSource source);
bool storage_ipc_parent_stop_should_force_reap(bool worker_live, bool already_forced,
                                               uint64_t now_us, uint64_t deadline_us);
void storage_ipc_make_control(StorageControlMessage *msg, StorageControlType type,
                              uint64_t timestamp_us);
void storage_ipc_make_event(StorageWorkerEvent *event, StorageWorkerEventType type,
                            uint32_t channel, int32_t error_code,
                            uint64_t received_bytes, const char *reason);
int storage_ipc_write_control(int fd, const StorageControlMessage *msg);
int storage_ipc_write_control_deadline(int fd, const StorageControlMessage *msg,
                                       uint64_t deadline_us);
int storage_ipc_read_control(int fd, StorageControlMessage *msg);
void storage_ipc_control_reader_init(StorageControlReader *reader);
/* Returns 0 for one complete message, 1 for EOF before any byte, and -1 for
 * timeout/protocol/I/O failure.  errno is ETIMEDOUT, EPROTO, or the I/O error.
 * The reader retains a partial message across ETIMEDOUT/EAGAIN. */
int storage_ipc_read_control_deadline(int fd, StorageControlReader *reader,
                                      StorageControlMessage *msg,
                                      uint64_t deadline_us);
int storage_ipc_write_event(int fd, const StorageWorkerEvent *event);
int storage_ipc_try_write_perf(int fd, const StorageWorkerEvent *event,
                               _Atomic uint64_t *dropped_perf_samples);
int storage_ipc_try_write_diag(int fd, const StorageWorkerEvent *event,
                               _Atomic uint64_t *dropped_diag_events);
int storage_ipc_write_event_deadline(int fd, const StorageWorkerEvent *event,
                                     uint64_t deadline_us);
int storage_ipc_validate_event(const StorageWorkerEvent *event);
int storage_ipc_read_event_raw(int fd, StorageWorkerEvent *event);
int storage_ipc_read_event(int fd, StorageWorkerEvent *event);

#endif
