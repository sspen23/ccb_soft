#include "ccb_storage_perf.h"
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int g_fd = -2;
static bool g_open_failure_reported;
static int storage_perf_open(void)
{
    const char *enabled = getenv("SRC_REAL_PERF_LOG_ENABLE");
    const char *path;
    if (g_fd != -2) return g_fd;
    if (enabled && strcmp(enabled, "0") == 0) { g_fd = -1; return -1; }
    path = getenv("SRC_REAL_PERF_LOG_FILE"); if (!path || !path[0]) path = "/tmp/storage_perf.log";
    g_fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (g_fd < 0 && !g_open_failure_reported) g_open_failure_reported = true;
    return g_fd;
}
int storage_perf_log_event(const StorageWorkerEvent *e, const char *task)
{
    char line[1024]; int n; int fd;
    if (!e || (fd = storage_perf_open()) < 0) return -1;
    switch (e->type) {
    case STORAGE_WORKER_PERF_SAMPLE:
        n = snprintf(line, sizeof(line),
                     "storage_perf task=%s channel=%u ts_us=%llu window_start_us=%llu window_end_us=%llu dma_bytes_delta=%llu nvme_bytes_delta=%llu dma_writable=%u completed_unharvested=%u ready_slots=%u nvme_busy_slots=%u requeue_pending=%u free_slots=%u active_qd=%u active_qd_max=%u submit_stall_count=%llu submit_stall_max_us=%llu writer_schedule_gap_count=%llu writer_schedule_gap_max_us=%llu sq_full_wait_count=%llu sq_full_wait_max_us=%llu cq_empty_wait_count=%llu cq_empty_wait_max_us=%llu submit_mmio_count=%llu submit_mmio_max_us=%llu completion_process_count=%llu completion_process_max_us=%llu no_progress_sleep_count=%llu dropped_perf_samples=%llu receive_integrity_ok=%u storage_integrity_ok=%u\n",
                     task ? task : "", e->channel, (unsigned long long)e->timestamp_us,
                     (unsigned long long)e->perf.window_start_us,
                     (unsigned long long)e->perf.window_end_us,
                     (unsigned long long)e->perf.dma_bytes_delta,
                     (unsigned long long)e->perf.nvme_bytes_delta,
                     e->perf.dma_writable, e->perf.completed_unharvested,
                     e->perf.ready_slots, e->perf.nvme_busy_slots,
                     e->perf.requeue_pending, e->perf.free_slots,
                     e->perf.active_qd, e->perf.active_qd_max,
                     (unsigned long long)e->perf.submit_stall_count,
                     (unsigned long long)e->perf.submit_stall_max_us,
                     (unsigned long long)e->perf.writer_schedule_gap_count,
                     (unsigned long long)e->perf.writer_schedule_gap_max_us,
                     (unsigned long long)e->perf.sq_full_wait_count,
                     (unsigned long long)e->perf.sq_full_wait_max_us,
                     (unsigned long long)e->perf.cq_empty_wait_count,
                     (unsigned long long)e->perf.cq_empty_wait_max_us,
                     (unsigned long long)e->perf.submit_mmio_count,
                     (unsigned long long)e->perf.submit_mmio_max_us,
                     (unsigned long long)e->perf.completion_process_count,
                     (unsigned long long)e->perf.completion_process_max_us,
                     (unsigned long long)e->perf.no_progress_sleep_count,
                     (unsigned long long)e->perf.dropped_perf_samples,
                     e->perf.receive_integrity_ok, e->perf.storage_integrity_ok);
        break;
    case STORAGE_WORKER_FATAL:
        n = snprintf(line, sizeof(line), "storage_fatal task=%s channel=%u ts_us=%llu error=%d bytes=%llu reason=%s\n",
                     task ? task : "", e->channel, (unsigned long long)e->timestamp_us,
                     e->error_code, (unsigned long long)e->received_bytes, e->reason);
        break;
    case STORAGE_WORKER_FINAL_RESULT:
        n = snprintf(line, sizeof(line), "storage_final task=%s channel=%u ts_us=%llu error=%d dma_bytes=%llu nvme_bytes=%llu file_bytes=%llu persisted=%u receive_integrity_ok=%u storage_integrity_ok=%u integrity_ok=%u reason=%s\n",
                     task ? task : "", e->channel, (unsigned long long)e->timestamp_us,
                     e->error_code, (unsigned long long)e->result.dma_received_bytes,
                     (unsigned long long)e->result.nvme_completed_bytes,
                     (unsigned long long)e->result.file_bytes, e->result.data_persisted ? 1u : 0u,
                     e->result.receive_integrity_ok ? 1u : 0u,
                     e->result.storage_integrity_ok ? 1u : 0u,
                     e->result.integrity_ok ? 1u : 0u, e->reason);
        break;
    case STORAGE_WORKER_DIAG_EVENT:
        n = snprintf(line, sizeof(line), "storage_diag task=%s channel=%u ts_us=%llu sequence=%llu event_id=%u event_channel=%u flags=%u arg0=%llu arg1=%llu\n",
                     task ? task : "", e->channel, (unsigned long long)e->timestamp_us,
                     (unsigned long long)e->diag.sequence, e->diag.event_id,
                     e->diag.channel, e->diag.flags,
                     (unsigned long long)e->diag.arg0, (unsigned long long)e->diag.arg1);
        break;
    default:
        return 0;
    }
    if (n <= 0 || (size_t)n >= sizeof(line)) return -1;
    if (write(fd, line, (size_t)n) != n) return -1;
    if (getenv("SRC_REAL_PERF_LOG_FSYNC") && strcmp(getenv("SRC_REAL_PERF_LOG_FSYNC"), "1") == 0)
        return fsync(fd);
    return 0;
}
void storage_perf_log_close(void) { if (g_fd >= 0) close(g_fd); g_fd = -2; g_open_failure_reported = false; }
