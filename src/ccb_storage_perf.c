#include "ccb_storage_perf.h"
#include "storage_config.h"
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int g_fd = -2;
static bool g_open_failure_reported;
static int storage_perf_open(void)
{
    const AppConfig *config = storage_config_get();
    const char *path = "/tmp/storage_perf.log";

    if (g_fd != -2) return g_fd;
    if (!config || !config->perf_enabled) { g_fd = -1; return -1; }
    g_fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (g_fd < 0 && !g_open_failure_reported) {
        /* A bad path must not turn every incoming event into another open(2)
         * attempt.  Keep logging disabled until the next explicit close/reset
         * (which is the lifecycle boundary for a new task/process). */
        g_open_failure_reported = true;
        g_fd = -1;
    }
    return g_fd;
}
int storage_perf_log_event(const StorageWorkerEvent *e, const char *task)
{
    char line[4096]; int n; int fd;
    if (!e || (fd = storage_perf_open()) < 0) return -1;
    switch (e->type) {
    case STORAGE_WORKER_PERF_SAMPLE:
        n = snprintf(line, sizeof(line),
                     "storage_perf task=%s channel=%u ts_us=%llu window_start_us=%llu window_end_us=%llu dma_bytes_delta=%llu nvme_bytes_delta=%llu dma_writable=%u completed_unharvested=%u ready_slots=%u nvme_busy_slots=%u requeue_pending=%u free_slots=%u active_qd=%u active_qd_max=%u submit_stall_count=%llu submit_stall_max_us=%llu writer_schedule_gap_count=%llu writer_schedule_gap_max_us=%llu sq_full_wait_count=%llu sq_full_wait_max_us=%llu cq_empty_wait_count=%llu cq_empty_wait_max_us=%llu submit_mmio_count=%llu submit_mmio_max_us=%llu completion_process_count=%llu completion_process_max_us=%llu queue_empty_wait_us=%llu writer_active_us=%llu no_progress_sleep_count=%llu writer_no_progress_sleep_count=%llu dropped_perf_samples=%llu dropped_diag_events=%llu receive_integrity_ok=%u storage_integrity_ok=%u\n",
                     task ? task : "", e->channel, (unsigned long long)e->timestamp_us,
                     (unsigned long long)e->payload.perf.window_start_us,
                     (unsigned long long)e->payload.perf.window_end_us,
                     (unsigned long long)e->payload.perf.dma_bytes_delta,
                     (unsigned long long)e->payload.perf.nvme_bytes_delta,
                     e->payload.perf.dma_writable,
                     e->payload.perf.completed_unharvested,
                     e->payload.perf.ready_slots, e->payload.perf.nvme_busy_slots,
                     e->payload.perf.requeue_pending, e->payload.perf.free_slots,
                     e->payload.perf.active_qd, e->payload.perf.active_qd_max,
                     (unsigned long long)e->payload.perf.submit_stall_count,
                     (unsigned long long)e->payload.perf.submit_stall_max_us,
                     (unsigned long long)e->payload.perf.writer_schedule_gap_count,
                     (unsigned long long)e->payload.perf.writer_schedule_gap_max_us,
                     (unsigned long long)e->payload.perf.sq_full_wait_count,
                     (unsigned long long)e->payload.perf.sq_full_wait_max_us,
                     (unsigned long long)e->payload.perf.cq_empty_wait_count,
                     (unsigned long long)e->payload.perf.cq_empty_wait_max_us,
                     (unsigned long long)e->payload.perf.submit_mmio_count,
                     (unsigned long long)e->payload.perf.submit_mmio_max_us,
                     (unsigned long long)e->payload.perf.completion_process_count,
                     (unsigned long long)e->payload.perf.completion_process_max_us,
                     (unsigned long long)e->payload.perf.queue_empty_wait_us,
                     (unsigned long long)e->payload.perf.writer_active_us,
                     (unsigned long long)e->payload.perf.no_progress_sleep_count,
                     (unsigned long long)e->payload.perf.writer_no_progress_sleep_count,
                     (unsigned long long)e->payload.perf.dropped_perf_samples,
                     (unsigned long long)e->payload.perf.dropped_diag_events,
                     e->payload.perf.receive_integrity_ok,
                     e->payload.perf.storage_integrity_ok);
        break;
    case STORAGE_WORKER_FATAL:
        n = snprintf(line, sizeof(line), "storage_fatal task=%s channel=%u ts_us=%llu error=%d bytes=%llu reason=%s\n",
                     task ? task : "", e->channel, (unsigned long long)e->timestamp_us,
                     e->payload.fatal.error_code,
                     (unsigned long long)e->payload.fatal.received_bytes,
                     e->payload.fatal.reason);
        break;
    case STORAGE_WORKER_FINAL_RESULT:
        n = snprintf(line, sizeof(line), "storage_final task=%s channel=%u ts_us=%llu error=%d"
                     " dma_bytes=%llu dma_observed_bytes=%llu"
                     " dma_harvested_payload_bytes=%llu queued_payload_bytes=%llu"
                     " nvme_bytes=%llu nvme_media_bytes=%llu nvme_padding_bytes=%llu"
                     " tail_unqueued_bytes=%llu completed_unharvested_bytes=%llu"
                     " file_bytes=%llu stop_epoch=%llu stop_request_us=%llu"
                     " packet_boundary_us=%llu dma_quiesced_us=%llu"
                     " last_bd_complete_us=%llu last_bd_harvest_us=%llu"
                     " producer_done_us=%llu writer_drained_us=%llu final_us=%llu"
                     " harvested_bd_count=%llu queued_slot_count=%llu"
                     " completed_slot_count=%llu recycled_slot_count=%llu"
                     " ready_count=%u active_count=%u global_inflight=%u"
                     " submit_count=%llu completion_count=%llu"
                     " completed_unharvested=%u free_dma_bd=%u ring_occupied_bytes=%llu"
                     " persisted=%u receive_integrity_ok=%u storage_integrity_ok=%u"
                     " integrity_ok=%u primary_reason=%s secondary_reason=%s reason=%s\n",
                     task ? task : "", e->channel, (unsigned long long)e->timestamp_us,
                     e->payload.final.error_code,
                     (unsigned long long)e->payload.final.result.dma_received_bytes,
                     (unsigned long long)e->payload.final.result.dma_observed_bytes,
                     (unsigned long long)e->payload.final.result.dma_harvested_payload_bytes,
                     (unsigned long long)e->payload.final.result.queued_payload_bytes,
                     (unsigned long long)e->payload.final.result.nvme_completed_bytes,
                     (unsigned long long)e->payload.final.result.nvme_media_bytes,
                     (unsigned long long)e->payload.final.result.nvme_padding_bytes,
                     (unsigned long long)e->payload.final.result.tail_unqueued_bytes,
                     (unsigned long long)e->payload.final.result.completed_unharvested_bytes,
                     (unsigned long long)e->payload.final.result.file_bytes,
                     (unsigned long long)e->payload.final.result.stop_epoch,
                     (unsigned long long)e->payload.final.result.stop_request_us,
                     (unsigned long long)e->payload.final.result.packet_boundary_us,
                     (unsigned long long)e->payload.final.result.dma_quiesced_us,
                     (unsigned long long)e->payload.final.result.last_bd_complete_us,
                     (unsigned long long)e->payload.final.result.last_bd_harvest_us,
                     (unsigned long long)e->payload.final.result.producer_done_us,
                     (unsigned long long)e->payload.final.result.writer_drained_us,
                     (unsigned long long)e->payload.final.result.final_us,
                     (unsigned long long)e->payload.final.result.harvested_bd_count,
                     (unsigned long long)e->payload.final.result.queued_slot_count,
                     (unsigned long long)e->payload.final.result.completed_slot_count,
                     (unsigned long long)e->payload.final.result.recycled_slot_count,
                     e->payload.final.result.final_ready_count,
                     e->payload.final.result.final_active_count,
                     e->payload.final.result.final_global_inflight,
                     (unsigned long long)e->payload.final.result.submit_count,
                     (unsigned long long)e->payload.final.result.completion_count,
                     e->payload.final.result.final_completed_unharvested,
                     e->payload.final.result.final_free_dma_bd,
                     (unsigned long long)e->payload.final.result.final_ring_occupied_bytes,
                     e->payload.final.result.data_persisted ? 1u : 0u,
                     e->payload.final.result.receive_integrity_ok ? 1u : 0u,
                     e->payload.final.result.storage_integrity_ok ? 1u : 0u,
                     e->payload.final.result.integrity_ok ? 1u : 0u,
                     e->payload.final.result.integrity_risk[0] != '\0'
                         ? e->payload.final.result.integrity_risk : "none",
                     e->payload.final.result.secondary_reason[0] != '\0'
                         ? e->payload.final.result.secondary_reason : "none",
                     e->payload.final.reason);
        break;
    case STORAGE_WORKER_DIAG_EVENT:
        n = snprintf(line, sizeof(line), "storage_diag task=%s channel=%u ts_us=%llu sequence=%llu event_id=%u event_channel=%u flags=%u arg0=%llu arg1=%llu\n",
                     task ? task : "", e->channel, (unsigned long long)e->timestamp_us,
                     (unsigned long long)e->payload.diag.sequence,
                     e->payload.diag.event_id,
                     e->payload.diag.channel, e->payload.diag.flags,
                     (unsigned long long)e->payload.diag.arg0,
                     (unsigned long long)e->payload.diag.arg1);
        break;
    default:
        return 0;
    }
    if (n <= 0 || (size_t)n >= sizeof(line)) return -1;
    if (write(fd, line, (size_t)n) != n) return -1;
    return 0;
}
void storage_perf_log_close(void) { if (g_fd >= 0) close(g_fd); g_fd = -2; g_open_failure_reported = false; }
