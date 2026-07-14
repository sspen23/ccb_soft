#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "ccb_storage_perf.h"
int main(void)
{
    StorageWorkerEvent e; FILE *f; char line[2048];
    setenv("CCB_PERF_ENABLE", "1", 1);
    unlink("/tmp/storage_perf.log");
    storage_ipc_make_event(&e, STORAGE_WORKER_PERF_SAMPLE, 1u,
                           STORAGE_ERR_NONE, 42u, "sample");
    assert(storage_perf_log_event(&e, "task") == 0);
    storage_perf_log_close(); f = fopen("/tmp/storage_perf.log", "r"); assert(f);
    e.payload.perf.window_start_us = 1u; e.payload.perf.window_end_us = 2u;
    e.payload.perf.dma_bytes_delta = 42u; e.payload.perf.nvme_bytes_delta = 41u;
    e.payload.perf.dma_writable = 1u; e.payload.perf.completed_unharvested = 2u;
    e.payload.perf.ready_slots = 3u; e.payload.perf.nvme_busy_slots = 4u;
    e.payload.perf.requeue_pending = 5u; e.payload.perf.free_slots = 6u;
    e.payload.perf.active_qd = 7u; e.payload.perf.active_qd_max = 8u;
    e.payload.perf.submit_stall_count = 9u; e.payload.perf.submit_stall_max_us = 10u;
    e.payload.perf.writer_schedule_gap_count = 11u;
    e.payload.perf.writer_schedule_gap_max_us = 12u;
    e.payload.perf.sq_full_wait_count = 13u; e.payload.perf.sq_full_wait_max_us = 14u;
    e.payload.perf.cq_empty_wait_count = 15u; e.payload.perf.cq_empty_wait_max_us = 16u;
    e.payload.perf.dropped_perf_samples = 17u;
    e.payload.perf.submit_mmio_count = 18u; e.payload.perf.submit_mmio_max_us = 19u;
    e.payload.perf.completion_process_count = 20u;
    e.payload.perf.completion_process_max_us = 21u;
    e.payload.perf.queue_empty_wait_us = 22u; e.payload.perf.writer_active_us = 23u;
    e.payload.perf.no_progress_sleep_count = 24u;
    e.payload.perf.writer_no_progress_sleep_count = 25u;
    e.payload.perf.dropped_diag_events = 26u;
    e.payload.perf.receive_integrity_ok = 1u;
    e.payload.perf.storage_integrity_ok = 1u;
    assert(storage_perf_log_event(&e, "task") == 0);
    storage_perf_log_close(); f = fopen("/tmp/storage_perf.log", "r"); assert(f);
    assert(fgets(line, sizeof(line), f) && strstr(line, "storage_perf task=task"));
    assert(fgets(line, sizeof(line), f) && strstr(line, "free_slots=6") &&
           strstr(line, "submit_mmio_count=18") && strstr(line, "queue_empty_wait_us=22") &&
           strstr(line, "writer_no_progress_sleep_count=25") &&
           strstr(line, "dropped_perf_samples=17") && strstr(line, "dropped_diag_events=26")); fclose(f);
    storage_ipc_make_event(&e, STORAGE_WORKER_FATAL, 1u,
                           STORAGE_ERR_UNKNOWN_CID, 9u, "fatal_reason");
    assert(storage_perf_log_event(&e, "task") == 0);
    storage_ipc_make_event(&e, STORAGE_WORKER_FINAL_RESULT, 1u,
                           STORAGE_ERR_NONE, 9u, "final_reason");
    e.payload.final.result.dma_received_bytes =
        e.payload.final.result.nvme_completed_bytes =
        e.payload.final.result.file_bytes = 9u;
    e.payload.final.result.dma_observed_bytes = 11u;
    e.payload.final.result.dma_harvested_payload_bytes = 9u;
    e.payload.final.result.queued_payload_bytes = 9u;
    e.payload.final.result.nvme_media_bytes = 10u;
    e.payload.final.result.nvme_padding_bytes = 1u;
    e.payload.final.result.tail_unqueued_bytes = 2u;
    e.payload.final.result.stop_epoch = 77u;
    e.payload.final.result.submit_count = e.payload.final.result.completion_count = 4u;
    snprintf(e.payload.final.result.integrity_risk,
             sizeof(e.payload.final.result.integrity_risk), "%s", "primary_reason");
    snprintf(e.payload.final.result.secondary_reason,
             sizeof(e.payload.final.result.secondary_reason), "%s", "secondary_reason");
    assert(storage_perf_log_event(&e, "task") == 0);
    storage_ipc_make_event(&e, STORAGE_WORKER_DIAG_EVENT, 1u,
                           STORAGE_ERR_NONE, 0u, "diag");
    e.payload.diag.sequence = 3u;
    e.payload.diag.event_id = STORAGE_EVENT_WORKER_FATAL;
    assert(storage_perf_log_event(&e, "task") == 0);
    storage_perf_log_close(); f = fopen("/tmp/storage_perf.log", "r"); assert(f);
    while (fgets(line, sizeof(line), f)) { if (strstr(line, "storage_fatal")) break; }
    assert(strstr(line, "fatal_reason"));
    assert(fgets(line, sizeof(line), f) && strstr(line, "storage_final"));
    assert(strstr(line, "nvme_media_bytes=10") &&
           strstr(line, "dma_observed_bytes=11") &&
           strstr(line, "tail_unqueued_bytes=2") && strstr(line, "stop_epoch=77") &&
           strstr(line, "primary_reason=primary_reason") &&
           strstr(line, "secondary_reason=secondary_reason"));
    assert(fgets(line, sizeof(line), f) && strstr(line, "storage_diag")); fclose(f);
    storage_perf_log_close();
    puts("mock_storage_perf_test: ok"); return 0;
}
