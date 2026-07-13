#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "ccb_storage_perf.h"
int main(void)
{
    StorageWorkerEvent e; FILE *f; char line[2048];
    setenv("SRC_REAL_PERF_LOG_FILE", "/tmp/mock_storage_perf.log", 1);
    setenv("SRC_REAL_PERF_LOG_ENABLE", "1", 1); unlink("/tmp/mock_storage_perf.log");
    storage_ipc_make_event(&e, STORAGE_WORKER_PERF_SAMPLE, 1u, 0, 42u, "sample");
    assert(storage_perf_log_event(&e, "task") == 0);
    storage_perf_log_close(); f = fopen("/tmp/mock_storage_perf.log", "r"); assert(f);
    e.perf.window_start_us = 1u; e.perf.window_end_us = 2u;
    e.perf.dma_bytes_delta = 42u; e.perf.nvme_bytes_delta = 41u;
    e.perf.dma_writable = 1u; e.perf.completed_unharvested = 2u; e.perf.ready_slots = 3u;
    e.perf.nvme_busy_slots = 4u; e.perf.requeue_pending = 5u; e.perf.free_slots = 6u;
    e.perf.active_qd = 7u; e.perf.active_qd_max = 8u; e.perf.submit_stall_count = 9u;
    e.perf.submit_stall_max_us = 10u; e.perf.writer_schedule_gap_count = 11u;
    e.perf.writer_schedule_gap_max_us = 12u; e.perf.sq_full_wait_count = 13u;
    e.perf.sq_full_wait_max_us = 14u; e.perf.cq_empty_wait_count = 15u;
    e.perf.cq_empty_wait_max_us = 16u; e.perf.dropped_perf_samples = 17u;
    e.perf.receive_integrity_ok = 1u; e.perf.storage_integrity_ok = 1u;
    assert(storage_perf_log_event(&e, "task") == 0);
    storage_perf_log_close(); f = fopen("/tmp/mock_storage_perf.log", "r"); assert(f);
    assert(fgets(line, sizeof(line), f) && strstr(line, "storage_perf task=task"));
    assert(fgets(line, sizeof(line), f) && strstr(line, "free_slots=6") && strstr(line, "dropped_perf_samples=17")); fclose(f);
    storage_ipc_make_event(&e, STORAGE_WORKER_FATAL, 1u, -3, 9u, "fatal_reason");
    assert(storage_perf_log_event(&e, "task") == 0);
    storage_ipc_make_event(&e, STORAGE_WORKER_FINAL_RESULT, 1u, 0, 9u, "final_reason");
    e.result.dma_received_bytes = e.result.nvme_completed_bytes = e.result.file_bytes = 9u;
    assert(storage_perf_log_event(&e, "task") == 0);
    storage_ipc_make_event(&e, STORAGE_WORKER_DIAG_EVENT, 1u, 0, 0u, "diag");
    e.diag.sequence = 3u; e.diag.event_id = STORAGE_EVENT_WORKER_FATAL;
    assert(storage_perf_log_event(&e, "task") == 0);
    storage_perf_log_close(); f = fopen("/tmp/mock_storage_perf.log", "r"); assert(f);
    while (fgets(line, sizeof(line), f)) { if (strstr(line, "storage_fatal")) break; }
    assert(strstr(line, "fatal_reason"));
    assert(fgets(line, sizeof(line), f) && strstr(line, "storage_final"));
    assert(fgets(line, sizeof(line), f) && strstr(line, "storage_diag")); fclose(f);
    storage_perf_log_close();
    setenv("SRC_REAL_PERF_LOG_FILE", "/tmp/no_such_dir/mock.log", 1);
    assert(storage_perf_log_event(&e, "task") != 0);
    setenv("SRC_REAL_PERF_LOG_FILE", "/tmp/mock_storage_perf.log", 1);
    assert(storage_perf_log_event(&e, "task") != 0);
    storage_perf_log_close();
    puts("mock_storage_perf_test: ok"); return 0;
}
