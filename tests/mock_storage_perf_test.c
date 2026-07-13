#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "ccb_storage_perf.h"
int main(void)
{
    StorageWorkerEvent e; FILE *f; char line[512];
    setenv("SRC_REAL_PERF_LOG_FILE", "/tmp/mock_storage_perf.log", 1);
    setenv("SRC_REAL_PERF_LOG_ENABLE", "1", 1); unlink("/tmp/mock_storage_perf.log");
    storage_ipc_make_event(&e, STORAGE_WORKER_PERF_SAMPLE, 1u, 0, 42u, "sample");
    assert(storage_perf_log_event(&e, "task") == 0);
    storage_perf_log_close(); f = fopen("/tmp/mock_storage_perf.log", "r"); assert(f);
    assert(fgets(line, sizeof(line), f) && strstr(line, "task=task") && strstr(line, "bytes=42")); fclose(f);
    puts("mock_storage_perf_test: ok"); return 0;
}
