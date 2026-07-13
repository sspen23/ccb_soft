#include "ccb_storage_perf.h"
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int g_fd = -2;
static int storage_perf_open(void)
{
    const char *enabled = getenv("SRC_REAL_PERF_LOG_ENABLE");
    const char *path;
    if (g_fd != -2) return g_fd;
    if (enabled && strcmp(enabled, "0") == 0) { g_fd = -1; return -1; }
    path = getenv("SRC_REAL_PERF_LOG_FILE"); if (!path || !path[0]) path = "/tmp/storage_perf.log";
    g_fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
    return g_fd;
}
int storage_perf_log_event(const StorageWorkerEvent *e, const char *task)
{
    char line[512]; int n; int fd;
    if (!e || (fd = storage_perf_open()) < 0) return -1;
    n = snprintf(line, sizeof(line), "storage_event task=%s channel=%u type=%u ts_us=%llu bytes=%llu rc=%d integrity=%u\n",
                 task ? task : "", e->channel, e->type,
                 (unsigned long long)e->timestamp_us, (unsigned long long)e->received_bytes,
                 e->error_code, e->result.integrity_ok ? 1u : 0u);
    if (n <= 0 || (size_t)n >= sizeof(line)) return -1;
    return write(fd, line, (size_t)n) == n ? 0 : -1;
}
void storage_perf_log_close(void) { if (g_fd >= 0) close(g_fd); g_fd = -2; }
