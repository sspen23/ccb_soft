#ifndef CCB_STORAGE_PERF_H
#define CCB_STORAGE_PERF_H

#include "ccb_storage_ipc.h"
int storage_perf_log_event(const StorageWorkerEvent *event, const char *task);
void storage_perf_log_close(void);
#endif
