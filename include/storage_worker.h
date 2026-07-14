#ifndef STORAGE_WORKER_H
#define STORAGE_WORKER_H

#include <stdbool.h>

typedef enum {
    WORKER_INIT = 0,
    WORKER_READY,
    WORKER_ARMED,
    WORKER_RUNNING,
    WORKER_WAIT_BOUNDARY,
    WORKER_DMA_QUIESCING,
    WORKER_HARVESTING,
    WORKER_DRAINING,
    WORKER_FINALIZING,
    WORKER_DONE,
    WORKER_FAILED
} StorageWorkerState;

int storage_worker_transition(StorageWorkerState *state,
                              StorageWorkerState expected,
                              StorageWorkerState next);
bool storage_worker_stop_latched(StorageWorkerState state);
bool storage_worker_producer_done(StorageWorkerState state);
bool storage_worker_can_requeue(StorageWorkerState state);
const char *storage_worker_state_name(StorageWorkerState state);

#endif
