#include "storage_worker.h"

static bool transition_allowed(StorageWorkerState expected,
                               StorageWorkerState next)
{
    if (next == WORKER_FAILED)
        return expected >= WORKER_INIT && expected < WORKER_DONE;
    switch (expected) {
    case WORKER_INIT: return next == WORKER_READY;
    case WORKER_READY: return next == WORKER_ARMED;
    case WORKER_ARMED: return next == WORKER_RUNNING;
    case WORKER_RUNNING:
        return next == WORKER_WAIT_BOUNDARY || next == WORKER_DRAINING;
    case WORKER_WAIT_BOUNDARY: return next == WORKER_DMA_QUIESCING;
    case WORKER_DMA_QUIESCING: return next == WORKER_HARVESTING;
    case WORKER_HARVESTING: return next == WORKER_DRAINING;
    case WORKER_DRAINING: return next == WORKER_FINALIZING;
    case WORKER_FINALIZING: return next == WORKER_DONE;
    default: return false;
    }
}

int storage_worker_transition(StorageWorkerState *state,
                              StorageWorkerState expected,
                              StorageWorkerState next)
{
    if (!state || *state != expected || !transition_allowed(expected, next))
        return -1;
    *state = next;
    return 0;
}

bool storage_worker_stop_latched(StorageWorkerState state)
{
    return state >= WORKER_WAIT_BOUNDARY;
}

bool storage_worker_producer_done(StorageWorkerState state)
{
    return state >= WORKER_DRAINING;
}

bool storage_worker_can_requeue(StorageWorkerState state)
{
    return state == WORKER_RUNNING;
}

const char *storage_worker_state_name(StorageWorkerState state)
{
    switch (state) {
    case WORKER_INIT: return "init";
    case WORKER_READY: return "ready";
    case WORKER_ARMED: return "armed";
    case WORKER_RUNNING: return "running";
    case WORKER_WAIT_BOUNDARY: return "wait_boundary";
    case WORKER_DMA_QUIESCING: return "dma_quiescing";
    case WORKER_HARVESTING: return "harvesting";
    case WORKER_DRAINING: return "draining";
    case WORKER_FINALIZING: return "finalizing";
    case WORKER_DONE: return "done";
    case WORKER_FAILED: return "failed";
    default: return "invalid";
    }
}
