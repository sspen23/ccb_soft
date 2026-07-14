#include "storage_controller.h"

#include <string.h>

static bool transition_allowed(CaptureState expected, CaptureState next)
{
    if (next == CAPTURE_FAILED)
        return expected > CAPTURE_IDLE && expected < CAPTURE_DONE;
    switch (expected) {
    case CAPTURE_PREPARING:
        return next == CAPTURE_READY || next == CAPTURE_STOPPING;
    case CAPTURE_READY:
        return next == CAPTURE_STARTING || next == CAPTURE_STOPPING;
    case CAPTURE_STARTING:
        return next == CAPTURE_RUNNING || next == CAPTURE_STOPPING;
    case CAPTURE_RUNNING: return next == CAPTURE_STOPPING;
    case CAPTURE_STOPPING: return next == CAPTURE_COMMITTING;
    case CAPTURE_COMMITTING: return next == CAPTURE_DONE;
    default: return false;
    }
}

void capture_task_init(CaptureTask *task)
{
    if (!task) return;
    memset(task, 0, sizeof(*task));
    task->state = CAPTURE_IDLE;
}

int capture_task_begin(CaptureTask *task, const char *task_id,
                       uint32_t target_mask, uint64_t deadline_us)
{
    if (!task || !task_id || task_id[0] == '\0' || target_mask == 0u ||
        (task->state != CAPTURE_IDLE && task->state != CAPTURE_DONE &&
         task->state != CAPTURE_FAILED))
        return -1;
    capture_task_init(task);
    task->state = CAPTURE_PREPARING;
    task->target_mask = target_mask;
    task->deadline_us = deadline_us;
    (void)strncpy(task->task_id, task_id, sizeof(task->task_id) - 1u);
    return 0;
}

int capture_task_transition(CaptureTask *task, CaptureState expected,
                            CaptureState next)
{
    if (!task || task->state != expected ||
        !transition_allowed(expected, next))
        return -1;
    task->state = next;
    return 0;
}

int capture_task_request_stop(CaptureTask *task, uint64_t stop_epoch)
{
    CaptureState current;

    if (!task || stop_epoch == 0u) return -1;
    if (task->state == CAPTURE_STOPPING || task->state == CAPTURE_COMMITTING ||
        task->state == CAPTURE_DONE || task->state == CAPTURE_FAILED) {
        if (task->stop_epoch == 0u) task->stop_epoch = stop_epoch;
        return task->stop_epoch == stop_epoch ? 0 : -1;
    }
    current = task->state;
    if (capture_task_transition(task, current, CAPTURE_STOPPING) != 0)
        return -1;
    task->stop_epoch = stop_epoch;
    return 0;
}

void capture_task_fail(CaptureTask *task, StorageErrorCode error)
{
    CaptureState current;

    if (!task || error == STORAGE_ERR_NONE) return;
    storage_error_record(&task->primary_error, &task->secondary_error, error);
    current = task->state;
    if (current > CAPTURE_IDLE && current < CAPTURE_DONE)
        (void)capture_task_transition(task, current, CAPTURE_FAILED);
}

bool capture_task_busy(const CaptureTask *task)
{
    return task && task->state >= CAPTURE_PREPARING &&
           task->state <= CAPTURE_COMMITTING;
}

const char *capture_state_name(CaptureState state)
{
    switch (state) {
    case CAPTURE_IDLE: return "idle";
    case CAPTURE_PREPARING: return "preparing";
    case CAPTURE_READY: return "ready";
    case CAPTURE_STARTING: return "starting";
    case CAPTURE_RUNNING: return "running";
    case CAPTURE_STOPPING: return "stopping";
    case CAPTURE_COMMITTING: return "committing";
    case CAPTURE_DONE: return "done";
    case CAPTURE_FAILED: return "failed";
    default: return "invalid";
    }
}
