#ifndef STORAGE_CONTROLLER_H
#define STORAGE_CONTROLLER_H

#include "storage_error.h"

#include <stdbool.h>
#include <stdint.h>

typedef enum {
    CAPTURE_IDLE = 0,
    CAPTURE_PREPARING,
    CAPTURE_READY,
    CAPTURE_STARTING,
    CAPTURE_RUNNING,
    CAPTURE_STOPPING,
    CAPTURE_COMMITTING,
    CAPTURE_DONE,
    CAPTURE_FAILED
} CaptureState;

typedef struct {
    CaptureState state;
    char task_id[12];
    uint32_t target_mask;
    uint64_t deadline_us;
    uint64_t stop_epoch;
    StorageErrorCode primary_error;
    StorageErrorCode secondary_error;
} CaptureTask;

void capture_task_init(CaptureTask *task);
int capture_task_begin(CaptureTask *task, const char *task_id,
                       uint32_t target_mask, uint64_t deadline_us);
int capture_task_transition(CaptureTask *task, CaptureState expected,
                            CaptureState next);
int capture_task_request_stop(CaptureTask *task, uint64_t stop_epoch);
void capture_task_fail(CaptureTask *task, StorageErrorCode error);
bool capture_task_busy(const CaptureTask *task);
const char *capture_state_name(CaptureState state);

#endif
