#include <assert.h>
#include <stdio.h>

#include "storage_controller.h"

int main(void)
{
    CaptureTask task;

    capture_task_init(&task);
    assert(capture_task_begin(&task, "task-1", 0x7u, 100u) == 0);
    assert(capture_task_transition(&task, CAPTURE_PREPARING,
                                   CAPTURE_READY) == 0);
    assert(capture_task_transition(&task, CAPTURE_READY,
                                   CAPTURE_STARTING) == 0);
    assert(capture_task_transition(&task, CAPTURE_STARTING,
                                   CAPTURE_RUNNING) == 0);
    assert(capture_task_request_stop(&task, 1234u) == 0);
    assert(task.stop_epoch == 1234u);
    assert(capture_task_request_stop(&task, 1234u) == 0);
    assert(capture_task_request_stop(&task, 4321u) != 0);
    assert(capture_task_transition(&task, CAPTURE_STOPPING,
                                   CAPTURE_COMMITTING) == 0);
    assert(capture_task_transition(&task, CAPTURE_COMMITTING,
                                   CAPTURE_DONE) == 0);
    assert(!capture_task_busy(&task));

    assert(capture_task_begin(&task, "task-2", 0x1u, 200u) == 0);
    capture_task_fail(&task, STORAGE_ERR_DMA_INIT);
    capture_task_fail(&task, STORAGE_ERR_OWNERSHIP);
    assert(task.state == CAPTURE_FAILED);
    assert(task.primary_error == STORAGE_ERR_DMA_INIT);
    assert(task.secondary_error == STORAGE_ERR_OWNERSHIP);
    puts("mock_storage_controller_test: ok");
    return 0;
}
