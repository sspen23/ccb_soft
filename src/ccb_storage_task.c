#include "ccb_storage_task.h"

#include <string.h>
#include <unistd.h>

static bool storage_fd_is_kept(int fd, const int *keep_fds, size_t keep_count)
{
    size_t i;

    for (i = 0u; i < keep_count; ++i) {
        if (fd == keep_fds[i]) return true;
    }
    return false;
}

void storage_task_close_fds(Task *task)
{
    if (!task) return;
    if (task->output_fd >= 0) close(task->output_fd);
    if (task->control_fd >= 0) close(task->control_fd);
    if (task->event_fd >= 0) close(task->event_fd);
    task->output_fd = -1;
    task->control_fd = -1;
    task->event_fd = -1;
}

void storage_task_reset_runtime(Task *task)
{
    if (!task) return;
    storage_task_close_fds(task);
    task->pid = -1;
    task->start_time = 0;
    task->state = IDLE;
    task->stop_send_attempts = 0u;
    memset(&task->worker_event, 0, sizeof(task->worker_event));
    memset(&task->first_fatal, 0, sizeof(task->first_fatal));
    memset(&task->final_result, 0, sizeof(task->final_result));
    task->worker_phase = 0u;
    task->ready_seen = false;
    task->armed_seen = false;
    task->running_seen = false;
    task->drained_seen = false;
    task->fatal_seen = false;
    task->output_used = 0u;
    task->output[0] = '\0';
    task->echo_line_used = 0u;
    task->echo_line[0] = '\0';
    task->final_result_seen = false;
    task->final_data_persisted = false;
    task->final_integrity_ok = false;
    task->final_status_success = false;
    task->final_receive_seen = false;
    task->split_mismatch_reported = false;
    task->final_dma_received_bytes = 0u;
    memset(&task->planned_file, 0, sizeof(task->planned_file));
    task->has_planned_file = false;
    /* Do not let a previous channel's task identifier participate in a
     * later single-channel aggregate or stale-event diagnostic.  The task
     * slot is fully reusable only after all runtime identity is reset. */
    memset(task->task_id, 0, sizeof(task->task_id));
    task->overpass_time = 0;
}

void storage_child_close_inherited_fds(Task *tasks, size_t task_count,
                                       const int *keep_fds, size_t keep_count)
{
    size_t i;

    if (!tasks) return;
    for (i = 0u; i < task_count; ++i) {
        int *fds[] = { &tasks[i].output_fd, &tasks[i].control_fd, &tasks[i].event_fd };
        size_t j;

        for (j = 0u; j < sizeof(fds) / sizeof(fds[0]); ++j) {
            if (*fds[j] >= 0 && !storage_fd_is_kept(*fds[j], keep_fds, keep_count)) {
                close(*fds[j]);
                *fds[j] = -1;
            }
        }
    }
}
