#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#include "ccb_storage_task.h"

static void init_task(Task *task)
{
    memset(task, 0, sizeof(*task));
    task->output_fd = -1;
    task->control_fd = -1;
    task->event_fd = -1;
}

static void test_all_exit_paths_close_fds(void)
{
    static const char *const paths[] = {
        "normal_exit", "ready_failure", "arm_failure", "run_failure",
        "fork_exec_failure", "event_eof", "worker_fatal", "stop_timeout"
    };
    size_t i;

    for (i = 0u; i < sizeof(paths) / sizeof(paths[0]); ++i) {
        Task task;
        int output_pipe[2];
        int control_pipe[2];
        int event_pipe[2];

        (void)paths[i];
        assert(pipe(output_pipe) == 0);
        assert(pipe(control_pipe) == 0);
        assert(pipe(event_pipe) == 0);
        init_task(&task);
        task.output_fd = output_pipe[0];
        task.control_fd = control_pipe[1];
        task.event_fd = event_pipe[0];
        task.state = RUNNING;
        task.has_planned_file = true;
        snprintf(task.task_id, sizeof(task.task_id), "old-task");

        if ((i & 1u) == 0u) storage_task_close_fds(&task);
        else storage_task_reset_runtime(&task);
        assert(task.output_fd == -1);
        assert(task.control_fd == -1);
        assert(task.event_fd == -1);
        if ((i & 1u) != 0u) {
            assert(task.pid == -1);
            assert(task.task_id[0] == '\0');
        }

        close(output_pipe[1]);
        close(control_pipe[0]);
        close(event_pipe[1]);
    }
}

static void test_child_does_not_hold_unrelated_writer(void)
{
    Task tasks[2];
    int data_pipe[2];
    int ready_pipe[2];
    int release_pipe[2];
    pid_t pid;
    char byte = 0;
    int status = 0;

    assert(pipe(data_pipe) == 0);
    assert(pipe(ready_pipe) == 0);
    assert(pipe(release_pipe) == 0);
    init_task(&tasks[0]);
    init_task(&tasks[1]);
    tasks[0].event_fd = data_pipe[1];

    pid = fork();
    assert(pid >= 0);
    if (pid == 0) {
        close(data_pipe[0]);
        close(ready_pipe[0]);
        close(release_pipe[1]);
        storage_child_close_inherited_fds(tasks, 2u, NULL, 0u);
        if (write(ready_pipe[1], "R", 1u) != 1) _exit(2);
        if (read(release_pipe[0], &byte, 1u) != 1) _exit(3);
        _exit(0);
    }

    close(ready_pipe[1]);
    close(release_pipe[0]);
    close(data_pipe[1]);
    assert(read(ready_pipe[0], &byte, 1u) == 1);
    assert(read(data_pipe[0], &byte, 1u) == 0);
    assert(write(release_pipe[1], "X", 1u) == 1);
    assert(waitpid(pid, &status, 0) == pid);
    assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);

    close(data_pipe[0]);
    close(ready_pipe[0]);
    close(release_pipe[1]);
}

int main(void)
{
    test_all_exit_paths_close_fds();
    test_child_does_not_hold_unrelated_writer();
    puts("mock_storage_task_test: ok");
    return 0;
}
