#include "storage_process.h"

#include <errno.h>
#include <signal.h>
#include <sys/wait.h>

int storage_process_poll(pid_t pid, int *status)
{
    pid_t result;

    if (pid <= 0 || !status) {
        errno = EINVAL;
        return -1;
    }
    result = waitpid(pid, status, WNOHANG);
    if (result == pid) return 1;
    if (result == 0 || (result < 0 && errno == EINTR)) return 0;
    return -1;
}

int storage_process_signal(pid_t pid, int signal_number)
{
    if (pid <= 0 || signal_number <= 0) {
        errno = EINVAL;
        return -1;
    }
    return kill(pid, signal_number);
}
