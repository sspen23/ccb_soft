#ifndef STORAGE_PROCESS_H
#define STORAGE_PROCESS_H

#include <sys/types.h>

int storage_process_poll(pid_t pid, int *status);
int storage_process_signal(pid_t pid, int signal_number);

#endif
