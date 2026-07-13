#ifndef CCB_STORAGE_TASK_H
#define CCB_STORAGE_TASK_H

#include "ccb_storage_ipc.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>
#include <time.h>

#define STORAGE_TASK_OUTPUT_SIZE 4096u
#define STORAGE_TASK_ECHO_LINE_SIZE 2048u

typedef enum {
    IDLE,
    RUNNING,
    ERROR
} TaskState;

typedef struct {
    int channel_id;
    int proto_file_type_code;
    const char *file_type_name;
    uint64_t size_bytes;
    int file_index;
    uint64_t start_lba;
    uint64_t sector_count;
    uint8_t calibration_type;
} PlannedFile;

typedef struct {
    pid_t pid;
    time_t start_time;
    TaskState state;
    char name[32];
    int output_fd;
    int control_fd;
    int event_fd;
    unsigned int stop_send_attempts;
    StorageWorkerEvent worker_event;
    StorageWorkerEvent first_fatal;
    WriteResult final_result;
    uint32_t worker_phase;
    bool ready_seen;
    bool armed_seen;
    bool running_seen;
    bool drained_seen;
    bool fatal_seen;
    char output[STORAGE_TASK_OUTPUT_SIZE];
    size_t output_used;
    char echo_line[STORAGE_TASK_ECHO_LINE_SIZE];
    size_t echo_line_used;
    bool final_result_seen;
    bool final_data_persisted;
    bool final_integrity_ok;
    bool final_status_success;
    bool final_receive_seen;
    bool split_mismatch_reported;
    uint64_t final_dma_received_bytes;
    PlannedFile planned_file;
    bool has_planned_file;
    char task_id[12];
    time_t overpass_time;
    long timeout_seconds;
} Task;

void storage_task_close_fds(Task *task);
void storage_task_reset_runtime(Task *task);
void storage_child_close_inherited_fds(Task *tasks, size_t task_count,
                                       const int *keep_fds, size_t keep_count);

#endif
