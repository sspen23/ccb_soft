#ifndef CCB_STORAGE_SUPERVISOR_H
#define CCB_STORAGE_SUPERVISOR_H

#include "ccb_storage_ipc.h"

typedef enum { STORAGE_TASK_ACTIVE = 0, STORAGE_TASK_SUCCESS, STORAGE_TASK_FAILED } StorageTaskTerminal;
typedef struct {
    uint32_t target_channel_mask, ready_mask, armed_mask, running_mask, drained_mask, final_seen_mask;
    uint32_t idle_candidate_mask, auto_drain_sent_mask;
    uint32_t fatal_seen_mask;
    uint32_t unavailable_mask, worker_exited_mask;
    uint32_t stop_requested_mask, stop_sent_mask, stop_failed_mask;
    uint64_t stop_epoch;
    uint64_t auto_drain_epoch;
    uint32_t stop_phase[NUM_CHANNELS];
    bool first_fatal, result_known_failed, aggregate_ready, auto_drain_triggered;
    StorageErrorCode primary_error, secondary_error;
    uint32_t fatal_channel; char fatal_reason[64]; char secondary_reason[64];
    WriteResult final_result[NUM_CHANNELS];
    bool aggregate_emitted;
    StorageTaskTerminal terminal;
} StorageTaskSupervisor;

void storage_supervisor_init(StorageTaskSupervisor *s, uint32_t target_mask);
int storage_supervisor_handle_event(StorageTaskSupervisor *s, const StorageWorkerEvent *e);
int storage_supervisor_handle_event_for_channel(StorageTaskSupervisor *s,
                                                uint32_t expected_channel,
                                                const StorageWorkerEvent *e);
void storage_supervisor_protocol_fail(StorageTaskSupervisor *s, uint32_t channel,
                                      const char *reason);
void storage_supervisor_mark_unavailable(StorageTaskSupervisor *s, uint32_t channel,
                                         const char *reason);
int storage_supervisor_handle_worker_eof(StorageTaskSupervisor *s, uint32_t channel);
int storage_supervisor_handle_worker_exit(StorageTaskSupervisor *s, uint32_t channel, int exit_code);
uint32_t storage_supervisor_stop_mask(const StorageTaskSupervisor *s);
bool storage_supervisor_auto_drain_ready(const StorageTaskSupervisor *s);
bool storage_supervisor_begin_auto_drain(StorageTaskSupervisor *s,
                                         uint64_t drain_epoch);
uint32_t storage_supervisor_auto_drain_pending_mask(
    const StorageTaskSupervisor *s);
void storage_supervisor_mark_auto_drain_sent(StorageTaskSupervisor *s,
                                             uint32_t channel);
void storage_supervisor_request_stop(StorageTaskSupervisor *s, uint32_t channel_mask);
uint32_t storage_supervisor_peek_stop_mask(const StorageTaskSupervisor *s);
void storage_supervisor_mark_stop_sent(StorageTaskSupervisor *s, uint32_t channel);
void storage_supervisor_mark_stop_failed(StorageTaskSupervisor *s, uint32_t channel);
bool storage_supervisor_is_terminal(const StorageTaskSupervisor *s);
StorageTaskTerminal storage_supervisor_result_status(StorageTaskSupervisor *s);

#endif
