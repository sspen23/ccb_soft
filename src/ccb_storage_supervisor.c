#include "ccb_storage_supervisor.h"
#include <string.h>
#include <stdio.h>

static uint32_t bit(uint32_t ch) { return ch < NUM_CHANNELS ? 1u << ch : 0u; }

static void fail(StorageTaskSupervisor *s, uint32_t ch, StorageErrorCode code,
                 const char *reason)
{
    const char *failure_reason = reason && reason[0] != '\0' ? reason : "worker_fatal";
    StorageErrorCode previous_primary = s->primary_error;
    StorageErrorCode previous_secondary = s->secondary_error;

    if (storage_error_class(code) != STORAGE_ERROR_FATAL)
        code = STORAGE_ERR_INTERNAL;
    storage_error_record(&s->primary_error, &s->secondary_error, code);
    if (previous_primary == STORAGE_ERR_NONE && s->primary_error != STORAGE_ERR_NONE) {
        s->first_fatal = true;
        s->fatal_channel = ch;
        snprintf(s->fatal_reason, sizeof(s->fatal_reason), "%s", failure_reason);
    } else if (previous_secondary == STORAGE_ERR_NONE &&
               s->secondary_error != STORAGE_ERR_NONE) {
        snprintf(s->secondary_reason, sizeof(s->secondary_reason), "%s", failure_reason);
    }
    s->result_known_failed = true;
    s->stop_requested_mask |= s->target_channel_mask & ~s->final_seen_mask;
    if (s->aggregate_ready) s->terminal = STORAGE_TASK_FAILED;
}

static void fail_worker_exit_without_final(StorageTaskSupervisor *s, uint32_t ch)
{
    fail(s, ch, STORAGE_ERR_WORKER_EXIT, "worker_exit_without_final");
}

static void fail_task_result(StorageTaskSupervisor *s, uint32_t ch,
                             const WriteResult *result,
                             StorageErrorCode fallback_code,
                             const char *fallback_reason)
{
    StorageErrorCode primary = fallback_code;
    const char *reason = fallback_reason;

    if (result && storage_error_code_valid(result->primary_error) &&
        result->primary_error != STORAGE_ERR_NONE)
        primary = result->primary_error;
    if (result && result->integrity_risk[0] != '\0')
        reason = result->integrity_risk;
    if (!storage_error_code_valid(primary) || primary == STORAGE_ERR_NONE)
        primary = STORAGE_ERR_INTEGRITY;
    storage_error_record(&s->primary_error, &s->secondary_error, primary);
    if (result && storage_error_code_valid(result->secondary_error) &&
        result->secondary_error != STORAGE_ERR_NONE)
        storage_error_record(&s->primary_error, &s->secondary_error,
                             result->secondary_error);
    s->fatal_channel = ch;
    snprintf(s->fatal_reason, sizeof(s->fatal_reason), "%s",
             reason && reason[0] != '\0' ? reason : "storage_integrity_failed");
    if (result && result->secondary_reason[0] != '\0')
        snprintf(s->secondary_reason, sizeof(s->secondary_reason), "%s",
                 result->secondary_reason);
    s->result_known_failed = true;
    s->terminal = STORAGE_TASK_FAILED;
}

void storage_supervisor_init(StorageTaskSupervisor *s, uint32_t target_mask)
{ memset(s, 0, sizeof(*s)); s->target_channel_mask = target_mask; s->terminal = STORAGE_TASK_ACTIVE; }
void storage_supervisor_protocol_fail(StorageTaskSupervisor *s, uint32_t ch,
                                      const char *reason)
{
    if (s) fail(s, ch, STORAGE_ERR_IPC,
                reason && reason[0] != '\0' ? reason : "event_protocol_invalid");
}
void storage_supervisor_mark_unavailable(StorageTaskSupervisor *s, uint32_t ch,
                                         const char *reason)
{
    uint32_t b = bit(ch);

    if (!s || b == 0u || (s->target_channel_mask & b) == 0u) return;
    s->worker_exited_mask |= b;
    s->unavailable_mask |= b;
    fail(s, ch, STORAGE_ERR_WORKER_EXIT,
         reason && reason[0] != '\0' ? reason : "worker_unavailable");
    (void)storage_supervisor_result_status(s);
}

int storage_supervisor_handle_event_for_channel(StorageTaskSupervisor *s,
                                                uint32_t expected_channel,
                                                const StorageWorkerEvent *e)
{
    if (!s || !storage_ipc_validate_event(e)) {
        storage_supervisor_protocol_fail(s, expected_channel, "event_protocol_invalid");
        return -1;
    }
    if (e->channel != expected_channel) {
        storage_supervisor_protocol_fail(s, expected_channel, "event_channel_mismatch");
        return -1;
    }
    return storage_supervisor_handle_event(s, e);
}

int storage_supervisor_handle_event(StorageTaskSupervisor *s, const StorageWorkerEvent *e)
{
    uint32_t b;
    if (!s || !e || !(b = bit(e->channel)) || !(s->target_channel_mask & b)) return -1;
    switch (e->type) {
    case STORAGE_WORKER_READY:
        if ((s->ready_mask & b) != 0u || (s->armed_mask & b) != 0u ||
            (s->running_mask & b) != 0u) {
            fail(s, e->channel, STORAGE_ERR_IPC_SEQUENCE, "invalid_ready_sequence");
            return -1;
        }
        s->ready_mask |= b;
        break;
    case STORAGE_WORKER_ARMED:
        if ((s->ready_mask & b) == 0u || (s->armed_mask & b) != 0u ||
            (s->running_mask & b) != 0u) {
            fail(s, e->channel, STORAGE_ERR_IPC_SEQUENCE, "invalid_armed_sequence");
            return -1;
        }
        s->armed_mask |= b;
        break;
    case STORAGE_WORKER_RUNNING:
        if ((s->armed_mask & b) == 0u || (s->running_mask & b) != 0u) {
            fail(s, e->channel, STORAGE_ERR_IPC_SEQUENCE, "invalid_running_sequence");
            return -1;
        }
        s->running_mask |= b;
        break;
    case STORAGE_WORKER_INPUT_IDLE_CANDIDATE:
        if ((s->running_mask & b) == 0u || (s->drained_mask & b) != 0u ||
            (s->final_seen_mask & b) != 0u) {
            fail(s, e->channel, STORAGE_ERR_IPC_SEQUENCE,
                 "invalid_input_idle_sequence");
            return -1;
        }
        s->idle_candidate_mask |= b;
        break;
    case STORAGE_WORKER_INPUT_ACTIVE:
        if ((s->running_mask & b) == 0u || (s->drained_mask & b) != 0u ||
            (s->final_seen_mask & b) != 0u || s->auto_drain_triggered) {
            fail(s, e->channel, STORAGE_ERR_IPC_SEQUENCE,
                 "invalid_input_active_sequence");
            return -1;
        }
        s->idle_candidate_mask &= ~b;
        break;
    case STORAGE_WORKER_DRAIN_READY:
        if ((s->running_mask & b) == 0u || (s->drained_mask & b) != 0u ||
            (s->final_seen_mask & b) != 0u ||
            e->payload.drain_ready.drain_epoch == 0u) {
            fail(s, e->channel, STORAGE_ERR_IPC_SEQUENCE, "invalid_drained_sequence");
            return -1;
        }
        if (s->stop_epoch == 0u)
            s->stop_epoch = e->payload.drain_ready.drain_epoch;
        if (s->stop_epoch != e->payload.drain_ready.drain_epoch) {
            fail(s, e->channel, STORAGE_ERR_IPC_SEQUENCE,
                 "drain_epoch_mismatch");
            return -1;
        }
        s->drained_mask |= b;
        s->drain_result[e->channel] = e->payload.drain_ready;
        break;
        case STORAGE_WORKER_FATAL:
            if ((s->final_seen_mask & b) != 0u ||
                (s->fatal_seen_mask & b) != 0u) {
                fail(s, e->channel, STORAGE_ERR_IPC_SEQUENCE, "invalid_fatal_sequence");
                return -1;
            }
        s->fatal_seen_mask |= b;
        fail(s, e->channel, e->payload.fatal.error_code,
             e->payload.fatal.reason);
        break;
    case STORAGE_WORKER_FINAL_RESULT:
        if (s->final_seen_mask & b) {
            fail(s, e->channel, STORAGE_ERR_IPC_SEQUENCE, "duplicate_final");
            return -1;
        }
        if ((s->drained_mask & b) == 0u && (s->fatal_seen_mask & b) == 0u) {
            fail(s, e->channel, STORAGE_ERR_IPC_SEQUENCE, "invalid_final_sequence");
            return -1;
        }
        s->final_seen_mask |= b;
        s->final_result[e->channel] = e->payload.final.result;
        break;
    case STORAGE_WORKER_PERF_SAMPLE:
        if ((s->running_mask & b) == 0u || (s->final_seen_mask & b) != 0u) {
            fail(s, e->channel, STORAGE_ERR_IPC_SEQUENCE, "invalid_perf_sequence");
            return -1;
        }
        break;
    case STORAGE_WORKER_DIAG_EVENT:
        if ((s->running_mask & b) == 0u && (s->fatal_seen_mask & b) == 0u &&
            (s->final_seen_mask & b) == 0u) {
            fail(s, e->channel, STORAGE_ERR_IPC_SEQUENCE, "invalid_diag_sequence");
            return -1;
        }
        break;
    case STORAGE_WORKER_STOP_PHASE:
        if ((s->running_mask & b) == 0u || (s->final_seen_mask & b) != 0u ||
            e->payload.phase.stop_epoch == 0u ||
            e->payload.phase.stop_phase < STORAGE_WORKER_STOP_REQUESTED ||
            e->payload.phase.stop_phase > STORAGE_WORKER_FAILED_FATAL) {
            fail(s, e->channel, STORAGE_ERR_IPC_SEQUENCE, "invalid_stop_phase_event");
            return -1;
        }
        if (s->stop_epoch == 0u) s->stop_epoch = e->payload.phase.stop_epoch;
        if (s->stop_epoch != e->payload.phase.stop_epoch) {
            fail(s, e->channel, STORAGE_ERR_IPC_SEQUENCE, "stop_epoch_mismatch");
            return -1;
        }
        if (e->payload.phase.stop_phase == STORAGE_WORKER_FAILED_FATAL) {
            if (s->stop_phase[e->channel] == STORAGE_WORKER_FAILED_FATAL) {
                fail(s, e->channel, STORAGE_ERR_IPC_SEQUENCE, "duplicate_stop_phase");
                return -1;
            }
        } else if (e->payload.phase.stop_phase != s->stop_phase[e->channel] + 1u &&
                   !(s->stop_phase[e->channel] == STORAGE_WORKER_STOP_REQUESTED &&
                     e->payload.phase.stop_phase == STORAGE_WORKER_DMA_QUIESCED &&
                     e->payload.phase.error_code == STORAGE_ERR_STOP_BOUNDARY_TIMEOUT)) {
            fail(s, e->channel, STORAGE_ERR_IPC_SEQUENCE, "invalid_stop_phase_sequence");
            return -1;
        }
        s->stop_phase[e->channel] = e->payload.phase.stop_phase;
        break;
    default:
        fail(s, e->channel, STORAGE_ERR_IPC, "event_type_invalid");
        return -1;
    }
    (void)storage_supervisor_result_status(s); return 0;
}
int storage_supervisor_handle_worker_eof(StorageTaskSupervisor *s, uint32_t ch)
{
    uint32_t b = bit(ch);

    if (!s || b == 0u || (s->target_channel_mask & b) == 0u) return -1;
    if ((s->final_seen_mask & b) != 0u) return 0;
    fail(s, ch, STORAGE_ERR_WORKER_EXIT, "event_eof_without_final");
    s->unavailable_mask |= b;
    (void)storage_supervisor_result_status(s);
    return -1;
}
int storage_supervisor_handle_worker_exit(StorageTaskSupervisor *s, uint32_t ch, int code)
{
    uint32_t b = bit(ch);

    if (!s || b == 0u || (s->target_channel_mask & b) == 0u) return -1;
    s->worker_exited_mask |= b;
    if ((s->final_seen_mask & b) == 0u) {
        if (code != 0) fail_worker_exit_without_final(s, ch);
        else fail(s, ch, STORAGE_ERR_WORKER_EXIT, "event_eof_without_final");
        s->unavailable_mask |= b;
        (void)storage_supervisor_result_status(s);
        return -1;
    }
    if (code != 0) {
        fail(s, ch, STORAGE_ERR_WORKER_EXIT, "worker_exit_failed");
        (void)storage_supervisor_result_status(s);
        return -1;
    }
    (void)storage_supervisor_result_status(s);
    return 0;
}
uint32_t storage_supervisor_stop_mask(const StorageTaskSupervisor *s) { return s ? s->stop_requested_mask : 0u; }
bool storage_supervisor_auto_drain_ready(const StorageTaskSupervisor *s)
{
    return s && !s->auto_drain_triggered && s->target_channel_mask != 0u &&
           (s->idle_candidate_mask & s->target_channel_mask) ==
               s->target_channel_mask;
}

bool storage_supervisor_begin_auto_drain(StorageTaskSupervisor *s,
                                         uint64_t drain_epoch)
{
    if (!storage_supervisor_auto_drain_ready(s) || drain_epoch == 0u)
        return false;
    s->auto_drain_triggered = true;
    s->auto_drain_epoch = drain_epoch;
    return true;
}

uint32_t storage_supervisor_auto_drain_pending_mask(
    const StorageTaskSupervisor *s)
{
    return s && s->auto_drain_triggered
               ? s->target_channel_mask & ~s->auto_drain_sent_mask : 0u;
}

void storage_supervisor_mark_auto_drain_sent(StorageTaskSupervisor *s,
                                             uint32_t channel)
{
    uint32_t b = bit(channel);

    if (s && s->auto_drain_triggered &&
        (s->target_channel_mask & b) != 0u)
        s->auto_drain_sent_mask |= b;
}
void storage_supervisor_request_stop(StorageTaskSupervisor *s, uint32_t channel_mask)
{
    if (s) s->stop_requested_mask |= channel_mask & s->target_channel_mask;
}
uint32_t storage_supervisor_peek_stop_mask(const StorageTaskSupervisor *s)
{ return s ? s->stop_requested_mask & ~s->stop_sent_mask : 0u; }
void storage_supervisor_mark_stop_sent(StorageTaskSupervisor *s, uint32_t ch)
{
    uint32_t b = bit(ch);
    if (s && (s->stop_requested_mask & b) != 0u) {
        s->stop_sent_mask |= b;
        s->stop_failed_mask &= ~b;
    }
}
void storage_supervisor_mark_stop_failed(StorageTaskSupervisor *s, uint32_t ch)
{
    uint32_t b = bit(ch);
    if (s && (s->stop_requested_mask & b) != 0u) {
        s->stop_sent_mask &= ~b;
        s->stop_failed_mask |= b;
    }
}
bool storage_supervisor_is_terminal(const StorageTaskSupervisor *s) { return s && s->terminal != STORAGE_TASK_ACTIVE; }
StorageTaskTerminal storage_supervisor_result_status(StorageTaskSupervisor *s)
{
    uint32_t i;
    if (!s || s->terminal != STORAGE_TASK_ACTIVE) return s ? s->terminal : STORAGE_TASK_FAILED;
    if ((s->final_seen_mask | s->unavailable_mask) != s->target_channel_mask) return STORAGE_TASK_ACTIVE;
    s->aggregate_ready = true;
    if (s->first_fatal || s->unavailable_mask != 0u)
        return s->terminal = STORAGE_TASK_FAILED;
    for (i = 0u; i < NUM_CHANNELS; ++i) {
        WriteResult *r;
        if ((s->target_channel_mask & bit(i)) == 0u) continue;
        r = &s->final_result[i];
        if (!r->receive_integrity_ok) {
            fail_task_result(s, i, r, STORAGE_ERR_INTEGRITY,
                             "receive_integrity_failed");
            return s->terminal;
        }
        if (!r->data_persisted || !r->storage_integrity_ok || !r->integrity_ok) {
            fail_task_result(s, i, r, STORAGE_ERR_INTEGRITY,
                             "storage_integrity_failed");
            return s->terminal;
        }
        if (r->dma_received_bytes != r->nvme_completed_bytes) {
            fail_task_result(s, i, r, STORAGE_ERR_BYTE_MISMATCH,
                             "dma_nvme_byte_mismatch");
            return s->terminal;
        }
        if (r->nvme_completed_bytes != r->file_bytes) {
            fail_task_result(s, i, r, STORAGE_ERR_BYTE_MISMATCH,
                             "nvme_file_byte_mismatch");
            return s->terminal;
        }
    }
    if ((s->target_channel_mask & 3u) == 3u &&
        s->final_result[0].dma_received_bytes != s->final_result[1].dma_received_bytes) {
        fail_task_result(s, 0u, &s->final_result[0],
                         STORAGE_ERR_BYTE_MISMATCH,
                         "split_channel_byte_mismatch");
        return s->terminal;
    }
    return s->terminal = STORAGE_TASK_SUCCESS;
}
