#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "ccb_storage_supervisor.h"

static StorageWorkerEvent event(uint32_t type, uint32_t channel)
{
    StorageWorkerEvent value;

    storage_ipc_make_event(&value, type, channel,
                           type == STORAGE_WORKER_FATAL
                               ? STORAGE_ERR_INTERNAL : STORAGE_ERR_NONE,
                           0u, "worker_fatal");
    if (type == STORAGE_WORKER_FINAL_RESULT) {
        value.payload.final.result.data_persisted = true;
        value.payload.final.result.receive_integrity_ok = true;
        value.payload.final.result.storage_integrity_ok = true;
        value.payload.final.result.integrity_ok = true;
        value.payload.final.result.dma_received_bytes = 10u;
        value.payload.final.result.nvme_completed_bytes = 10u;
        value.payload.final.result.file_bytes = 10u;
    } else if (type == STORAGE_WORKER_DRAIN_READY) {
        value.payload.drain_ready.drain_epoch = 77u;
        value.payload.drain_ready.primary_error = STORAGE_ERR_NONE;
        value.payload.drain_ready.secondary_error = STORAGE_ERR_NONE;
        value.payload.drain_ready.integrity_ok = 1u;
    }
    return value;
}

static void assert_reason(const StorageTaskSupervisor *s, const char *reason)
{
    assert(s->fatal_reason[0] != '\0');
    assert(strcmp(s->fatal_reason, reason) == 0);
}

static void advance_to_running(StorageTaskSupervisor *s, uint32_t channel)
{
    StorageWorkerEvent value;
    uint32_t bit = 1u << channel;

    if ((s->ready_mask & bit) == 0u) {
        value = event(STORAGE_WORKER_READY, channel);
        assert(storage_supervisor_handle_event(s, &value) == 0);
    }
    if ((s->armed_mask & bit) == 0u) {
        value = event(STORAGE_WORKER_ARMED, channel);
        assert(storage_supervisor_handle_event(s, &value) == 0);
    }
    if ((s->running_mask & bit) == 0u) {
        value = event(STORAGE_WORKER_RUNNING, channel);
        assert(storage_supervisor_handle_event(s, &value) == 0);
    }
}

static int handle_final(StorageTaskSupervisor *s, StorageWorkerEvent *value)
{
    StorageWorkerEvent drained;
    uint32_t bit = 1u << value->channel;

    advance_to_running(s, value->channel);
    if ((s->drained_mask & bit) == 0u) {
        drained = event(STORAGE_WORKER_DRAIN_READY, value->channel);
        assert(storage_supervisor_handle_event(s, &drained) == 0);
    }
    return storage_supervisor_handle_event(s, value);
}

static void test_worker_exit_without_final(void)
{
    StorageTaskSupervisor s;

    storage_supervisor_init(&s, 3u);
    assert(storage_supervisor_handle_worker_exit(&s, 0u, 7) != 0);
    assert((s.unavailable_mask & 1u) != 0u);
    assert_reason(&s, "worker_exit_without_final");
    assert(!s.aggregate_ready);
    assert(storage_supervisor_handle_worker_exit(&s, 1u, 0) != 0);
    assert(s.aggregate_ready);
    assert(storage_supervisor_result_status(&s) == STORAGE_TASK_FAILED);

    storage_supervisor_init(&s, 1u);
    assert(storage_supervisor_handle_worker_exit(&s, 0u, 0) != 0);
    assert_reason(&s, "event_eof_without_final");
    assert(storage_supervisor_result_status(&s) == STORAGE_TASK_FAILED);
}

static void test_unavailable_worker_aggregates(void)
{
    StorageTaskSupervisor s;
    StorageWorkerEvent value;

    storage_supervisor_init(&s, 7u);
    storage_supervisor_mark_unavailable(&s, 1u, "fork_exec_failed");
    assert((s.unavailable_mask & (1u << 1u)) != 0u);
    assert_reason(&s, "fork_exec_failed");
    value = event(STORAGE_WORKER_FINAL_RESULT, 0u);
    assert(handle_final(&s, &value) == 0);
    value = event(STORAGE_WORKER_FINAL_RESULT, 2u);
    assert(handle_final(&s, &value) == 0);
    assert(s.aggregate_ready);
    assert(storage_supervisor_result_status(&s) == STORAGE_TASK_FAILED);
}

static void test_stop_send_state(void)
{
    StorageTaskSupervisor s;
    StorageWorkerEvent value;

    storage_supervisor_init(&s, 3u);
    value = event(STORAGE_WORKER_FATAL, 0u);
    assert(storage_supervisor_handle_event(&s, &value) == 0);
    assert(storage_supervisor_peek_stop_mask(&s) == 3u);
    assert(s.primary_error == STORAGE_ERR_INTERNAL);
    assert(s.stop_sent_mask == 0u);

    storage_supervisor_mark_stop_failed(&s, 0u);
    assert(storage_supervisor_peek_stop_mask(&s) == 3u);
    assert((s.stop_failed_mask & 1u) != 0u);

    storage_supervisor_mark_stop_sent(&s, 0u);
    assert(storage_supervisor_peek_stop_mask(&s) == 2u);
    assert((s.stop_failed_mask & 1u) == 0u);
    storage_supervisor_mark_stop_sent(&s, 0u);
    assert(storage_supervisor_peek_stop_mask(&s) == 2u);

    storage_supervisor_init(&s, 1u);
    value = event(STORAGE_WORKER_FATAL, 0u);
    value.payload.fatal.reason[0] = '\0';
    assert(storage_supervisor_handle_event(&s, &value) == 0);
    assert_reason(&s, "worker_fatal");
}

static void test_invalid_sequences(void)
{
    StorageTaskSupervisor s;
    StorageWorkerEvent value;

    storage_supervisor_init(&s, 1u);
    value = event(STORAGE_WORKER_READY, 0u);
    assert(storage_supervisor_handle_event(&s, &value) == 0);
    assert(storage_supervisor_handle_event(&s, &value) != 0);
    assert_reason(&s, "invalid_ready_sequence");
    assert(s.primary_error == STORAGE_ERR_IPC_SEQUENCE);

    storage_supervisor_init(&s, 1u);
    value = event(STORAGE_WORKER_FATAL, 0u);
    snprintf(value.payload.fatal.reason, sizeof(value.payload.fatal.reason),
             "%s", "primary_reason");
    assert(storage_supervisor_handle_event(&s, &value) == 0);
    value = event(STORAGE_WORKER_FATAL, 0u);
    snprintf(value.payload.fatal.reason, sizeof(value.payload.fatal.reason),
             "%s", "secondary_reason");
    assert(storage_supervisor_handle_event(&s, &value) != 0);
    assert_reason(&s, "primary_reason");
    assert(strcmp(s.secondary_reason, "invalid_fatal_sequence") == 0);

    storage_supervisor_init(&s, 1u);
    value = event(STORAGE_WORKER_ARMED, 0u);
    assert(storage_supervisor_handle_event(&s, &value) != 0);
    assert_reason(&s, "invalid_armed_sequence");

    storage_supervisor_init(&s, 1u);
    value = event(STORAGE_WORKER_RUNNING, 0u);
    assert(storage_supervisor_handle_event(&s, &value) != 0);
    assert_reason(&s, "invalid_running_sequence");

    storage_supervisor_init(&s, 1u);
    value = event(STORAGE_WORKER_DRAIN_READY, 0u);
    assert(storage_supervisor_handle_event(&s, &value) != 0);
    assert_reason(&s, "invalid_drained_sequence");

    storage_supervisor_init(&s, 1u);
    value = event(STORAGE_WORKER_FINAL_RESULT, 0u);
    assert(storage_supervisor_handle_event(&s, &value) != 0);
    assert_reason(&s, "invalid_final_sequence");
}

static void test_event_pipe_ownership(void)
{
    StorageTaskSupervisor s;
    StorageWorkerEvent value;

    storage_supervisor_init(&s, 3u);
    value = event(STORAGE_WORKER_READY, 1u);
    assert(storage_supervisor_handle_event_for_channel(&s, 0u, &value) != 0);
    assert_reason(&s, "event_channel_mismatch");
    assert(storage_supervisor_peek_stop_mask(&s) == 3u);
    assert(s.ready_mask == 0u);

    storage_supervisor_init(&s, 3u);
    value = event(STORAGE_WORKER_FINAL_RESULT, 0u);
    value.magic = 0u;
    assert(storage_supervisor_handle_event_for_channel(&s, 0u, &value) != 0);
    assert_reason(&s, "event_protocol_invalid");
    assert(storage_supervisor_peek_stop_mask(&s) == 3u);
    assert(s.final_seen_mask == 0u);
}

static void test_final_and_aggregate(void)
{
    StorageTaskSupervisor s;
    StorageWorkerEvent value;

    storage_supervisor_init(&s, 3u);
    value = event(STORAGE_WORKER_FINAL_RESULT, 0u);
    assert(handle_final(&s, &value) == 0);
    assert(!s.aggregate_ready);
    assert(storage_supervisor_result_status(&s) == STORAGE_TASK_ACTIVE);
    assert(storage_supervisor_handle_worker_eof(&s, 0u) == 0);
    assert(s.final_seen_mask == 1u);
    assert(s.final_result[0].file_bytes == 10u);
    assert(!s.aggregate_ready);
    value = event(STORAGE_WORKER_FINAL_RESULT, 1u);
    assert(handle_final(&s, &value) == 0);
    assert(s.aggregate_ready);
    assert(storage_supervisor_result_status(&s) == STORAGE_TASK_SUCCESS);

    storage_supervisor_init(&s, 1u);
    value = event(STORAGE_WORKER_FINAL_RESULT, 0u);
    assert(handle_final(&s, &value) == 0);
    assert(storage_supervisor_handle_event(&s, &value) != 0);
    assert_reason(&s, "duplicate_final");
    assert(storage_supervisor_result_status(&s) == STORAGE_TASK_FAILED);
}

static void test_diag_after_final_is_best_effort(void)
{
    StorageTaskSupervisor s;
    StorageWorkerEvent value;

    storage_supervisor_init(&s, 1u);
    value = event(STORAGE_WORKER_FINAL_RESULT, 0u);
    assert(handle_final(&s, &value) == 0);
    assert(storage_supervisor_result_status(&s) == STORAGE_TASK_SUCCESS);

    value = event(STORAGE_WORKER_DIAG_EVENT, 0u);
    assert(storage_supervisor_handle_event(&s, &value) == 0);
    assert(storage_supervisor_result_status(&s) == STORAGE_TASK_SUCCESS);
    assert(!s.first_fatal);
}

static void test_result_failure_reasons(void)
{
    StorageTaskSupervisor s;
    StorageWorkerEvent value;

#define CHECK_RESULT_REASON(field, reason)                                      \
    do {                                                                        \
        storage_supervisor_init(&s, 1u);                                        \
        value = event(STORAGE_WORKER_FINAL_RESULT, 0u);                         \
        value.payload.final.result.field = false;                               \
        assert(handle_final(&s, &value) == 0);                                  \
        assert(storage_supervisor_result_status(&s) == STORAGE_TASK_FAILED);    \
        assert_reason(&s, reason);                                              \
    } while (0)

    CHECK_RESULT_REASON(receive_integrity_ok, "receive_integrity_failed");
    CHECK_RESULT_REASON(data_persisted, "storage_integrity_failed");
    CHECK_RESULT_REASON(storage_integrity_ok, "storage_integrity_failed");
    CHECK_RESULT_REASON(integrity_ok, "storage_integrity_failed");

#undef CHECK_RESULT_REASON

    storage_supervisor_init(&s, 1u);
    value = event(STORAGE_WORKER_FINAL_RESULT, 0u);
    value.payload.final.result.nvme_completed_bytes = 9u;
    assert(handle_final(&s, &value) == 0);
    assert_reason(&s, "dma_nvme_byte_mismatch");

    storage_supervisor_init(&s, 1u);
    value = event(STORAGE_WORKER_FINAL_RESULT, 0u);
    value.payload.final.result.file_bytes = 9u;
    assert(handle_final(&s, &value) == 0);
    assert_reason(&s, "nvme_file_byte_mismatch");

    storage_supervisor_init(&s, 3u);
    value = event(STORAGE_WORKER_FINAL_RESULT, 0u);
    assert(handle_final(&s, &value) == 0);
    value = event(STORAGE_WORKER_FINAL_RESULT, 1u);
    value.payload.final.result.dma_received_bytes = 11u;
    value.payload.final.result.nvme_completed_bytes = 11u;
    value.payload.final.result.file_bytes = 11u;
    assert(handle_final(&s, &value) == 0);
    assert_reason(&s, "split_channel_byte_mismatch");
}

static void test_one_channel_failure_rejects_aggregate(void)
{
    StorageTaskSupervisor s;
    StorageWorkerEvent value;

    storage_supervisor_init(&s, 3u);
    value = event(STORAGE_WORKER_FINAL_RESULT, 0u);
    assert(handle_final(&s, &value) == 0);
    assert(storage_supervisor_result_status(&s) == STORAGE_TASK_ACTIVE);
    value = event(STORAGE_WORKER_FINAL_RESULT, 1u);
    value.payload.final.result.storage_integrity_ok = false;
    assert(handle_final(&s, &value) == 0);
    assert(s.aggregate_ready);
    assert(storage_supervisor_result_status(&s) == STORAGE_TASK_FAILED);
    assert_reason(&s, "storage_integrity_failed");
    assert(s.primary_error == STORAGE_ERR_INTEGRITY);
}

static void test_payload_media_accounting(void)
{
    StorageTaskSupervisor s;
    StorageWorkerEvent value;

    storage_supervisor_init(&s, 1u);
    value = event(STORAGE_WORKER_FINAL_RESULT, 0u);
    value.payload.final.result.dma_received_bytes = 419692512u;
    value.payload.final.result.nvme_completed_bytes = 419692512u;
    value.payload.final.result.nvme_media_bytes = 419692544u;
    value.payload.final.result.nvme_padding_bytes = 32u;
    value.payload.final.result.file_bytes = 419692512u;
    assert(handle_final(&s, &value) == 0);
    assert(storage_supervisor_result_status(&s) == STORAGE_TASK_SUCCESS);
}

static StorageWorkerEvent stop_phase(uint32_t channel, uint64_t epoch,
                                     StorageWorkerStopPhase phase,
                                     const char *reason)
{
    StorageWorkerEvent value = event(STORAGE_WORKER_STOP_PHASE, channel);

    value.payload.phase.stop_epoch = epoch;
    value.payload.phase.stop_phase = (uint32_t)phase;
    snprintf(value.payload.phase.reason, sizeof(value.payload.phase.reason),
             "%s", reason ? reason : "phase");
    return value;
}

static void test_multichannel_stop_epoch_and_deferred_isolation(void)
{
    StorageTaskSupervisor s;
    StorageWorkerEvent value;
    uint32_t channel;
    uint32_t phase;

    storage_supervisor_init(&s, 3u);
    advance_to_running(&s, 0u);
    advance_to_running(&s, 1u);
    for (channel = 0u; channel < 2u; ++channel) {
        for (phase = STORAGE_WORKER_STOP_REQUESTED;
             phase <= STORAGE_WORKER_FINALIZED; ++phase) {
            value = stop_phase(channel, 77u, (StorageWorkerStopPhase)phase,
                               "graceful_stop");
            assert(storage_supervisor_handle_event(&s, &value) == 0);
        }
    }
    assert(s.stop_epoch == 77u);
    assert(!s.first_fatal && s.stop_requested_mask == 0u);

    storage_supervisor_init(&s, 3u);
    advance_to_running(&s, 0u);
    advance_to_running(&s, 1u);
    value = stop_phase(0u, 88u, STORAGE_WORKER_STOP_REQUESTED, "stop_requested");
    assert(storage_supervisor_handle_event(&s, &value) == 0);
    value = stop_phase(0u, 88u, STORAGE_WORKER_DMA_QUIESCED,
                       "boundary_timeout_quiesced");
    value.payload.phase.error_code = STORAGE_ERR_STOP_BOUNDARY_TIMEOUT;
    assert(storage_supervisor_handle_event(&s, &value) == 0);
    assert(!s.first_fatal && s.stop_requested_mask == 0u);
    value = stop_phase(1u, 89u, STORAGE_WORKER_STOP_REQUESTED, "stop_requested");
    assert(storage_supervisor_handle_event(&s, &value) != 0);
    assert_reason(&s, "stop_epoch_mismatch");
    assert(s.primary_error == STORAGE_ERR_IPC_SEQUENCE);
}

static void test_multichannel_input_idle_coordination(void)
{
    StorageTaskSupervisor s;
    StorageWorkerEvent value;
    uint32_t channel;

    storage_supervisor_init(&s, 7u);
    for (channel = 0u; channel < 3u; ++channel)
        advance_to_running(&s, channel);

    value = event(STORAGE_WORKER_INPUT_IDLE_CANDIDATE, 2u);
    assert(storage_supervisor_handle_event(&s, &value) == 0);
    assert(!storage_supervisor_auto_drain_ready(&s));
    value = event(STORAGE_WORKER_INPUT_IDLE_CANDIDATE, 0u);
    assert(storage_supervisor_handle_event(&s, &value) == 0);
    assert(!storage_supervisor_auto_drain_ready(&s));
    value = event(STORAGE_WORKER_INPUT_ACTIVE, 2u);
    assert(storage_supervisor_handle_event(&s, &value) == 0);
    assert(!storage_supervisor_auto_drain_ready(&s));
    value = event(STORAGE_WORKER_INPUT_IDLE_CANDIDATE, 2u);
    assert(storage_supervisor_handle_event(&s, &value) == 0);
    value = event(STORAGE_WORKER_INPUT_IDLE_CANDIDATE, 1u);
    assert(storage_supervisor_handle_event(&s, &value) == 0);
    assert(storage_supervisor_auto_drain_ready(&s));
    assert(storage_supervisor_begin_auto_drain(&s, 123u));
    assert(s.auto_drain_epoch == 123u);
    assert(storage_supervisor_auto_drain_pending_mask(&s) == 7u);
    storage_supervisor_mark_auto_drain_sent(&s, 0u);
    storage_supervisor_mark_auto_drain_sent(&s, 1u);
    storage_supervisor_mark_auto_drain_sent(&s, 2u);
    assert(storage_supervisor_auto_drain_pending_mask(&s) == 0u);
    assert(!storage_supervisor_begin_auto_drain(&s, 124u));
    value = event(STORAGE_WORKER_INPUT_IDLE_CANDIDATE, 1u);
    assert(storage_supervisor_handle_event(&s, &value) == 0);
    assert(s.auto_drain_epoch == 123u);
}

int main(void)
{
    test_worker_exit_without_final();
    test_unavailable_worker_aggregates();
    test_stop_send_state();
    test_invalid_sequences();
    test_event_pipe_ownership();
    test_final_and_aggregate();
    test_diag_after_final_is_best_effort();
    test_result_failure_reasons();
    test_one_channel_failure_rejects_aggregate();
    test_payload_media_accounting();
    test_multichannel_stop_epoch_and_deferred_isolation();
    test_multichannel_input_idle_coordination();
    puts("mock_storage_supervisor_test: ok");
    return 0;
}
