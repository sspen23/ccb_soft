#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "ccb_storage_supervisor.h"

static StorageWorkerEvent event(uint32_t type, uint32_t channel)
{
    StorageWorkerEvent value;

    storage_ipc_make_event(&value, type, channel, 0, 0u, "worker_fatal");
    value.result.data_persisted = true;
    value.result.receive_integrity_ok = true;
    value.result.storage_integrity_ok = true;
    value.result.integrity_ok = true;
    value.result.dma_received_bytes = 10u;
    value.result.nvme_completed_bytes = 10u;
    value.result.file_bytes = 10u;
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
        drained = event(STORAGE_WORKER_DRAINED, value->channel);
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

static void test_stop_send_state(void)
{
    StorageTaskSupervisor s;
    StorageWorkerEvent value;

    storage_supervisor_init(&s, 3u);
    value = event(STORAGE_WORKER_FATAL, 0u);
    assert(storage_supervisor_handle_event(&s, &value) == 0);
    assert(storage_supervisor_peek_stop_mask(&s) == 3u);
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
    value.reason[0] = '\0';
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

    storage_supervisor_init(&s, 1u);
    value = event(STORAGE_WORKER_ARMED, 0u);
    assert(storage_supervisor_handle_event(&s, &value) != 0);
    assert_reason(&s, "invalid_armed_sequence");

    storage_supervisor_init(&s, 1u);
    value = event(STORAGE_WORKER_RUNNING, 0u);
    assert(storage_supervisor_handle_event(&s, &value) != 0);
    assert_reason(&s, "invalid_running_sequence");

    storage_supervisor_init(&s, 1u);
    value = event(STORAGE_WORKER_DRAINED, 0u);
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

static void test_result_failure_reasons(void)
{
    StorageTaskSupervisor s;
    StorageWorkerEvent value;

#define CHECK_RESULT_REASON(field, reason)                                      \
    do {                                                                        \
        storage_supervisor_init(&s, 1u);                                        \
        value = event(STORAGE_WORKER_FINAL_RESULT, 0u);                         \
        value.result.field = false;                                             \
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
    value.result.nvme_completed_bytes = 9u;
    assert(handle_final(&s, &value) == 0);
    assert_reason(&s, "dma_nvme_byte_mismatch");

    storage_supervisor_init(&s, 1u);
    value = event(STORAGE_WORKER_FINAL_RESULT, 0u);
    value.result.file_bytes = 9u;
    assert(handle_final(&s, &value) == 0);
    assert_reason(&s, "nvme_file_byte_mismatch");

    storage_supervisor_init(&s, 3u);
    value = event(STORAGE_WORKER_FINAL_RESULT, 0u);
    assert(handle_final(&s, &value) == 0);
    value = event(STORAGE_WORKER_FINAL_RESULT, 1u);
    value.result.dma_received_bytes = 11u;
    value.result.nvme_completed_bytes = 11u;
    value.result.file_bytes = 11u;
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
    value.result.storage_integrity_ok = false;
    assert(handle_final(&s, &value) == 0);
    assert(s.aggregate_ready);
    assert(storage_supervisor_result_status(&s) == STORAGE_TASK_FAILED);
    assert_reason(&s, "storage_integrity_failed");
}

int main(void)
{
    test_worker_exit_without_final();
    test_stop_send_state();
    test_invalid_sequences();
    test_event_pipe_ownership();
    test_final_and_aggregate();
    test_result_failure_reasons();
    test_one_channel_failure_rejects_aggregate();
    puts("mock_storage_supervisor_test: ok");
    return 0;
}
