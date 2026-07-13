#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "ccb_hw.h"

typedef struct {
    uint16_t submitted[32];
    NvmeCompletion completions[32];
    unsigned submitted_count;
    unsigned completion_count;
    unsigned completion_index;
    unsigned callbacks;
    unsigned fail_callback;
    int sq_full_once;
    unsigned poll_count;
    unsigned release_after_polls;
    int release_on_yield;
    uint16_t delayed_cid;
    unsigned yield_count;
    unsigned sleep_count;
    uint64_t now_us;
    unsigned fail_submit_call;
    unsigned reset_calls;
    int reset_result;
} Mock;

static void queue_completion(Mock *mock, uint16_t cid, int error);

static int submit(void *opaque, uint16_t cid, uint64_t lba, uint32_t sectors, uint64_t ddr_addr)
{
    Mock *mock = opaque;
    (void)lba;
    (void)sectors;
    (void)ddr_addr;
    if (mock->sq_full_once) {
        mock->sq_full_once = 0;
        return 1;
    }
    if (mock->fail_submit_call != 0u &&
        mock->submitted_count + 1u == mock->fail_submit_call) return -1;
    mock->submitted[mock->submitted_count++] = cid;
    return 0;
}

static int poll_completion(void *opaque, NvmeCompletion *completion)
{
    Mock *mock = opaque;
    ++mock->poll_count;
    if (mock->release_after_polls != 0u && mock->poll_count >= mock->release_after_polls &&
        mock->completion_count == 0u) {
        queue_completion(mock, mock->delayed_cid, 0);
    }
    if (mock->completion_index >= mock->completion_count) return 0;
    *completion = mock->completions[mock->completion_index++];
    return 1;
}

static uint64_t monotonic_us(void *opaque)
{
    Mock *mock = opaque;
    return mock->now_us++;
}

static void sleep_us(void *opaque, uint32_t us)
{
    Mock *mock = opaque;
    ++mock->sleep_count;
    mock->now_us += us;
}

static void yield_cpu(void *opaque)
{
    Mock *mock = opaque;
    ++mock->yield_count;
    if (mock->release_on_yield && mock->completion_count == 0u) {
        queue_completion(mock, mock->delayed_cid, 0);
    }
}

static int reset_engine(void *opaque)
{
    Mock *mock = opaque;
    ++mock->reset_calls;
    return mock->reset_result;
}

static int done(void *opaque, const NvmeWriteSlotReq *req)
{
    Mock *mock = opaque;
    (void)req;
    ++mock->callbacks;
    return mock->fail_callback ? -1 : 0;
}

static NvmeCrossSlotConfig config(void)
{
    NvmeCrossSlotConfig value = {
        .max_active_slots = 2u,
        .target_qd = 4u,
        .cq_batch = 32u,
        .writer_budget_us = 300u,
        .busy_poll_us = 20u,
        .empty_sleep_us = 1u,
        .no_progress_timeout_us = 1000u,
    };
    return value;
}

static NvmeWriteSlotReq request(uint32_t slot, uint64_t sectors)
{
    NvmeWriteSlotReq value;
    memset(&value, 0, sizeof(value));
    value.slot = slot;
    value.start_lba = slot * 16u;
    value.sectors = sectors;
    value.hw_addr = slot * 4096u;
    value.bytes = sectors * 512u;
    return value;
}

static NvmeCrossSlotEngine *engine(ChannelRuntime *rt, Mock *mock)
{
    NvmeCrossSlotOps ops = { submit, poll_completion, monotonic_us, sleep_us,
                             yield_cpu, reset_engine };
    NvmeCrossSlotConfig value = config();
    memset(rt, 0, sizeof(*rt));
    rt->nvme_qd_effective = 4u;
    rt->nvme_cmd_sectors = 1u;
    return nvme_cross_slot_engine_create_with_ops_config(rt, &value, &ops, mock);
}

static void queue_completion(Mock *mock, uint16_t cid, int error)
{
    NvmeCompletion *completion = &mock->completions[mock->completion_count++];
    memset(completion, 0, sizeof(*completion));
    completion->cid = cid;
    completion->error = error != 0;
    completion->status_code = error ? 1u : 0u;
}

static void test_multislot_out_of_order_and_budget(void)
{
    ChannelRuntime rt;
    Mock mock;
    NvmeCrossSlotEngine *value;
    NvmeWriteSlotReq a = request(1u, 2u);
    NvmeWriteSlotReq b = request(2u, 2u);

    memset(&mock, 0, sizeof(mock));
    value = engine(&rt, &mock);
    assert(value);
    assert(nvme_cross_slot_engine_add(value, &a) == 0);
    assert(nvme_cross_slot_engine_add(value, &b) == 0);
    assert(nvme_cross_slot_engine_step(value, 1u, done, &mock) == 0);
    assert(mock.submitted_count == 4u);
    assert(mock.callbacks == 0u);
    queue_completion(&mock, mock.submitted[2], 0);
    queue_completion(&mock, mock.submitted[1], 0);
    assert(nvme_cross_slot_engine_step(value, 300u, done, &mock) == 0);
    assert(mock.callbacks == 0u);
    queue_completion(&mock, mock.submitted[0], 0);
    assert(nvme_cross_slot_engine_step(value, 300u, done, &mock) == 0);
    assert(mock.callbacks == 1u);
    assert(nvme_cross_slot_engine_active(value) == 1u);
    queue_completion(&mock, mock.submitted[3], 0);
    assert(nvme_cross_slot_engine_step(value, 300u, done, &mock) == 0);
    assert(mock.callbacks == 2u);
    assert(nvme_cross_slot_engine_active(value) == 0u);
    nvme_cross_slot_engine_destroy(value);
}

static void test_completion_failures(void)
{
    ChannelRuntime rt;
    Mock mock;
    NvmeCrossSlotEngine *value;
    NvmeWriteSlotReq req = request(3u, 2u);

    memset(&mock, 0, sizeof(mock)); value = engine(&rt, &mock);
    assert(nvme_cross_slot_engine_add(value, &req) == 0);
    assert(nvme_cross_slot_engine_step(value, 300u, done, &mock) == 0);
    queue_completion(&mock, mock.submitted[0], 0);
    queue_completion(&mock, mock.submitted[0], 0);
    assert(nvme_cross_slot_engine_step(value, 300u, done, &mock) != 0);
    assert(strcmp(nvme_cross_slot_engine_last_error(value), "duplicate_completion_cid") == 0);
    assert(nvme_cross_slot_engine_state(value) == NVME_CROSS_SLOT_ABORT_REQUESTED);
    queue_completion(&mock, mock.submitted[1], 0);
    assert(nvme_cross_slot_engine_drain_abort(value, mock.now_us + 100u) == 0);
    nvme_cross_slot_engine_destroy(value);

    memset(&mock, 0, sizeof(mock)); value = engine(&rt, &mock);
    assert(nvme_cross_slot_engine_add(value, &req) == 0);
    assert(nvme_cross_slot_engine_step(value, 300u, done, &mock) == 0);
    queue_completion(&mock, 0xffffu, 0);
    assert(nvme_cross_slot_engine_step(value, 300u, done, &mock) != 0);
    assert(strcmp(nvme_cross_slot_engine_last_error(value), "unknown_completion_cid") == 0);
    assert(nvme_cross_slot_engine_state(value) == NVME_CROSS_SLOT_ABORT_REQUESTED);
    queue_completion(&mock, mock.submitted[0], 0);
    queue_completion(&mock, mock.submitted[1], 0);
    assert(nvme_cross_slot_engine_drain_abort(value, mock.now_us + 100u) == 0);
    nvme_cross_slot_engine_destroy(value);

    memset(&mock, 0, sizeof(mock)); value = engine(&rt, &mock);
    assert(nvme_cross_slot_engine_add(value, &req) == 0);
    assert(nvme_cross_slot_engine_step(value, 300u, done, &mock) == 0);
    queue_completion(&mock, mock.submitted[0], 1);
    assert(nvme_cross_slot_engine_step(value, 300u, done, &mock) != 0);
    assert(strcmp(nvme_cross_slot_engine_last_error(value), "completion_status_error") == 0);
    assert(nvme_cross_slot_engine_inflight(value) == 1u);
    queue_completion(&mock, mock.submitted[1], 0);
    assert(nvme_cross_slot_engine_drain_abort(value, mock.now_us + 100u) == 0);
    assert(nvme_cross_slot_engine_inflight(value) == 0u);
    assert(mock.callbacks == 0u);
    nvme_cross_slot_engine_destroy(value);
}

static void test_capacity_sq_full_and_callback(void)
{
    ChannelRuntime rt;
    Mock mock;
    NvmeCrossSlotEngine *value;
    NvmeCrossSlotStats stats;
    NvmeWriteSlotReq a = request(4u, 1u), b = request(5u, 1u), c = request(6u, 1u);

    memset(&mock, 0, sizeof(mock)); value = engine(&rt, &mock);
    assert(nvme_cross_slot_engine_add(value, &a) == 0);
    assert(nvme_cross_slot_engine_add(value, &b) == 0);
    assert(!nvme_cross_slot_engine_can_accept(value));
    assert(nvme_cross_slot_engine_add(value, &c) == 1);
    mock.sq_full_once = 1;
    assert(nvme_cross_slot_engine_step(value, 300u, done, &mock) == 0);
    assert(mock.submitted_count == 2u);
    nvme_cross_slot_engine_get_stats(value, &stats);
    assert(stats.sq_full_wait_count == 1u && stats.submit_mmio_count >= 3u);
    assert(nvme_cross_slot_engine_active(value) == 2u);
    assert(nvme_cross_slot_engine_step(value, 300u, done, &mock) == 0);
    assert(mock.submitted_count == 2u);
    queue_completion(&mock, mock.submitted[0], 0);
    queue_completion(&mock, mock.submitted[1], 0);
    assert(nvme_cross_slot_engine_step(value, 300u, done, &mock) == 0);
    assert(mock.callbacks == 2u);
    nvme_cross_slot_engine_destroy(value);

    memset(&mock, 0, sizeof(mock)); mock.fail_callback = 1u; value = engine(&rt, &mock);
    assert(nvme_cross_slot_engine_add(value, &a) == 0);
    assert(nvme_cross_slot_engine_step(value, 300u, done, &mock) == 0);
    queue_completion(&mock, mock.submitted[0], 0);
    assert(nvme_cross_slot_engine_step(value, 300u, done, &mock) != 0);
    assert(strcmp(nvme_cross_slot_engine_last_error(value), "slot_callback_failed") == 0);
    assert(nvme_cross_slot_engine_drain_abort(value, mock.now_us + 10u) == 0);
    nvme_cross_slot_engine_destroy(value);
}

static void test_stall_policy_and_stats(void)
{
    ChannelRuntime rt;
    Mock mock;
    NvmeCrossSlotEngine *value;
    NvmeCrossSlotStats stats;
    NvmeWriteSlotReq req = request(7u, 1u);

    memset(&mock, 0, sizeof(mock));
    value = engine(&rt, &mock);
    assert(nvme_cross_slot_engine_add(value, &req) == 0);
    mock.delayed_cid = 1u;
    mock.release_after_polls = 2u;
    assert(nvme_cross_slot_engine_step(value, 300u, done, &mock) == 0);
    nvme_cross_slot_engine_get_stats(value, &stats);
    assert(mock.callbacks == 1u && stats.cq_empty_wait_count > 0u);
    assert(stats.completion_process_count == 1u && stats.submit_mmio_count == 1u);
    nvme_cross_slot_engine_destroy(value);

    memset(&mock, 0, sizeof(mock));
    value = engine(&rt, &mock);
    assert(nvme_cross_slot_engine_add(value, &req) == 0);
    mock.delayed_cid = 1u;
    mock.release_on_yield = 1;
    assert(nvme_cross_slot_engine_step(value, 300u, done, &mock) == 0);
    assert(mock.yield_count > 0u && mock.callbacks == 1u);
    nvme_cross_slot_engine_destroy(value);

    memset(&mock, 0, sizeof(mock));
    value = engine(&rt, &mock);
    assert(nvme_cross_slot_engine_add(value, &req) == 0);
    assert(nvme_cross_slot_engine_step(value, 25u, done, &mock) == 0);
    nvme_cross_slot_engine_get_stats(value, &stats);
    assert(mock.sleep_count > 0u && stats.no_progress_sleep_count > 0u);
    assert(mock.submitted_count == 1u);
    assert(nvme_cross_slot_engine_step(value, 25u, done, &mock) == 0);
    assert(mock.submitted_count == 1u);
    nvme_cross_slot_engine_destroy(value);
}

static void test_no_progress_timeout(void)
{
    ChannelRuntime rt;
    Mock mock;
    NvmeCrossSlotEngine *value;
    NvmeCrossSlotConfig value_config = config();
    NvmeCrossSlotOps ops = { submit, poll_completion, monotonic_us, sleep_us,
                             yield_cpu, reset_engine };
    NvmeWriteSlotReq req = request(8u, 1u);

    value_config.busy_poll_us = 0u;
    value_config.empty_sleep_us = 1u;
    value_config.no_progress_timeout_us = 3u;
    memset(&mock, 0, sizeof(mock));
    memset(&rt, 0, sizeof(rt));
    rt.nvme_qd_effective = 4u;
    rt.nvme_cmd_sectors = 1u;
    value = nvme_cross_slot_engine_create_with_ops_config(&rt, &value_config, &ops, &mock);
    assert(value && nvme_cross_slot_engine_add(value, &req) == 0);
    assert(nvme_cross_slot_engine_step(value, 300u, done, &mock) != 0);
    assert(strcmp(nvme_cross_slot_engine_last_error(value), "no_progress_timeout") == 0);
    assert(nvme_cross_slot_engine_drain_abort(value, mock.now_us + 2u) == 0);
    nvme_cross_slot_engine_destroy(value);
}

static void test_abort_reset_and_submit_failure(void)
{
    ChannelRuntime rt;
    Mock mock;
    NvmeCrossSlotEngine *value;
    NvmeWriteSlotReq req = request(9u, 2u);

    memset(&mock, 0, sizeof(mock));
    value = engine(&rt, &mock);
    assert(nvme_cross_slot_engine_add(value, &req) == 0);
    nvme_cross_slot_engine_request_abort(value, "test_abort");
    nvme_cross_slot_engine_request_abort(value, "ignored_second_abort");
    assert(nvme_cross_slot_engine_step(value, 300u, done, &mock) != 0);
    assert(mock.submitted_count == 0u);
    assert(nvme_cross_slot_engine_drain_abort(value, mock.now_us + 10u) == 0);
    assert(nvme_cross_slot_engine_is_quiesced(value));
    nvme_cross_slot_engine_destroy(value);

    memset(&mock, 0, sizeof(mock)); mock.fail_submit_call = 2u;
    value = engine(&rt, &mock);
    assert(nvme_cross_slot_engine_add(value, &req) == 0);
    assert(nvme_cross_slot_engine_step(value, 300u, done, &mock) != 0);
    assert(strcmp(nvme_cross_slot_engine_last_error(value), "submit_failed") == 0);
    assert(nvme_cross_slot_engine_inflight(value) == 1u);
    assert(nvme_cross_slot_engine_drain_abort(value, mock.now_us + 2u) == 0);
    assert(mock.reset_calls == 1u && nvme_cross_slot_engine_is_quiesced(value));
    nvme_cross_slot_engine_destroy(value);

    memset(&mock, 0, sizeof(mock)); mock.fail_submit_call = 2u; mock.reset_result = -1;
    value = engine(&rt, &mock);
    assert(nvme_cross_slot_engine_add(value, &req) == 0);
    assert(nvme_cross_slot_engine_step(value, 300u, done, &mock) != 0);
    assert(nvme_cross_slot_engine_drain_abort(value, mock.now_us + 2u) != 0);
    assert(mock.reset_calls == 1u);
    assert(nvme_cross_slot_engine_state(value) == NVME_CROSS_SLOT_FAILED);
    assert(!nvme_cross_slot_engine_is_quiesced(value));
    /* Deliberately not destroyed: reset did not confirm DDR-safe quiescence. */
}

static void test_multislot_error_drains_without_callbacks(void)
{
    ChannelRuntime rt;
    Mock mock;
    NvmeCrossSlotEngine *value;
    NvmeWriteSlotReq a = request(10u, 1u), b = request(11u, 1u);

    memset(&mock, 0, sizeof(mock)); value = engine(&rt, &mock);
    assert(nvme_cross_slot_engine_add(value, &a) == 0);
    assert(nvme_cross_slot_engine_add(value, &b) == 0);
    assert(nvme_cross_slot_engine_step(value, 300u, done, &mock) == 0);
    queue_completion(&mock, mock.submitted[0], 1);
    queue_completion(&mock, mock.submitted[1], 0);
    assert(nvme_cross_slot_engine_step(value, 300u, done, &mock) != 0);
    assert(nvme_cross_slot_engine_drain_abort(value, mock.now_us + 100u) == 0);
    assert(nvme_cross_slot_engine_inflight(value) == 0u);
    assert(mock.callbacks == 0u);
    nvme_cross_slot_engine_destroy(value);
}

int main(void)
{
    test_multislot_out_of_order_and_budget();
    test_completion_failures();
    test_capacity_sq_full_and_callback();
    test_stall_policy_and_stats();
    test_no_progress_timeout();
    test_abort_reset_and_submit_failure();
    test_multislot_error_drains_without_callbacks();
    puts("mock_nvme_cross_slot_test: ok");
    return 0;
}
