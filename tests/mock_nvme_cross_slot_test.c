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
    uint64_t now_us;
} Mock;

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
    mock->submitted[mock->submitted_count++] = cid;
    return 0;
}

static int poll_completion(void *opaque, NvmeCompletion *completion)
{
    Mock *mock = opaque;
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
    mock->now_us += us;
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
    NvmeCrossSlotOps ops = { submit, poll_completion, monotonic_us, sleep_us };
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
    nvme_cross_slot_engine_destroy(value);

    memset(&mock, 0, sizeof(mock)); value = engine(&rt, &mock);
    assert(nvme_cross_slot_engine_add(value, &req) == 0);
    assert(nvme_cross_slot_engine_step(value, 300u, done, &mock) == 0);
    queue_completion(&mock, 0xffffu, 0);
    assert(nvme_cross_slot_engine_step(value, 300u, done, &mock) != 0);
    assert(strcmp(nvme_cross_slot_engine_last_error(value), "unknown_completion_cid") == 0);
    nvme_cross_slot_engine_destroy(value);

    memset(&mock, 0, sizeof(mock)); value = engine(&rt, &mock);
    assert(nvme_cross_slot_engine_add(value, &req) == 0);
    assert(nvme_cross_slot_engine_step(value, 300u, done, &mock) == 0);
    queue_completion(&mock, mock.submitted[0], 1);
    assert(nvme_cross_slot_engine_step(value, 300u, done, &mock) != 0);
    assert(strcmp(nvme_cross_slot_engine_last_error(value), "completion_status_error") == 0);
    nvme_cross_slot_engine_destroy(value);
}

static void test_capacity_sq_full_and_callback(void)
{
    ChannelRuntime rt;
    Mock mock;
    NvmeCrossSlotEngine *value;
    NvmeWriteSlotReq a = request(4u, 1u), b = request(5u, 1u), c = request(6u, 1u);

    memset(&mock, 0, sizeof(mock)); value = engine(&rt, &mock);
    assert(nvme_cross_slot_engine_add(value, &a) == 0);
    assert(nvme_cross_slot_engine_add(value, &b) == 0);
    assert(!nvme_cross_slot_engine_can_accept(value));
    assert(nvme_cross_slot_engine_add(value, &c) == 1);
    mock.sq_full_once = 1;
    assert(nvme_cross_slot_engine_step(value, 300u, done, &mock) == 0);
    assert(mock.submitted_count == 0u);
    assert(nvme_cross_slot_engine_active(value) == 2u);
    assert(nvme_cross_slot_engine_step(value, 300u, done, &mock) == 0);
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
    nvme_cross_slot_engine_destroy(value);
}

int main(void)
{
    test_multislot_out_of_order_and_budget();
    test_completion_failures();
    test_capacity_sq_full_and_callback();
    puts("mock_nvme_cross_slot_test: ok");
    return 0;
}
