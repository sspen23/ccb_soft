#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "ccb_commands.h"
#include "ccb_hw.h"

#define MAX_COMMANDS 256u
#define SLOT_SECTORS (8u * 1024u * 1024u / 512u)

typedef struct {
    NvmeCompletion completions[MAX_COMMANDS];
    uint16_t submitted[MAX_COMMANDS];
    unsigned completion_head;
    unsigned completion_tail;
    unsigned submit_count;
    unsigned complete_count;
    unsigned done_count;
    bool slot_done[16];
    uint64_t now_us;
} WriterMock;

static int submit(void *opaque, uint16_t cid, uint64_t lba,
                  uint32_t sectors, uint64_t ddr_addr)
{
    WriterMock *mock = opaque;
    NvmeCompletion *completion;

    (void)lba;
    (void)sectors;
    (void)ddr_addr;
    assert(mock->submit_count < MAX_COMMANDS);
    assert(mock->completion_tail < MAX_COMMANDS);
    mock->submitted[mock->submit_count++] = cid;
    completion = &mock->completions[mock->completion_tail++];
    memset(completion, 0, sizeof(*completion));
    completion->cid = cid;
    return NVME_SUBMIT_ACCEPTED;
}

static int poll_completion(void *opaque, NvmeCompletion *completion)
{
    WriterMock *mock = opaque;

    if (mock->completion_head == mock->completion_tail) return 0;
    *completion = mock->completions[mock->completion_head++];
    ++mock->complete_count;
    return 1;
}

static uint64_t monotonic_us(void *opaque)
{
    WriterMock *mock = opaque;
    return mock->now_us++;
}

static void sleep_us(void *opaque, uint32_t us)
{
    WriterMock *mock = opaque;
    mock->now_us += us;
}

static void yield_cpu(void *opaque)
{
    WriterMock *mock = opaque;
    ++mock->now_us;
}

static int reset_engine(void *opaque)
{
    (void)opaque;
    return -1;
}

static int slot_done(void *opaque, const NvmeWriteSlotReq *req)
{
    WriterMock *mock = opaque;

    assert(req->slot < 16u);
    assert(!mock->slot_done[req->slot]);
    mock->slot_done[req->slot] = true;
    ++mock->done_count;
    return 0;
}

static NvmeWriteSlotReq request(uint32_t slot, uint64_t sectors)
{
    NvmeWriteSlotReq req;

    memset(&req, 0, sizeof(req));
    req.slot = slot;
    req.start_lba = (uint64_t)slot * SLOT_SECTORS;
    req.sectors = sectors;
    req.hw_addr = (uint64_t)slot * 8u * 1024u * 1024u;
    req.bytes = sectors * 512u;
    req.media_bytes = req.bytes;
    return req;
}

static NvmeCrossSlotEngine *create_engine(ChannelRuntime *rt, WriterMock *mock,
                                         uint32_t max_active)
{
    NvmeCrossSlotOps ops = {
        submit, poll_completion, monotonic_us, sleep_us, yield_cpu, reset_engine,
    };
    NvmeCrossSlotConfig config = {
        .max_active_slots = max_active,
        .target_qd = max_active == 1u ? 1u : 4u,
        .cq_batch = max_active == 1u ? 1u : 4u,
        .writer_budget_us = 100000u,
        .busy_poll_us = 0u,
        .empty_sleep_us = 0u,
        .no_progress_timeout_us = 1000000u,
    };

    memset(rt, 0, sizeof(*rt));
    rt->dma_desc_count = 16u;
    rt->nvme_qd_effective = config.target_qd;
    rt->nvme_cmd_sectors = 512u;
    return nvme_cross_slot_engine_create_with_ops_config(rt, &config, &ops, mock);
}

static void step_until_idle(NvmeCrossSlotEngine *engine, WriterMock *mock)
{
    unsigned guard = 0u;

    while (nvme_cross_slot_engine_active(engine) != 0u) {
        assert(++guard < 1000u);
        assert(nvme_cross_slot_engine_step(engine, 100000u, slot_done, mock) == 0);
    }
    assert(nvme_cross_slot_engine_inflight(engine) == 0u);
}

static void test_max_active_one_keeps_processing_ready_slots(void)
{
    ChannelRuntime rt;
    WriterMock mock;
    NvmeCrossSlotEngine *engine;
    NvmeWriteSlotReq first = request(0u, SLOT_SECTORS);
    NvmeWriteSlotReq second = request(1u, SLOT_SECTORS);
    uint32_t ready_count = 1u;
    unsigned not_attempted = 0u;
    StorageCrossSlotWriterDecision decision;

    memset(&mock, 0, sizeof(mock));
    engine = create_engine(&rt, &mock, 1u);
    assert(engine != NULL);
    assert(nvme_cross_slot_engine_add(engine, &first) == 0);
    assert(!nvme_cross_slot_engine_can_accept(engine));
    ++not_attempted;
    step_until_idle(engine, &mock);

    decision = storage_cross_slot_writer_decide(true, ready_count, false,
                                                nvme_cross_slot_engine_active(engine),
                                                nvme_cross_slot_engine_inflight(engine));
    assert(decision == STORAGE_CROSS_SLOT_WRITER_CONTINUE);
    assert(not_attempted == 1u);
    assert(nvme_cross_slot_engine_add(engine, &second) == 0);
    --ready_count;
    step_until_idle(engine, &mock);
    assert(storage_cross_slot_writer_decide(true, ready_count, false, 0u, 0u) ==
           STORAGE_CROSS_SLOT_WRITER_DRAINED);
    assert(mock.submit_count == 64u);
    assert(mock.complete_count == 64u);
    assert(mock.done_count == 2u && mock.slot_done[0] && mock.slot_done[1]);
    assert(nvme_cross_slot_engine_is_quiesced(engine));
    nvme_cross_slot_engine_destroy(engine);
}

static void test_wait_and_terminal_decisions(void)
{
    assert(storage_cross_slot_writer_decide(false, 0u, false, 0u, 0u) ==
           STORAGE_CROSS_SLOT_WRITER_WAIT_FOR_QUEUE);
    assert(storage_cross_slot_writer_decide(true, 0u, false, 0u, 0u) ==
           STORAGE_CROSS_SLOT_WRITER_DRAINED);
    assert(storage_cross_slot_writer_decide(true, 1u, false, 0u, 0u) ==
           STORAGE_CROSS_SLOT_WRITER_CONTINUE);
    assert(storage_cross_slot_writer_decide(true, 0u, true, 0u, 0u) ==
           STORAGE_CROSS_SLOT_WRITER_QUEUE_ERROR);
}

static void test_full_max_active_four_continues_with_backlog(void)
{
    ChannelRuntime rt;
    WriterMock mock;
    NvmeCrossSlotEngine *engine;
    NvmeWriteSlotReq reqs[6];
    uint32_t ready_count = 2u;
    unsigned i;

    memset(&mock, 0, sizeof(mock));
    engine = create_engine(&rt, &mock, 4u);
    assert(engine != NULL);
    for (i = 0u; i < 6u; ++i) reqs[i] = request(i, 1u);
    for (i = 0u; i < 4u; ++i) assert(nvme_cross_slot_engine_add(engine, &reqs[i]) == 0);
    assert(!nvme_cross_slot_engine_can_accept(engine));
    step_until_idle(engine, &mock);
    assert(storage_cross_slot_writer_decide(true, ready_count, false, 0u, 0u) ==
           STORAGE_CROSS_SLOT_WRITER_CONTINUE);
    for (i = 4u; i < 6u; ++i) {
        assert(nvme_cross_slot_engine_add(engine, &reqs[i]) == 0);
        --ready_count;
    }
    step_until_idle(engine, &mock);
    assert(storage_cross_slot_writer_decide(true, ready_count, false, 0u, 0u) ==
           STORAGE_CROSS_SLOT_WRITER_DRAINED);
    assert(mock.submit_count == 6u && mock.complete_count == 6u && mock.done_count == 6u);
    for (i = 0u; i < 6u; ++i) assert(mock.slot_done[i]);
    assert(nvme_cross_slot_engine_is_quiesced(engine));
    nvme_cross_slot_engine_destroy(engine);
}

static void test_command_budget_bounds_one_step(void)
{
    ChannelRuntime rt;
    WriterMock mock;
    NvmeCrossSlotEngine *engine;
    NvmeWriteSlotReq req = request(0u, SLOT_SECTORS);

    memset(&mock, 0, sizeof(mock));
    engine = create_engine(&rt, &mock, 4u);
    assert(engine != NULL);
    assert(nvme_cross_slot_engine_add(engine, &req) == 0);
    assert(nvme_cross_slot_engine_step(engine, 100000u, slot_done, &mock) == 0);
    assert(nvme_cross_slot_engine_active(engine) == 1u);
    assert(mock.submit_count == 16u && mock.complete_count == 16u);
    assert(mock.done_count == 0u);
    step_until_idle(engine, &mock);
    assert(mock.submit_count == 32u && mock.complete_count == 32u);
    assert(mock.done_count == 1u);
    nvme_cross_slot_engine_destroy(engine);
}

static void test_admission_budget(void)
{
    assert(storage_cross_slot_admission_limit(0u) == 0u);
    assert(storage_cross_slot_admission_limit(1u) == 1u);
    assert(storage_cross_slot_admission_limit(4u) == 4u);
    assert(storage_cross_slot_admission_limit(8u) == 4u);
}

int main(void)
{
    test_max_active_one_keeps_processing_ready_slots();
    test_wait_and_terminal_decisions();
    test_full_max_active_four_continues_with_backlog();
    test_command_budget_bounds_one_step();
    test_admission_budget();
    puts("mock_cross_slot_writer_lifecycle_test: ok");
    return 0;
}
