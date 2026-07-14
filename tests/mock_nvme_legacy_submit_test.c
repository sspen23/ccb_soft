#include <assert.h>
#include <pthread.h>
#include <sched.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "ccb_hw.h"

#define TEST_QUEUE_BASE 0x80u
#define TEST_TX_CTRL 0x30u
#define TEST_TX_STATUS 0x34u
#define TEST_CUR_CQ_CID 0x0cu
#define TEST_CMD_PENDING (1u << 0)
#define TEST_CQ_EMPTY (1u << 2)

typedef enum { CLEAR_ACCEPT, STOP_WITH_CQ, STOP_NO_CQ, STOP_UNKNOWN_CQ } SimMode;
typedef struct { volatile uint32_t *regs; SimMode mode; uint16_t cid; } Sim;

static void *simulate_doorbell(void *opaque)
{
    Sim *sim = opaque;
    volatile uint32_t *ctrl = &sim->regs[(TEST_QUEUE_BASE + TEST_TX_CTRL) / 4u];
    while ((*ctrl & TEST_CMD_PENDING) == 0u) sched_yield();
    if (sim->mode == CLEAR_ACCEPT) {
        *ctrl = 0u;
        return NULL;
    }
    nvme_request_stop();
    if (sim->mode == STOP_NO_CQ) return NULL;
    usleep(100u);
    *ctrl = 0u;
    sim->regs[(TEST_QUEUE_BASE + TEST_TX_STATUS) / 4u] = 0u;
    sim->regs[(TEST_QUEUE_BASE + TEST_CUR_CQ_CID) / 4u] =
        sim->mode == STOP_UNKNOWN_CQ ? 0xfffeu : sim->cid;
    return NULL;
}

static void init_runtime(ChannelRuntime *rt, ChannelConfig *cfg, volatile uint32_t *regs)
{
    memset(rt, 0, sizeof(*rt));
    memset(cfg, 0, sizeof(*cfg));
    memset((void *)regs, 0, 0x100u);
    cfg->id = 2;
    rt->cfg = cfg;
    rt->nvme.virt = (volatile uint8_t *)regs;
    rt->nvme.valid = true;
    rt->gopt.timeout_us = 5000u;
    rt->nvme_qd_effective = 1u;
    rt->nvme_cmd_sectors = 1u;
    rt->nvme_feed_mode = NVME_FEED_MODE_LEGACY;
    regs[(TEST_QUEUE_BASE + TEST_TX_STATUS) / 4u] = TEST_CQ_EMPTY;
    nvme_clear_stop_request();
}

static void test_stop_before_doorbell(void)
{
    ChannelRuntime rt; ChannelConfig cfg; volatile uint32_t regs[0x100u / 4u];
    init_runtime(&rt, &cfg, regs);
    nvme_request_stop();
    assert(nvme_submit_command_async(&rt, 1u, 1u, 0u, 1u, 0u) ==
           NVME_SUBMIT_STOPPED_BEFORE_DOORBELL);
    assert((regs[(TEST_QUEUE_BASE + TEST_TX_CTRL) / 4u] & TEST_CMD_PENDING) == 0u);
    nvme_clear_stop_request();
}

static void test_doorbell_accept(void)
{
    ChannelRuntime rt; ChannelConfig cfg; volatile uint32_t regs[0x100u / 4u];
    Sim sim; pthread_t thread;
    init_runtime(&rt, &cfg, regs);
    sim.regs = regs; sim.mode = CLEAR_ACCEPT; sim.cid = 1u;
    assert(pthread_create(&thread, NULL, simulate_doorbell, &sim) == 0);
    assert(nvme_submit_command_async(&rt, 1u, 1u, 0u, 1u, 0u) == NVME_SUBMIT_ACCEPTED);
    assert(pthread_join(thread, NULL) == 0);
}

static void test_ambiguous_submit_drains_matching_cq(void)
{
    ChannelRuntime rt; ChannelConfig cfg; volatile uint32_t regs[0x100u / 4u];
    Sim sim; pthread_t thread;
    init_runtime(&rt, &cfg, regs);
    sim.regs = regs; sim.mode = STOP_WITH_CQ; sim.cid = 1u;
    assert(pthread_create(&thread, NULL, simulate_doorbell, &sim) == 0);
    /* The legacy writer retains the pending CID/slot through STOP, drains its
     * CQ, and only then reports completion to its caller. */
    assert(nvme_write_slot_qd_payload(&rt, 3u, 0u, 1u, 512u, 0u) == 0);
    assert(pthread_join(thread, NULL) == 0);
    assert(!rt.nvme_ownership_unresolved);
    nvme_clear_stop_request();
}

static void test_ambiguous_submit_reset_unavailable(void)
{
    ChannelRuntime rt; ChannelConfig cfg; volatile uint32_t regs[0x100u / 4u];
    Sim sim; pthread_t thread;
    init_runtime(&rt, &cfg, regs);
    sim.regs = regs; sim.mode = STOP_NO_CQ; sim.cid = 1u;
    assert(pthread_create(&thread, NULL, simulate_doorbell, &sim) == 0);
    assert(nvme_write_slot_qd_payload(&rt, 4u, 0u, 1u, 512u, 0u) != 0);
    assert(pthread_join(thread, NULL) == 0);
    assert(rt.nvme_ownership_unresolved);
    assert(strstr(rt.nvme_last_error, "submit_accept_timeout") != NULL);
    channel_runtime_close(&rt);
    assert(rt.nvme.valid);
    nvme_clear_stop_request();
}

static void test_ambiguous_submit_unknown_cid_is_fatal(void)
{
    ChannelRuntime rt; ChannelConfig cfg; volatile uint32_t regs[0x100u / 4u];
    Sim sim; pthread_t thread;
    init_runtime(&rt, &cfg, regs);
    sim.regs = regs; sim.mode = STOP_UNKNOWN_CQ; sim.cid = 1u;
    assert(pthread_create(&thread, NULL, simulate_doorbell, &sim) == 0);
    assert(nvme_write_slot_qd_payload(&rt, 5u, 0u, 1u, 512u, 0u) != 0);
    assert(pthread_join(thread, NULL) == 0);
    assert(rt.nvme_ownership_unresolved);
    nvme_clear_stop_request();
}

int main(void)
{
    test_stop_before_doorbell();
    test_doorbell_accept();
    test_ambiguous_submit_drains_matching_cq();
    test_ambiguous_submit_reset_unavailable();
    test_ambiguous_submit_unknown_cid_is_fatal();
    puts("mock_nvme_legacy_submit_test: ok");
    return 0;
}
