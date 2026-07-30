#include <assert.h>
#include <pthread.h>
#include <sched.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "ccb_config.h"
#include "ccb_hw.h"

#define TEST_QUEUE_BASE 0x80u
#define TEST_CUR_CQ_CID 0x0cu
#define TEST_PRP2_LO 0x18u
#define TEST_PRP2_HI 0x1cu
#define TEST_TX_CTRL 0x30u
#define TEST_CMD_PENDING (1u << 0)
#define TEST_PAGE_BYTES 4096u
#define TEST_PRP_BYTES 0x8000u
#define TEST_PRP_BASE 0xc0000000ull
#define TEST_CH1_PRP_BASE 0xc2000000ull

typedef struct {
    volatile uint32_t *regs;
} DoorbellSim;

static void *clear_doorbell(void *opaque)
{
    DoorbellSim *sim = opaque;
    volatile uint32_t *ctrl =
        &sim->regs[(TEST_QUEUE_BASE + TEST_TX_CTRL) / sizeof(uint32_t)];

    while ((*ctrl & TEST_CMD_PENDING) == 0u) {
        sched_yield();
    }
    *ctrl = 0u;
    return NULL;
}

static uint64_t read_u64_words(const volatile uint32_t *words,
                               uint32_t byte_offset)
{
    uint32_t index = byte_offset / sizeof(uint32_t);

    return (uint64_t)words[index] |
           ((uint64_t)words[index + 1u] << 32u);
}

static void test_prp_shapes(void)
{
    uint64_t entries[TEST_PAGE_BYTES / sizeof(uint64_t)];
    uint64_t prp2;
    uint32_t count;
    uint64_t prp1 = 0x01000000ull;

    memset(entries, 0, sizeof(entries));
    assert(nvme_build_prp(prp1, 4u * 1024u, TEST_PAGE_BYTES,
                          0u, NULL, 0u, &prp2, &count) == 0);
    assert(prp2 == 0u);
    assert(count == 0u);

    assert(nvme_build_prp(prp1, 8u * 1024u, TEST_PAGE_BYTES,
                          0u, NULL, 0u, &prp2, &count) == 0);
    assert(prp2 == prp1 + TEST_PAGE_BYTES);
    assert(count == 0u);

    assert(nvme_build_prp(prp1, 12u * 1024u, TEST_PAGE_BYTES,
                          TEST_PRP_BASE, entries, 512u,
                          &prp2, &count) == 0);
    assert(prp2 == TEST_PRP_BASE);
    assert(count == 2u);
    assert(entries[0] == prp1 + TEST_PAGE_BYTES);
    assert(entries[1] == prp1 + 2u * TEST_PAGE_BYTES);

    assert(nvme_build_prp(prp1, 256u * 1024u, TEST_PAGE_BYTES,
                          TEST_PRP_BASE, entries, 512u,
                          &prp2, &count) == 0);
    assert(prp2 == TEST_PRP_BASE);
    assert(count == 63u);
    assert(entries[0] == prp1 + TEST_PAGE_BYTES);
    assert(entries[62] == prp1 + 63u * TEST_PAGE_BYTES);

    assert(nvme_build_prp(prp1, 256u * 1024u, TEST_PAGE_BYTES,
                          TEST_PRP_BASE, entries, 62u,
                          &prp2, &count) != 0);
    assert(nvme_build_prp(prp1, 12u * 1024u, TEST_PAGE_BYTES,
                          TEST_PRP_BASE + 8u, entries, 512u,
                          &prp2, &count) != 0);
    assert(nvme_build_prp(prp1 + 1u, 4u * 1024u, TEST_PAGE_BYTES,
                          0u, NULL, 0u, &prp2, &count) != 0);
}

static void test_channel_prp_config(void)
{
    const ChannelConfig *ch0 = find_channel(HIGH_I_CHANNEL_ID);
    const ChannelConfig *ch1 = find_channel(HIGH_Q_CHANNEL_ID);
    const ChannelConfig *ch2 = find_channel(LOW_SPEED_CHANNEL_ID);

    assert(ch0 != NULL);
    assert(ch0->nvme_manual_prp);
    assert(ch0->prp_list_cpu_base == TEST_PRP_BASE);
    assert(ch0->prp_list_hw_base == TEST_PRP_BASE);
    assert(ch0->prp_list_size == TEST_PRP_BYTES);

    assert(ch1 != NULL);
    assert(ch1->nvme_manual_prp);
    assert(ch1->prp_list_cpu_base == TEST_CH1_PRP_BASE);
    assert(ch1->prp_list_hw_base == TEST_CH1_PRP_BASE);
    assert(ch1->prp_list_size == TEST_PRP_BYTES);

    assert(ch2 != NULL);
    assert(!ch2->nvme_manual_prp);
}

static void test_default_read_command_size(void)
{
    ChannelRuntime rt;
    int channel_id;

    unsetenv("SRC_REAL_NVME_READ_CMD_KIB");
    unsetenv("SRC_REAL_NVME_READ_CMD_KIB_CH0");
    unsetenv("SRC_REAL_NVME_READ_CMD_KIB_CH1");
    unsetenv("SRC_REAL_NVME_READ_CMD_KIB_CH2");
    memset(&rt, 0, sizeof(rt));
    rt.nvme_page_size = TEST_PAGE_BYTES;
    rt.nvme_max_dts_bytes = 1024u * 1024u;

    for (channel_id = 0; channel_id < NUM_CHANNELS; ++channel_id) {
        rt.cfg = find_channel(channel_id);
        assert(rt.cfg != NULL);
        assert(nvme_read_command_sectors(&rt) == 512u);
    }
}

static void init_manual_runtime(ChannelRuntime *rt,
                                ChannelConfig *cfg,
                                volatile uint32_t *regs,
                                volatile uint32_t *prp_words)
{
    memset(rt, 0, sizeof(*rt));
    memset(cfg, 0, sizeof(*cfg));
    memset((void *)regs, 0, 0x100u);
    memset((void *)prp_words, 0, TEST_PRP_BYTES);

    cfg->id = HIGH_I_CHANNEL_ID;
    cfg->nvme_manual_prp = true;
    cfg->prp_list_cpu_base = TEST_PRP_BASE;
    cfg->prp_list_hw_base = TEST_PRP_BASE;
    cfg->prp_list_size = TEST_PRP_BYTES;
    rt->cfg = cfg;
    rt->gopt.timeout_us = 5000u;
    rt->nvme_page_size = TEST_PAGE_BYTES;
    rt->nvme_block_size = 512u;
    rt->nvme_qd_effective = NVME_PRP_SLOT_CAPACITY;
    rt->nvme_prp_slot_count = NVME_PRP_SLOT_CAPACITY;
    rt->nvme.virt = (volatile uint8_t *)regs;
    rt->nvme.valid = true;
    rt->prp_list.virt = (volatile uint8_t *)prp_words;
    rt->prp_list.size = TEST_PRP_BYTES;
    rt->prp_list.valid = true;
    nvme_clear_stop_request();
}

static int submit_with_doorbell(ChannelRuntime *rt,
                                volatile uint32_t *regs,
                                uint16_t cid,
                                uint32_t sectors,
                                uint64_t prp1)
{
    DoorbellSim sim = { .regs = regs };
    pthread_t thread;
    int rc;

    assert(pthread_create(&thread, NULL, clear_doorbell, &sim) == 0);
    rc = nvme_submit_command_async(rt, 2u, cid, 0u, sectors, prp1);
    assert(pthread_join(thread, NULL) == 0);
    return rc;
}

static void test_auto_prp_channel_is_unchanged(void)
{
    ChannelRuntime rt;
    ChannelConfig cfg;
    volatile uint32_t regs[0x100u / sizeof(uint32_t)];

    memset(&rt, 0, sizeof(rt));
    memset(&cfg, 0, sizeof(cfg));
    memset((void *)regs, 0, sizeof(regs));
    cfg.id = LOW_SPEED_CHANNEL_ID;
    rt.cfg = &cfg;
    rt.gopt.timeout_us = 5000u;
    rt.nvme.virt = (volatile uint8_t *)regs;
    rt.nvme.valid = true;
    regs[(TEST_QUEUE_BASE + TEST_PRP2_LO) / sizeof(uint32_t)] = 0x11223344u;
    regs[(TEST_QUEUE_BASE + TEST_PRP2_HI) / sizeof(uint32_t)] = 0x55667788u;
    nvme_clear_stop_request();

    assert(submit_with_doorbell(&rt, regs, 1u, 512u, 0u) ==
           NVME_SUBMIT_ACCEPTED);
    assert(read_u64_words(regs, TEST_QUEUE_BASE + TEST_PRP2_LO) ==
           0x5566778811223344ull);
    assert(rt.nvme_prp_slot_count == 0u);
}

static void test_qd8_slot_lifetime(void)
{
    ChannelRuntime rt;
    ChannelConfig cfg;
    volatile uint32_t regs[0x100u / sizeof(uint32_t)];
    volatile uint32_t prp_words[TEST_PRP_BYTES / sizeof(uint32_t)];
    NvmeCompletion completion;
    uint32_t i;

    init_manual_runtime(&rt, &cfg, regs, prp_words);
    for (i = 0u; i < NVME_PRP_SLOT_CAPACITY; ++i) {
        uint64_t prp1 = (uint64_t)i * 256u * 1024u;
        uint64_t expected_prp2 = TEST_PRP_BASE +
                                 (uint64_t)i * TEST_PAGE_BYTES;
        uint32_t slot_offset = i * TEST_PAGE_BYTES;

        assert(submit_with_doorbell(&rt, regs, (uint16_t)(i + 1u),
                                    512u, prp1) == NVME_SUBMIT_ACCEPTED);
        assert(rt.nvme_prp_slot_in_use[i]);
        assert(rt.nvme_prp_slot_cid[i] == i + 1u);
        assert(read_u64_words(regs,
                              TEST_QUEUE_BASE + TEST_PRP2_LO) ==
               expected_prp2);
        assert(read_u64_words(prp_words, slot_offset) ==
               prp1 + TEST_PAGE_BYTES);
        assert(read_u64_words(prp_words,
                              slot_offset + 62u * sizeof(uint64_t)) ==
               prp1 + 63u * TEST_PAGE_BYTES);
    }

    assert(nvme_submit_command_async(&rt, 2u, 9u, 0u, 512u,
                                     0x00800000ull) ==
           NVME_SUBMIT_NOT_ACCEPTED);
    assert((regs[(TEST_QUEUE_BASE + TEST_TX_CTRL) / sizeof(uint32_t)] &
            TEST_CMD_PENDING) == 0u);

    regs[(TEST_QUEUE_BASE + TEST_CUR_CQ_CID) / sizeof(uint32_t)] = 3u;
    assert(nvme_poll_cq(&rt, &completion, 1000u) == 0);
    assert(completion.cid == 3u);
    assert(!rt.nvme_prp_slot_in_use[2]);

    assert(submit_with_doorbell(&rt, regs, 9u, 512u,
                                0x00800000ull) ==
           NVME_SUBMIT_ACCEPTED);
    assert(rt.nvme_prp_slot_in_use[2]);
    assert(rt.nvme_prp_slot_cid[2] == 9u);
    assert(read_u64_words(regs, TEST_QUEUE_BASE + TEST_PRP2_LO) ==
           TEST_PRP_BASE + 2u * TEST_PAGE_BYTES);

    /* A two-page command uses direct PRP2 and does not consume a list slot,
     * even while all eight list slots are occupied. */
    assert(submit_with_doorbell(&rt, regs, 10u, 16u,
                                0x01000000ull) ==
           NVME_SUBMIT_ACCEPTED);
    assert(read_u64_words(regs, TEST_QUEUE_BASE + TEST_PRP2_LO) ==
           0x01000000ull + TEST_PAGE_BYTES);

    /* A one-page command explicitly clears stale manual PRP2 registers. */
    assert(submit_with_doorbell(&rt, regs, 11u, 8u,
                                0x02000000ull) ==
           NVME_SUBMIT_ACCEPTED);
    assert(read_u64_words(regs, TEST_QUEUE_BASE + TEST_PRP2_LO) == 0u);
}

int main(void)
{
    test_prp_shapes();
    test_channel_prp_config();
    test_default_read_command_size();
    test_auto_prp_channel_is_unchanged();
    test_qd8_slot_lifetime();
    puts("mock_nvme_prp_test: ok");
    return 0;
}
