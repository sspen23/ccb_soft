#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "ccb_cli.h"
#include "ccb_config.h"

static ParsedArgs direct_read_args(int channel_id, uint64_t ddr_offset)
{
    ParsedArgs args;

    memset(&args, 0, sizeof(args));
    args.has_channel = true;
    args.channel_id = channel_id;
    args.has_lba = true;
    args.lba = 0x1000u;
    args.has_size = true;
    args.size_bytes = 256u * 1024u;
    args.has_ddr_offset = true;
    args.ddr_offset = ddr_offset;
    return args;
}

static void test_direct_read_offset_bounds(void)
{
    const ChannelConfig *cfg = find_channel(HIGH_I_CHANNEL_ID);
    ParsedArgs args;
    uint64_t ddr_hw_addr;

    assert(cfg != NULL);

    args = direct_read_args(HIGH_I_CHANNEL_ID, 0u);
    assert(validate_read_args(&args) == 0);
    ddr_hw_addr = cfg->ddr_hw_base + args.ddr_offset;
    assert(ddr_hw_addr == cfg->ddr_hw_base);

    args.ddr_offset = cfg->dma_ring_bytes - args.size_bytes;
    assert(validate_read_args(&args) == 0);

    args.ddr_offset += SECTOR_SIZE;
    assert(validate_read_args(&args) != 0);

    args.ddr_offset = 1u;
    assert(validate_read_args(&args) != 0);
}

static void test_metadata_read_offset(void)
{
    const ChannelConfig *cfg = find_channel(HIGH_Q_CHANNEL_ID);
    ParsedArgs args;

    assert(cfg != NULL);
    memset(&args, 0, sizeof(args));
    args.has_channel = true;
    args.channel_id = HIGH_Q_CHANNEL_ID;
    args.has_task_no = true;
    memcpy(args.task_no, "task", sizeof("task"));
    args.has_file_index = true;
    args.file_index = 3u;
    args.has_ddr_offset = true;
    args.ddr_offset = cfg->dma_ring_bytes - 4096u;

    assert(validate_read_args(&args) == 0);
}

int main(void)
{
    test_direct_read_offset_bounds();
    test_metadata_read_offset();
    puts("mock_ddr_address_args_test: PASS");
    return 0;
}
