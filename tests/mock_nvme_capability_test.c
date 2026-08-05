#include <assert.h>
#include <stdint.h>
#include <stdio.h>

#include "ccb_hw.h"

int main(void)
{
    uint32_t blocks = 0u;
    uint32_t bytes = 0u;

    assert(nvme_decode_max_transfer(1024u, 512u, &blocks, &bytes) == 0);
    assert(blocks == 1024u);
    assert(bytes == 512u * 1024u);

    assert(nvme_clamp_command_bytes(1024u * 1024u, bytes, 512u,
                                    &bytes) == 0);
    assert(bytes == 512u * 1024u);
    assert(nvme_clamp_command_bytes(512u * 1024u, bytes, 512u,
                                    &bytes) == 0);
    assert(bytes == 512u * 1024u);

    assert(nvme_decode_max_transfer(512u, 512u, &blocks, &bytes) == 0);
    assert(blocks == 512u);
    assert(bytes == 256u * 1024u);
    assert(nvme_clamp_command_bytes(512u * 1024u, bytes, 512u,
                                    &bytes) == 0);
    assert(bytes == 256u * 1024u);

    assert(nvme_decode_max_transfer(0u, 512u, &blocks, &bytes) != 0);
    assert(nvme_decode_max_transfer(0u, 0u, &blocks, &bytes) != 0);
    assert(nvme_decode_max_transfer(0x2000u, 512u, &blocks, &bytes) != 0);
    assert(nvme_clamp_command_bytes(512u * 1024u, 0u, 512u,
                                    &bytes) != 0);

    puts("mock_nvme_capability_test: ok");
    return 0;
}
