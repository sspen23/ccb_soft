#ifndef CCB_CONFIG_H
#define CCB_CONFIG_H

#include "ccb_types.h"

/* Network MM2S path defaults from system.dts. */
#define TCP_SWITCH_BASE_DEFAULT         0xA0040000ull
#define TCP_SWITCH_INPUT_DEFAULT        0u
#define TCP_DMA_BASE_DEFAULT            0x41E00000ull
#define TCP_DESC_CPU_BASE_DEFAULT       0x20000000ull
#define TCP_DESC_DMA_BASE_DEFAULT       0x10000000ull
#define TCP_DDR_HW_ADDR_DEFAULT         0x00000000ull
#define TCP_MAX_BYTES_PER_DESC          (16u * 1024u * 1024u)
/* ch0/ch1 fill one TCP buffer with two independent 8 MiB SSD reads. */
#define SSD_READ_BYTES_PER_CHUNK_HIGH   (8u * 1024u * 1024u)

/* Static channel configuration table defined in src/ccb_config.c. */
extern const ChannelConfig kChannels[NUM_CHANNELS];

/* Find channel config by numeric channel id (0/1/2). */
const ChannelConfig *find_channel(int id);

#endif
