#ifndef CCB_TCP_TRANSFER_H
#define CCB_TCP_TRANSFER_H

#include <stdbool.h>
#include <stdint.h>

#include "ccb_types.h"

typedef struct {
    uint64_t switch_base;
    uint32_t switch_input_select;
    uint64_t dma_base;
    uint64_t desc_cpu_base;
    uint64_t desc_dma_base;
    uint64_t ddr_hw_addr;
    uint64_t transfer_bytes;
    uint32_t timeout_us;
    bool dry_run;
} TcpTransferConfig;

void tcp_transfer_default_config(TcpTransferConfig *cfg, uint64_t transfer_bytes, GlobalOptions gopt);
int tcp_transfer_send(const TcpTransferConfig *cfg);
int tcp_transfer_reset(const TcpTransferConfig *cfg);
void tcp_transfer_request_stop(void);

#endif
