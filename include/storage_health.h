#ifndef STORAGE_HEALTH_H
#define STORAGE_HEALTH_H

#include "storage_error.h"

#include <stdbool.h>
#include <stdint.h>

typedef struct {
    uint32_t channel;
    bool pcie_link;
    bool nvme_ready;
    bool capacity_valid;
    uint32_t logical_block_bytes;
    uint32_t max_transfer_raw;
    uint32_t max_transfer_blocks;
    uint32_t max_transfer_bytes;
    uint32_t requested_command_bytes;
    uint32_t effective_command_bytes;
    uint32_t nvme_qd;
    uint64_t checked_us;
    StorageErrorCode error;
} StorageHealthSnapshot;

typedef enum {
    STORAGE_HEALTH_RETRYING = 0,
    STORAGE_HEALTH_OK,
    STORAGE_HEALTH_FAILED
} StorageHealthResult;

typedef StorageErrorCode (*StorageHealthProbeFn)(
    uint32_t channel, void *ctx, StorageHealthSnapshot *snapshot);

int storage_health_start(StorageHealthProbeFn probe, void *ctx,
                         uint32_t refresh_interval_ms);
void storage_health_stop(void);
void storage_health_set_busy(bool busy);
void storage_health_request_refresh(void);
StorageHealthResult storage_health_query(uint64_t max_age_us,
                                         StorageHealthSnapshot snapshots[3]);

#endif
