#ifndef STORAGE_ERROR_H
#define STORAGE_ERROR_H

#include <stdbool.h>

typedef enum {
    STORAGE_ERR_NONE = 0,
    STORAGE_ERR_CONFIG,
    STORAGE_ERR_PCIE_LINK,
    STORAGE_ERR_NVME_PROBE,
    STORAGE_ERR_DMA_INIT,
    STORAGE_ERR_DMA_DESCRIPTOR,
    STORAGE_ERR_QUEUE,
    STORAGE_ERR_UNKNOWN_CID,
    STORAGE_ERR_DUPLICATE_CID,
    STORAGE_ERR_OWNERSHIP,
    STORAGE_ERR_NVME_TIMEOUT,
    STORAGE_ERR_TAIL_UNALIGNED,
    STORAGE_ERR_LATE_COMPLETION,
    STORAGE_ERR_STOP_BOUNDARY_TIMEOUT,
    STORAGE_ERR_STOP_HARVEST_TIMEOUT,
    STORAGE_ERR_IPC,
    STORAGE_ERR_IPC_SEQUENCE,
    STORAGE_ERR_WORKER_EXIT,
    STORAGE_ERR_INTEGRITY,
    STORAGE_ERR_BYTE_MISMATCH,
    STORAGE_ERR_DMA_STOP,
    STORAGE_ERR_COMMIT,
    STORAGE_ERR_ZERO_PAYLOAD,
    STORAGE_ERR_INTERNAL,
    STORAGE_ERR_COUNT
} StorageErrorCode;

typedef enum {
    STORAGE_ERROR_NONE = 0,
    STORAGE_ERROR_DEFERRED,
    STORAGE_ERROR_FATAL
} StorageErrorClass;

StorageErrorClass storage_error_class(StorageErrorCode code);
const char *storage_error_string(StorageErrorCode code);
bool storage_error_code_valid(StorageErrorCode code);
void storage_error_record(StorageErrorCode *primary, StorageErrorCode *secondary,
                          StorageErrorCode code);

#endif
