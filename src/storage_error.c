#include "storage_error.h"

StorageErrorClass storage_error_class(StorageErrorCode code)
{
    switch (code) {
    case STORAGE_ERR_NONE:
        return STORAGE_ERROR_NONE;
    case STORAGE_ERR_TAIL_UNALIGNED:
    case STORAGE_ERR_LATE_COMPLETION:
    case STORAGE_ERR_STOP_BOUNDARY_TIMEOUT:
    case STORAGE_ERR_STOP_HARVEST_TIMEOUT:
        return STORAGE_ERROR_DEFERRED;
    default:
        return STORAGE_ERROR_FATAL;
    }
}

const char *storage_error_string(StorageErrorCode code)
{
    switch (code) {
    case STORAGE_ERR_NONE: return "none";
    case STORAGE_ERR_CONFIG: return "config";
    case STORAGE_ERR_PCIE_LINK: return "pcie_link";
    case STORAGE_ERR_NVME_PROBE: return "nvme_probe";
    case STORAGE_ERR_DMA_INIT: return "dma_init";
    case STORAGE_ERR_DMA_DESCRIPTOR: return "dma_descriptor";
    case STORAGE_ERR_QUEUE: return "queue";
    case STORAGE_ERR_UNKNOWN_CID: return "unknown_cid";
    case STORAGE_ERR_DUPLICATE_CID: return "duplicate_cid";
    case STORAGE_ERR_OWNERSHIP: return "ownership";
    case STORAGE_ERR_NVME_TIMEOUT: return "nvme_timeout";
    case STORAGE_ERR_TAIL_UNALIGNED: return "tail_unaligned";
    case STORAGE_ERR_LATE_COMPLETION: return "late_completion";
    case STORAGE_ERR_STOP_BOUNDARY_TIMEOUT: return "stop_boundary_timeout";
    case STORAGE_ERR_STOP_HARVEST_TIMEOUT: return "stop_harvest_timeout";
    case STORAGE_ERR_IPC: return "ipc";
    case STORAGE_ERR_IPC_SEQUENCE: return "ipc_sequence";
    case STORAGE_ERR_WORKER_EXIT: return "worker_exit";
    case STORAGE_ERR_INTEGRITY: return "integrity";
    case STORAGE_ERR_BYTE_MISMATCH: return "byte_mismatch";
    case STORAGE_ERR_DMA_STOP: return "dma_stop";
    case STORAGE_ERR_COMMIT: return "commit";
    case STORAGE_ERR_ZERO_PAYLOAD: return "zero_payload";
    case STORAGE_ERR_INTERNAL: return "internal";
    default: return "invalid_error_code";
    }
}

bool storage_error_code_valid(StorageErrorCode code)
{
    return code >= STORAGE_ERR_NONE && code < STORAGE_ERR_COUNT;
}

void storage_error_record(StorageErrorCode *primary, StorageErrorCode *secondary,
                          StorageErrorCode code)
{
    if (!primary || !secondary || storage_error_class(code) == STORAGE_ERROR_NONE)
        return;
    if (*primary == STORAGE_ERR_NONE) {
        *primary = code;
    } else if (*secondary == STORAGE_ERR_NONE && *primary != code) {
        *secondary = code;
    }
}
