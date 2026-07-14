#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "storage_error.h"

int main(void)
{
    StorageErrorCode primary = STORAGE_ERR_NONE;
    StorageErrorCode secondary = STORAGE_ERR_NONE;

    assert(storage_error_class(STORAGE_ERR_NONE) == STORAGE_ERROR_NONE);
    assert(storage_error_class(STORAGE_ERR_TAIL_UNALIGNED) == STORAGE_ERROR_DEFERRED);
    assert(storage_error_class(STORAGE_ERR_LATE_COMPLETION) == STORAGE_ERROR_DEFERRED);
    assert(storage_error_class(STORAGE_ERR_STOP_BOUNDARY_TIMEOUT) == STORAGE_ERROR_DEFERRED);
    assert(storage_error_class(STORAGE_ERR_STOP_HARVEST_TIMEOUT) == STORAGE_ERROR_DEFERRED);
    assert(storage_error_class(STORAGE_ERR_UNKNOWN_CID) == STORAGE_ERROR_FATAL);
    assert(storage_error_class(STORAGE_ERR_DUPLICATE_CID) == STORAGE_ERROR_FATAL);
    assert(storage_error_class(STORAGE_ERR_OWNERSHIP) == STORAGE_ERROR_FATAL);
    assert(strcmp(storage_error_string(STORAGE_ERR_UNKNOWN_CID), "unknown_cid") == 0);
    assert(storage_error_code_valid(STORAGE_ERR_NVME_TIMEOUT));
    assert(!storage_error_code_valid(STORAGE_ERR_COUNT));

    storage_error_record(&primary, &secondary, STORAGE_ERR_UNKNOWN_CID);
    storage_error_record(&primary, &secondary, STORAGE_ERR_DMA_STOP);
    storage_error_record(&primary, &secondary, STORAGE_ERR_COMMIT);
    assert(primary == STORAGE_ERR_UNKNOWN_CID);
    assert(secondary == STORAGE_ERR_DMA_STOP);
    puts("mock_storage_error_test: ok");
    return 0;
}
