#ifndef CCB_STORAGE_COMMIT_H
#define CCB_STORAGE_COMMIT_H

#include "ccb_types.h"
#include "file_list.h"

#include <stdbool.h>
#include <stddef.h>

#define STORAGE_COMMIT_REASON_SIZE 64u

typedef struct {
    uint32_t channel;
    uint32_t metadata_slot;
    FileEntry metadata;
    FileRecord record;
} StorageCommitItem;

typedef struct {
    void *ctx;
    int (*db_begin)(void *ctx);
    int (*record_exists)(void *ctx, const FileRecord *record);
    int (*metadata_write)(void *ctx, const StorageCommitItem *item);
    int (*metadata_rollback)(void *ctx, const StorageCommitItem *item);
    int (*record_insert)(void *ctx, const FileRecord *record);
    int (*record_count)(void *ctx, const char *task_id, int *count);
    int (*task_total_update)(void *ctx, const char *task_id, int count);
    int (*task_status_update)(void *ctx, const char *task_id, TaskStatus status);
    int (*db_commit)(void *ctx);
    int (*db_rollback)(void *ctx);
    int (*flash_sync)(void *ctx);
    int (*sync_mark_pending)(void *ctx, const char *task_id);
    int (*sync_mark_complete)(void *ctx, const char *task_id);
} StorageCommitOps;

typedef struct {
    bool attempted;
    bool success;
    bool sync_pending;
    char reason[STORAGE_COMMIT_REASON_SIZE];
} StorageCommitState;

void storage_commit_state_reset(StorageCommitState *state);
int storage_commit_run_once(StorageCommitState *state, const char *task_id,
                            const StorageCommitItem *items, size_t count,
                            const StorageCommitOps *ops);

#endif
