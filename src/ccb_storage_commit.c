#include "ccb_storage_commit.h"

#include <stdio.h>
#include <string.h>

static void set_reason(StorageCommitState *state, const char *reason)
{
    snprintf(state->reason, sizeof(state->reason), "%s", reason ? reason : "commit_failed");
}

void storage_commit_state_reset(StorageCommitState *state)
{
    if (!state) return;
    memset(state, 0, sizeof(*state));
    set_reason(state, "not_attempted");
}

static int rollback_failure(StorageCommitState *state, const char *task_id,
                            const StorageCommitItem *items, size_t metadata_count,
                            const StorageCommitOps *ops, bool transaction_open,
                            const char *reason)
{
    bool metadata_rollback_failed = false;

    set_reason(state, reason);
    if (transaction_open && ops->db_rollback && ops->db_rollback(ops->ctx) != 0) {
        set_reason(state, "db_rollback_failed");
    }
    while (metadata_count > 0u) {
        --metadata_count;
        if (!ops->metadata_rollback ||
            ops->metadata_rollback(ops->ctx, &items[metadata_count]) != 0) {
            metadata_rollback_failed = true;
        }
    }
    if (metadata_rollback_failed) set_reason(state, "metadata_rollback_failed");
    if (!ops->task_status_update ||
        ops->task_status_update(ops->ctx, task_id, TASK_FAILED) != 0) {
        if (!metadata_rollback_failed) set_reason(state, "task_failed_update_failed");
    }
    state->success = false;
    return -1;
}

int storage_commit_run_once(StorageCommitState *state, const char *task_id,
                            const StorageCommitItem *items, size_t count,
                            const StorageCommitOps *ops)
{
    size_t i;
    size_t metadata_count = 0u;
    int total = 0;
    bool transaction_open = false;

    if (!state) return -1;
    if (state->attempted) return state->success ? 0 : -1;
    state->attempted = true;
    state->success = false;
    if (!task_id || task_id[0] == '\0' || !items || count == 0u ||
        count > NUM_CHANNELS || !ops || !ops->db_begin || !ops->record_exists ||
        !ops->metadata_write || !ops->record_insert || !ops->record_count ||
        !ops->task_total_update || !ops->task_status_update || !ops->db_commit ||
        !ops->db_rollback) {
        set_reason(state, "invalid_commit_input");
        return -1;
    }
    if (ops->db_begin(ops->ctx) != 0) {
        set_reason(state, "db_begin_failed");
        (void)ops->task_status_update(ops->ctx, task_id, TASK_FAILED);
        return -1;
    }
    transaction_open = true;

    for (i = 0u; i < count; ++i) {
        int exists = ops->record_exists(ops->ctx, &items[i].record);
        if (exists != 0) {
            return rollback_failure(state, task_id, items, metadata_count, ops,
                                    transaction_open,
                                    exists > 0 ? "duplicate_record" : "duplicate_check_failed");
        }
    }
    for (i = 0u; i < count; ++i) {
        ++metadata_count;
        if (ops->metadata_write(ops->ctx, &items[i]) != 0) {
            return rollback_failure(state, task_id, items, metadata_count, ops,
                                    transaction_open, "metadata_write_failed");
        }
    }
    for (i = 0u; i < count; ++i) {
        if (ops->record_insert(ops->ctx, &items[i].record) != 0) {
            return rollback_failure(state, task_id, items, metadata_count, ops,
                                    transaction_open, "db_insert_failed");
        }
    }
    if (ops->record_count(ops->ctx, task_id, &total) != 0) {
        return rollback_failure(state, task_id, items, metadata_count, ops,
                                transaction_open, "db_count_failed");
    }
    if (ops->task_total_update(ops->ctx, task_id, total) != 0) {
        return rollback_failure(state, task_id, items, metadata_count, ops,
                                transaction_open, "total_files_update_failed");
    }
    if (ops->task_status_update(ops->ctx, task_id, TASK_COMPLETED) != 0) {
        return rollback_failure(state, task_id, items, metadata_count, ops,
                                transaction_open, "task_completed_update_failed");
    }
    if (ops->db_commit(ops->ctx) != 0) {
        return rollback_failure(state, task_id, items, metadata_count, ops,
                                transaction_open, "db_commit_failed");
    }
    transaction_open = false;
    /* Flash is a post-commit side effect.  At this point the database
     * transaction is already durable, so a flash failure must not pretend
     * that an SQLite rollback restored the committed records.  Keep the
     * complete primary-store result, report the side-effect failure, and
     * let the caller retry synchronization without publishing success. */
    if (ops->flash_sync && ops->flash_sync(ops->ctx) != 0) {
        set_reason(state, "flash_sync_failed");
        if (!ops->task_status_update ||
            ops->task_status_update(ops->ctx, task_id, TASK_FAILED) != 0) {
            set_reason(state, "task_failed_update_failed");
        }
        state->success = false;
        return -1;
    }
    state->success = true;
    set_reason(state, "none");
    return 0;
}
