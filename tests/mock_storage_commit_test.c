#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "ccb_commands.h"
#include "ccb_storage_commit.h"

typedef struct {
    int begin_calls, duplicate_calls, metadata_calls, metadata_rollback_calls;
    int insert_calls, count_calls, total_calls, status_calls, commit_calls, rollback_calls;
    int flash_calls;
    int pending_calls, complete_calls;
    int fail_metadata_call, fail_insert_call;
    int fail_commit, fail_flash, fail_metadata_rollback, duplicate, fail_completed_status;
    int fail_pending;
    int inserted, base_count;
    uint64_t metadata_bytes, inserted_bytes;
    TaskStatus last_status;
} Mock;

static int begin(void *p) { ((Mock *)p)->begin_calls++; return 0; }
static int exists(void *p, const FileRecord *r)
{ Mock *m = p; (void)r; m->duplicate_calls++; return m->duplicate ? 1 : 0; }
static int metadata_write_mock(void *p, const StorageCommitItem *item)
{ Mock *m = p; m->metadata_calls++; m->metadata_bytes += item->metadata.file_size_bytes; return m->metadata_calls == m->fail_metadata_call ? -1 : 0; }
static int metadata_rollback_mock(void *p, const StorageCommitItem *item)
{ Mock *m = p; (void)item; m->metadata_rollback_calls++; return m->fail_metadata_rollback ? -1 : 0; }
static int insert(void *p, const FileRecord *r)
{ Mock *m = p; m->insert_calls++; if (m->insert_calls == m->fail_insert_call) return -1; m->inserted++; m->inserted_bytes += r->file_size; return 0; }
static int count(void *p, const char *task, int *out)
{ Mock *m = p; (void)task; m->count_calls++; *out = m->base_count + m->inserted; return 0; }
static int total(void *p, const char *task, int value)
{ Mock *m = p; (void)task; m->total_calls++; assert(value == m->base_count + m->inserted); return 0; }
static int status(void *p, const char *task, TaskStatus value)
{ Mock *m = p; (void)task; m->status_calls++; m->last_status = value; return value == TASK_COMPLETED && m->fail_completed_status ? -1 : 0; }
static int commit(void *p) { Mock *m = p; m->commit_calls++; return m->fail_commit ? -1 : 0; }
static int rollback(void *p) { Mock *m = p; m->rollback_calls++; m->inserted = 0; m->inserted_bytes = 0u; return 0; }
static int flash(void *p) { Mock *m = p; m->flash_calls++; return m->fail_flash ? -1 : 0; }
static int mark_pending(void *p, const char *task)
{ Mock *m = p; (void)task; m->pending_calls++; return m->fail_pending ? -1 : 0; }
static int mark_complete(void *p, const char *task) { Mock *m = p; (void)task; m->complete_calls++; return 0; }

static StorageCommitOps ops(Mock *m)
{
    StorageCommitOps value;
    memset(&value, 0, sizeof(value));
    value.ctx = m; value.db_begin = begin; value.record_exists = exists;
    value.metadata_write = metadata_write_mock; value.metadata_rollback = metadata_rollback_mock;
    value.record_insert = insert; value.record_count = count;
    value.task_total_update = total; value.task_status_update = status;
    value.db_commit = commit; value.db_rollback = rollback; value.flash_sync = flash;
    value.sync_mark_pending = mark_pending; value.sync_mark_complete = mark_complete;
    return value;
}

static void make_items(StorageCommitItem items[NUM_CHANNELS])
{
    unsigned i;
    memset(items, 0, sizeof(StorageCommitItem) * NUM_CHANNELS);
    for (i = 0; i < NUM_CHANNELS; ++i) {
        items[i].channel = i; items[i].metadata_slot = i;
        items[i].metadata.valid = 1u; items[i].record.file_index = (int)i + 1;
        items[i].metadata.file_size_bytes = 1024u * (i + 1u);
        items[i].record.file_size = items[i].metadata.file_size_bytes;
        snprintf(items[i].record.task_id, sizeof(items[i].record.task_id), "task1");
    }
}

static int run(Mock *m, StorageCommitState *state, StorageCommitItem *items)
{
    StorageCommitOps value = ops(m);
    storage_commit_state_reset(state);
    return storage_commit_run_once(state, "task1", items, NUM_CHANNELS, &value);
}

static void test_all_success_and_once(void)
{
    Mock m = {.base_count = 2}; StorageCommitState state; StorageCommitItem items[NUM_CHANNELS];
    StorageCommitOps value;
    make_items(items); value = ops(&m); storage_commit_state_reset(&state);
    assert(storage_commit_run_once(&state, "task1", items, NUM_CHANNELS, &value) == 0);
    assert(state.success && strcmp(state.reason, "none") == 0);
    assert(m.metadata_calls == 3 && m.insert_calls == 3 && m.commit_calls == 1);
    assert(m.flash_calls == 1 && m.complete_calls == 1);
    assert(m.last_status == TASK_COMPLETED);
    assert(storage_commit_run_once(&state, "task1", items, NUM_CHANNELS, &value) == 0);
    assert(m.commit_calls == 1);
}

static void test_commit_without_automatic_replica_sync(void)
{
    Mock m = {.base_count = 2};
    StorageCommitState state;
    StorageCommitItem items[NUM_CHANNELS];
    StorageCommitOps value;

    make_items(items);
    value = ops(&m);
    value.flash_sync = NULL;
    value.sync_mark_pending = NULL;
    value.sync_mark_complete = NULL;
    storage_commit_state_reset(&state);

    assert(storage_commit_run_once(&state, "task1", items, NUM_CHANNELS,
                                   &value) == 0);
    assert(state.success && !state.sync_pending &&
           strcmp(state.reason, "none") == 0);
    assert(m.commit_calls == 1 && m.flash_calls == 0);
    assert(m.pending_calls == 0 && m.complete_calls == 0);
}

static void test_failures(void)
{
    Mock m; StorageCommitState state; StorageCommitItem items[NUM_CHANNELS];
    make_items(items);

    memset(&m, 0, sizeof(m)); m.fail_metadata_call = 2;
    assert(run(&m, &state, items) != 0); assert(strcmp(state.reason, "metadata_write_failed") == 0);
    assert(m.metadata_rollback_calls == 2 && m.rollback_calls == 1 && m.last_status == TASK_FAILED);

    memset(&m, 0, sizeof(m)); m.fail_insert_call = 2;
    assert(run(&m, &state, items) != 0); assert(strcmp(state.reason, "db_insert_failed") == 0);
    assert(m.metadata_rollback_calls == 3 && m.inserted == 0);

    memset(&m, 0, sizeof(m)); m.fail_commit = 1;
    assert(run(&m, &state, items) != 0); assert(strcmp(state.reason, "db_commit_failed") == 0);
    assert(m.rollback_calls == 1 && m.metadata_rollback_calls == 3);

    memset(&m, 0, sizeof(m)); m.fail_insert_call = 1; m.fail_metadata_rollback = 1;
    assert(run(&m, &state, items) != 0); assert(strcmp(state.reason, "metadata_rollback_failed") == 0);

    memset(&m, 0, sizeof(m)); m.duplicate = 1;
    assert(run(&m, &state, items) != 0); assert(strcmp(state.reason, "duplicate_record") == 0);
    assert(m.metadata_calls == 0 && m.insert_calls == 0);

    memset(&m, 0, sizeof(m)); m.fail_completed_status = 1;
    assert(run(&m, &state, items) != 0); assert(strcmp(state.reason, "task_completed_update_failed") == 0);
    assert(m.last_status == TASK_FAILED && m.status_calls == 2);

    memset(&m, 0, sizeof(m)); m.fail_flash = 1;
    assert(run(&m, &state, items) == 0); assert(strcmp(state.reason, "sync_pending") == 0);
    assert(m.commit_calls == 1 && m.rollback_calls == 0 && m.metadata_rollback_calls == 0);
    assert(m.last_status == TASK_COMPLETED && state.success && state.sync_pending);

    memset(&m, 0, sizeof(m)); m.fail_flash = 1; m.fail_pending = 1;
    assert(run(&m, &state, items) != 0);
    assert(strcmp(state.reason, "sync_outbox_persist_failed") == 0);
    assert(!state.success && !state.sync_pending && m.last_status == TASK_COMPLETED);
}

static void test_modes(void)
{
    assert(storage_write_mode_commits_locally(STORAGE_WRITE_STANDALONE));
    assert(!storage_write_mode_commits_locally(STORAGE_WRITE_SUPERVISED));
}

static void test_failed_task_commits_persisted_files(void)
{
    Mock m;
    StorageCommitState state;
    StorageCommitItem items[NUM_CHANNELS];
    StorageCommitOps value;

    memset(&m, 0, sizeof(m));
    make_items(items);
    value = ops(&m);
    storage_commit_state_reset(&state);
    assert(storage_commit_run_once_status(&state, "task1", items,
                                          2u, TASK_FAILED,
                                          &value) == 0);
    assert(state.success);
    assert(m.metadata_calls == 2);
    assert(m.insert_calls == 2);
    assert(m.metadata_bytes == 3u * 1024u);
    assert(m.inserted_bytes == 3u * 1024u);
    assert(m.last_status == TASK_FAILED);
}

int main(void)
{
    test_all_success_and_once();
    test_commit_without_automatic_replica_sync();
    test_failures();
    test_modes();
    test_failed_task_commits_persisted_files();
    puts("mock_storage_commit_test: ok");
    return 0;
}
