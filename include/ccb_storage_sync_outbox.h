#ifndef CCB_STORAGE_SYNC_OUTBOX_H
#define CCB_STORAGE_SYNC_OUTBOX_H

#include <sys/types.h>

typedef struct {
    void *ctx;
    int (*mkdir_path)(void *ctx, const char *path, mode_t mode);
    int (*open_file)(void *ctx, const char *path, int flags, mode_t mode);
    int (*open_directory)(void *ctx, const char *path);
    ssize_t (*write_file)(void *ctx, int fd, const void *buf, size_t count);
    int (*fsync_fd)(void *ctx, int fd);
    int (*close_fd)(void *ctx, int fd);
    int (*rename_path)(void *ctx, const char *old_path, const char *new_path);
    int (*unlink_path)(void *ctx, const char *path);
} StorageSyncOutboxOps;

/* Fsync the directory containing marker entries.  A failure is always
 * reported to the caller because rename/unlink durability depends on it. */
int fsync_directory_path(const char *path);

int storage_sync_outbox_mark_pending(const char *outbox_dir, const char *task_id);
int storage_sync_outbox_mark_complete(const char *outbox_dir, const char *task_id);

/* Host-test seam; production callers pass no custom operations. */
int storage_sync_outbox_mark_pending_with_ops(const char *outbox_dir, const char *task_id,
                                              const StorageSyncOutboxOps *ops);
int storage_sync_outbox_mark_complete_with_ops(const char *outbox_dir, const char *task_id,
                                               const StorageSyncOutboxOps *ops);

#endif
