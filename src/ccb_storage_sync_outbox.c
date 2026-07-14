#include "ccb_storage_sync_outbox.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

#define STORAGE_SYNC_OUTBOX_TEMP_ATTEMPTS 64u

static int default_mkdir(void *ctx, const char *path, mode_t mode)
{
    (void)ctx;
    return mkdir(path, mode);
}

static int default_open_file(void *ctx, const char *path, int flags, mode_t mode)
{
    (void)ctx;
    return open(path, flags, mode);
}

static int default_open_directory(void *ctx, const char *path)
{
    (void)ctx;
    return open(path, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
}

static ssize_t default_write_file(void *ctx, int fd, const void *buf, size_t count)
{
    (void)ctx;
    return write(fd, buf, count);
}

static int default_fsync_fd(void *ctx, int fd)
{
    (void)ctx;
    return fsync(fd);
}

static int default_close_fd(void *ctx, int fd)
{
    (void)ctx;
    return close(fd);
}

static int default_rename_path(void *ctx, const char *old_path, const char *new_path)
{
    (void)ctx;
    return rename(old_path, new_path);
}

static int default_unlink_path(void *ctx, const char *path)
{
    (void)ctx;
    return unlink(path);
}

static const StorageSyncOutboxOps g_default_ops = {
    NULL,
    default_mkdir,
    default_open_file,
    default_open_directory,
    default_write_file,
    default_fsync_fd,
    default_close_fd,
    default_rename_path,
    default_unlink_path,
};

static const StorageSyncOutboxOps *outbox_ops(const StorageSyncOutboxOps *ops)
{
    return ops ? ops : &g_default_ops;
}

static bool outbox_ops_valid(const StorageSyncOutboxOps *ops)
{
    return ops && ops->mkdir_path && ops->open_file && ops->open_directory &&
           ops->write_file && ops->fsync_fd && ops->close_fd && ops->rename_path &&
           ops->unlink_path;
}

static int outbox_task_id_valid(const char *task_id)
{
    size_t i;

    if (!task_id || task_id[0] == '\0') return 0;
    for (i = 0u; task_id[i] != '\0'; ++i) {
        if (!((task_id[i] >= 'A' && task_id[i] <= 'Z') ||
              (task_id[i] >= 'a' && task_id[i] <= 'z') ||
              (task_id[i] >= '0' && task_id[i] <= '9') ||
              task_id[i] == '_' || task_id[i] == '-')) return 0;
    }
    return 1;
}

static int outbox_marker_path(const char *outbox_dir, const char *task_id,
                              char *path, size_t path_size)
{
    int n;

    if (!outbox_dir || outbox_dir[0] == '\0' || !outbox_task_id_valid(task_id) || !path) {
        errno = EINVAL;
        return -1;
    }
    n = snprintf(path, path_size, "%s/%s.pending", outbox_dir, task_id);
    if (n < 0 || (size_t)n >= path_size) {
        errno = ENAMETOOLONG;
        return -1;
    }
    return 0;
}

static int outbox_write_all(const StorageSyncOutboxOps *ops, int fd,
                            const void *buf, size_t length)
{
    const unsigned char *bytes = buf;
    size_t used = 0u;

    while (used < length) {
        ssize_t n = ops->write_file(ops->ctx, fd, bytes + used, length - used);
        if (n > 0) {
            used += (size_t)n;
            continue;
        }
        if (n < 0 && errno == EINTR) continue;
        if (n == 0) errno = EIO;
        return -1;
    }
    return 0;
}

static int outbox_fsync_directory(const char *path, const StorageSyncOutboxOps *ops)
{
    int fd;
    int saved_errno;

    fd = ops->open_directory(ops->ctx, path);
    if (fd < 0) return -1;
    if (ops->fsync_fd(ops->ctx, fd) != 0) {
        saved_errno = errno;
        (void)ops->close_fd(ops->ctx, fd);
        errno = saved_errno;
        return -1;
    }
    if (ops->close_fd(ops->ctx, fd) != 0) return -1;
    return 0;
}

int fsync_directory_path(const char *path)
{
    return outbox_fsync_directory(path, &g_default_ops);
}

static int outbox_make_temp_path(const char *outbox_dir, const char *task_id,
                                 uint32_t attempt, char *path, size_t path_size)
{
    int n = snprintf(path, path_size, "%s/.%s.pending.tmp.%ld.%u",
                     outbox_dir, task_id, (long)getpid(), attempt);

    if (n < 0 || (size_t)n >= path_size) {
        errno = ENAMETOOLONG;
        return -1;
    }
    return 0;
}

int storage_sync_outbox_mark_pending_with_ops(const char *outbox_dir, const char *task_id,
                                              const StorageSyncOutboxOps *custom_ops)
{
    const StorageSyncOutboxOps *ops = outbox_ops(custom_ops);
    char final_path[PATH_MAX];
    char temp_path[PATH_MAX];
    uint32_t attempt;
    int fd = -1;
    int saved_errno;

    if (!outbox_ops_valid(ops) || outbox_marker_path(outbox_dir, task_id,
                                                       final_path, sizeof(final_path)) != 0) {
        return -1;
    }
    if (ops->mkdir_path(ops->ctx, outbox_dir, 0755) != 0 && errno != EEXIST) return -1;

    for (attempt = 0u; attempt < STORAGE_SYNC_OUTBOX_TEMP_ATTEMPTS; ++attempt) {
        if (outbox_make_temp_path(outbox_dir, task_id, attempt, temp_path,
                                  sizeof(temp_path)) != 0) return -1;
        fd = ops->open_file(ops->ctx, temp_path, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
        if (fd >= 0) break;
        if (errno != EEXIST) return -1;
    }
    if (fd < 0) {
        errno = EEXIST;
        return -1;
    }
    if (outbox_write_all(ops, fd, task_id, strlen(task_id)) != 0 ||
        outbox_write_all(ops, fd, "\n", 1u) != 0 || ops->fsync_fd(ops->ctx, fd) != 0) {
        saved_errno = errno;
        (void)ops->close_fd(ops->ctx, fd);
        (void)ops->unlink_path(ops->ctx, temp_path);
        errno = saved_errno;
        return -1;
    }
    if (ops->close_fd(ops->ctx, fd) != 0) {
        saved_errno = errno;
        (void)ops->unlink_path(ops->ctx, temp_path);
        errno = saved_errno;
        return -1;
    }
    fd = -1;
    /* Both paths are formed from outbox_dir, so this rename never crosses
     * directories and preserves atomic replacement of a repeated marker. */
    if (ops->rename_path(ops->ctx, temp_path, final_path) != 0) {
        saved_errno = errno;
        (void)ops->unlink_path(ops->ctx, temp_path);
        errno = saved_errno;
        return -1;
    }
    if (outbox_fsync_directory(outbox_dir, ops) != 0) return -1;
    return 0;
}

int storage_sync_outbox_mark_pending(const char *outbox_dir, const char *task_id)
{
    return storage_sync_outbox_mark_pending_with_ops(outbox_dir, task_id, NULL);
}

int storage_sync_outbox_mark_complete_with_ops(const char *outbox_dir, const char *task_id,
                                               const StorageSyncOutboxOps *custom_ops)
{
    const StorageSyncOutboxOps *ops = outbox_ops(custom_ops);
    char final_path[PATH_MAX];

    if (!outbox_ops_valid(ops) || outbox_marker_path(outbox_dir, task_id,
                                                       final_path, sizeof(final_path)) != 0) {
        return -1;
    }
    if (ops->unlink_path(ops->ctx, final_path) != 0) {
        if (errno == ENOENT) return 0;
        return -1;
    }
    return outbox_fsync_directory(outbox_dir, ops);
}

int storage_sync_outbox_mark_complete(const char *outbox_dir, const char *task_id)
{
    return storage_sync_outbox_mark_complete_with_ops(outbox_dir, task_id, NULL);
}
