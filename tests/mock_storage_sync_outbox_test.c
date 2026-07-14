#include <assert.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "ccb_storage_sync_outbox.h"

typedef struct {
    int fail_write;
    int fail_rename;
    int fail_directory_fsync;
    int directory_fsync_calls;
} MockIo;

static int io_mkdir(void *ctx, const char *path, mode_t mode)
{ (void)ctx; return mkdir(path, mode); }
static int io_open_file(void *ctx, const char *path, int flags, mode_t mode)
{ (void)ctx; return open(path, flags, mode); }
static int io_open_directory(void *ctx, const char *path)
{ (void)ctx; return open(path, O_RDONLY | O_DIRECTORY); }
static ssize_t io_write_file(void *ctx, int fd, const void *buf, size_t count)
{ MockIo *io = ctx; if (io->fail_write) { errno = EIO; return -1; } return write(fd, buf, count); }
static int io_fsync_fd(void *ctx, int fd)
{
    MockIo *io = ctx;
    struct stat st;

    assert(fstat(fd, &st) == 0);
    if (S_ISDIR(st.st_mode)) {
        ++io->directory_fsync_calls;
        if (io->fail_directory_fsync) { errno = EIO; return -1; }
    }
    return fsync(fd);
}
static int io_close_fd(void *ctx, int fd)
{ (void)ctx; return close(fd); }
static int io_rename_path(void *ctx, const char *old_path, const char *new_path)
{ MockIo *io = ctx; if (io->fail_rename) { errno = EIO; return -1; } return rename(old_path, new_path); }
static int io_unlink_path(void *ctx, const char *path)
{ (void)ctx; return unlink(path); }

static StorageSyncOutboxOps test_ops(MockIo *io)
{
    StorageSyncOutboxOps ops = {
        io, io_mkdir, io_open_file, io_open_directory, io_write_file,
        io_fsync_fd, io_close_fd, io_rename_path, io_unlink_path,
    };
    return ops;
}

static void marker_path(const char *outbox, const char *task_id, char *path, size_t size)
{
    assert(snprintf(path, size, "%s/%s.pending", outbox, task_id) < (int)size);
}

static void assert_no_temp_files(const char *outbox)
{
    DIR *dir = opendir(outbox);
    struct dirent *entry;

    assert(dir != NULL);
    while ((entry = readdir(dir)) != NULL) {
        assert(strstr(entry->d_name, ".tmp.") == NULL);
    }
    assert(closedir(dir) == 0);
}

static void cleanup_outbox(const char *outbox, const char *root)
{
    DIR *dir = opendir(outbox);
    struct dirent *entry;
    char path[512];

    if (dir) {
        while ((entry = readdir(dir)) != NULL) {
            if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) continue;
            assert(snprintf(path, sizeof(path), "%s/%s", outbox, entry->d_name) <
                   (int)sizeof(path));
            assert(unlink(path) == 0);
        }
        assert(closedir(dir) == 0);
    }
    assert(rmdir(outbox) == 0);
    assert(rmdir(root) == 0);
}

static void test_marker_lifecycle(void)
{
    char root[] = "/tmp/mock_storage_outbox_XXXXXX";
    char outbox[512];
    char marker[512];
    char content[64] = {0};
    FILE *file;
    MockIo io = {0};
    StorageSyncOutboxOps ops = test_ops(&io);

    assert(mkdtemp(root) != NULL);
    assert(snprintf(outbox, sizeof(outbox), "%s/outbox", root) < (int)sizeof(outbox));
    assert(storage_sync_outbox_mark_pending_with_ops(outbox, "R2607140001", &ops) == 0);
    marker_path(outbox, "R2607140001", marker, sizeof(marker));
    file = fopen(marker, "r");
    assert(file != NULL);
    assert(fgets(content, sizeof(content), file) != NULL);
    assert(strcmp(content, "R2607140001\n") == 0);
    assert(fclose(file) == 0);
    assert(io.directory_fsync_calls == 1);
    assert_no_temp_files(outbox);

    assert(storage_sync_outbox_mark_pending_with_ops(outbox, "R2607140001", &ops) == 0);
    assert(io.directory_fsync_calls == 2);
    assert_no_temp_files(outbox);
    assert(fsync_directory_path(outbox) == 0);

    assert(storage_sync_outbox_mark_complete_with_ops(outbox, "R2607140001", &ops) == 0);
    assert(access(marker, F_OK) != 0 && errno == ENOENT);
    assert(io.directory_fsync_calls == 3);
    assert(storage_sync_outbox_mark_complete_with_ops(outbox, "R2607140001", &ops) == 0);
    assert(io.directory_fsync_calls == 3);
    assert_no_temp_files(outbox);
    cleanup_outbox(outbox, root);
}

static void test_failures_leave_no_temp(void)
{
    char root[] = "/tmp/mock_storage_outbox_fail_XXXXXX";
    char outbox[512];
    char marker[512];
    MockIo io = {0};
    StorageSyncOutboxOps ops;

    assert(mkdtemp(root) != NULL);
    assert(snprintf(outbox, sizeof(outbox), "%s/outbox", root) < (int)sizeof(outbox));
    marker_path(outbox, "task1", marker, sizeof(marker));

    io.fail_write = 1; ops = test_ops(&io);
    assert(storage_sync_outbox_mark_pending_with_ops(outbox, "task1", &ops) != 0);
    assert(access(marker, F_OK) != 0 && errno == ENOENT);
    assert_no_temp_files(outbox);

    memset(&io, 0, sizeof(io)); io.fail_rename = 1; ops = test_ops(&io);
    assert(storage_sync_outbox_mark_pending_with_ops(outbox, "task1", &ops) != 0);
    assert(access(marker, F_OK) != 0 && errno == ENOENT);
    assert_no_temp_files(outbox);

    memset(&io, 0, sizeof(io)); io.fail_directory_fsync = 1; ops = test_ops(&io);
    assert(storage_sync_outbox_mark_pending_with_ops(outbox, "task1", &ops) != 0);
    assert(access(marker, F_OK) == 0);
    assert(io.directory_fsync_calls == 1);
    assert_no_temp_files(outbox);

    io.fail_directory_fsync = 0;
    assert(storage_sync_outbox_mark_pending_with_ops(outbox, "task1", &ops) == 0);
    io.fail_directory_fsync = 1;
    assert(storage_sync_outbox_mark_complete_with_ops(outbox, "task1", &ops) != 0);
    assert(access(marker, F_OK) != 0 && errno == ENOENT);
    assert_no_temp_files(outbox);
    cleanup_outbox(outbox, root);
}

static void test_invalid_task_id(void)
{
    char root[] = "/tmp/mock_storage_outbox_invalid_XXXXXX";
    char outbox[512];

    assert(mkdtemp(root) != NULL);
    assert(snprintf(outbox, sizeof(outbox), "%s/outbox", root) < (int)sizeof(outbox));
    errno = 0;
    assert(storage_sync_outbox_mark_pending(outbox, "../bad") != 0);
    assert(errno == EINVAL);
    assert(access(outbox, F_OK) != 0 && errno == ENOENT);
    assert(rmdir(root) == 0);
}

int main(void)
{
    test_marker_lifecycle();
    test_failures_leave_no_temp();
    test_invalid_task_id();
    puts("mock_storage_sync_outbox_test: ok");
    return 0;
}
