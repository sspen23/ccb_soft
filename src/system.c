#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <termios.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <poll.h>
#include <dirent.h>
#ifdef __linux__
#include <sys/mman.h>
#endif

#include "serial_proto.h"
#include "logger.h"
#include "log_config.h"
#include "file_list.h"
#include "db_config.h"
#include "ccb_cli.h"
#include "ccb_commands.h"
#include "ccb_config.h"
#include "ccb_hw.h"
#include "ccb_metadata.h"
#include "ccb_storage_ipc.h"
#include "ccb_storage_supervisor.h"
#include "ccb_storage_task.h"
#include "ccb_storage_perf.h"
#include "ccb_storage_commit.h"
#include "ccb_storage_log.h"
#include "ccb_tcp_transfer.h"
#include "debug_uart.h"

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

#ifndef DB_STORAGE_DIR
#define DB_STORAGE_DIR "."
#endif

#ifndef LOG_DB_PATH
#define LOG_DB_PATH DB_STORAGE_DIR "/logs.db"
#endif

#ifndef FILELIST_DB_PATH
#define FILELIST_DB_PATH DB_STORAGE_DIR "/filelist.db"
#endif

#ifndef FLASH_FILELIST_DB_PATH
#define FLASH_FILELIST_DB_PATH "/mnt/spi1/filelist.db"
#endif

#ifndef UART_DEV_PATH
#define UART_DEV_PATH "/dev/ttyUL1"
#endif

#ifndef STORAGE_META_DIR
#define STORAGE_META_DIR "/run/ccb_nvme_process_test"
#endif

#define STORAGE_SYNC_OUTBOX_DIR_DEFAULT DB_STORAGE_DIR "/storage_sync_pending"

/* ================== Configuration ================== */

#define BAUD B115200
#define TASK_TIMEOUT 300
#define STORAGE_WORKER_ARG "--storage-worker"
#define STORAGE_WRITE_ARG "storage-write"
#define DDR_PATTERN_STORE_ARG "ddr-pattern-store"
#define SSD_LBA_WRAP_TEST_ARG "ssd-lba-wrap-test"
#define SSD_CONTINUOUS_PATTERN_TEST_ARG "ssd-continuous-pattern-test"
#define DMA_RX_BENCHMARK_ARG "dma-rx-benchmark"
#define NETWORK_WORKER_ARG "--network-worker"
#define NETWORK_SEND_ARG "network-send"
#define STORAGE_TASK_COUNT NUM_CHANNELS
#define NETWORK_DDR_OFFSET_BYTES (4u * 1024u)

#ifndef FILE_LIST_READ
#define FILE_LIST_READ 0x11
#endif

#ifndef FILE_LIST_SYNC_FLASH
#define FILE_LIST_SYNC_FLASH 0x22
#endif

#ifndef FILE_LIST_CLEAR
#define FILE_LIST_CLEAR 0xFF
#endif

/* ================== Task structures ================== */

typedef struct {
    bool valid;
    char task_id[12];
    time_t overpass_time;
    uint8_t task_file_mode;
    uint8_t usb_transfer_enable;
    uint8_t calibration_type;
    uint8_t envelope_clock_raw;
    uint16_t envelope_duration;
} LastTaskContext;

Task storage_tasks[STORAGE_TASK_COUNT] = {
    {.state = IDLE, .name = "storage0", .output_fd = -1, .control_fd = -1, .event_fd = -1, .timeout_seconds = 0},
    {.state = IDLE, .name = "storage1", .output_fd = -1, .control_fd = -1, .event_fd = -1, .timeout_seconds = 0},
    {.state = IDLE, .name = "storage2", .output_fd = -1, .control_fd = -1, .event_fd = -1, .timeout_seconds = 0},
};
Task transfer_task = {
    .state = IDLE, .name = "transfer", .output_fd = -1, .control_fd = -1,
    .event_fd = -1, .timeout_seconds = TASK_TIMEOUT
};
LastTaskContext g_last_task = {0};
static StorageTaskSupervisor g_storage_supervisor;
static StorageCommitState g_storage_commit_state;

typedef struct {
    bool active;
    bool forced_reap;
    char task_id[12];
    uint8_t acq_type;
    uint8_t failure_type;
    uint64_t deadline_us;
    int requested_workers;
} StoragePendingStop;

static StoragePendingStop g_pending_storage_stop;

typedef enum {
    STORAGE_START_PREP_READY = 1,
    STORAGE_START_ARM_WAIT,
    STORAGE_START_RUN_WAIT,
    STORAGE_START_FAILED
} StorageStartPhase;

typedef struct {
    bool active;
    bool prepare;
    bool failure_ack_sent;
    StorageStartPhase phase;
    char task_id[12];
    uint8_t acq_type;
    uint8_t failure_type;
    int target_count;
    uint64_t deadline_us;
} StoragePendingStart;

static StoragePendingStart g_pending_storage_start;

#define STORAGE_START_PENDING 0xFEu

static void poll_task_output(Task *task);
static void drain_storage_events(Task *task, bool report_eof);
static void poll_storage_events(Task *task);
static void finalize_storage_task(Task *task, int exit_code);
static void storage_try_send_pending_stops(void);
static void storage_finish_worker_exit(Task *task, int exit_code, int signal_number);
static void storage_handle_worker_exit(Task *task, int status);
static int storage_commit_supervised_results(const char *task_id);
static void storage_service_pending_stop(void);
static void storage_service_pending_start(void);
static void system_emit_line(StorageLogSeverity severity, const char *fmt, ...);

static void storage_supervisor_emit_aggregate(void)
{
    uint64_t bytes[NUM_CHANNELS] = {0u};
    uint32_t i;
    const char *task_id = "";
    const char *secondary_reason = "none";
    StorageTaskTerminal status = storage_supervisor_result_status(&g_storage_supervisor);
    if (g_storage_supervisor.aggregate_emitted || status == STORAGE_TASK_ACTIVE) return;
    for (i = 0u; i < NUM_CHANNELS; ++i) {
        bytes[i] = g_storage_supervisor.final_result[i].file_bytes;
        if ((g_storage_supervisor.target_channel_mask & (1u << i)) != 0u &&
            task_id[0] == '\0' && storage_tasks[i].task_id[0] != '\0') {
            task_id = storage_tasks[i].task_id;
        }
    }
    if (status == STORAGE_TASK_SUCCESS && storage_commit_supervised_results(task_id) != 0) {
        storage_supervisor_protocol_fail(&g_storage_supervisor, NUM_CHANNELS,
                                         g_storage_commit_state.reason);
        status = STORAGE_TASK_FAILED;
    } else if (status == STORAGE_TASK_FAILED && task_id[0] != '\0') {
        (void)task_update_status(task_id, TASK_FAILED);
    }
    if (g_storage_supervisor.secondary_reason[0] != '\0') {
        secondary_reason = g_storage_supervisor.secondary_reason;
    } else {
        for (i = 0u; i < NUM_CHANNELS; ++i) {
            if ((g_storage_supervisor.target_channel_mask & (1u << i)) != 0u &&
                g_storage_supervisor.final_result[i].secondary_reason[0] != '\0') {
                secondary_reason = g_storage_supervisor.final_result[i].secondary_reason;
                break;
            }
        }
    }
    system_emit_line(STORAGE_LOG_ALWAYS_CRITICAL,
                     "storage_capture_complete task=%s status=%s ch0_bytes=%" PRIu64
                     " ch1_bytes=%" PRIu64 " ch2_bytes=%" PRIu64 " integrity_ok=%u"
                     " primary_reason=%s secondary_reason=%s reason=%s",
                     task_id,
                     status == STORAGE_TASK_SUCCESS ? "success" : "failed", bytes[0], bytes[1], bytes[2],
                     status == STORAGE_TASK_SUCCESS ? 1u : 0u,
                     status == STORAGE_TASK_SUCCESS && g_storage_commit_state.sync_pending ? "sync_pending" :
                         (status == STORAGE_TASK_SUCCESS ? "none" :
                         (g_storage_commit_state.attempted && !g_storage_commit_state.success
                              ? g_storage_commit_state.reason : g_storage_supervisor.fatal_reason)),
                     secondary_reason,
                     status == STORAGE_TASK_SUCCESS && g_storage_commit_state.sync_pending ? "sync_pending" :
                         (status == STORAGE_TASK_SUCCESS ? "none" :
                         (g_storage_commit_state.attempted && !g_storage_commit_state.success
                              ? g_storage_commit_state.reason : g_storage_supervisor.fatal_reason)));
    system_emit_line(STORAGE_LOG_SUMMARY,
                     "storage_supervisor_aggregate task=%s status=%s target_mask=0x%02X"
                     " primary_reason=%s secondary_reason=%s",
                     task_id, status == STORAGE_TASK_SUCCESS ? "success" : "failed",
                     g_storage_supervisor.target_channel_mask,
                     status == STORAGE_TASK_SUCCESS ? "none" : g_storage_supervisor.fatal_reason,
                     secondary_reason);
    g_storage_supervisor.aggregate_emitted = true;
}

static int storage_send_control(Task *task, StorageControlType type, uint64_t timestamp_us);

int serial_fd = -1;
static const char *g_program_path = NULL;

static int configure_tcp_for_channel(const ChannelConfig *cfg, TcpTransferConfig *tcp_cfg);

static const char *get_serial_device_path(void)
{
    const char *env_path = getenv("UART_DEV_PATH");
    if (env_path && env_path[0] != '\0') {
        return env_path;
    }
    return UART_DEV_PATH;
}

static const char *get_storage_meta_dir(void)
{
    const char *env_path = getenv("CCB_PROCESS_META_DIR");
    if (env_path && env_path[0] != '\0') {
        return env_path;
    }
    return STORAGE_META_DIR;
}

static const char *get_current_working_dir(char *buf, size_t len)
{
    if (!buf || len == 0u) {
        return "";
    }
    if (getcwd(buf, len)) {
        return buf;
    }
    snprintf(buf, len, "unknown(errno=%d)", errno);
    return buf;
}

static const char *get_flash_filelist_db_path(void)
{
    const char *env_path = getenv("SRC_REAL_FLASH_FILELIST_DB_PATH");
    if (env_path && env_path[0] != '\0') {
        return env_path;
    }
    env_path = getenv("CCB_FLASH_FILELIST_DB_PATH");
    if (env_path && env_path[0] != '\0') {
        return env_path;
    }
    return FLASH_FILELIST_DB_PATH;
}

static int copy_file_path(const char *src_path, const char *dst_path);
static int sync_metadata_files_to_flash(const char *flash_path);

static int flash_parent_dir(const char *flash_path, char *out, size_t out_size)
{
    char *slash;

    if (!flash_path || !out || out_size == 0u ||
        snprintf(out, out_size, "%s", flash_path) >= (int)out_size) {
        return -1;
    }
    slash = strrchr(out, '/');
    if (!slash || slash == out) {
        return -1;
    }
    *slash = '\0';
    return 0;
}

static int flash_storage_is_mounted(const char *flash_path)
{
    FILE *fp;
    char parent[PATH_MAX];
    char device[PATH_MAX];
    char mount_point[PATH_MAX];
    char fs_type[64];
    char options[256];
    int dump;
    int pass;

    if (flash_parent_dir(flash_path, parent, sizeof(parent)) != 0) {
        return 0;
    }
    fp = fopen("/proc/mounts", "r");
    if (!fp) {
        return 0;
    }
    while (fscanf(fp, "%4095s %4095s %63s %255s %d %d\n",
                  device, mount_point, fs_type, options, &dump, &pass) == 6) {
        if (strcmp(parent, mount_point) == 0) {
            fclose(fp);
            return 1;
        }
    }
    fclose(fp);
    return 0;
}

static int build_metadata_path(char *out,
                               size_t out_size,
                               const char *dir,
                               int channel_id,
                               const char *suffix)
{
    int n = snprintf(out, out_size, "%s/meta_ch%d.bin%s", dir, channel_id, suffix ? suffix : "");
    return (n > 0 && (size_t)n < out_size) ? 0 : -1;
}

static Task *storage_task_for_channel(int channel_id)
{
    size_t i;

    for (i = 0; i < STORAGE_TASK_COUNT; ++i) {
        if (kChannels[i].id == channel_id) {
            return &storage_tasks[i];
        }
    }
    return NULL;
}

static bool storage_any_state(TaskState state)
{
    size_t i;

    for (i = 0; i < STORAGE_TASK_COUNT; ++i) {
        if (storage_tasks[i].state == state) {
            return true;
        }
    }
    return false;
}

static bool storage_any_running(void)
{
    return storage_any_state(RUNNING);
}

static bool storage_any_live_worker(void)
{
    size_t i;
    for (i = 0u; i < STORAGE_TASK_COUNT; ++i)
        if (storage_tasks[i].pid > 0) return true;
    return false;
}

static bool storage_any_error(void)
{
    return storage_any_state(ERROR);
}

static int storage_state_summary(void)
{
    if (storage_any_running()) {
        return RUNNING;
    }
    if (storage_any_error()) {
        return ERROR;
    }
    return IDLE;
}

static void stop_all_storage_tasks(void)
{
    size_t i;

    for (i = 0; i < STORAGE_TASK_COUNT; ++i) {
        if (storage_tasks[i].pid > 0) {
            int status = 0;
            pid_t ret;

            (void)kill(storage_tasks[i].pid, SIGKILL);
            ret = waitpid(storage_tasks[i].pid, &status, WNOHANG);
            if (ret > 0) {
                storage_handle_worker_exit(&storage_tasks[i], status);
            } else if (ret < 0 && errno != EINTR && errno != ECHILD) {
                LOG_ERROR("STORAGE", "bounded SIGKILL reap failed pid=%d errno=%d",
                          storage_tasks[i].pid, errno);
            }
        }
    }
    storage_try_send_pending_stops();
    storage_supervisor_emit_aggregate();
}

static int request_storage_stop_all(void)
{
    size_t i;
    int requested = 0;
    uint32_t stop_mask = 0u;

    for (i = 0; i < STORAGE_TASK_COUNT; ++i) {
        if (storage_tasks[i].pid > 0) {
            stop_mask |= 1u << i;
            ++requested;
        }
    }
    storage_supervisor_request_stop(&g_storage_supervisor, stop_mask);
    storage_try_send_pending_stops();
    return requested;
}

/* ================== Helpers ================== */

static uint8_t sync_filelist_db_to_flash(void)
{
    const char *flash_path = get_flash_filelist_db_path();
    char tmp_path[PATH_MAX];

    if (storage_any_running() || transfer_task.state == RUNNING) {
        LOG_WARN("FILE_LIST", "Reject flash sync while worker is running");
        return ACK_RETRYING;
    }
    if (!flash_storage_is_mounted(flash_path)) {
        LOG_ERROR("FILE_LIST", "Flash storage is not mounted for path: %s", flash_path);
        dbg_printf("[DBG][FLASH] sync rejected flash mount unavailable path=%s\n", flash_path);
        return ACK_FAILED;
    }
    if (snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", flash_path) >= (int)sizeof(tmp_path)) {
        LOG_ERROR("FILE_LIST", "Flash DB path is too long: %s", flash_path);
        return ACK_FAILED;
    }

    (void)unlink(tmp_path);
    if (file_list_backup_to_path(tmp_path) != 0) {
        LOG_ERROR("FILE_LIST", "Flash DB backup failed: %s", tmp_path);
        (void)unlink(tmp_path);
        return ACK_FAILED;
    }
    if (sync_metadata_files_to_flash(flash_path) != 0) {
        LOG_ERROR("FILE_LIST", "Flash metadata sync failed: %s", flash_path);
        (void)unlink(tmp_path);
        return ACK_FAILED;
    }
    if (rename(tmp_path, flash_path) != 0) {
        LOG_ERROR("FILE_LIST", "Flash DB rename failed: %s -> %s errno=%d",
                  tmp_path, flash_path, errno);
        dbg_printf("[DBG][FLASH] sync rename failed tmp=%s dst=%s errno=%d\n",
                   tmp_path, flash_path, errno);
        (void)unlink(tmp_path);
        return ACK_FAILED;
    }
    sync();

    LOG_INFO("FILE_LIST", "Flash DB sync complete: %s", flash_path);
    dbg_printf("[DBG][FLASH] filelist DB synced runtime=%s flash=%s\n",
               FILELIST_DB_PATH, flash_path);
    return ACK_SUCCESS;
}

static const char *storage_sync_outbox_dir(void)
{
    const char *value = getenv("SRC_REAL_STORAGE_SYNC_OUTBOX_DIR");
    return value && value[0] != '\0' ? value : STORAGE_SYNC_OUTBOX_DIR_DEFAULT;
}

static int storage_sync_outbox_path(const char *task_id, char *path, size_t size)
{
    size_t i;
    if (!task_id || task_id[0] == '\0' || !path) return -1;
    for (i = 0u; task_id[i] != '\0'; ++i) {
        if (!((task_id[i] >= 'A' && task_id[i] <= 'Z') ||
              (task_id[i] >= 'a' && task_id[i] <= 'z') ||
              (task_id[i] >= '0' && task_id[i] <= '9') ||
              task_id[i] == '_' || task_id[i] == '-')) return -1;
    }
    return snprintf(path, size, "%s/%s.pending", storage_sync_outbox_dir(), task_id) <
           (int)size ? 0 : -1;
}

static int storage_sync_outbox_mark_pending(const char *task_id)
{
    char path[PATH_MAX];
    int fd;
    size_t length;
    size_t used = 0u;
    if (mkdir(storage_sync_outbox_dir(), 0755) != 0 && errno != EEXIST) return -1;
    if (storage_sync_outbox_path(task_id, path, sizeof(path)) != 0) return -1;
    fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return -1;
    length = strlen(task_id);
    while (used < length) {
        ssize_t n = write(fd, task_id + used, length - used);
        if (n > 0) { used += (size_t)n; continue; }
        if (n < 0 && errno == EINTR) continue;
        break;
    }
    if (used != length || write(fd, "\n", 1u) != 1 || fsync(fd) != 0) {
        int saved_errno = errno;
        (void)close(fd);
        errno = saved_errno;
        return -1;
    }
    if (close(fd) != 0) return -1;
    return 0;
}

static int storage_sync_outbox_mark_complete(const char *task_id)
{
    char path[PATH_MAX];
    if (storage_sync_outbox_path(task_id, path, sizeof(path)) != 0) return -1;
    return unlink(path) == 0 || errno == ENOENT ? 0 : -1;
}

static void storage_sync_outbox_retry(void)
{
    DIR *dir = opendir(storage_sync_outbox_dir());
    struct dirent *entry;
    bool pending = false;
    if (!dir) return;
    while ((entry = readdir(dir)) != NULL) {
        size_t n = strlen(entry->d_name);
        if (n > strlen(".pending") &&
            strcmp(entry->d_name + n - strlen(".pending"), ".pending") == 0) {
            pending = true;
            break;
        }
    }
    (void)closedir(dir);
    if (!pending) return;
    if (sync_filelist_db_to_flash() != ACK_SUCCESS) return;
    dir = opendir(storage_sync_outbox_dir());
    if (!dir) return;
    while ((entry = readdir(dir)) != NULL) {
        size_t n = strlen(entry->d_name);
        if (n > strlen(".pending") &&
            strcmp(entry->d_name + n - strlen(".pending"), ".pending") == 0) {
            char path[PATH_MAX];
            if (snprintf(path, sizeof(path), "%s/%s", storage_sync_outbox_dir(),
                         entry->d_name) < (int)sizeof(path)) (void)unlink(path);
        }
    }
    (void)closedir(dir);
}

static int copy_file_path(const char *src_path, const char *dst_path)
{
    int src_fd = -1;
    int dst_fd = -1;
    int rc = -1;
    char buf[8192];

    src_fd = open(src_path, O_RDONLY);
    if (src_fd < 0) {
        return -1;
    }
    dst_fd = open(dst_path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (dst_fd < 0) {
        goto out;
    }
    while (1) {
        ssize_t n = read(src_fd, buf, sizeof(buf));
        ssize_t done = 0;
        if (n == 0) {
            rc = 0;
            break;
        }
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }
        while (done < n) {
            ssize_t w = write(dst_fd, buf + done, (size_t)(n - done));
            if (w < 0) {
                if (errno == EINTR) {
                    continue;
                }
                goto out;
            }
            done += w;
        }
    }
    if (rc == 0 && fsync(dst_fd) != 0) {
        rc = -1;
    }

out:
    if (dst_fd >= 0) {
        close(dst_fd);
    }
    if (src_fd >= 0) {
        close(src_fd);
    }
    return rc;
}

static int sync_metadata_files_to_flash(const char *flash_path)
{
    char flash_dir[PATH_MAX];
    size_t i;

    if (flash_parent_dir(flash_path, flash_dir, sizeof(flash_dir)) != 0) {
        return -1;
    }
    for (i = 0u; i < NUM_CHANNELS; ++i) {
        char src_path[PATH_MAX];
        char dst_path[PATH_MAX];
        char tmp_path[PATH_MAX];
        struct stat st;

        if (build_metadata_path(src_path, sizeof(src_path), get_storage_meta_dir(), kChannels[i].id, "") != 0 ||
            build_metadata_path(dst_path, sizeof(dst_path), flash_dir, kChannels[i].id, "") != 0 ||
            build_metadata_path(tmp_path, sizeof(tmp_path), flash_dir, kChannels[i].id, ".tmp") != 0) {
            return -1;
        }
        if (stat(src_path, &st) != 0) {
            if (errno == ENOENT) {
                (void)unlink(dst_path);
                continue;
            }
            return -1;
        }
        (void)unlink(tmp_path);
        if (copy_file_path(src_path, tmp_path) != 0 || rename(tmp_path, dst_path) != 0) {
            (void)unlink(tmp_path);
            return -1;
        }
        dbg_printf("[DBG][FLASH] metadata synced channel=%d runtime=%s flash=%s\n",
                   kChannels[i].id, src_path, dst_path);
    }
    return 0;
}

static int parse_overpass_time(const char *time_str, time_t *out_ts)
{
    struct tm tm_time;
    int year;
    int mon;
    int mday;
    int hour;
    int min;
    int sec;
    time_t ts;

    if (!time_str || !out_ts) {
        return -1;
    }

    memset(&tm_time, 0, sizeof(tm_time));
    if (sscanf(time_str, "%4d%2d%2d%2d%2d%2d", &year, &mon, &mday, &hour, &min, &sec) != 6) {
        return -1;
    }

    tm_time.tm_year = year - 1900;
    tm_time.tm_mon = mon - 1;
    tm_time.tm_mday = mday;
    tm_time.tm_hour = hour;
    tm_time.tm_min = min;
    tm_time.tm_sec = sec;

    ts = mktime(&tm_time);
    if (ts == (time_t)-1) {
        return -1;
    }
    *out_ts = ts;
    return 0;
}

static uint16_t proto_be16(const void *field)
{
    const uint8_t *p = (const uint8_t *)field;
    return (uint16_t)(((uint16_t)p[0] << 8) | (uint16_t)p[1]);
}

static int16_t proto_be16_signed(const void *field)
{
    return (int16_t)proto_be16(field);
}

static void build_task_description_json(char *json_buf, size_t buf_size, const CmdTaskInfo *cmd)
{
    uint16_t envelope_duration;

    if (!json_buf || buf_size == 0u || !cmd) {
        return;
    }
    envelope_duration = proto_be16(cmd->envelope_duration);

    snprintf(json_buf, buf_size,
             "{"
             "\"work_mode\":0x%02X,"
             "\"rcs_value\":0x%02X,"
             "\"delay_setting\":0x%02X,"
             "\"envelope_clock\":0x%02X,"
             "\"envelope_duration\":0x%04X,"
             "\"task_file_mode\":0x%02X,"
             "\"usb_transfer_enable\":0x%02X,"
             "\"calibration_type\":0x%02X"
             "}",
             cmd->work_mode,
             cmd->rcs_value,
             cmd->delay_setting,
             cmd->envelope_clock,
             envelope_duration,
             cmd->task_file_mode,
             cmd->usb_transfer_enable,
             cmd->calibration_type);
    json_buf[buf_size - 1u] = '\0';
}

static void build_task_payload_json(char *json_buf, size_t buf_size, const CmdTaskInfo *cmd)
{
    char task_id[12] = {0};
    char overpass[15] = {0};
    uint16_t azimuth_angle;
    int16_t elevation_angle;
    uint16_t envelope_duration;
    uint16_t period_setting;

    if (!json_buf || buf_size == 0u || !cmd) {
        return;
    }

    memcpy(task_id, cmd->task_id, 11u);
    memcpy(overpass, cmd->overpass_time, 14u);
    azimuth_angle = proto_be16(cmd->azimuth_angle);
    elevation_angle = proto_be16_signed(cmd->elevation_angle);
    envelope_duration = proto_be16(cmd->envelope_duration);
    period_setting = proto_be16(cmd->period_setting);

    snprintf(json_buf, buf_size,
             "{"
             "\"task_id\":\"%s\","
             "\"overpass_time\":\"%s\","
             "\"work_mode\":%u,"
             "\"rcs_value\":%u,"
             "\"delay_setting\":%u,"
             "\"envelope_clock\":%u,"
             "\"iq_clock\":%u,"
             "\"rx_bandwidth\":%u,"
             "\"azimuth_angle\":%u,"
             "\"elevation_angle\":%d,"
             "\"envelope_duration\":%u,"
             "\"iq_duration\":%u,"
             "\"lo_select\":%u,"
             "\"freq_select\":%u,"
             "\"bandwidth_setting\":%u,"
             "\"pulse_width\":%u,"
             "\"period_setting\":%u,"
             "\"task_file_mode\":%u,"
             "\"usb_transfer_enable\":%u,"
             "\"calibration_type\":%u"
             "}",
             task_id,
             overpass,
             (unsigned)cmd->work_mode,
             (unsigned)cmd->rcs_value,
             (unsigned)cmd->delay_setting,
             (unsigned)cmd->envelope_clock,
             (unsigned)cmd->iq_clock,
             (unsigned)cmd->rx_bandwidth,
             (unsigned)azimuth_angle,
             (int)elevation_angle,
             (unsigned)envelope_duration,
             (unsigned)cmd->iq_duration,
             (unsigned)cmd->lo_select,
             (unsigned)cmd->freq_select,
             (unsigned)cmd->bandwidth_setting,
             (unsigned)cmd->pulse_width,
             (unsigned)period_setting,
             (unsigned)cmd->task_file_mode,
             (unsigned)cmd->usb_transfer_enable,
             (unsigned)cmd->calibration_type);
    json_buf[buf_size - 1u] = '\0';
}

/* ================== Serial port ================== */

int set_serial(int fd)
{
    struct termios tty;

    if (tcgetattr(fd, &tty) != 0) return -1;

    cfsetospeed(&tty, BAUD);
    cfsetispeed(&tty, BAUD);

    tty.c_cflag |= (CLOCAL | CREAD);
    tty.c_cflag |= CS8;
    tty.c_cflag &= ~PARENB;
    tty.c_cflag &= ~CSTOPB;

    tty.c_lflag = 0;
    tty.c_oflag = 0;
    tty.c_iflag = 0;

    tty.c_cc[VMIN] = 0;
    tty.c_cc[VTIME] = 1;

    return tcsetattr(fd, TCSANOW, &tty);
}

int serial_send(uint8_t *buf, int len)
{
    int done = 0;

    while (done < len) {
        ssize_t n = write(serial_fd, buf + done, (size_t)(len - done));
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            dbg_printf("[DBG][UART] send failed errno=%d done=%d len=%d\n", errno, done, len);
            return -1;
        }
        if (n == 0) {
            dbg_printf("[DBG][UART] send returned zero done=%d len=%d\n", done, len);
            return -1;
        }
        done += (int)n;
    }
    return done;
}

/* ================== Task control ================== */

static int storage_send_control(Task *task, StorageControlType type, uint64_t timestamp_us)
{
    StorageControlMessage msg;

    /* The UART loop calls this from its periodic service slice.  Keep one
     * attempt short and let the caller's bounded retry/fallback policy make
     * progress; never spend the whole critical-event budget parked in poll(). */
    const uint64_t send_slice_us = 1000ull;

    if (!task || task->control_fd < 0) return -1;
    storage_ipc_make_control(&msg, type, timestamp_us);
    return storage_ipc_write_control_deadline(task->control_fd, &msg,
                                              storage_ipc_monotonic_us() + send_slice_us);
}

#define STORAGE_STOP_MAX_SEND_ATTEMPTS 3u

static void storage_fallback_stop(Task *task, uint32_t channel, int send_errno)
{
    int signal_rc;
    int signal_errno;

    if (!task) return;
    if (task->control_fd >= 0) {
        close(task->control_fd);
        task->control_fd = -1;
    }
    signal_rc = kill(task->pid, SIGTERM);
    signal_errno = errno;
    if (signal_rc == 0 || signal_errno == ESRCH) {
        LOG_WARN("STORAGE",
                 "STOP control failed; SIGTERM fallback requested: name=%s pid=%d ch=%u errno=%d",
                 task->name, task->pid, (unsigned)channel, send_errno);
    } else {
        LOG_ERROR("STORAGE",
                  "STOP control and SIGTERM fallback failed: name=%s pid=%d ch=%u errno=%d signal_errno=%d",
                  task->name, task->pid, (unsigned)channel, send_errno, signal_errno);
    }
    storage_supervisor_mark_stop_failed(&g_storage_supervisor, channel);
    task->stop_send_attempts = STORAGE_STOP_MAX_SEND_ATTEMPTS + 1u;
}

static void storage_try_send_pending_stops(void)
{
    uint32_t pending = storage_supervisor_peek_stop_mask(&g_storage_supervisor);
    size_t i;

    for (i = 0u; i < STORAGE_TASK_COUNT; ++i) {
        Task *task = &storage_tasks[i];
        int send_errno;

        if ((pending & (1u << i)) == 0u || task->pid <= 0 ||
            task->stop_send_attempts > STORAGE_STOP_MAX_SEND_ATTEMPTS) {
            continue;
        }
        if (task->control_fd < 0) {
            storage_fallback_stop(task, (uint32_t)i, EBADF);
            continue;
        }
        if (storage_send_control(task, STORAGE_CTRL_STOP,
                                 storage_ipc_monotonic_us()) == 0) {
            storage_supervisor_mark_stop_sent(&g_storage_supervisor, (uint32_t)i);
            LOG_INFO("STORAGE", "STOP sent: name=%s pid=%d task=%s",
                     task->name, task->pid, task->task_id);
            continue;
        }

        send_errno = errno;
        ++task->stop_send_attempts;
        storage_supervisor_mark_stop_failed(&g_storage_supervisor, (uint32_t)i);
        if (send_errno != EAGAIN && send_errno != EWOULDBLOCK && send_errno != EINTR) {
            storage_fallback_stop(task, (uint32_t)i, send_errno);
        } else if (task->stop_send_attempts >= STORAGE_STOP_MAX_SEND_ATTEMPTS) {
            storage_fallback_stop(task, (uint32_t)i, send_errno);
        }
    }
}

pid_t start_process(const char *prog, const char *arg)
{
    pid_t pid = fork();

    if (pid == 0) {
        execl(prog, prog, arg, NULL);
        perror("exec failed");
        exit(1);
    }

    return pid;
}

void start_task(Task *task, const char *prog, const char *arg)
{
    if (task->state == RUNNING) {
        LOG_WARN(task->name, "%s task is busy", task->name);
        printf("%s busy\n", task->name);
        return;
    }

    task->pid = start_process(prog, arg);
    task->start_time = time(NULL);
    task->state = RUNNING;

    LOG_INFO(task->name, "Task started: %s (PID=%d)", prog, task->pid);
    printf("%s start pid=%d\n", task->name, task->pid);
}

void stop_task(Task *task)
{
    if (task->state == RUNNING) {
        kill(task->pid, SIGKILL);
        task->state = IDLE;
        storage_task_close_fds(task);
        LOG_INFO(task->name, "Task stopped");
        printf("%s stopped\n", task->name);
    }
}

static void storage_finish_worker_exit(Task *task, int exit_code, int signal_number)
{
    bool is_storage;

    if (!task) return;
    is_storage = task->has_planned_file;

    /* The child is reaped, so a nonblocking drain now reaches the true pipe end. */
    poll_task_output(task);
    if (is_storage) {
        drain_storage_events(task, false);
        (void)storage_supervisor_handle_worker_exit(
            &g_storage_supervisor, (uint32_t)task->planned_file.channel_id, exit_code);
    }
    storage_task_close_fds(task);

    if (signal_number != 0) {
        LOG_ERROR(task->name, "Task terminated by signal %d", signal_number);
        dbg_printf("[DBG][TASK] %s terminated by signal=%d output=%s\n",
                   task->name, signal_number, task->output);
    }
    task->pid = -1;
    if (is_storage) {
        finalize_storage_task(task, exit_code);
    } else if (exit_code == 0) {
        task->state = IDLE;
        LOG_INFO(task->name, "Task finished successfully");
        printf("%s finished OK\n", task->name);
    } else {
        task->state = ERROR;
        LOG_ERROR(task->name, "Task failed with status %d", exit_code);
        printf("%s failed\n", task->name);
    }

    if (is_storage) {
        storage_try_send_pending_stops();
        storage_supervisor_emit_aggregate();
    }
}

static void storage_handle_worker_exit(Task *task, int status)
{
    int exit_code = WIFEXITED(status) ? WEXITSTATUS(status) : 1;
    int signal_number = WIFSIGNALED(status) ? WTERMSIG(status) : 0;

    storage_finish_worker_exit(task, exit_code, signal_number);
}

void check_task(Task *task)
{
    if (!task || task->pid <= 0) return;

    {
        int status;
        pid_t ret = waitpid(task->pid, &status, WNOHANG);

        if (ret == 0) {
            poll_task_output(task);
            if (task->has_planned_file) poll_storage_events(task);
            if (task->timeout_seconds > 0 &&
                time(NULL) - task->start_time > task->timeout_seconds) {
                LOG_ERROR(task->name, "Task timeout after %ld seconds", task->timeout_seconds);
                printf("%s timeout\n", task->name);
                (void)kill(task->pid, SIGKILL);
                ret = waitpid(task->pid, &status, WNOHANG);
                if (ret > 0) storage_handle_worker_exit(task, status);
                else if (ret < 0 && errno != EINTR) {
                    LOG_ERROR(task->name, "waitpid after timeout failed: pid=%d errno=%d",
                              task->pid, errno);
                } else {
                    /* Keep the pid until a later poll reaps it; never block
                     * the UART/control loop on an unbounded waitpid. */
                    task->state = ERROR;
                }
            }
            return;
        }

        if (ret > 0) {
            storage_handle_worker_exit(task, status);
        } else if (ret < 0 && errno != EINTR) {
            LOG_ERROR(task->name, "waitpid failed: pid=%d errno=%d", task->pid, errno);
            storage_finish_worker_exit(task, 1, 0);
        }
    }
}

/* ================== storage worker helpers ================== */


static int metadata_delete_file(int channel_id, const char *task_id, int file_index)
{
    const ChannelConfig *cfg = find_channel(channel_id);
    ChannelRuntime rt;
    FileEntry table[MAX_FILES_TOTAL];
    FileEntry entry;
    int slot = -1;

    if (!cfg || !task_id || file_index < 0 || file_index > UINT16_MAX) {
        return -1;
    }

    memset(&rt, 0, sizeof(rt));
    rt.cfg = cfg;
    if (metadata_read(&rt, table) != 0) {
        return -1;
    }
    if (metadata_find_by_task(table, task_id, (uint32_t)file_index, &slot, &entry) != 0) {
        return -1;
    }
    table[slot].valid = 0u;
    return metadata_write(&rt, table);
}

static int metadata_clear_all_supported_channels(void)
{
    size_t i;
    int rc = 0;

    for (i = 0; i < NUM_CHANNELS; ++i) {
        ChannelRuntime rt;
        FileEntry table[MAX_FILES_TOTAL];

        memset(&rt, 0, sizeof(rt));
        rt.cfg = &kChannels[i];
        memset(table, 0, sizeof(table));
        if (metadata_write(&rt, table) != 0) {
            rc = -1;
        }
    }
    return rc;
}

static int metadata_delete_task_all_supported_channels(const char *task_id)
{
    size_t i;
    int rc = 0;

    if (!task_id || task_id[0] == '\0') {
        return -1;
    }

    for (i = 0; i < NUM_CHANNELS; ++i) {
        ChannelRuntime rt;
        FileEntry table[MAX_FILES_TOTAL];
        uint32_t j;
        int changed = 0;

        memset(&rt, 0, sizeof(rt));
        rt.cfg = &kChannels[i];
        if (metadata_read(&rt, table) != 0) {
            rc = -1;
            continue;
        }

        for (j = 0; j < MAX_FILES_TOTAL; ++j) {
            char stored_task[12];

            if (table[j].valid != 1u) {
                continue;
            }
            memset(stored_task, 0, sizeof(stored_task));
            memcpy(stored_task, table[j].task_no, 11u);
            if (strcmp(stored_task, task_id) != 0) {
                continue;
            }
            table[j].valid = 0u;
            changed = 1;
        }

        if (changed && metadata_write(&rt, table) != 0) {
            rc = -1;
        }
        if (changed) {
            dbg_printf("[DBG][FILE_OP] metadata task cleanup ch=%d task=%s\n",
                       kChannels[i].id, task_id);
        }
    }

    return rc;
}

static int build_file_plan(const LastTaskContext *ctx, PlannedFile *files, int *out_count)
{
    int n = 0;

    if (!ctx || !files || !out_count) {
        return -1;
    }

    switch (ctx->task_file_mode) {
    case TASK_FILE_MODE_CALIB_ONLY:
        files[n++] = (PlannedFile){2, FILE_TYPE_CALIB, "CALIB", 0u, 0, 0u, 0u, ctx->calibration_type};
        break;
    case TASK_FILE_MODE_LOW_ONLY:
        files[n++] = (PlannedFile){2, FILE_TYPE_LOW, "LOW_SPEED", 0u, 0, 0u, 0u, 0u};
        break;
    case TASK_FILE_MODE_HIGH_ONLY:
        /* Product mode 0x33 would start ch0/ch1 without the ch2 envelope.
         * Keep the protocol value for compatibility but reject it explicitly
         * instead of silently changing the requested task. */
        return -1;
    case TASK_FILE_MODE_ALL:
        files[n++] = (PlannedFile){2, FILE_TYPE_LOW, "LOW_SPEED", 0u, 0, 0u, 0u, 0u};
        files[n++] = (PlannedFile){0, FILE_TYPE_I, "HIGH_I", 0u, 0, 0u, 0u, 0u};
        files[n++] = (PlannedFile){1, FILE_TYPE_Q, "HIGH_Q", 0u, 0, 0u, 0u, 0u};
        break;
    default:
        return -1;
    }

    *out_count = n;
    return 0;
}

static uint8_t acq_type_from_task_mode(uint8_t task_file_mode)
{
    switch (task_file_mode) {
    case TASK_FILE_MODE_CALIB_ONLY:
        return ACQ_TYPE_ENVELOPE;
    case TASK_FILE_MODE_LOW_ONLY:
        return ACQ_TYPE_ENVELOPE;
    case TASK_FILE_MODE_HIGH_ONLY:
        return (uint8_t)(ACQ_TYPE_HIGH_I | ACQ_TYPE_HIGH_Q);
    case TASK_FILE_MODE_ALL:
        return (uint8_t)(ACQ_TYPE_ENVELOPE | ACQ_TYPE_HIGH_I | ACQ_TYPE_HIGH_Q);
    default:
        return 0u;
    }
}

static uint8_t failure_type_from_acq_type(uint8_t acq_type)
{
    uint8_t failure_type = FAIL_TYPE_NONE;

    if (acq_type & (ACQ_TYPE_ENVELOPE | ACQ_TYPE_CALIB)) {
        failure_type |= FAIL_TYPE_LOW;
    }
    if (acq_type & ACQ_TYPE_HIGH_I) {
        failure_type |= FAIL_TYPE_HIGH_I;
    }
    if (acq_type & ACQ_TYPE_HIGH_Q) {
        failure_type |= FAIL_TYPE_HIGH_Q;
    }
    return failure_type;
}

static int env_flag_enabled(const char *name)
{
    const char *value = getenv(name);

    if (!value || value[0] == '\0') {
        return 0;
    }
    if (strcmp(value, "0") == 0 ||
        strcmp(value, "false") == 0 ||
        strcmp(value, "FALSE") == 0 ||
        strcmp(value, "off") == 0 ||
        strcmp(value, "OFF") == 0 ||
        strcmp(value, "no") == 0 ||
        strcmp(value, "NO") == 0) {
        return 0;
    }
    return 1;
}

static uint32_t env_u32_or_default(const char *name, uint32_t default_value)
{
    const char *value = getenv(name);
    char *end = NULL;
    unsigned long parsed;

    if (!value || value[0] == '\0') {
        return default_value;
    }
    errno = 0;
    parsed = strtoul(value, &end, 0);
    if (errno != 0 || end == value || *end != '\0' || parsed == 0u || parsed > UINT32_MAX) {
        return default_value;
    }
    return (uint32_t)parsed;
}

static int env_u32_allow_zero(const char *name, uint32_t max_value, uint32_t *out)
{
    const char *value = getenv(name);
    char *end = NULL;
    unsigned long parsed;

    if (!value || value[0] == '\0' || !out) {
        return 0;
    }
    errno = 0;
    parsed = strtoul(value, &end, 0);
    if (errno != 0 || end == value || *end != '\0' || parsed > max_value) {
        return 0;
    }
    *out = (uint32_t)parsed;
    return 1;
}

static uint64_t system_wall_time_us(void)
{
    struct timespec ts;
#ifdef CLOCK_MONOTONIC_RAW
    if (clock_gettime(CLOCK_MONOTONIC_RAW, &ts) != 0 &&
        clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
#else
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
#endif
        return 0u;
    }
    return (uint64_t)ts.tv_sec * 1000000ull + (uint64_t)ts.tv_nsec / 1000ull;
}

static uint32_t network_limit_mb_s(void)
{
    uint32_t limit_mb_s = 0u;

    (void)env_u32_allow_zero("SRC_REAL_NETWORK_LIMIT_MB_S", 10000u, &limit_mb_s);
    return limit_mb_s;
}

static long network_task_timeout_seconds(void)
{
    uint32_t timeout_seconds = 0u;

    (void)env_u32_allow_zero("SRC_REAL_NETWORK_TASK_TIMEOUT_SECONDS",
                             (uint32_t)LONG_MAX,
                             &timeout_seconds);
    return (long)timeout_seconds;
}

static void network_throttle_after_chunk(uint64_t chunk_start_us,
                                         uint64_t bytes,
                                         const char *task_no,
                                         uint32_t file_index,
                                         uint32_t chunk_index,
                                         uint32_t total_chunks)
{
    uint32_t limit_mb_s = network_limit_mb_s();
    uint64_t target_us;
    uint64_t now_us;
    uint64_t elapsed_us;
    uint64_t sleep_us;

    if (limit_mb_s == 0u || bytes == 0u || chunk_start_us == 0u) {
        return;
    }

    target_us = bytes / (uint64_t)limit_mb_s;
    now_us = system_wall_time_us();
    elapsed_us = now_us >= chunk_start_us ? now_us - chunk_start_us : 0u;
    if (elapsed_us >= target_us) {
        return;
    }

    sleep_us = target_us - elapsed_us;
    dbg_printf("[DBG][NET] throttle task=%s file_index=%u chunk_index=%u total_chunks=%u"
               " bytes=%" PRIu64 " limit_mb_s=%u elapsed_us=%" PRIu64
               " sleep_us=%" PRIu64 "\n",
               task_no ? task_no : "",
               (unsigned)file_index,
               (unsigned)chunk_index,
               (unsigned)total_chunks,
               bytes,
               (unsigned)limit_mb_s,
               elapsed_us,
               sleep_us);

    while (sleep_us > 0u && !nvme_stop_requested()) {
        uint64_t step_us = sleep_us > 10000u ? 10000u : sleep_us;
        usleep((useconds_t)step_us);
        sleep_us -= step_us;
    }
}

/* STOP is serviced from the main loop rather than from the UART frame
 * handler.  This keeps UART input and worker event draining alive while the
 * independent DMA/NVMe/writer deadlines run in the child processes. */
static void storage_service_pending_stop(void)
{
    StorageTaskTerminal terminal;
    uint8_t result;

    if (!g_pending_storage_stop.active) return;
    if (storage_any_live_worker()) {
        if (storage_ipc_parent_stop_should_force_reap(
                true, g_pending_storage_stop.forced_reap,
                storage_ipc_monotonic_us(), g_pending_storage_stop.deadline_us)) {
            LOG_ERROR("STORAGE", "pending STOP deadline expired task=%s; forcing reap",
                      g_pending_storage_stop.task_id);
            g_pending_storage_stop.forced_reap = true;
            stop_all_storage_tasks();
        }
        return;
    }

    terminal = storage_supervisor_result_status(&g_storage_supervisor);
    storage_supervisor_emit_aggregate();
    result = terminal == STORAGE_TASK_SUCCESS ? ACK_SUCCESS : ACK_FAILED;
    proto_send_acq_ack(result,
                       g_pending_storage_stop.acq_type,
                       result == ACK_SUCCESS ? FAIL_TYPE_NONE : g_pending_storage_stop.failure_type);
    system_emit_line(STORAGE_LOG_SUMMARY, "serial_cmd_ack cmd=0x21 action=stop task_no=%s ack_status=0x%02X"
                     " result_code=0x%02X worker_state=%d storage_state=%d reason=%s",
                     g_pending_storage_stop.task_id,
                     result,
                     result,
                     result == ACK_SUCCESS ? IDLE : ERROR,
                     storage_state_summary(),
                     result == ACK_SUCCESS ? "stop_complete" : "storage_worker_failed");
    memset(&g_pending_storage_stop, 0, sizeof(g_pending_storage_stop));
}

static int network_output_has_stop_request(const Task *task)
{
    return task &&
           (strstr(task->output, "tcp transfer stop requested") != NULL ||
            strstr(task->output, "network worker done rc=-2") != NULL ||
            strstr(task->output, "network_send_stopped") != NULL);
}

static uint8_t wait_network_worker_finished_after_stop(void)
{
    uint32_t waited_ms = 0u;
    uint32_t timeout_ms = env_u32_or_default("SRC_REAL_NETWORK_STOP_TIMEOUT_MS", 30000u);

    while (transfer_task.state == RUNNING) {
        check_task(&transfer_task);
        if (transfer_task.state != RUNNING) {
            break;
        }
        if (waited_ms >= timeout_ms) {
            LOG_ERROR("NETWORK",
                      "Timed out waiting for network worker to stop: pid=%d timeout_ms=%u",
                      transfer_task.pid,
                      (unsigned)timeout_ms);
            dbg_printf("[DBG][NET] stop wait timeout pid=%d timeout_ms=%u\n",
                       transfer_task.pid,
                       (unsigned)timeout_ms);
            kill(transfer_task.pid, SIGKILL);
            (void)waitpid(transfer_task.pid, NULL, 0);
            if (transfer_task.output_fd >= 0) {
                close(transfer_task.output_fd);
                transfer_task.output_fd = -1;
            }
            transfer_task.state = ERROR;
            return ACK_FAILED;
        }
        usleep(1000);
        ++waited_ms;
    }

    if (transfer_task.state == ERROR) {
        if (network_output_has_stop_request(&transfer_task)) {
            LOG_INFO("NETWORK", "Network worker stopped by request: output=%s",
                     transfer_task.output);
            dbg_printf("[DBG][NET] stop wait accepted requested stop output=%s\n",
                       transfer_task.output);
            transfer_task.state = IDLE;
            return ACK_SUCCESS;
        }
        LOG_ERROR("NETWORK", "Network worker failed while stopping: output=%s",
                  transfer_task.output);
        dbg_printf("[DBG][NET] stop wait failed output=%s\n", transfer_task.output);
        return ACK_FAILED;
    }

    dbg_printf("[DBG][NET] stop wait done waited_ms=%u\n", (unsigned)waited_ms);
    return ACK_SUCCESS;
}

static const char *file_type_name_from_proto(uint32_t proto_file_type)
{
    switch (proto_file_type) {
    case FILE_TYPE_LOW:
        return "LOW_SPEED";
    case FILE_TYPE_I:
        return "HIGH_I";
    case FILE_TYPE_Q:
        return "HIGH_Q";
    case FILE_TYPE_CALIB:
        return "CALIB";
    default:
        return "UNKNOWN";
    }
}

typedef struct {
    FileEntry metadata_backup[NUM_CHANNELS][MAX_FILES_TOTAL];
    bool metadata_backup_valid[NUM_CHANNELS];
} StorageCommitContext;

static int storage_commit_db_begin(void *ctx)
{
    (void)ctx;
    return file_db_begin();
}

static int storage_commit_record_exists(void *ctx, const FileRecord *record)
{
    FileRecord existing;
    (void)ctx;
    memset(&existing, 0, sizeof(existing));
    return file_query_by_index(record->task_id, record->file_index, &existing) == 0 ? 1 : 0;
}

static int storage_commit_metadata_write(void *opaque, const StorageCommitItem *item)
{
    StorageCommitContext *ctx = opaque;
    ChannelRuntime rt;
    FileEntry table[MAX_FILES_TOTAL];

    if (!ctx || !item || item->channel >= NUM_CHANNELS ||
        item->metadata_slot >= MAX_FILES_TOTAL || !find_channel((int)item->channel)) return -1;
    memset(&rt, 0, sizeof(rt));
    rt.cfg = find_channel((int)item->channel);
    if (metadata_read(&rt, table) != 0) return -1;
    memcpy(ctx->metadata_backup[item->channel], table, sizeof(table));
    ctx->metadata_backup_valid[item->channel] = true;
    if (table[item->metadata_slot].valid == 1u) return -1;
    table[item->metadata_slot] = item->metadata;
    return metadata_write(&rt, table);
}

static int storage_commit_metadata_rollback(void *opaque, const StorageCommitItem *item)
{
    StorageCommitContext *ctx = opaque;
    ChannelRuntime rt;

    if (!ctx || !item || item->channel >= NUM_CHANNELS ||
        !ctx->metadata_backup_valid[item->channel] || !find_channel((int)item->channel)) return -1;
    memset(&rt, 0, sizeof(rt));
    rt.cfg = find_channel((int)item->channel);
    if (metadata_write(&rt, ctx->metadata_backup[item->channel]) != 0) return -1;
    ctx->metadata_backup_valid[item->channel] = false;
    return 0;
}

static int storage_commit_record_insert(void *ctx, const FileRecord *record)
{
    (void)ctx;
    return file_add(record);
}

static int storage_commit_record_count(void *ctx, const char *task_id, int *count)
{
    (void)ctx;
    *count = file_count_by_task(task_id);
    return *count < 0 ? -1 : 0;
}

static int storage_commit_total_update(void *ctx, const char *task_id, int count)
{
    (void)ctx;
    return task_update_total_files(task_id, count);
}

static int storage_commit_status_update(void *ctx, const char *task_id, TaskStatus status)
{
    (void)ctx;
    return task_update_status(task_id, status);
}

static int storage_commit_db_commit(void *ctx)
{
    (void)ctx;
    return file_db_commit();
}

static int storage_commit_db_rollback(void *ctx)
{
    (void)ctx;
    return file_db_rollback();
}

static int storage_commit_flash_sync(void *ctx)
{
    (void)ctx;
    return sync_filelist_db_to_flash() == ACK_SUCCESS ? 0 : -1;
}

static int storage_commit_sync_mark_pending(void *ctx, const char *task_id)
{
    (void)ctx;
    return storage_sync_outbox_mark_pending(task_id);
}

static int storage_commit_sync_mark_complete(void *ctx, const char *task_id)
{
    (void)ctx;
    return storage_sync_outbox_mark_complete(task_id);
}

static int storage_commit_supervised_results(const char *task_id)
{
    StorageCommitContext ctx;
    StorageCommitItem items[NUM_CHANNELS];
    StorageCommitOps ops;
    size_t count = 0u;
    uint32_t channel;

    memset(&ctx, 0, sizeof(ctx));
    memset(items, 0, sizeof(items));
    memset(&ops, 0, sizeof(ops));
    for (channel = 0u; channel < NUM_CHANNELS; ++channel) {
        const Task *task;
        const PlannedFile *planned;
        const WriteResult *result;
        StorageCommitItem *item;

        if ((g_storage_supervisor.target_channel_mask & (1u << channel)) == 0u) continue;
        task = &storage_tasks[channel];
        planned = &task->planned_file;
        result = &g_storage_supervisor.final_result[channel];
        if (!task->has_planned_file || planned->channel_id != (int)channel ||
            result->channel_id != (int)channel || result->metadata_slot >= MAX_FILES_TOTAL ||
            result->file_index > UINT16_MAX || result->sector_count > UINT32_MAX ||
            strcmp(task->task_id, task_id) != 0 || strcmp(result->task_no, task_id) != 0) {
            snprintf(g_storage_commit_state.reason, sizeof(g_storage_commit_state.reason),
                     "%s", "provisional_result_mismatch");
            g_storage_commit_state.attempted = true;
            g_storage_commit_state.success = false;
            (void)task_update_status(task_id, TASK_FAILED);
            return -1;
        }
        item = &items[count++];
        item->channel = channel;
        item->metadata_slot = result->metadata_slot;
        memcpy(item->metadata.task_no, task_id,
               strlen(task_id) < sizeof(item->metadata.task_no)
                   ? strlen(task_id) : sizeof(item->metadata.task_no));
        item->metadata.file_cnt = 1u;
        item->metadata.file_type = (uint8_t)planned->proto_file_type_code;
        item->metadata.file_index = (uint16_t)result->file_index;
        item->metadata.file_size_bytes = result->file_bytes > UINT32_MAX
                                             ? UINT32_MAX : (uint32_t)result->file_bytes;
        item->metadata.start_lba = result->start_lba;
        item->metadata.sector_count = (uint32_t)result->sector_count;
        item->metadata.valid = 1u;

        snprintf(item->record.task_id, sizeof(item->record.task_id), "%s", task_id);
        item->record.overpass_time = task->overpass_time;
        item->record.file_index = (int)result->file_index;
        snprintf(item->record.file_type, sizeof(item->record.file_type), "%s",
                 file_type_name_from_proto((uint32_t)planned->proto_file_type_code));
        item->record.channel_id = (int)channel;
        item->record.proto_file_type_code = planned->proto_file_type_code;
        item->record.calibration_type = planned->calibration_type;
        item->record.start_sector = result->start_lba;
        item->record.sector_count = result->sector_count;
        item->record.file_size = result->file_bytes;
        snprintf(item->record.filename, sizeof(item->record.filename), "%s_%s_%u.bin",
                 task_id, item->record.file_type, (unsigned)result->file_index);
    }

    ops.ctx = &ctx;
    ops.db_begin = storage_commit_db_begin;
    ops.record_exists = storage_commit_record_exists;
    ops.metadata_write = storage_commit_metadata_write;
    ops.metadata_rollback = storage_commit_metadata_rollback;
    ops.record_insert = storage_commit_record_insert;
    ops.record_count = storage_commit_record_count;
    ops.task_total_update = storage_commit_total_update;
    ops.task_status_update = storage_commit_status_update;
    ops.db_commit = storage_commit_db_commit;
    ops.db_rollback = storage_commit_db_rollback;
    ops.flash_sync = storage_commit_flash_sync;
    ops.sync_mark_pending = storage_commit_sync_mark_pending;
    ops.sync_mark_complete = storage_commit_sync_mark_complete;
    return storage_commit_run_once(&g_storage_commit_state, task_id, items, count, &ops);
}

static void append_task_output(Task *task, const char *buf, size_t len)
{
    size_t capacity;

    if (!task || !buf || len == 0u) {
        return;
    }
    capacity = sizeof(task->output) - 1u;
    if (len >= capacity) {
        memcpy(task->output, buf + len - capacity, capacity);
        task->output_used = capacity;
        task->output[task->output_used] = '\0';
        return;
    }
    if (task->output_used + len > capacity) {
        size_t drop = task->output_used + len - capacity;
        memmove(task->output, task->output + drop, task->output_used - drop);
        task->output_used -= drop;
    }
    memcpy(task->output + task->output_used, buf, len);
    task->output_used += len;
    task->output[task->output_used] = '\0';
}

static int system_env_flag_enabled(const char *name)
{
    const char *value = getenv(name);

    if (!value || value[0] == '\0') {
        return 0;
    }
    if (strcmp(value, "0") == 0 ||
        strcmp(value, "false") == 0 ||
        strcmp(value, "FALSE") == 0 ||
        strcmp(value, "off") == 0 ||
        strcmp(value, "OFF") == 0 ||
        strcmp(value, "no") == 0 ||
        strcmp(value, "NO") == 0) {
        return 0;
    }
    return 1;
}

static void system_emit_line(StorageLogSeverity severity, const char *fmt, ...)
{
    char line[1024];
    va_list ap;
    int n;
    size_t len;

    if (!fmt || !storage_log_severity_enabled(severity)) {
        return;
    }
    va_start(ap, fmt);
    n = vsnprintf(line, sizeof(line), fmt, ap);
    va_end(ap);
    if (n < 0) {
        return;
    }
    len = (size_t)n;
    if (len >= sizeof(line)) {
        len = sizeof(line) - 2u;
    }
    if (len == 0u || line[len - 1u] != '\n') {
        line[len++] = '\n';
    }
    line[len] = '\0';
    {
        ssize_t written = write(STDOUT_FILENO, line, len);
        (void)written;
    }
}

static void system_write_stdout_line(const char *line)
{
    size_t len;
    ssize_t written;

    if (!line) {
        return;
    }
    len = strlen(line);
    written = write(STDOUT_FILENO, line, len);
    (void)written;
    if (len == 0u || line[len - 1u] != '\n') {
        written = write(STDOUT_FILENO, "\n", 1u);
        (void)written;
    }
}

/* Worker stdout is line-oriented and has no severity field.  The worker has
 * already applied StorageLogSeverity before writing; this final quiet-mode
 * gate only preserves critical failure reporting if an older worker emits a
 * line directly. */
static bool worker_output_is_quiet_critical(const char *text)
{
    const char *console = getenv("SRC_REAL_CONSOLE_LOG_LEVEL");

    if (!text) {
        return false;
    }
    if (console && strcmp(console, "none") == 0) return false;
    if (console && (strcmp(console, "debug") == 0 || strcmp(console, "trace") == 0)) {
        return true;
    }
    if ((!console || strcmp(console, "critical") != 0) &&
        storage_log_severity_enabled(STORAGE_LOG_SUMMARY)) {
        return true;
    }
    return strstr(text, "storage_ddr_full") != NULL ||
           strstr(text, "storage_receive_failed") != NULL ||
           strstr(text, "storage_first_dma_timeout") != NULL ||
           strstr(text, "storage_ring_config_error") != NULL ||
           strstr(text, "dma_bd_exhausted") != NULL ||
           (strstr(text, "storage_result") != NULL && strstr(text, "status=failed") != NULL);
}

static void echo_worker_line(Task *task, const char *line)
{
    if (!task || !line) {
        return;
    }
    if (worker_output_is_quiet_critical(line)) {
        system_write_stdout_line(line);
        return;
    }
    if (getenv("SRC_REAL_ECHO_WORKER_OUTPUT") != NULL) {
        if (system_env_flag_enabled("SRC_REAL_ECHO_WORKER_OUTPUT")) {
            system_write_stdout_line(line);
        }
        return;
    }
    if (task == &transfer_task &&
        (dbg_category_enabled("NET") || dbg_category_enabled("TCP"))) {
        system_write_stdout_line(line);
        return;
    }
    if (task != &transfer_task &&
        (dbg_category_enabled("WRITE") ||
         dbg_category_enabled("DMA") ||
         dbg_category_enabled("STORAGE"))) {
        system_write_stdout_line(line);
        return;
    }
}

static void echo_worker_trace_output(Task *task, const char *buf, size_t len)
{
    size_t i;

    if (!task || !buf || len == 0u) {
        return;
    }
    for (i = 0u; i < len; ++i) {
        char c = buf[i];
        if (task->echo_line_used + 1u >= sizeof(task->echo_line)) {
            task->echo_line[task->echo_line_used] = '\0';
            echo_worker_line(task, task->echo_line);
            task->echo_line_used = 0u;
        }
        task->echo_line[task->echo_line_used++] = c;
        if (c == '\n') {
            task->echo_line[task->echo_line_used] = '\0';
            echo_worker_line(task, task->echo_line);
            task->echo_line_used = 0u;
        }
    }
}

static void poll_task_output(Task *task)
{
    if (!task || task->output_fd < 0) {
        return;
    }

    while (1) {
        char tmp[1024];
        ssize_t n = read(task->output_fd, tmp, sizeof(tmp));
        if (n > 0) {
            size_t copy_n = (size_t)n;
            if (copy_n > sizeof(tmp)) {
                copy_n = sizeof(tmp);
            }
            append_task_output(task, tmp, copy_n);
            echo_worker_trace_output(task, tmp, copy_n);
            continue;
        }
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            return;
        }
        return;
    }
}

static void drain_storage_events(Task *task, bool report_eof)
{
    StorageWorkerEvent event;
    int rc = -1;
    uint32_t expected_channel;
    if (!task || task->event_fd < 0) return;
    expected_channel = (uint32_t)task->planned_file.channel_id;
    while ((rc = storage_ipc_read_event_raw(task->event_fd, &event)) == 0) {
        int supervisor_rc;

        supervisor_rc = storage_supervisor_handle_event_for_channel(
            &g_storage_supervisor, expected_channel, &event);
        if (supervisor_rc != 0) {
            StorageWorkerEvent protocol_fatal;

            storage_ipc_make_event(&protocol_fatal, STORAGE_WORKER_FATAL,
                                   expected_channel, -1, 0u,
                                   g_storage_supervisor.fatal_reason[0] != '\0'
                                       ? g_storage_supervisor.fatal_reason
                                       : "event_protocol_invalid");
            task->fatal_seen = true;
            task->first_fatal = protocol_fatal;
            continue;
        }
        if (event.type == STORAGE_WORKER_PERF_SAMPLE || event.type == STORAGE_WORKER_FATAL ||
            event.type == STORAGE_WORKER_FINAL_RESULT || event.type == STORAGE_WORKER_DIAG_EVENT) {
            (void)storage_perf_log_event(&event, task->task_id);
        }
        task->worker_event = event;
        task->worker_phase = event.type;
        if (event.type == STORAGE_WORKER_READY && supervisor_rc == 0) task->ready_seen = true;
        else if (event.type == STORAGE_WORKER_ARMED && supervisor_rc == 0) task->armed_seen = true;
        else if (event.type == STORAGE_WORKER_RUNNING && supervisor_rc == 0) task->running_seen = true;
        else if (event.type == STORAGE_WORKER_DRAINED && supervisor_rc == 0) task->drained_seen = true;
        else if (event.type == STORAGE_WORKER_FINAL_RESULT) {
            if (task->final_result_seen) {
                task->fatal_seen = true;
                continue;
            }
            task->final_result_seen = true;
            task->final_result = event.result;
            task->final_data_persisted = event.result.data_persisted;
            task->final_integrity_ok = event.result.integrity_ok;
            task->final_status_success = event.result.data_persisted && event.result.integrity_ok;
            task->final_receive_seen = true;
            task->final_dma_received_bytes = event.result.dma_received_bytes;
        } else if (event.type == STORAGE_WORKER_FATAL && !task->fatal_seen) {
            task->fatal_seen = true;
            task->first_fatal = event;
        }
    }
    if (rc < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
        storage_supervisor_protocol_fail(&g_storage_supervisor, expected_channel,
                                         "event_read_integrity_failed");
        task->fatal_seen = true;
        storage_ipc_make_event(&task->first_fatal, STORAGE_WORKER_FATAL,
                               expected_channel, -1, 0u,
                               "event_read_integrity_failed");
    }
    if (rc == 1 && task->event_fd >= 0 && report_eof) {
        (void)storage_supervisor_handle_worker_eof(
            &g_storage_supervisor, (uint32_t)task->planned_file.channel_id);
        close(task->event_fd);
        task->event_fd = -1;
    }
}

static void poll_storage_events(Task *task)
{
    drain_storage_events(task, true);
    storage_try_send_pending_stops();
    storage_supervisor_emit_aggregate();
}

static int start_storage_worker(const PlannedFile *planned, const char *task_id, time_t overpass_time)
{
    int pipefd[2];
    int control_pipe[2];
    int event_pipe[2];
    pid_t pid;
    char channel_str[16];
    char size_str[32];
    char file_index_str[16];
    char proto_type_str[16];
    char calibration_type_str[16];
    char timeout_str[32];
    char dma_desc_str[32];
    const char *argv[32];
    int argc = 0;
    const char *program = g_program_path ? g_program_path : "./src_real_app";
    Task *task = NULL;

    if (!planned || !task_id || task_id[0] == '\0') {
        dbg_printf("[DBG][STORAGE] invalid start request planned=%p task=%p\n",
                   (const void *)planned, (const void *)task_id);
        return -1;
    }
    task = storage_task_for_channel(planned->channel_id);
    if (!task) {
        LOG_ERROR("STORAGE", "No storage task slot for channel %d", planned->channel_id);
        return -1;
    }
    if (task->state == RUNNING) {
        LOG_WARN("STORAGE", "Storage worker is busy");
        dbg_printf("[DBG][STORAGE] worker busy current_pid=%d task=%s\n",
                   task->pid, task->task_id);
        return -1;
    }

    snprintf(channel_str, sizeof(channel_str), "%d", planned->channel_id);
    snprintf(size_str, sizeof(size_str), "%" PRIu64, planned->size_bytes);
    snprintf(file_index_str, sizeof(file_index_str), "%d", planned->file_index);
    snprintf(proto_type_str, sizeof(proto_type_str), "%d", planned->proto_file_type_code);
    snprintf(calibration_type_str, sizeof(calibration_type_str), "%u", (unsigned)planned->calibration_type);
    {
        char env_name[48];
        const char *dma_desc_env;

        snprintf(env_name, sizeof(env_name), "SRC_REAL_STORAGE_DMA_DESC_BYTES_CH%d", planned->channel_id);
        dma_desc_env = getenv(env_name);
        if (!dma_desc_env || dma_desc_env[0] == '\0') {
            dma_desc_env = getenv("SRC_REAL_STORAGE_DMA_DESC_BYTES");
        }
        if (dma_desc_env && dma_desc_env[0] != '\0') {
            snprintf(dma_desc_str, sizeof(dma_desc_str), "%s", dma_desc_env);
        } else {
            dma_desc_str[0] = '\0';
        }
    }

    argv[argc++] = program;
    argv[argc++] = STORAGE_WORKER_ARG;
    if (env_flag_enabled("SRC_REAL_STORAGE_DRY_RUN")) {
        argv[argc++] = "--dry-run";
    }
    if (env_flag_enabled("SRC_REAL_STORAGE_SKIP_LINK_CHECK")) {
        argv[argc++] = "--skip-link-check";
    }
    {
        const char *timeout_env = getenv("SRC_REAL_STORAGE_TIMEOUT_US");
        if (timeout_env && timeout_env[0] != '\0') {
            snprintf(timeout_str, sizeof(timeout_str), "%s", timeout_env);
            argv[argc++] = "--timeout-us";
            argv[argc++] = timeout_str;
        }
    }
    argv[argc++] = STORAGE_WRITE_ARG;
    argv[argc++] = "--channel";
    argv[argc++] = channel_str;
    if (planned->size_bytes > 0u) {
        argv[argc++] = "--size";
        argv[argc++] = size_str;
    }
    argv[argc++] = "--task-no";
    argv[argc++] = task_id;
    argv[argc++] = "--file-index";
    argv[argc++] = file_index_str;
    argv[argc++] = "--ssd-lba";
    argv[argc++] = "auto";
    argv[argc++] = "--source";
    argv[argc++] = "transfer";
    argv[argc++] = "--proto-file-type";
    argv[argc++] = proto_type_str;
    argv[argc++] = "--calibration-type";
    argv[argc++] = calibration_type_str;
    if (dma_desc_str[0] != '\0') {
        argv[argc++] = "--dma-desc-bytes";
        argv[argc++] = dma_desc_str;
    }
    argv[argc] = NULL;

    if (pipe(pipefd) != 0) {
        LOG_ERROR("STORAGE", "pipe failed: errno=%d", errno);
        dbg_printf("[DBG][STORAGE] pipe failed errno=%d\n", errno);
        return -1;
    }
    if (pipe(control_pipe) != 0) {
        close(pipefd[0]);
        close(pipefd[1]);
        LOG_ERROR("STORAGE", "control pipe failed: errno=%d", errno);
        return -1;
    }
    if (pipe(event_pipe) != 0) {
        close(pipefd[0]); close(pipefd[1]); close(control_pipe[0]); close(control_pipe[1]);
        LOG_ERROR("STORAGE", "event pipe failed: errno=%d", errno);
        return -1;
    }

    pid = fork();
    if (pid < 0) {
        close(pipefd[0]);
        close(pipefd[1]);
        close(control_pipe[0]);
        close(control_pipe[1]);
        close(event_pipe[0]);
        close(event_pipe[1]);
        LOG_ERROR("STORAGE", "fork failed: errno=%d", errno);
        dbg_printf("[DBG][STORAGE] fork failed errno=%d\n", errno);
        return -1;
    }
    if (pid == 0) {
        const char *meta_dir = get_storage_meta_dir();
        char start_fd[16];
        char event_fd[16];
        int keep_fds[3] = { control_pipe[0], event_pipe[1], pipefd[1] };
        storage_child_close_inherited_fds(storage_tasks, STORAGE_TASK_COUNT,
                                          keep_fds, 3u);
        setenv("CCB_PROCESS_META_DIR", meta_dir, 1);
        snprintf(start_fd, sizeof(start_fd), "%d", control_pipe[0]);
        setenv("SRC_REAL_START_FD", start_fd, 1);
        setenv("SRC_REAL_STORAGE_CONTROL_FD", start_fd, 1);
        snprintf(event_fd, sizeof(event_fd), "%d", event_pipe[1]);
        setenv("SRC_REAL_STORAGE_EVENT_FD", event_fd, 1);
        {
            int flags = fcntl(event_pipe[1], F_GETFL, 0);
            if (flags >= 0) (void)fcntl(event_pipe[1], F_SETFL, flags | O_NONBLOCK);
        }
        dup2(pipefd[1], STDOUT_FILENO);
        dup2(pipefd[1], STDERR_FILENO);
        close(pipefd[0]);
        close(pipefd[1]);
        close(control_pipe[1]);
        close(event_pipe[0]);
        execvp(program, (char *const *)argv);
        perror("execvp");
        _exit(127);
    }

    close(pipefd[1]);
    close(control_pipe[0]);
    close(event_pipe[1]);
    {
        int flags = fcntl(pipefd[0], F_GETFL, 0);
        if (flags >= 0) {
            (void)fcntl(pipefd[0], F_SETFL, flags | O_NONBLOCK);
        }
    }
    {
        int flags = fcntl(event_pipe[0], F_GETFL, 0);
        if (flags >= 0) {
            (void)fcntl(event_pipe[0], F_SETFL, flags | O_NONBLOCK);
        }
    }
    {
        int flags = fcntl(control_pipe[1], F_GETFL, 0);
        if (flags >= 0) {
            (void)fcntl(control_pipe[1], F_SETFL, flags | O_NONBLOCK);
        }
    }

    task->pid = pid;
    task->start_time = time(NULL);
    task->state = RUNNING;
    task->output_fd = pipefd[0];
    task->control_fd = control_pipe[1];
    task->event_fd = event_pipe[0];
    task->stop_send_attempts = 0u;
    task->output_used = 0u;
    task->output[0] = '\0';
    task->echo_line_used = 0u;
    task->echo_line[0] = '\0';
    task->final_result_seen = false;
    task->final_data_persisted = false;
    task->final_integrity_ok = false;
    task->final_status_success = false;
    task->final_receive_seen = false;
    task->split_mismatch_reported = false;
    task->final_dma_received_bytes = 0u;
    task->worker_phase = 0u;
    task->ready_seen = task->armed_seen = task->running_seen = false;
    task->drained_seen = task->fatal_seen = false;
    memset(&task->worker_event, 0, sizeof(task->worker_event));
    memset(&task->first_fatal, 0, sizeof(task->first_fatal));
    task->planned_file = *planned;
    task->has_planned_file = true;
    memset(task->task_id, 0, sizeof(task->task_id));
    memcpy(task->task_id, task_id, sizeof(task->task_id) - 1u);
    task->overpass_time = overpass_time;

    dbg_verbose_printf("[DBG][STORAGE] worker forked pid=%d task=%s ch=%d idx=%d mode=%s size=%" PRIu64 "\n",
                       pid,
                       task_id,
                       planned->channel_id,
                       planned->file_index,
                       planned->size_bytes > 0u ? "bounded" : "continuous",
                       planned->size_bytes);

    LOG_INFO("STORAGE",
             "Storage worker launched: pid=%d task=%s ch=%d idx=%d mode=%s size=%" PRIu64,
             pid,
             task_id,
             planned->channel_id,
             planned->file_index,
             planned->size_bytes > 0u ? "bounded" : "continuous",
             planned->size_bytes);
    return 0;
}

static bool storage_pending_start_target(const Task *task)
{
    return task && task->pid > 0 && task->has_planned_file &&
           (g_storage_supervisor.target_channel_mask &
            (1u << task->planned_file.channel_id)) != 0u &&
           strcmp(task->task_id, g_pending_storage_start.task_id) == 0;
}

static void storage_pending_start_fail(void)
{
    if (!g_pending_storage_start.active ||
        g_pending_storage_start.phase == STORAGE_START_FAILED) return;
    g_pending_storage_start.phase = STORAGE_START_FAILED;
    (void)request_storage_stop_all();
}

static void storage_pending_start_send_failure(void)
{
    if (!g_pending_storage_start.active || g_pending_storage_start.failure_ack_sent) return;
    g_pending_storage_start.failure_ack_sent = true;
    if (g_pending_storage_start.prepare) {
        proto_send_ack(CMD_TASK_INFO, ACK_FAILED);
        (void)task_update_status(g_pending_storage_start.task_id, TASK_FAILED);
    } else {
        proto_send_acq_ack(ACK_FAILED, g_pending_storage_start.acq_type,
                           g_pending_storage_start.failure_type);
    }
    system_emit_line(STORAGE_LOG_ALWAYS_CRITICAL, "storage_start_failed task=%s phase=%s reason=start_barrier_failed",
                     g_pending_storage_start.task_id,
                     g_pending_storage_start.prepare ? "prepare" : "run");
}

static int storage_begin_prepare_wait(const char *task_id, uint8_t acq_type,
                                      uint8_t failure_type)
{
    uint32_t timeout_us = env_u32_or_default("SRC_REAL_STORAGE_PREP_TIMEOUT_US", 5000000u);

    if (!task_id || task_id[0] == '\0' || g_pending_storage_start.active) return -1;
    memset(&g_pending_storage_start, 0, sizeof(g_pending_storage_start));
    g_pending_storage_start.active = true;
    g_pending_storage_start.prepare = true;
    g_pending_storage_start.phase = STORAGE_START_PREP_READY;
    g_pending_storage_start.acq_type = acq_type;
    g_pending_storage_start.failure_type = failure_type;
    snprintf(g_pending_storage_start.task_id, sizeof(g_pending_storage_start.task_id), "%s", task_id);
    g_pending_storage_start.target_count = 0;
    g_pending_storage_start.deadline_us = storage_ipc_monotonic_us() + timeout_us;
    for (size_t i = 0u; i < STORAGE_TASK_COUNT; ++i) {
        if ((g_storage_supervisor.target_channel_mask & (1u << i)) != 0u)
            ++g_pending_storage_start.target_count;
    }
    if (g_pending_storage_start.target_count == 0) storage_pending_start_fail();
    return 0;
}

static int storage_begin_run_barrier(const char *task_id, uint8_t acq_type,
                                     uint8_t failure_type)
{
    uint64_t start_us = storage_ipc_monotonic_us();
    uint32_t timeout_us = env_u32_or_default("SRC_REAL_STORAGE_START_TIMEOUT_US", 5000000u);
    size_t i;

    if (!task_id || task_id[0] == '\0' || g_pending_storage_start.active) return -1;
    memset(&g_pending_storage_start, 0, sizeof(g_pending_storage_start));
    g_pending_storage_start.active = true;
    g_pending_storage_start.prepare = false;
    g_pending_storage_start.phase = STORAGE_START_ARM_WAIT;
    g_pending_storage_start.acq_type = acq_type;
    g_pending_storage_start.failure_type = failure_type;
    snprintf(g_pending_storage_start.task_id, sizeof(g_pending_storage_start.task_id), "%s", task_id);
    for (i = 0u; i < STORAGE_TASK_COUNT; ++i) {
        Task *task = &storage_tasks[i];
        if ((g_storage_supervisor.target_channel_mask & (1u << i)) == 0u) continue;
        ++g_pending_storage_start.target_count;
        check_task(task);
        if (task->state != RUNNING || task->control_fd < 0 ||
            storage_send_control(task, STORAGE_CTRL_ARM, start_us) != 0) {
            storage_pending_start_fail();
            break;
        }
    }
    if (g_pending_storage_start.target_count == 0)
        storage_pending_start_fail();
    g_pending_storage_start.deadline_us = storage_ipc_monotonic_us() + timeout_us;
    return 0;
}

/* Progress START/prepare one bounded slice.  check_task() drains all worker
 * pipes, so the UART loop remains responsive while the child-side deadlines
 * continue to run. */
static void storage_service_pending_start(void)
{
    uint32_t ready = 0u;
    uint32_t armed = 0u;
    uint32_t running = 0u;
    size_t i;

    if (!g_pending_storage_start.active) return;
    for (i = 0u; i < STORAGE_TASK_COUNT; ++i) {
        Task *task = &storage_tasks[i];
        if ((g_storage_supervisor.target_channel_mask & (1u << i)) == 0u ||
            strcmp(task->task_id, g_pending_storage_start.task_id) != 0) continue;
        check_task(task);
        if (storage_pending_start_target(task)) {
            if (task->fatal_seen && g_pending_storage_start.phase != STORAGE_START_FAILED) {
                storage_pending_start_fail();
            }
            if (task->ready_seen) ++ready;
            if (task->armed_seen) ++armed;
            if (task->running_seen) ++running;
        } else if (g_pending_storage_start.phase != STORAGE_START_FAILED) {
            storage_pending_start_fail();
        }
    }
    if (g_pending_storage_start.phase != STORAGE_START_FAILED &&
        storage_ipc_monotonic_us() >= g_pending_storage_start.deadline_us) {
        storage_pending_start_fail();
    }
    if (g_pending_storage_start.phase == STORAGE_START_PREP_READY &&
        ready == (uint32_t)g_pending_storage_start.target_count) {
        if (task_update_status(g_pending_storage_start.task_id, TASK_RUNNING) != 0) {
            storage_pending_start_fail();
        } else {
            proto_send_ack(CMD_TASK_INFO, ACK_SUCCESS);
            system_emit_line(STORAGE_LOG_SUMMARY, "serial_cmd_ack cmd=0x11 action=prestart task_no=%s ack_status=0x%02X result_code=0x%02X worker_state=%d storage_state=%d reason=storage_prepared",
                             g_pending_storage_start.task_id, ACK_SUCCESS, ACK_SUCCESS,
                             RUNNING, storage_state_summary());
            memset(&g_pending_storage_start, 0, sizeof(g_pending_storage_start));
            return;
        }
    }
    if (g_pending_storage_start.phase == STORAGE_START_ARM_WAIT &&
        armed == (uint32_t)g_pending_storage_start.target_count) {
        uint64_t run_timestamp = storage_ipc_monotonic_us();
        bool failed = false;
        for (i = 0u; i < STORAGE_TASK_COUNT; ++i) {
            Task *task = &storage_tasks[i];
            if ((g_storage_supervisor.target_channel_mask & (1u << i)) == 0u) continue;
            if (task->state != RUNNING || storage_send_control(task, STORAGE_CTRL_RUN,
                                                               run_timestamp) != 0) {
                failed = true;
                break;
            }
        }
        if (failed) {
            storage_pending_start_fail();
        } else {
            g_pending_storage_start.phase = STORAGE_START_RUN_WAIT;
            g_pending_storage_start.deadline_us = storage_ipc_monotonic_us() +
                env_u32_or_default("SRC_REAL_STORAGE_START_TIMEOUT_US", 5000000u);
        }
    }
    if (g_pending_storage_start.phase == STORAGE_START_RUN_WAIT &&
        running == (uint32_t)g_pending_storage_start.target_count) {
        proto_send_acq_ack(ACK_SUCCESS, g_pending_storage_start.acq_type, FAIL_TYPE_NONE);
        system_emit_line(STORAGE_LOG_SUMMARY, "serial_cmd_ack cmd=0x21 action=start task_no=%s ack_status=0x%02X result_code=0x%02X worker_state=%d storage_state=%d reason=all_workers_started",
                         g_pending_storage_start.task_id, ACK_SUCCESS, ACK_SUCCESS,
                         RUNNING, storage_state_summary());
        memset(&g_pending_storage_start, 0, sizeof(g_pending_storage_start));
        return;
    }
    if (g_pending_storage_start.phase == STORAGE_START_FAILED) {
        (void)request_storage_stop_all();
        if (!storage_any_live_worker()) {
            storage_pending_start_send_failure();
            storage_supervisor_emit_aggregate();
            memset(&g_pending_storage_start, 0, sizeof(g_pending_storage_start));
        }
    }
}

static void finalize_storage_task(Task *task, int exit_code)
{
    const WriteResult *result;
    bool success;

    if (!task || !task->has_planned_file) return;
    result = &task->final_result;
    success = task->final_result_seen && exit_code == 0 && result->data_persisted &&
              result->receive_integrity_ok && result->storage_integrity_ok &&
              result->integrity_ok &&
              result->dma_received_bytes == result->nvme_completed_bytes &&
              result->nvme_completed_bytes == result->file_bytes;
    task->state = success ? IDLE : ERROR;
    if (!success) (void)request_storage_stop_all();
}
static int ensure_task_for_standalone_storage(const char *task_id, time_t *out_overpass)
{
    TaskInfo info;
    time_t now = time(NULL);

    if (!task_id || task_id[0] == '\0' || !out_overpass) {
        return -1;
    }
    memset(&info, 0, sizeof(info));
    if (task_query(task_id, &info) == 0) {
        *out_overpass = info.overpass_time;
        return 0;
    }
    if (task_create_with_payload(task_id, now, "standalone storage-write", "{}") != 0) {
        return -1;
    }
    *out_overpass = now;
    return 0;
}

static int start_network_worker(const FileRecord *rec)
{
    int pipefd[2];
    pid_t pid;
    char file_index_str[16];
    char proto_type_str[16];
    char calibration_type_str[16];
    char timeout_str[32];
    const char *argv[24];
    int argc = 0;
    const char *program = g_program_path ? g_program_path : "./src_real_app";

    if (!rec || rec->task_id[0] == '\0') {
        return -1;
    }
    if (storage_any_running() || transfer_task.state == RUNNING) {
        LOG_WARN("NETWORK", "Reject network send while worker is busy");
        return -1;
    }

    snprintf(file_index_str, sizeof(file_index_str), "%d", rec->file_index);
    snprintf(proto_type_str, sizeof(proto_type_str), "%d", rec->proto_file_type_code);
    snprintf(calibration_type_str, sizeof(calibration_type_str), "%d", rec->calibration_type);

    argv[argc++] = program;
    argv[argc++] = NETWORK_WORKER_ARG;
    if (env_flag_enabled("SRC_REAL_NETWORK_DRY_RUN")) {
        argv[argc++] = "--dry-run";
    }
    if (env_flag_enabled("SRC_REAL_NETWORK_SKIP_LINK_CHECK")) {
        argv[argc++] = "--skip-link-check";
    }
    {
        const char *timeout_env = getenv("SRC_REAL_NETWORK_TIMEOUT_US");
        if (timeout_env && timeout_env[0] != '\0') {
            snprintf(timeout_str, sizeof(timeout_str), "%s", timeout_env);
            argv[argc++] = "--timeout-us";
            argv[argc++] = timeout_str;
        }
    }
    argv[argc++] = NETWORK_SEND_ARG;
    argv[argc++] = "--task-no";
    argv[argc++] = rec->task_id;
    argv[argc++] = "--file-index";
    argv[argc++] = file_index_str;
    argv[argc++] = "--proto-file-type";
    argv[argc++] = proto_type_str;
    argv[argc++] = "--calibration-type";
    argv[argc++] = calibration_type_str;
    argv[argc] = NULL;

    if (pipe(pipefd) != 0) {
        LOG_ERROR("NETWORK", "pipe failed: errno=%d", errno);
        return -1;
    }
    pid = fork();
    if (pid < 0) {
        close(pipefd[0]);
        close(pipefd[1]);
        LOG_ERROR("NETWORK", "fork failed: errno=%d", errno);
        return -1;
    }
    if (pid == 0) {
        setenv("CCB_PROCESS_META_DIR", get_storage_meta_dir(), 1);
        dup2(pipefd[1], STDOUT_FILENO);
        dup2(pipefd[1], STDERR_FILENO);
        close(pipefd[0]);
        close(pipefd[1]);
        execvp(program, (char *const *)argv);
        perror("execvp");
        _exit(127);
    }

    close(pipefd[1]);
    {
        int flags = fcntl(pipefd[0], F_GETFL, 0);
        if (flags >= 0) {
            (void)fcntl(pipefd[0], F_SETFL, flags | O_NONBLOCK);
        }
    }

    transfer_task.pid = pid;
    transfer_task.start_time = time(NULL);
    transfer_task.state = RUNNING;
    transfer_task.output_fd = pipefd[0];
    transfer_task.output_used = 0u;
    transfer_task.output[0] = '\0';
    transfer_task.echo_line_used = 0u;
    transfer_task.echo_line[0] = '\0';
    transfer_task.has_planned_file = false;
    memset(transfer_task.task_id, 0, sizeof(transfer_task.task_id));
    memcpy(transfer_task.task_id, rec->task_id, sizeof(transfer_task.task_id) - 1u);
    transfer_task.overpass_time = rec->overpass_time;
    transfer_task.timeout_seconds = network_task_timeout_seconds();

    LOG_INFO("NETWORK",
             "Network worker started: pid=%d task=%s idx=%d type=0x%02X size=%" PRIu64,
             pid,
             rec->task_id,
             rec->file_index,
             rec->proto_file_type_code,
             rec->file_size);
    dbg_verbose_printf("[DBG][NET] worker forked pid=%d task=%s idx=%d type=0x%02X size=%" PRIu64 "\n",
                       pid, rec->task_id, rec->file_index, rec->proto_file_type_code, rec->file_size);
    dbg_printf("[DBG][NET] worker timeout_seconds=%ld task=%s idx=%d\n",
               transfer_task.timeout_seconds,
               rec->task_id,
               rec->file_index);
    return 0;
}

static int record_storage_result_to_db(const ParsedArgs *args, const WriteResult *result)
{
    FileRecord existing;
    FileRecord rec;
    char filename[256] = {0};
    time_t overpass = 0;

    if (!args || !result || !args->has_proto_file_type) {
        return -1;
    }
    if (ensure_task_for_standalone_storage(args->task_no, &overpass) != 0) {
        fprintf(stderr, "Failed to create/query task_info for task=%s\n", args->task_no);
        return -1;
    }
    memset(&existing, 0, sizeof(existing));
    if (file_query_by_index(args->task_no, (int)result->file_index, &existing) == 0) {
        fprintf(stderr, "Duplicate DB file record: task=%s file_index=%u\n",
                args->task_no, (unsigned)result->file_index);
        return -1;
    }

    memset(&rec, 0, sizeof(rec));
    snprintf(filename, sizeof(filename), "%s_%s_%u.bin",
             args->task_no,
             file_type_name_from_proto(args->proto_file_type),
             (unsigned)result->file_index);
    strncpy(rec.task_id, args->task_no, sizeof(rec.task_id) - 1);
    rec.overpass_time = overpass;
    rec.file_index = (int)result->file_index;
    snprintf(rec.file_type, sizeof(rec.file_type), "%s",
             file_type_name_from_proto(args->proto_file_type));
    rec.channel_id = result->channel_id;
    rec.proto_file_type_code = (int)args->proto_file_type;
    rec.calibration_type = args->has_calibration_type ? (int)args->calibration_type : 0;
    rec.start_sector = result->start_lba;
    rec.sector_count = result->sector_count;
    rec.file_size = result->size_bytes;
    snprintf(rec.filename, sizeof(rec.filename), "%s", filename);

    if (file_db_begin() != 0) {
        return -1;
    }
    if (file_add(&rec) != 0 || file_db_commit() != 0) {
        (void)file_db_rollback();
        return -1;
    }
    {
        int total = file_count_by_task(args->task_no);
        if (total >= 0) {
            (void)task_update_total_files(args->task_no, total);
        }
    }
    (void)task_update_status(args->task_no, TASK_COMPLETED);
    printf("db_write_done task=%s file_index=%u proto_file_type=%u calibration_type=%u\n",
           args->task_no,
           (unsigned)result->file_index,
           (unsigned)args->proto_file_type,
           args->has_calibration_type ? (unsigned)args->calibration_type : 0u);
    return 0;
}

static int advance_duplicate_file_index_from_db(ParsedArgs *args)
{
    int count;
    int next_index;
    uint32_t requested_index;

    if (!args) {
        return -1;
    }
    requested_index = args->file_index;
    count = file_count_by_task(args->task_no);
    if (count < 0) {
        return -1;
    }
    if (count == 0) {
        return 0;
    }
    if (file_next_index_by_task(args->task_no, &next_index) != 0 ||
        next_index <= 0 || next_index > UINT16_MAX) {
        return -1;
    }
    args->file_index = (uint32_t)next_index;
    printf("storage_file_index_advanced task=%s requested_file_index=%u"
           " effective_file_index=%u source=database\n",
           args->task_no,
           (unsigned)requested_index,
           (unsigned)args->file_index);
    fflush(stdout);
    return 0;
}

typedef enum {
    NETWORK_SLOT_FREE = 0,
    NETWORK_SLOT_READING,
    NETWORK_SLOT_READY,
    NETWORK_SLOT_SENDING
} NetworkSlotState;

typedef struct {
    NetworkSlotState state;
    uint32_t chunk_index;
    uint32_t ring_slot;
    uint64_t lba;
    uint64_t chunk_bytes;
    uint64_t read_sectors;
    uint64_t send_bytes;
    uint64_t ddr_hw;
} NetworkPipelineSlot;

typedef struct {
    pthread_mutex_t lock;
    pthread_cond_t free_cond;
    pthread_cond_t ready_cond;
    NetworkPipelineSlot *slots;
    uint32_t slot_count;
    uint32_t total_chunks;
    uint32_t next_send_chunk;
    int producer_done;
    int error_rc;
    const char *task_no;
    uint32_t file_index;
    const ChannelConfig *cfg;
    ChannelRuntime *rt;
    GlobalOptions gopt;
    uint64_t max_payload_bytes;
    uint64_t network_ring_stride;
    uint64_t network_ring_bytes;
    uint64_t total_file_bytes;
    uint64_t start_lba;
    uint64_t read_cmd_sectors;
    uint32_t proto_file_type;
    int verify_ddr_read;
    uint32_t verify_bytes;
} NetworkPipelineCtx;

static void format_hex_prefix(const volatile uint8_t *data, uint32_t bytes, char *out, size_t out_len)
{
    static const char hex[] = "0123456789ABCDEF";
    uint32_t i;
    size_t pos = 0u;

    if (!out || out_len == 0u) {
        return;
    }
    out[0] = '\0';
    if (!data) {
        return;
    }
    for (i = 0u; i < bytes && (pos + 2u) < out_len; ++i) {
        uint8_t v = data[i];
        out[pos++] = hex[(v >> 4u) & 0x0fu];
        out[pos++] = hex[v & 0x0fu];
    }
    out[pos] = '\0';
}

static const char *network_magic_hint(const volatile uint8_t *data, uint32_t bytes)
{
    uint32_t i;

    if (!data || bytes < 4u) {
        return "unknown";
    }
    for (i = 0u; i + 3u < bytes; ++i) {
        if (data[i] == 0x18u && data[i + 1u] == 0xEFu &&
            data[i + 2u] == 0xDCu && data[i + 3u] == 0x01u) {
            return "ch2_18efdc01";
        }
        if (data[i] == 0x18u && data[i + 1u] == 0xEFu &&
            data[i + 2u] == 0x01u && data[i + 3u] == 0xDCu) {
            return "ch0_18ef01dc";
        }
    }
    return "unknown";
}

static int network_verify_ddr_read_overwrite(NetworkPipelineCtx *ctx,
                                             const NetworkPipelineSlot *slot,
                                             uint64_t read_bytes)
{
    uint64_t ddr_offset;
    uint32_t verify_bytes;
    uint32_t prefix_bytes;
    uint32_t i;
    int all_a5 = 1;
    volatile uint8_t *sample;
    char prefix[129];

    if (!ctx || !slot || !ctx->verify_ddr_read || ctx->gopt.dry_run) {
        return 0;
    }
    if (slot->ddr_hw < ctx->cfg->ddr_hw_base) {
        return 0;
    }
    ddr_offset = slot->ddr_hw - ctx->cfg->ddr_hw_base;
    if (ddr_offset >= ctx->cfg->ddr_cpu_size || ddr_offset >= ctx->rt->ddr.size) {
        printf("network_ddr_read_verify task=%s file_index=%u proto_file_type=%u"
               " chunk=%u lba=0x%08" PRIx64 " sectors=%" PRIu64
               " ddr_hw=0x%08" PRIx64 " slot=%u slot_offset=%" PRIu64
               " result=skipped reason=outside_cpu_visible_ddr\n",
               ctx->task_no,
               (unsigned)ctx->file_index,
               (unsigned)ctx->proto_file_type,
               (unsigned)slot->chunk_index,
               slot->lba,
               slot->read_sectors,
               slot->ddr_hw,
               (unsigned)slot->ring_slot,
               ddr_offset);
        fflush(stdout);
        return 0;
    }
    verify_bytes = ctx->verify_bytes ? ctx->verify_bytes : 4096u;
    if (verify_bytes > read_bytes) {
        verify_bytes = (uint32_t)read_bytes;
    }
    if (ddr_offset + verify_bytes > ctx->rt->ddr.size) {
        verify_bytes = (uint32_t)(ctx->rt->ddr.size - ddr_offset);
    }
    if (verify_bytes == 0u) {
        return 0;
    }
    sample = ctx->rt->ddr.virt + ddr_offset;
    __sync_synchronize();
    for (i = 0u; i < verify_bytes; ++i) {
        if (sample[i] != 0xA5u) {
            all_a5 = 0;
            break;
        }
    }
    prefix_bytes = verify_bytes < 64u ? verify_bytes : 64u;
    format_hex_prefix(sample, prefix_bytes, prefix, sizeof(prefix));
    printf("network_ddr_read_verify task=%s file_index=%u proto_file_type=%u"
           " chunk=%u lba=0x%08" PRIx64 " sectors=%" PRIu64
           " ddr_hw=0x%08" PRIx64 " slot=%u slot_offset=%" PRIu64
           " sample_bytes=%u result=%s magic=%s prefix=%s\n",
           ctx->task_no,
           (unsigned)ctx->file_index,
           (unsigned)ctx->proto_file_type,
           (unsigned)slot->chunk_index,
           slot->lba,
           slot->read_sectors,
           slot->ddr_hw,
           (unsigned)slot->ring_slot,
           ddr_offset,
           (unsigned)verify_bytes,
           all_a5 ? "failed_still_a5" : "ok",
           network_magic_hint(sample, prefix_bytes),
           prefix);
    fflush(stdout);
    return all_a5 ? -1 : 0;
}

static void network_pipeline_set_error(NetworkPipelineCtx *ctx, int rc)
{
    pthread_mutex_lock(&ctx->lock);
    if (ctx->error_rc == 0) {
        ctx->error_rc = rc;
    }
    pthread_cond_broadcast(&ctx->free_cond);
    pthread_cond_broadcast(&ctx->ready_cond);
    pthread_mutex_unlock(&ctx->lock);
}

static void *network_read_thread(void *arg)
{
    NetworkPipelineCtx *ctx = (NetworkPipelineCtx *)arg;
    uint64_t remaining_bytes = ctx->total_file_bytes;
    uint64_t cur_lba = ctx->start_lba;
    uint32_t chunk_index = 0u;

    while (remaining_bytes > 0u) {
        uint64_t chunk_bytes = remaining_bytes > ctx->max_payload_bytes ?
                               ctx->max_payload_bytes : remaining_bytes;
        uint64_t read_sectors = bytes_to_sectors(chunk_bytes);
        uint64_t read_bytes = read_sectors * (uint64_t)SECTOR_SIZE;
        uint64_t send_bytes = chunk_bytes;
        uint64_t file_offset = (uint64_t)chunk_index * ctx->max_payload_bytes;
        uint64_t sectors_before = file_offset / (uint64_t)SECTOR_SIZE;
        uint64_t read_cmd_sectors = ctx->read_cmd_sectors ? ctx->read_cmd_sectors : 512u;
        uint64_t cmd_first = sectors_before / read_cmd_sectors;
        uint64_t cmd_count = (read_sectors + read_cmd_sectors - 1u) / read_cmd_sectors;
        uint64_t cmd_last = cmd_first + (cmd_count ? cmd_count - 1u : 0u);
        uint32_t ring_slot = chunk_index % ctx->slot_count;
        uint64_t slot_base = (uint64_t)ring_slot * ctx->network_ring_stride;
        uint64_t network_ddr_hw = ctx->cfg->ddr_hw_base + slot_base + NETWORK_DDR_OFFSET_BYTES;
        NetworkPipelineSlot *slot = &ctx->slots[ring_slot];
        int read_rc;

        if (nvme_stop_requested()) {
            network_pipeline_set_error(ctx, -2);
            return NULL;
        }
        if (slot_base + NETWORK_DDR_OFFSET_BYTES + read_bytes > ctx->network_ring_bytes ||
            slot_base + NETWORK_DDR_OFFSET_BYTES + send_bytes > ctx->network_ring_bytes) {
            fprintf(stderr, "Invalid network chunk size: chunk=%" PRIu64
                    " read=%" PRIu64 " send=%" PRIu64 "\n",
                    chunk_bytes, read_bytes, send_bytes);
            network_pipeline_set_error(ctx, -1);
            return NULL;
        }

        pthread_mutex_lock(&ctx->lock);
        while (slot->state != NETWORK_SLOT_FREE && ctx->error_rc == 0) {
            pthread_cond_wait(&ctx->free_cond, &ctx->lock);
        }
        if (ctx->error_rc != 0) {
            pthread_mutex_unlock(&ctx->lock);
            return NULL;
        }
        slot->state = NETWORK_SLOT_READING;
        slot->chunk_index = chunk_index;
        slot->ring_slot = ring_slot;
        slot->lba = cur_lba;
        slot->chunk_bytes = chunk_bytes;
        slot->read_sectors = read_sectors;
        slot->send_bytes = send_bytes;
        slot->ddr_hw = network_ddr_hw;
        pthread_mutex_unlock(&ctx->lock);

        dbg_printf("[DBG][NET] read chunk=%u slot=%u task=%s idx=%u lba=0x%08" PRIx64
                   " ddr=0x%08" PRIx64 " bytes=%" PRIu64
                   " sectors=%" PRIu64 " send_bytes=%" PRIu64
                   " nvme_cmd_first=%" PRIu64 " nvme_cmd_last=%" PRIu64
                   " nvme_cmd_sectors=%" PRIu64 "\n",
                   (unsigned)chunk_index,
                   (unsigned)ring_slot,
                   ctx->task_no,
                   (unsigned)ctx->file_index,
                   cur_lba,
                   network_ddr_hw,
                   chunk_bytes,
                   read_sectors,
                   send_bytes,
                   cmd_first,
                   cmd_last,
                   read_cmd_sectors);
        if ((cmd_first / 2048u) != (cmd_last / 2048u)) {
            dbg_printf("[DBG][NET] 2048cmd boundary task=%s idx=%u chunk=%u"
                       " file_offset=%" PRIu64 " lba=0x%08" PRIx64
                       " cmd_first=%" PRIu64 " cmd_last=%" PRIu64
                       " cmd_sectors=%" PRIu64 "\n",
                       ctx->task_no,
                       (unsigned)ctx->file_index,
                       (unsigned)chunk_index,
                       file_offset,
                       cur_lba,
                       cmd_first,
                       cmd_last,
                       read_cmd_sectors);
        }
        if ((file_offset % (512ull * 1024ull * 1024ull)) == 0u) {
            dbg_printf("[DBG][NET] 512MiB boundary task=%s idx=%u chunk=%u"
                       " file_offset=%" PRIu64 " lba=0x%08" PRIx64
                       " slot=%u ddr=0x%08" PRIx64 "\n",
                       ctx->task_no,
                       (unsigned)ctx->file_index,
                       (unsigned)chunk_index,
                       file_offset,
                       cur_lba,
                       (unsigned)ring_slot,
                       network_ddr_hw);
        }

        if (ctx->verify_ddr_read && !ctx->gopt.dry_run) {
            uint64_t ddr_offset = network_ddr_hw - ctx->cfg->ddr_hw_base;
            uint32_t verify_bytes = ctx->verify_bytes ? ctx->verify_bytes : 4096u;

            if (network_ddr_hw >= ctx->cfg->ddr_hw_base &&
                ddr_offset < ctx->cfg->ddr_cpu_size &&
                ddr_offset < ctx->rt->ddr.size) {
                if (verify_bytes > read_bytes) {
                    verify_bytes = (uint32_t)read_bytes;
                }
                if (ddr_offset + verify_bytes > ctx->rt->ddr.size) {
                    verify_bytes = (uint32_t)(ctx->rt->ddr.size - ddr_offset);
                }
                if (verify_bytes > 0u) {
                    memset((void *)(ctx->rt->ddr.virt + ddr_offset), 0xA5, verify_bytes);
                    __sync_synchronize();
                }
            }
        }

        read_rc = nvme_rw(ctx->rt, false, cur_lba, read_sectors, network_ddr_hw);
        if (read_rc == -2) {
            dbg_printf("[DBG][NET] nvme read stopped task=%s idx=%u chunk=%u lba=0x%08" PRIx64 "\n",
                       ctx->task_no,
                       (unsigned)ctx->file_index,
                       (unsigned)chunk_index,
                       cur_lba);
            network_pipeline_set_error(ctx, -2);
            return NULL;
        }
        if (read_rc != 0) {
            fprintf(stderr, "NVMe read failed before network send: task=%s file_index=%u lba=0x%08" PRIx64 "\n",
                    ctx->task_no, (unsigned)ctx->file_index, cur_lba);
            network_pipeline_set_error(ctx, -1);
            return NULL;
        }
        if (network_verify_ddr_read_overwrite(ctx, slot, read_bytes) != 0) {
            fprintf(stderr,
                    "NVMe read did not overwrite DDR sentinel: task=%s file_index=%u chunk=%u lba=0x%08" PRIx64 "\n",
                    ctx->task_no,
                    (unsigned)ctx->file_index,
                    (unsigned)chunk_index,
                    cur_lba);
            network_pipeline_set_error(ctx, -1);
            return NULL;
        }

        pthread_mutex_lock(&ctx->lock);
        slot->state = NETWORK_SLOT_READY;
        pthread_cond_broadcast(&ctx->ready_cond);
        pthread_mutex_unlock(&ctx->lock);

        cur_lba += read_sectors;
        remaining_bytes -= chunk_bytes;
        ++chunk_index;
    }

    pthread_mutex_lock(&ctx->lock);
    ctx->producer_done = 1;
    pthread_cond_broadcast(&ctx->ready_cond);
    pthread_mutex_unlock(&ctx->lock);
    return NULL;
}

static void *network_send_thread(void *arg)
{
    NetworkPipelineCtx *ctx = (NetworkPipelineCtx *)arg;

    while (ctx->next_send_chunk < ctx->total_chunks) {
        uint32_t ring_slot = ctx->next_send_chunk % ctx->slot_count;
        NetworkPipelineSlot slot_copy;
        NetworkPipelineSlot *slot = &ctx->slots[ring_slot];
        TcpTransferConfig tcp_cfg;
        uint64_t chunk_start_us;
        int send_rc;

        pthread_mutex_lock(&ctx->lock);
        while ((slot->state != NETWORK_SLOT_READY ||
                slot->chunk_index != ctx->next_send_chunk) &&
               ctx->error_rc == 0) {
            if (ctx->producer_done && slot->state == NETWORK_SLOT_FREE) {
                ctx->error_rc = -1;
                break;
            }
            pthread_cond_wait(&ctx->ready_cond, &ctx->lock);
        }
        if (ctx->error_rc != 0) {
            pthread_mutex_unlock(&ctx->lock);
            return NULL;
        }
        slot->state = NETWORK_SLOT_SENDING;
        slot_copy = *slot;
        pthread_mutex_unlock(&ctx->lock);

        if (nvme_stop_requested()) {
            network_pipeline_set_error(ctx, -2);
            return NULL;
        }

        tcp_transfer_default_config(&tcp_cfg, slot_copy.send_bytes, ctx->gopt);
        if (configure_tcp_for_channel(ctx->cfg, &tcp_cfg) != 0) {
            fprintf(stderr, "Unsupported TCP route for channel %d\n", ctx->cfg->id);
            network_pipeline_set_error(ctx, -1);
            return NULL;
        }
        tcp_cfg.ddr_dma_base = slot_copy.ddr_hw;
        chunk_start_us = system_wall_time_us();
        dbg_verbose_printf("[DBG][NET] tcp route chunk=%u slot=%u ch=%d dma=0x%08" PRIx64
                           " switch=0x%08" PRIx64 " input=%u desc_cpu=0x%08" PRIx64
                           " desc_dma=0x%08" PRIx64 " ddr_dma=0x%08" PRIx64 "\n",
                           (unsigned)slot_copy.chunk_index,
                           (unsigned)slot_copy.ring_slot,
                           ctx->cfg->id,
                           tcp_cfg.dma_base,
                           tcp_cfg.switch_base,
                           (unsigned)tcp_cfg.switch_input_select,
                           tcp_cfg.desc_cpu_base,
                           tcp_cfg.desc_dma_base,
                           tcp_cfg.ddr_dma_base);

        send_rc = tcp_transfer_send(&tcp_cfg);
        if (send_rc == -2) {
            fprintf(stderr,
                    "network_send_stopped task=%s file_index=%u chunk_index=%u total_chunks=%u"
                    " lba=0x%08" PRIx64 " file_offset=%" PRIu64
                    " chunk_bytes=%" PRIu64 " sent_bytes=%" PRIu64 "\n",
                    ctx->task_no,
                    (unsigned)ctx->file_index,
                    (unsigned)slot_copy.chunk_index,
                    (unsigned)ctx->total_chunks,
                    slot_copy.lba,
                    (uint64_t)slot_copy.chunk_index * ctx->max_payload_bytes,
                    slot_copy.chunk_bytes,
                    slot_copy.send_bytes);
            network_pipeline_set_error(ctx, -2);
            return NULL;
        }
        if (send_rc != 0) {
            fprintf(stderr, "TCP MM2S send failed: task=%s file_index=%u chunk=%u\n",
                    ctx->task_no,
                    (unsigned)ctx->file_index,
                    (unsigned)slot_copy.chunk_index);
            network_pipeline_set_error(ctx, -1);
            return NULL;
        }
        network_throttle_after_chunk(chunk_start_us,
                                     slot_copy.send_bytes,
                                     ctx->task_no,
                                     ctx->file_index,
                                     slot_copy.chunk_index,
                                     ctx->total_chunks);

        dbg_printf("[DBG][NET] chunk done task=%s file_index=%u chunk_index=%u total_chunks=%u"
                   " slot=%u lba=0x%08" PRIx64 " file_offset=%" PRIu64
                   " file_bytes=%" PRIu64 " sent_bytes=%" PRIu64 " total_sent=%" PRIu64 "\n",
                   ctx->task_no,
                   (unsigned)ctx->file_index,
                   (unsigned)slot_copy.chunk_index,
                   (unsigned)ctx->total_chunks,
                   (unsigned)slot_copy.ring_slot,
                   slot_copy.lba,
                   (uint64_t)slot_copy.chunk_index * ctx->max_payload_bytes,
                   slot_copy.chunk_bytes,
                   slot_copy.send_bytes,
                   (uint64_t)slot_copy.chunk_index * ctx->max_payload_bytes + slot_copy.send_bytes);

        pthread_mutex_lock(&ctx->lock);
        slot->state = NETWORK_SLOT_FREE;
        ++ctx->next_send_chunk;
        pthread_cond_broadcast(&ctx->free_cond);
        pthread_mutex_unlock(&ctx->lock);
    }

    return NULL;
}

static int network_send_serial(const ParsedArgs *args,
                               GlobalOptions gopt,
                               const FileRecord *rec,
                               const ChannelConfig *cfg,
                               ChannelRuntime *rt,
                               uint64_t max_payload_bytes,
                               uint64_t network_ring_bytes,
                               uint32_t *sent_chunks_out)
{
    uint64_t remaining_bytes;
    uint64_t cur_lba;
    uint32_t chunk_index = 0u;
    uint32_t total_chunks;
    uint64_t read_cmd_sectors;
    int rc = -1;

    if (!args || !rec || !cfg || !rt) {
        return -1;
    }
    remaining_bytes = rec->file_size;
    cur_lba = (uint64_t)rec->start_sector;
    read_cmd_sectors = rt->nvme_cmd_sectors ? rt->nvme_cmd_sectors : 512u;
    total_chunks = (uint32_t)((rec->file_size + max_payload_bytes - 1u) / max_payload_bytes);

    while (remaining_bytes > 0u) {
        uint64_t chunk_bytes = remaining_bytes > max_payload_bytes ?
                               max_payload_bytes : remaining_bytes;
        uint64_t read_sectors = bytes_to_sectors(chunk_bytes);
        uint64_t read_bytes = read_sectors * (uint64_t)SECTOR_SIZE;
        uint64_t send_bytes = chunk_bytes;
        uint64_t file_offset = (uint64_t)chunk_index * max_payload_bytes;
        uint64_t sectors_before = file_offset / (uint64_t)SECTOR_SIZE;
        uint64_t cmd_first = sectors_before / read_cmd_sectors;
        uint64_t cmd_count = (read_sectors + read_cmd_sectors - 1u) / read_cmd_sectors;
        uint64_t cmd_last = cmd_first + (cmd_count ? cmd_count - 1u : 0u);
        uint64_t network_ddr_hw = cfg->ddr_hw_base + NETWORK_DDR_OFFSET_BYTES;
        NetworkPipelineCtx verify_ctx;
        NetworkPipelineSlot verify_slot;
        TcpTransferConfig tcp_cfg;
        uint64_t chunk_start_us = system_wall_time_us();
        int read_rc;
        int send_rc;

        if (nvme_stop_requested()) {
            fprintf(stderr,
                    "network_send_stopped task=%s file_index=%u chunk_index=%u total_chunks=%u"
                    " lba=0x%08" PRIx64 " file_offset=%" PRIu64
                    " remaining_bytes=%" PRIu64 "\n",
                    args->task_no,
                    (unsigned)args->file_index,
                    (unsigned)chunk_index,
                    (unsigned)total_chunks,
                    cur_lba,
                    file_offset,
                    remaining_bytes);
            rc = -2;
            break;
        }
        if (NETWORK_DDR_OFFSET_BYTES + read_bytes > network_ring_bytes ||
            NETWORK_DDR_OFFSET_BYTES + send_bytes > network_ring_bytes) {
            fprintf(stderr, "Invalid network chunk size: chunk=%" PRIu64
                    " read=%" PRIu64 " send=%" PRIu64 "\n",
                    chunk_bytes, read_bytes, send_bytes);
            rc = -1;
            break;
        }

        dbg_printf("[DBG][NET] read chunk_index=%u total_chunks=%u slot=0 task=%s idx=%u lba=0x%08" PRIx64
                   " ddr=0x%08" PRIx64 " bytes=%" PRIu64
                   " sectors=%" PRIu64 " send_bytes=%" PRIu64
                   " nvme_cmd_first=%" PRIu64 " nvme_cmd_last=%" PRIu64
                   " nvme_cmd_sectors=%" PRIu64 "\n",
                   (unsigned)chunk_index,
                   (unsigned)total_chunks,
                   args->task_no,
                   (unsigned)args->file_index,
                   cur_lba,
                   network_ddr_hw,
                   chunk_bytes,
                   read_sectors,
                   send_bytes,
                   cmd_first,
                   cmd_last,
                   read_cmd_sectors);
        if ((cmd_first / 2048u) != (cmd_last / 2048u)) {
            dbg_printf("[DBG][NET] 2048cmd boundary task=%s idx=%u chunk=%u"
                       " file_offset=%" PRIu64 " lba=0x%08" PRIx64
                       " cmd_first=%" PRIu64 " cmd_last=%" PRIu64
                       " cmd_sectors=%" PRIu64 "\n",
                       args->task_no,
                       (unsigned)args->file_index,
                       (unsigned)chunk_index,
                       file_offset,
                       cur_lba,
                       cmd_first,
                       cmd_last,
                       read_cmd_sectors);
        }
        if ((file_offset % (512ull * 1024ull * 1024ull)) == 0u) {
            dbg_printf("[DBG][NET] 512MiB boundary task=%s idx=%u chunk=%u"
                       " file_offset=%" PRIu64 " lba=0x%08" PRIx64
                       " slot=0 ddr=0x%08" PRIx64 "\n",
                       args->task_no,
                       (unsigned)args->file_index,
                       (unsigned)chunk_index,
                       file_offset,
                       cur_lba,
                       network_ddr_hw);
        }

        memset(&verify_ctx, 0, sizeof(verify_ctx));
        memset(&verify_slot, 0, sizeof(verify_slot));
        verify_ctx.task_no = args->task_no;
        verify_ctx.file_index = args->file_index;
        verify_ctx.cfg = cfg;
        verify_ctx.rt = rt;
        verify_ctx.gopt = gopt;
        verify_ctx.proto_file_type = (uint32_t)rec->proto_file_type_code;
        verify_ctx.verify_ddr_read = env_flag_enabled("SRC_REAL_NETWORK_VERIFY_DDR_READ");
        verify_ctx.verify_bytes = env_u32_or_default("SRC_REAL_NETWORK_VERIFY_BYTES", 4096u);
        if (verify_ctx.verify_bytes == 0u) {
            verify_ctx.verify_bytes = 4096u;
        }
        verify_slot.chunk_index = chunk_index;
        verify_slot.ring_slot = 0u;
        verify_slot.lba = cur_lba;
        verify_slot.read_sectors = read_sectors;
        verify_slot.ddr_hw = network_ddr_hw;

        if (verify_ctx.verify_ddr_read && !gopt.dry_run) {
            uint64_t ddr_offset = network_ddr_hw - cfg->ddr_hw_base;
            uint32_t verify_bytes = verify_ctx.verify_bytes;

            if (ddr_offset < cfg->ddr_cpu_size && ddr_offset < rt->ddr.size) {
                if (verify_bytes > read_bytes) {
                    verify_bytes = (uint32_t)read_bytes;
                }
                if (ddr_offset + verify_bytes > rt->ddr.size) {
                    verify_bytes = (uint32_t)(rt->ddr.size - ddr_offset);
                }
                if (verify_bytes > 0u) {
                    memset((void *)(rt->ddr.virt + ddr_offset), 0xA5, verify_bytes);
                    __sync_synchronize();
                }
            }
        }

        read_rc = nvme_rw(rt, false, cur_lba, read_sectors, network_ddr_hw);
        if (read_rc == -2) {
            dbg_printf("[DBG][NET] nvme read stopped task=%s idx=%u chunk=%u lba=0x%08" PRIx64 "\n",
                       args->task_no,
                       (unsigned)args->file_index,
                       (unsigned)chunk_index,
                       cur_lba);
            rc = -2;
            break;
        }
        if (read_rc != 0) {
            fprintf(stderr, "NVMe read failed before network send: task=%s file_index=%u lba=0x%08" PRIx64 "\n",
                    args->task_no, (unsigned)args->file_index, cur_lba);
            rc = -1;
            break;
        }
        if (network_verify_ddr_read_overwrite(&verify_ctx, &verify_slot, read_bytes) != 0) {
            fprintf(stderr,
                    "NVMe read did not overwrite DDR sentinel: task=%s file_index=%u chunk=%u lba=0x%08" PRIx64 "\n",
                    args->task_no,
                    (unsigned)args->file_index,
                    (unsigned)chunk_index,
                    cur_lba);
            rc = -1;
            break;
        }

        tcp_transfer_default_config(&tcp_cfg, send_bytes, gopt);
        if (configure_tcp_for_channel(cfg, &tcp_cfg) != 0) {
            fprintf(stderr, "Unsupported TCP route for channel %d\n", cfg->id);
            rc = -1;
            break;
        }
        tcp_cfg.ddr_dma_base = network_ddr_hw;

        send_rc = tcp_transfer_send(&tcp_cfg);
        if (send_rc == -2) {
            fprintf(stderr,
                    "network_send_stopped task=%s file_index=%u chunk_index=%u total_chunks=%u"
                    " lba=0x%08" PRIx64 " file_offset=%" PRIu64
                    " chunk_bytes=%" PRIu64 " sent_bytes=%" PRIu64 "\n",
                    args->task_no,
                    (unsigned)args->file_index,
                    (unsigned)chunk_index,
                    (unsigned)total_chunks,
                    cur_lba,
                    file_offset,
                    chunk_bytes,
                    send_bytes);
            rc = -2;
            break;
        }
        if (send_rc != 0) {
            fprintf(stderr, "TCP MM2S send failed: task=%s file_index=%u chunk=%u\n",
                    args->task_no,
                    (unsigned)args->file_index,
                    (unsigned)chunk_index);
            rc = -1;
            break;
        }
        network_throttle_after_chunk(chunk_start_us,
                                     send_bytes,
                                     args->task_no,
                                     args->file_index,
                                     chunk_index,
                                     total_chunks);

        dbg_printf("[DBG][NET] chunk done task=%s file_index=%u chunk_index=%u total_chunks=%u"
                   " slot=0 lba=0x%08" PRIx64 " file_offset=%" PRIu64
                   " file_bytes=%" PRIu64 " sent_bytes=%" PRIu64 " total_sent=%" PRIu64 "\n",
                   args->task_no,
                   (unsigned)args->file_index,
                   (unsigned)chunk_index,
                   (unsigned)total_chunks,
                   cur_lba,
                   file_offset,
                   chunk_bytes,
                   send_bytes,
                   file_offset + send_bytes);

        cur_lba += read_sectors;
        remaining_bytes -= chunk_bytes;
        ++chunk_index;
    }

    if (remaining_bytes == 0u) {
        rc = 0;
    }
    if (sent_chunks_out) {
        *sent_chunks_out = chunk_index;
    }
    return rc;
}

static int network_send_existing_file(const ParsedArgs *args, GlobalOptions gopt)
{
    FileRecord rec;
    const ChannelConfig *cfg;
    int route_channel_id;
    ChannelRuntime rt;
    uint64_t cur_lba;
    uint64_t max_payload_bytes;
    uint64_t total_file_bytes;
    uint64_t network_ring_stride;
    uint64_t network_ring_bytes;
    uint32_t network_ring_slots;
    uint32_t requested_pipeline_slots;
    uint32_t available_pipeline_slots;
    uint32_t chunk_index = 0u;
    int rc = -1;

    if (!args || !args->has_task_no || !args->has_file_index || !args->has_proto_file_type) {
        return -1;
    }
    memset(&rec, 0, sizeof(rec));
    if (file_query_by_index(args->task_no, (int)args->file_index, &rec) != 0) {
        fprintf(stderr, "Network target not found: task=%s file_index=%u\n",
                args->task_no, (unsigned)args->file_index);
        return -1;
    }
    if (rec.proto_file_type_code != (int)args->proto_file_type) {
        fprintf(stderr, "Network target type mismatch: task=%s file_index=%u req=0x%02x db=0x%02x\n",
                args->task_no,
                (unsigned)args->file_index,
                (unsigned)args->proto_file_type,
                rec.proto_file_type_code);
        return -1;
    }
    if (args->has_calibration_type &&
        args->proto_file_type == FILE_TYPE_CALIB &&
        rec.calibration_type != (int)args->calibration_type) {
        fprintf(stderr, "Network target calibration mismatch: task=%s file_index=%u req=0x%02x db=0x%02x\n",
                args->task_no,
                (unsigned)args->file_index,
                (unsigned)args->calibration_type,
                rec.calibration_type);
        return -1;
    }
    if (rec.file_size == 0u || rec.sector_count == 0u) {
        fprintf(stderr, "Invalid DB file record for network send: task=%s file_index=%u\n",
                args->task_no, (unsigned)args->file_index);
        return -1;
    }

    switch (rec.proto_file_type_code) {
    case FILE_TYPE_LOW:
    case FILE_TYPE_CALIB:
        route_channel_id = LOW_SPEED_CHANNEL_ID;
        break;
    case FILE_TYPE_I:
        route_channel_id = HIGH_I_CHANNEL_ID;
        break;
    case FILE_TYPE_Q:
        route_channel_id = HIGH_Q_CHANNEL_ID;
        break;
    default:
        fprintf(stderr, "Unsupported network file type: task=%s file_index=%u type=0x%02x\n",
                args->task_no,
                (unsigned)args->file_index,
                (unsigned)rec.proto_file_type_code);
        return -1;
    }

    if (rec.channel_id != route_channel_id) {
        dbg_printf("[DBG][NET] DB channel mismatch task=%s idx=%u type=0x%02X db_ch=%d route_ch=%d\n",
                   args->task_no,
                   (unsigned)args->file_index,
                   (unsigned)rec.proto_file_type_code,
                   rec.channel_id,
                   route_channel_id);
    }

    cfg = find_channel(route_channel_id);
    if (!cfg) {
        fprintf(stderr, "Unsupported channel for network send: %d\n", route_channel_id);
        return -1;
    }
    /*
     * Keep one TCP MM2S transaction equal to one source frame.  The downstream
     * TCP path sees TLAST from the DMA EOF bit, so batching multiple 16 MiB
     * frames into one 64 MiB DMA transaction would only produce one TLAST.
     */
    max_payload_bytes = TCP_MAX_BYTES_PER_DESC;
    /*
     * The download pipeline only needs a few low-address DDR slots for
     * double/triple buffering.  Keep the default window inside the CPU-visible
     * DDR aperture because ch2 high DDR addresses have shown wrap/repeat-like
     * behavior in the TCP/NVMe download path.
     */
    network_ring_bytes = cfg->ddr_cpu_size;
    if (network_ring_bytes == 0u || network_ring_bytes > cfg->dma_ring_bytes) {
        network_ring_bytes = cfg->dma_ring_bytes;
    }
    if (max_payload_bytes == 0u ||
        max_payload_bytes + NETWORK_DDR_OFFSET_BYTES > network_ring_bytes) {
        fprintf(stderr, "DDR ring too small for TCP frame size\n");
        return -1;
    }
    network_ring_stride = max_payload_bytes + NETWORK_DDR_OFFSET_BYTES;
    available_pipeline_slots = (uint32_t)(network_ring_bytes / network_ring_stride);
    if (available_pipeline_slots == 0u) {
        fprintf(stderr, "No usable DDR ring slot for TCP download\n");
        return -1;
    }
    requested_pipeline_slots = env_u32_or_default("SRC_REAL_NETWORK_PIPELINE_SLOTS", 1u);
    if (requested_pipeline_slots == 0u) {
        requested_pipeline_slots = 1u;
    }
    network_ring_slots = requested_pipeline_slots;
    if (network_ring_slots > available_pipeline_slots) {
        network_ring_slots = available_pipeline_slots;
    }

    memset(&rt, 0, sizeof(rt));
    if (channel_runtime_open(&rt, cfg, gopt) != 0) {
        return -1;
    }
    dbg_printf("[DBG][NET] DDR ring ch=%d base=0x%08" PRIx64
               " slots=%u requested_slots=%u available_slots=%u"
               " stride=%" PRIu64 " payload_bytes=%" PRIu64
               " offset_per_slot=%u ring_bytes=%" PRIu64 "\n",
               cfg->id,
               cfg->ddr_hw_base,
               (unsigned)network_ring_slots,
               (unsigned)requested_pipeline_slots,
               (unsigned)available_pipeline_slots,
               network_ring_stride,
               max_payload_bytes,
               (unsigned)NETWORK_DDR_OFFSET_BYTES,
               network_ring_bytes);
    if (nvme_stop_requested()) {
        dbg_printf("[DBG][NET] stop requested before nvme probe task=%s idx=%u\n",
                   args->task_no,
                   (unsigned)args->file_index);
        rc = -2;
        goto out;
    }
    if (nvme_probe(&rt) != 0) {
        goto out;
    }
    if (rt.nvme_max_lba == 0u) {
        fprintf(stderr, "NVMe capacity unavailable before network send: channel=%d\n", cfg->id);
        dbg_printf("[DBG][NET] nvme max_lba unavailable ch=%d, refuse network send\n", cfg->id);
        goto out;
    }

    total_file_bytes = rec.file_size;
    cur_lba = (uint64_t)rec.start_sector;
    chunk_index = (uint32_t)((total_file_bytes + max_payload_bytes - 1u) / max_payload_bytes);
    if (network_ring_slots <= 1u) {
        uint32_t sent_chunks = 0u;

        dbg_printf("[DBG][NET] serial start task=%s idx=%u chunks=%u\n",
                   args->task_no,
                   (unsigned)args->file_index,
                   (unsigned)chunk_index);
        rc = network_send_serial(args,
                                 gopt,
                                 &rec,
                                 cfg,
                                 &rt,
                                 max_payload_bytes,
                                 network_ring_bytes,
                                 &sent_chunks);
        dbg_printf("[DBG][NET] serial done task=%s idx=%u sent_chunks=%u total_chunks=%u rc=%d\n",
                   args->task_no,
                   (unsigned)args->file_index,
                   (unsigned)sent_chunks,
                   (unsigned)chunk_index,
                   rc);
        if (rc != 0) {
            goto out;
        }
    } else {
        NetworkPipelineCtx pipe_ctx;
        pthread_t read_thread;
        pthread_t send_thread;
        int read_created = 0;
        int send_created = 0;
        uint32_t i;

        memset(&pipe_ctx, 0, sizeof(pipe_ctx));
        pipe_ctx.slots = (NetworkPipelineSlot *)calloc(network_ring_slots, sizeof(NetworkPipelineSlot));
        if (!pipe_ctx.slots) {
            fprintf(stderr, "Failed to allocate network pipeline slots: count=%u\n",
                    (unsigned)network_ring_slots);
            goto out;
        }
        for (i = 0; i < network_ring_slots; ++i) {
            pipe_ctx.slots[i].state = NETWORK_SLOT_FREE;
        }
        if (pthread_mutex_init(&pipe_ctx.lock, NULL) != 0 ||
            pthread_cond_init(&pipe_ctx.free_cond, NULL) != 0 ||
            pthread_cond_init(&pipe_ctx.ready_cond, NULL) != 0) {
            fprintf(stderr, "Failed to initialize network pipeline sync objects\n");
            free(pipe_ctx.slots);
            goto out;
        }
        pipe_ctx.slot_count = network_ring_slots;
        pipe_ctx.total_chunks = chunk_index;
        pipe_ctx.task_no = args->task_no;
        pipe_ctx.file_index = args->file_index;
        pipe_ctx.cfg = cfg;
        pipe_ctx.rt = &rt;
        pipe_ctx.gopt = gopt;
        pipe_ctx.max_payload_bytes = max_payload_bytes;
        pipe_ctx.network_ring_stride = network_ring_stride;
        pipe_ctx.network_ring_bytes = network_ring_bytes;
        pipe_ctx.total_file_bytes = total_file_bytes;
        pipe_ctx.start_lba = cur_lba;
        pipe_ctx.read_cmd_sectors = rt.nvme_cmd_sectors ? rt.nvme_cmd_sectors : 512u;
        pipe_ctx.proto_file_type = (uint32_t)rec.proto_file_type_code;
        pipe_ctx.verify_ddr_read = env_flag_enabled("SRC_REAL_NETWORK_VERIFY_DDR_READ");
        pipe_ctx.verify_bytes = env_u32_or_default("SRC_REAL_NETWORK_VERIFY_BYTES", 4096u);
        if (pipe_ctx.verify_bytes == 0u) {
            pipe_ctx.verify_bytes = 4096u;
        }

        dbg_printf("[DBG][NET] pipeline start task=%s idx=%u chunks=%u slots=%u\n",
                   args->task_no,
                   (unsigned)args->file_index,
                   (unsigned)pipe_ctx.total_chunks,
                   (unsigned)pipe_ctx.slot_count);

        if (pthread_create(&read_thread, NULL, network_read_thread, &pipe_ctx) != 0) {
            fprintf(stderr, "Failed to create network read thread\n");
            free(pipe_ctx.slots);
            (void)pthread_cond_destroy(&pipe_ctx.ready_cond);
            (void)pthread_cond_destroy(&pipe_ctx.free_cond);
            (void)pthread_mutex_destroy(&pipe_ctx.lock);
            goto out;
        }
        read_created = 1;
        if (pthread_create(&send_thread, NULL, network_send_thread, &pipe_ctx) != 0) {
            fprintf(stderr, "Failed to create network send thread\n");
            network_pipeline_set_error(&pipe_ctx, -1);
        } else {
            send_created = 1;
        }

        if (read_created) {
            pthread_join(read_thread, NULL);
        }
        if (send_created) {
            pthread_join(send_thread, NULL);
        }

        rc = pipe_ctx.error_rc;
        chunk_index = pipe_ctx.next_send_chunk;
        if (rc == 0 && pipe_ctx.next_send_chunk == pipe_ctx.total_chunks) {
            rc = 0;
        } else if (rc == 0) {
            rc = -1;
        }

        dbg_printf("[DBG][NET] pipeline done task=%s idx=%u sent_chunks=%u total_chunks=%u rc=%d\n",
                   args->task_no,
                   (unsigned)args->file_index,
                   (unsigned)pipe_ctx.next_send_chunk,
                   (unsigned)pipe_ctx.total_chunks,
                   rc);

        (void)pthread_cond_destroy(&pipe_ctx.ready_cond);
        (void)pthread_cond_destroy(&pipe_ctx.free_cond);
        (void)pthread_mutex_destroy(&pipe_ctx.lock);
        free(pipe_ctx.slots);
        if (rc != 0) {
            goto out;
        }
    }

    printf("network_send_done task=%s file_index=%u proto_file_type=%u file_bytes=%" PRIu64 " chunks=%u\n",
           args->task_no,
           (unsigned)args->file_index,
           (unsigned)args->proto_file_type,
           total_file_bytes,
           (unsigned)chunk_index);
    rc = 0;

out:
    channel_runtime_close(&rt);
    return rc;
}

static int configure_tcp_for_channel(const ChannelConfig *cfg, TcpTransferConfig *tcp_cfg)
{
    uint32_t switch_input;

    if (!cfg || !tcp_cfg) {
        return -1;
    }

    /* FPGA top TCP switch: S00=ch0 HIGH_I, S01=ch1 HIGH_Q, S02=ch2 LOW/CALIB. */
    if (cfg->id == HIGH_I_CHANNEL_ID) {
        switch_input = 0u;
    } else if (cfg->id == HIGH_Q_CHANNEL_ID) {
        switch_input = 1u;
    } else if (cfg->id == LOW_SPEED_CHANNEL_ID) {
        switch_input = 2u;
    } else {
        return -1;
    }
    if (env_u32_allow_zero("SRC_REAL_TCP_SWITCH_INPUT", 15u, &switch_input)) {
        dbg_printf("[DBG][NET] TCP switch input override ch=%d input=%u\n",
                   cfg->id,
                   (unsigned)switch_input);
    }

    tcp_cfg->switch_base = TCP_SWITCH_BASE_DEFAULT;
    tcp_cfg->switch_input_select = switch_input;
    tcp_cfg->dma_base = cfg->dma_base;
    tcp_cfg->desc_cpu_base = cfg->desc_cpu_base;
    tcp_cfg->desc_dma_base = cfg->desc_dma_base;
    tcp_cfg->ddr_dma_base = cfg->ddr_hw_base;
    return 0;
}

static uint8_t start_storage_for_last_task(uint8_t *failure_type)
{
    PlannedFile planned[3];
    int planned_count = 0;
    int next_index = 0;
    int i;
    uint8_t acq_type = g_last_task.valid ? acq_type_from_task_mode(g_last_task.task_file_mode) : 0u;

    if (failure_type) {
        *failure_type = FAIL_TYPE_NONE;
    }
    if (!g_last_task.valid) {
        dbg_printf("[DBG][CAPTURE] prepare rejected: no valid 0x11\n");
        return ACK_INVALID_PARAM;
    }
    if (storage_any_running() || storage_any_live_worker() || g_pending_storage_stop.active) {
        if (failure_type) {
            *failure_type = failure_type_from_acq_type(acq_type);
        }
        LOG_WARN("CAPTURE", "Storage prepare rejected: worker still live");
        return ACK_RETRYING;
    }
    for (i = 0; i < (int)STORAGE_TASK_COUNT; ++i) {
        storage_task_reset_runtime(&storage_tasks[i]);
        storage_tasks[i].output_used = 0u;
        storage_tasks[i].output[0] = '\0';
    }
    if (build_file_plan(&g_last_task, planned, &planned_count) != 0 ||
        planned_count <= 0 ||
        planned_count > (int)STORAGE_TASK_COUNT) {
        if (failure_type) {
            *failure_type = failure_type_from_acq_type(acq_type);
        }
        dbg_printf("[DBG][CAPTURE] build plan failed task=%s mode=0x%02X count=%d\n",
                   g_last_task.task_id, g_last_task.task_file_mode, planned_count);
        (void)task_update_status(g_last_task.task_id, TASK_FAILED);
        return ACK_INVALID_PARAM;
    }
    { uint32_t mask = 0u; char count_text[16];
      for (i = 0; i < planned_count; ++i) mask |= 1u << planned[i].channel_id;
      storage_supervisor_init(&g_storage_supervisor, mask);
      storage_commit_state_reset(&g_storage_commit_state);
      (void)snprintf(count_text, sizeof(count_text), "%d", planned_count);
      (void)setenv("SRC_REAL_SUPERVISED_CHANNEL_COUNT", count_text, 1);
    }
    for (i = 0; i < planned_count; ++i) {
        if (!find_channel(planned[i].channel_id)) {
            if (failure_type) {
                *failure_type = failure_type_from_acq_type(acq_type);
            }
            LOG_WARN("CAPTURE", "Unsupported storage channel: task=%s ch=%d mode=0x%02X",
                     g_last_task.task_id, planned[i].channel_id, g_last_task.task_file_mode);
            dbg_printf("[DBG][CAPTURE] unsupported channel task=%s ch=%d mode=0x%02X\n",
                       g_last_task.task_id, planned[i].channel_id, g_last_task.task_file_mode);
            (void)task_update_status(g_last_task.task_id, TASK_FAILED);
            return ACK_FAILED;
        }
    }
    if (file_next_index_by_task(g_last_task.task_id, &next_index) != 0) {
        if (failure_type) {
            *failure_type = failure_type_from_acq_type(acq_type);
        }
        dbg_printf("[DBG][CAPTURE] file_next_index failed task=%s\n", g_last_task.task_id);
        (void)task_update_status(g_last_task.task_id, TASK_FAILED);
        return ACK_FAILED;
    }
    if (next_index <= 0 || next_index > (INT_MAX - planned_count + 1)) {
        if (failure_type) {
            *failure_type = failure_type_from_acq_type(acq_type);
        }
        dbg_printf("[DBG][CAPTURE] file_index overflow task=%s next=%d count=%d\n",
                   g_last_task.task_id, next_index, planned_count);
        (void)task_update_status(g_last_task.task_id, TASK_FAILED);
        return ACK_FAILED;
    }

    for (i = 0; i < planned_count; ++i) {
        planned[i].file_index = next_index + i;
        dbg_printf("[DBG][CAPTURE] launch storage task=%s ch=%d idx=%d size=%" PRIu64 "\n",
                   g_last_task.task_id, planned[i].channel_id, planned[i].file_index, planned[i].size_bytes);
        if (start_storage_worker(&planned[i], g_last_task.task_id, g_last_task.overpass_time) != 0) {
            int missing;
            if (failure_type) {
                *failure_type = failure_type_from_acq_type(acq_type);
            }
            dbg_printf("[DBG][CAPTURE] start_storage_worker failed task=%s ch=%d\n",
                       g_last_task.task_id, planned[i].channel_id);
            for (missing = i; missing < planned_count; ++missing) {
                storage_supervisor_mark_unavailable(&g_storage_supervisor,
                                                    (uint32_t)planned[missing].channel_id,
                                                    "fork_exec_failed");
            }
            if (storage_begin_prepare_wait(g_last_task.task_id, acq_type,
                                           failure_type ? *failure_type : FAIL_TYPE_NONE) == 0) {
                storage_pending_start_fail();
                return STORAGE_START_PENDING;
            }
            (void)request_storage_stop_all();
            (void)task_update_status(g_last_task.task_id, TASK_FAILED);
            return ACK_FAILED;
        }
    }
    /* Preparation is acknowledged only after the main-loop state machine has
     * observed READY from every child.  Do not sleep here: UART input and
     * worker event pipes must continue to be serviced while children wait on
     * their ARM/RUN gates. */
    if (storage_begin_prepare_wait(g_last_task.task_id, acq_type,
                                   failure_type ? *failure_type : FAIL_TYPE_NONE) != 0) {
        (void)request_storage_stop_all();
        (void)task_update_status(g_last_task.task_id, TASK_FAILED);
        return ACK_FAILED;
    }
    dbg_printf("[DBG][CAPTURE] storage workers launched; waiting asynchronously for READY task=%s count=%d\n",
               g_last_task.task_id, planned_count);
    return STORAGE_START_PENDING;
}

static uint8_t clamp_u8_int(int value)
{
    if (value <= 0) {
        return 0u;
    }
    if (value > 255) {
        return 255u;
    }
    return (uint8_t)value;
}

static uint8_t file_index_to_proto(int file_index)
{
    if (file_index <= 0) {
        return 0u;
    }
    if (file_index > 256) {
        return 255u;
    }
    /* Database file_index starts from 1; protocol file index starts from 0. */
    return (uint8_t)(file_index - 1);
}

static void send_file_list_records(const TaskFileListRecord *records, int count)
{
    int i;

    if (!records || count <= 0) {
        proto_send_file_list("", ACK_SUCCESS, 0u, 0u, 0u, 0u, 0u, 0u,
                             FILE_LIST_FLAG_END, 0u);
        return;
    }

    for (i = 0; i < count; ++i) {
        proto_send_file_list(records[i].task_id,
                             ACK_SUCCESS,
                             clamp_u8_int(records[i].total_files),
                             file_index_to_proto(records[i].file_index),
                             clamp_u8_int(records[i].proto_file_type_code),
                             (uint64_t)records[i].file_size,
                             (uint64_t)records[i].start_sector,
                             (uint32_t)records[i].sector_count,
                             (i == (count - 1)) ? FILE_LIST_FLAG_END : FILE_LIST_FLAG_CONTINUE,
                             clamp_u8_int(records[i].calibration_type));
    }
}

/* ================== Protocol handling ================== */

static void copy_proto_task_id(char out[12], const char in[11])
{
    memset(out, 0, 12u);
    memcpy(out, in, 11u);
}

static int cleanup_stale_empty_task_before_create(const char *task_id)
{
    TaskInfo info;
    int total;

    if (!task_id || task_id[0] == '\0') {
        return -1;
    }
    if (storage_any_running() || transfer_task.state == RUNNING) {
        return 0;
    }

    memset(&info, 0, sizeof(info));
    if (task_query(task_id, &info) != 0) {
        return 0;
    }

    total = file_count_by_task(task_id);
    if (total < 0) {
        LOG_ERROR("FILE_OP", "Stale task check count failed: task=%s", task_id);
        return -1;
    }
    if (total > 0) {
        return 0;
    }

    if (metadata_delete_task_all_supported_channels(task_id) != 0) {
        LOG_WARN("FILE_OP", "Stale task metadata cleanup failed: task=%s", task_id);
    }
    if (task_delete(task_id) != 0) {
        LOG_ERROR("FILE_OP", "Stale empty task delete failed: task=%s", task_id);
        return -1;
    }

    dbg_printf("[DBG][FILE_OP] stale empty task removed before create task=%s\n", task_id);
    return 0;
}

static int ensure_task_for_capture_config(const char *task_id,
                                          time_t overpass_timestamp,
                                          const char *description,
                                          const char *payload_json)
{
    TaskInfo info;

    if (!task_id || task_id[0] == '\0') {
        return -1;
    }

    if (cleanup_stale_empty_task_before_create(task_id) != 0) {
        LOG_ERROR("SYSTEM", "Stale task cleanup failed before create: %s", task_id);
        dbg_printf("[DBG][PROTO] 0x11 stale cleanup failed task=%s\n", task_id);
        return -1;
    }

    memset(&info, 0, sizeof(info));
    if (task_query(task_id, &info) == 0) {
        (void)task_update_status(task_id, TASK_PENDING);
        dbg_printf("[DBG][PROTO] 0x11 reuse existing task=%s next file_index will append\n",
                   task_id);
        return 0;
    }

    return task_create_with_payload(task_id, overpass_timestamp, description, payload_json);
}

static uint8_t clear_file_list_records(void)
{
    if (storage_any_running() || transfer_task.state == RUNNING) {
        LOG_WARN("FILE_LIST", "Reject clear while worker is running");
        return ACK_RETRYING;
    }
    if (metadata_clear_all_supported_channels() != 0) {
        LOG_ERROR("FILE_LIST", "Clear metadata failed");
        return ACK_FAILED;
    }
    if (file_clear_all() != 0) {
        LOG_ERROR("FILE_LIST", "Clear all file records failed");
        return ACK_FAILED;
    }
    return ACK_SUCCESS;
}

static uint8_t delete_file_from_command(const CmdFileOp *op)
{
    char task_id[12];
    int db_file_index;
    FileRecord rec;

    if (!op) {
        return ACK_INVALID_PARAM;
    }
    if (storage_any_running() || transfer_task.state == RUNNING) {
        return ACK_RETRYING;
    }

    copy_proto_task_id(task_id, op->task_id);
    if (task_id[0] == '\0') {
        return ACK_INVALID_PARAM;
    }

    db_file_index = (int)op->file_index + 1;
    memset(&rec, 0, sizeof(rec));
    if (file_query_by_index(task_id, db_file_index, &rec) != 0) {
        LOG_WARN("FILE_OP", "Delete target not found: task=%s proto_index=%u",
                 task_id, (unsigned)op->file_index);
        return ACK_FAILED;
    }

    if (rec.proto_file_type_code != (int)op->file_type) {
        LOG_WARN("FILE_OP",
                 "Delete target type mismatch: task=%s index=%d req_type=0x%02X db_type=0x%02X",
                 task_id, db_file_index, op->file_type, rec.proto_file_type_code);
        return ACK_INVALID_PARAM;
    }
    if (op->file_type == FILE_TYPE_CALIB && rec.calibration_type != (int)op->calibration_type) {
        LOG_WARN("FILE_OP",
                 "Delete target calibration mismatch: task=%s index=%d req=0x%02X db=0x%02X",
                 task_id, db_file_index, op->calibration_type, rec.calibration_type);
        return ACK_INVALID_PARAM;
    }

    if (metadata_delete_file(rec.channel_id, task_id, db_file_index) != 0) {
        LOG_WARN("FILE_OP",
                 "Metadata entry not found during delete, continuing DB cleanup: task=%s ch=%d idx=%d",
                 task_id, rec.channel_id, db_file_index);
    }

    if (file_delete(rec.id) != 0) {
        LOG_ERROR("FILE_OP", "DB delete failed: task=%s index=%d", task_id, db_file_index);
        return ACK_FAILED;
    }

    {
        int total = file_count_by_task(task_id);
        if (total < 0) {
            LOG_ERROR("FILE_OP", "DB count failed after delete: task=%s", task_id);
            return ACK_FAILED;
        }
        if (total == 0) {
            int metadata_cleanup_rc = metadata_delete_task_all_supported_channels(task_id);
            if (task_delete(task_id) != 0) {
                LOG_ERROR("FILE_OP", "Task delete failed after last file delete: task=%s", task_id);
                return ACK_FAILED;
            }
            if (metadata_cleanup_rc != 0) {
                LOG_ERROR("FILE_OP", "Metadata task cleanup failed: task=%s", task_id);
                return ACK_FAILED;
            }
            dbg_printf("[DBG][FILE_OP] task removed after last file delete task=%s\n", task_id);
        } else if (task_update_total_files(task_id, total) != 0) {
            LOG_ERROR("FILE_OP", "Task total update failed after delete: task=%s total=%d",
                      task_id, total);
            return ACK_FAILED;
        }
    }
    return ACK_SUCCESS;
}

static uint8_t start_network_from_command(const CmdFileOp *op)
{
    char task_id[12];
    int db_file_index;
    FileRecord rec;

    if (!op) {
        return ACK_INVALID_PARAM;
    }
    if (storage_any_running() || transfer_task.state == RUNNING) {
        return ACK_FAILED;
    }

    copy_proto_task_id(task_id, op->task_id);
    if (task_id[0] == '\0') {
        return ACK_INVALID_PARAM;
    }

    db_file_index = (int)op->file_index + 1;
    memset(&rec, 0, sizeof(rec));
    if (file_query_by_index(task_id, db_file_index, &rec) != 0) {
        LOG_WARN("NETWORK", "Network target not found: task=%s proto_index=%u",
                 task_id, (unsigned)op->file_index);
        return ACK_FAILED;
    }
    if (rec.proto_file_type_code != (int)op->file_type) {
        LOG_WARN("NETWORK",
                 "Network target type mismatch: task=%s index=%d req_type=0x%02X db_type=0x%02X",
                 task_id, db_file_index, op->file_type, rec.proto_file_type_code);
        return ACK_INVALID_PARAM;
    }
    if (op->file_type == FILE_TYPE_CALIB && rec.calibration_type != (int)op->calibration_type) {
        LOG_WARN("NETWORK",
                 "Network target calibration mismatch: task=%s index=%d req=0x%02X db=0x%02X",
                 task_id, db_file_index, op->calibration_type, rec.calibration_type);
        return ACK_INVALID_PARAM;
    }
    if (!find_channel(rec.channel_id)) {
        LOG_WARN("NETWORK", "Network target channel unsupported: task=%s index=%d ch=%d",
                 task_id, db_file_index, rec.channel_id);
        return ACK_FAILED;
    }
    if (start_network_worker(&rec) != 0) {
        return ACK_FAILED;
    }
    return ACK_SUCCESS;
}

static int probe_one_storage_channel(const ChannelConfig *cfg, GlobalOptions gopt)
{
    ChannelRuntime rt;
    int rc = -1;

    if (!cfg) {
        return -1;
    }

    memset(&rt, 0, sizeof(rt));
    dbg_verbose_printf("[DBG][DISK] probe start ch=%d name=%s timeout_us=%u\n",
                       cfg->id, cfg->name, (unsigned)gopt.timeout_us);

    if (channel_runtime_open(&rt, cfg, gopt) != 0) {
        LOG_ERROR("DISK", "Channel open failed: ch=%d name=%s", cfg->id, cfg->name);
        dbg_printf("[DBG][DISK] open failed ch=%d name=%s\n", cfg->id, cfg->name);
        return -1;
    }

    if (nvme_probe(&rt) != 0) {
        LOG_ERROR("DISK", "NVMe probe failed: ch=%d name=%s", cfg->id, cfg->name);
        dbg_printf("[DBG][DISK] nvme_probe failed ch=%d name=%s\n", cfg->id, cfg->name);
        goto out;
    }

    if (rt.nvme_block_size == 0u ||
        rt.nvme_max_dts_bytes < rt.nvme_block_size) {
        LOG_ERROR("DISK",
                  "Invalid NVMe capability: ch=%d block=%u max_dts=%u max_lba=0x%08" PRIx64,
                  cfg->id,
                  rt.nvme_block_size,
                  rt.nvme_max_dts_bytes,
                  rt.nvme_max_lba);
        dbg_printf("[DBG][DISK] capability failed ch=%d block=%u max_dts=%u max_lba=0x%08" PRIx64 "\n",
                   cfg->id,
                   rt.nvme_block_size,
                   rt.nvme_max_dts_bytes,
                   rt.nvme_max_lba);
        goto out;
    }
    if (rt.nvme_max_lba == 0u) {
        LOG_ERROR("DISK", "NVMe max_lba unavailable: ch=%d name=%s", cfg->id, cfg->name);
        dbg_printf("[DBG][DISK] max_lba unavailable ch=%d name=%s\n", cfg->id, cfg->name);
        goto out;
    }

    LOG_INFO("DISK",
             "NVMe probe OK: ch=%d name=%s block=%u max_dts=%u max_lba=0x%08" PRIx64,
             cfg->id,
             cfg->name,
             rt.nvme_block_size,
             rt.nvme_max_dts_bytes,
             rt.nvme_max_lba);
    dbg_verbose_printf("[DBG][DISK] probe ok ch=%d name=%s block=%u max_dts=%u max_lba=0x%08" PRIx64 "\n",
                       cfg->id,
                       cfg->name,
                       rt.nvme_block_size,
                       rt.nvme_max_dts_bytes,
                       rt.nvme_max_lba);
    rc = 0;

out:
    channel_runtime_close(&rt);
    return rc;
}

static uint8_t get_status_query_result(void)
{
    const int required_channels[] = {HIGH_I_CHANNEL_ID, HIGH_Q_CHANNEL_ID, LOW_SPEED_CHANNEL_ID};
    GlobalOptions gopt;
    size_t i;

    memset(&gopt, 0, sizeof(gopt));
    gopt.timeout_us = env_u32_or_default("SRC_REAL_STATUS_TIMEOUT_US", DEFAULT_TIMEOUT_US);
    gopt.dry_run = false;
    gopt.skip_link_check = false;

    for (i = 0u; i < (sizeof(required_channels) / sizeof(required_channels[0])); ++i) {
        const ChannelConfig *cfg = find_channel(required_channels[i]);
        dbg_printf("[DBG][DISK] status check probe ch=%d\n", required_channels[i]);
        if (!cfg || probe_one_storage_channel(cfg, gopt) != 0) {
            dbg_printf("[DBG][DISK] status check failed ch=%d\n", required_channels[i]);
            return ACK_FAILED;
        }
    }
    dbg_verbose_printf("[DBG][DISK] status check ok channels=ch0,ch1,ch2\n");
    return ACK_SUCCESS;
}

void handle_frame(uint8_t *f)
{
    uint8_t cmd = f[2];

    switch (cmd) {
    /* ---------- Task info (0x11) ---------- */
    case CMD_TASK_INFO: {
        CmdTaskInfo *task_cmd = (CmdTaskInfo *)f;
        char task_id[12] = {0};
        char overpass_time_str[15] = {0};
        char json_desc[512] = {0};
        char payload_json[2048] = {0};
        time_t overpass_timestamp;
        int ret;
        uint16_t envelope_duration;

        memcpy(task_id, task_cmd->task_id, 11u);
        memcpy(overpass_time_str, task_cmd->overpass_time, 14u);
        envelope_duration = proto_be16(task_cmd->envelope_duration);
        dbg_printf("[DBG][PROTO] RX 0x11 task=%s overpass=%s mode=0x%02X clk=%u dur=%u\n",
                   task_id,
                   overpass_time_str,
                   task_cmd->task_file_mode,
                   (unsigned)task_cmd->envelope_clock,
                   (unsigned)envelope_duration);
        system_emit_line(STORAGE_LOG_DEBUG, "serial_cmd_rx cmd=0x%02X action=prestart task_no=%s channel_mask=0x%02X storage_state=%d",
                         cmd,
                         task_id,
                         task_cmd->task_file_mode,
                         storage_state_summary());
        if (parse_overpass_time(overpass_time_str, &overpass_timestamp) != 0) {
            LOG_WARN("SYSTEM", "Invalid overpass_time in 0x11: %s", overpass_time_str);
            dbg_printf("[DBG][PROTO] 0x11 invalid overpass_time=%s\n", overpass_time_str);
            proto_send_ack(cmd, ACK_INVALID_PARAM);
            system_emit_line(STORAGE_LOG_SUMMARY, "serial_cmd_ack cmd=0x%02X action=prestart task_no=%s ack_status=0x%02X result_code=0x%02X worker_state=%d storage_state=%d reason=invalid_overpass_time",
                             cmd, task_id, ACK_INVALID_PARAM, ACK_INVALID_PARAM,
                             RUNNING, storage_state_summary());
            break;
        }
        if (storage_any_running() || storage_any_live_worker() ||
            g_pending_storage_start.active || g_pending_storage_stop.active ||
            transfer_task.state == RUNNING) {
            LOG_WARN("SYSTEM", "Reject 0x11 while worker is running: storage_state=%d transfer_state=%d",
                     storage_state_summary(),
                     transfer_task.state);
            dbg_printf("[DBG][PROTO] 0x11 busy storage_state=%d transfer_state=%d\n",
                       storage_state_summary(),
                       transfer_task.state);
            proto_send_ack(cmd, ACK_RETRYING);
            system_emit_line(STORAGE_LOG_SUMMARY, "serial_cmd_ack cmd=0x%02X action=prestart task_no=%s ack_status=0x%02X result_code=0x%02X worker_state=%d storage_state=%d reason=busy",
                             cmd, task_id, ACK_RETRYING, ACK_RETRYING,
                             RUNNING, storage_state_summary());
            break;
        }

        build_task_description_json(json_desc, sizeof(json_desc), task_cmd);
        build_task_payload_json(payload_json, sizeof(payload_json), task_cmd);
        ret = ensure_task_for_capture_config(task_id, overpass_timestamp, json_desc, payload_json);
        if (ret == 0) {
            uint8_t prep_failure_type = FAIL_TYPE_NONE;
            uint8_t prep_result;

            g_last_task.valid = true;
            memset(g_last_task.task_id, 0, sizeof(g_last_task.task_id));
            snprintf(g_last_task.task_id, sizeof(g_last_task.task_id), "%s", task_id);
            g_last_task.overpass_time = overpass_timestamp;
            g_last_task.task_file_mode = task_cmd->task_file_mode;
            g_last_task.usb_transfer_enable = task_cmd->usb_transfer_enable;
            g_last_task.calibration_type = task_cmd->calibration_type;
            g_last_task.envelope_clock_raw = task_cmd->envelope_clock;
            g_last_task.envelope_duration = envelope_duration;

            prep_result = start_storage_for_last_task(&prep_failure_type);
            if (prep_result == ACK_SUCCESS) {
                LOG_INFO("SYSTEM",
                         "TASK CONFIG OK and storage prepared: task=%s file_mode=0x%02X usb=0x%02X",
                         task_id, task_cmd->task_file_mode, task_cmd->usb_transfer_enable);
                dbg_printf("[DBG][PROTO] 0x11 ACK success task=%s storage prepared\n", task_id);
                proto_send_ack(cmd, ACK_SUCCESS);
                system_emit_line(STORAGE_LOG_SUMMARY, "serial_cmd_ack cmd=0x%02X action=prestart task_no=%s ack_status=0x%02X result_code=0x%02X worker_state=%d storage_state=%d reason=storage_prepared",
                                 cmd, task_id, ACK_SUCCESS, ACK_SUCCESS,
                                 RUNNING, storage_state_summary());
            } else if (prep_result == STORAGE_START_PENDING) {
                dbg_printf("[DBG][PROTO] 0x11 accepted; PREP ACK deferred until all workers READY task=%s\n",
                           task_id);
            } else {
                LOG_ERROR("SYSTEM",
                          "TASK CONFIG OK but storage prepare failed: task=%s file_mode=0x%02X result=0x%02X failure=0x%02X",
                          task_id, task_cmd->task_file_mode, prep_result, prep_failure_type);
                dbg_printf("[DBG][PROTO] 0x11 ACK fail task=%s prep_result=0x%02X failure=0x%02X\n",
                           task_id, prep_result, prep_failure_type);
                system_emit_line(STORAGE_LOG_ALWAYS_CRITICAL, "storage_start_failed task=%s phase=prepare reason=storage_prepare_failed"
                                 " result_code=0x%02X", task_id, prep_result);
                proto_send_ack(cmd, prep_result);
                system_emit_line(STORAGE_LOG_SUMMARY, "serial_cmd_ack cmd=0x%02X action=prestart task_no=%s ack_status=0x%02X result_code=0x%02X worker_state=%d storage_state=%d reason=storage_prepare_failed",
                                 cmd, task_id, prep_result, prep_result,
                                 ERROR, storage_state_summary());
            }
        } else {
            LOG_ERROR("SYSTEM", "TASK CONFIG FAILED: %s", task_id);
            dbg_printf("[DBG][PROTO] 0x11 task_create failed task=%s\n", task_id);
            proto_send_ack(cmd, ACK_FAILED);
            system_emit_line(STORAGE_LOG_SUMMARY, "serial_cmd_ack cmd=0x%02X action=prestart task_no=%s ack_status=0x%02X result_code=0x%02X worker_state=%d storage_state=%d reason=task_create_failed",
                             cmd, task_id, ACK_FAILED, ACK_FAILED,
                             ERROR, storage_state_summary());
        }
        break;
    }

    /* ---------- Acquisition control (0x21) ---------- */
    case CMD_ACQ_CTRL: {
        CmdAcqCtrl *acq_cmd = (CmdAcqCtrl *)f;
        uint8_t result = ACK_SUCCESS;
        uint8_t failure_type = FAIL_TYPE_NONE;
        uint8_t acq_type = g_last_task.valid ? acq_type_from_task_mode(g_last_task.task_file_mode) : 0u;
        int storage_state = storage_state_summary();

        dbg_printf("[DBG][PROTO] RX 0x21 switch=0x%02X task_valid=%u task=%s storage_state=%d\n",
                   acq_cmd->switch_flag,
                   g_last_task.valid ? 1u : 0u,
                   g_last_task.valid ? g_last_task.task_id : "",
                   storage_state);
        system_emit_line(STORAGE_LOG_DEBUG, "serial_cmd_rx cmd=0x%02X action=%s task_no=%s channel_mask=0x%02X storage_state=%d",
                         cmd,
                         acq_cmd->switch_flag == SWITCH_ON ? "start" :
                         (acq_cmd->switch_flag == SWITCH_OFF ? "stop" : "unknown"),
                         g_last_task.valid ? g_last_task.task_id : "",
                         g_last_task.valid ? g_last_task.task_file_mode : 0u,
                         storage_state);

        if (acq_cmd->switch_flag == SWITCH_ON) {
            if (g_pending_storage_start.active || g_pending_storage_stop.active) {
                /* Do not consume a second START while PREP/START is being
                 * progressed by the main loop. */
                proto_send_acq_ack(ACK_RETRYING, acq_type,
                                   FAIL_TYPE_LOW | FAIL_TYPE_HIGH_I | FAIL_TYPE_HIGH_Q);
                system_emit_line(STORAGE_LOG_SUMMARY, "serial_cmd_ack cmd=0x%02X action=start task_no=%s ack_status=0x%02X result_code=0x%02X worker_state=%d storage_state=%d reason=busy",
                                 cmd, g_last_task.valid ? g_last_task.task_id : "",
                                 ACK_RETRYING, ACK_RETRYING, RUNNING, storage_state_summary());
                return;
            }
            if (!g_last_task.valid ||
                storage_begin_run_barrier(g_last_task.task_id, acq_type,
                                          FAIL_TYPE_LOW | FAIL_TYPE_HIGH_I |
                                          FAIL_TYPE_HIGH_Q) != 0) {
                result = ACK_FAILED;
                failure_type = FAIL_TYPE_LOW | FAIL_TYPE_HIGH_I | FAIL_TYPE_HIGH_Q;
                (void)request_storage_stop_all();
                system_emit_line(STORAGE_LOG_ALWAYS_CRITICAL, "storage_start_failed task=%s phase=run_barrier reason=start_barrier_failed",
                                 g_last_task.valid ? g_last_task.task_id : "");
            } else {
                /* The RUN ACK is emitted by storage_service_pending_start once
                 * every worker has reported STRUCTURED RUNNING. */
                return;
            }
            proto_send_acq_ack(result, acq_type, failure_type);
            system_emit_line(STORAGE_LOG_SUMMARY, "serial_cmd_ack cmd=0x%02X action=start task_no=%s ack_status=0x%02X result_code=0x%02X worker_state=%d storage_state=%d reason=%s",
                             cmd,
                             g_last_task.valid ? g_last_task.task_id : "",
                             result,
                             result,
                             result == ACK_SUCCESS ? RUNNING : ERROR,
                             storage_state_summary(),
                             result == ACK_SUCCESS ? "all_workers_started" : "start_barrier_failed");
            system_emit_line(STORAGE_LOG_SUMMARY, "serial_cmd_result cmd=start task=%s status=%s storage_state=%d reason=%s",
                             g_last_task.valid ? g_last_task.task_id : "",
                             result == ACK_SUCCESS ? "success" : "failed",
                             storage_state_summary(),
                             result == ACK_SUCCESS ? "all_workers_started" : "start_barrier_failed");
        } else if (acq_cmd->switch_flag == SWITCH_OFF) {
            if (!g_last_task.valid || acq_type == 0u) {
                LOG_WARN("CAPTURE", "Stop rejected: no valid 0x11 task context");
                result = ACK_INVALID_PARAM;
                dbg_printf("[DBG][PROTO] 0x21 stop rejected result=0x%02X\n", result);
                system_emit_line(STORAGE_LOG_SUMMARY, "serial_cmd_result cmd=stop task=%s status=failed storage_done=0 data_persisted=0 integrity_ok=0 reason=no_valid_task",
                                 g_last_task.valid ? g_last_task.task_id : "");
            } else if (g_pending_storage_stop.active) {
                /* STOP is idempotent while the first stop is draining.  The
                 * original ACK is emitted by storage_service_pending_stop. */
                LOG_INFO("CAPTURE", "Duplicate STOP ignored while task=%s is draining",
                         g_pending_storage_stop.task_id);
                break;
            } else {
                StorageParentStopTimeoutConfig parent_stop_config;
                uint64_t stop_started_us;
                if (g_pending_storage_start.active) {
                    /* A manual STOP supersedes a still-pending PREP/START
                     * barrier; its deferred 0x11 failure ACK is completed by
                     * the same bounded worker reap. */
                    storage_pending_start_fail();
                }
                int stopped = request_storage_stop_all();
                LOG_INFO("CAPTURE",
                         "Stop capture command accepted: task=%s storage_state=%d stop_requested=%d",
                         g_last_task.task_id,
                         storage_state,
                         stopped);
                dbg_printf("[DBG][PROTO] 0x21 stop waiting task=%s storage_state=%d stop_requested=%d\n",
                           g_last_task.task_id,
                           storage_state,
                           stopped);
                memset(&g_pending_storage_stop, 0, sizeof(g_pending_storage_stop));
                g_pending_storage_stop.active = true;
                snprintf(g_pending_storage_stop.task_id,
                         sizeof(g_pending_storage_stop.task_id), "%s", g_last_task.task_id);
                g_pending_storage_stop.acq_type = acq_type;
                g_pending_storage_stop.failure_type = failure_type;
                g_pending_storage_stop.requested_workers = stopped;
                storage_ipc_parent_stop_timeout_config(&parent_stop_config,
                                                       DEFAULT_TIMEOUT_US);
                stop_started_us = storage_ipc_monotonic_us();
                g_pending_storage_stop.deadline_us = storage_ipc_saturating_add_u64(
                    stop_started_us, parent_stop_config.parent_timeout_us);
                system_emit_line(STORAGE_LOG_SUMMARY, "storage_parent_stop_config task=%s parent_timeout_us=%" PRIu64
                                 " source=%s dma_quiesce_us=%" PRIu64
                                 " stop_harvest_us=%" PRIu64
                                 " writer_drain_us=%" PRIu64
                                 " nvme_abort_us=%" PRIu64
                                 " margin_us=%" PRIu64,
                                 g_pending_storage_stop.task_id,
                                 parent_stop_config.parent_timeout_us,
                                 storage_ipc_parent_stop_timeout_source(
                                     parent_stop_config.source),
                                 parent_stop_config.dma_quiesce_us,
                                 parent_stop_config.stop_harvest_us,
                                 parent_stop_config.writer_drain_us,
                                 parent_stop_config.nvme_abort_us,
                                 parent_stop_config.margin_us);
                /* The binary STOP ACK is deferred until all worker terminals
                 * are observed.  No text ACK is emitted here. */
                return;
            }
            proto_send_acq_ack(result, acq_type, failure_type);
            system_emit_line(STORAGE_LOG_SUMMARY, "serial_cmd_ack cmd=0x%02X action=stop task_no=%s ack_status=0x%02X result_code=0x%02X worker_state=%d storage_state=%d reason=%s",
                             cmd,
                             g_last_task.valid ? g_last_task.task_id : "",
                             result,
                             result,
                             result == ACK_SUCCESS ? IDLE : ERROR,
                             storage_state_summary(),
                             result == ACK_SUCCESS ? "stop_complete" : "stop_failed");
        } else {
            LOG_WARN("CAPTURE", "Invalid switch flag: 0x%02X", acq_cmd->switch_flag);
            dbg_printf("[DBG][PROTO] 0x21 invalid switch=0x%02X\n", acq_cmd->switch_flag);
            proto_send_acq_ack(ACK_INVALID_PARAM, acq_type, failure_type);
            system_emit_line(STORAGE_LOG_SUMMARY, "serial_cmd_ack cmd=0x%02X action=unknown task_no=%s ack_status=0x%02X result_code=0x%02X worker_state=%d storage_state=%d reason=invalid_switch",
                             cmd,
                             g_last_task.valid ? g_last_task.task_id : "",
                             ACK_INVALID_PARAM,
                             ACK_INVALID_PARAM,
                             ERROR,
                             storage_state_summary());
        }
        break;
    }

    /* ---------- USB transfer control (0x31) ---------- */
    case CMD_USB_TRANSFER:
        if (f[3] == 0x11) {
            LOG_INFO("TRANSFER", "Start transfer command received");
            start_task(&transfer_task, "./send_file", "data.bin");
        } else {
            LOG_INFO("TRANSFER", "Stop transfer command received");
            stop_task(&transfer_task);
        }
        proto_send_ack(cmd, ACK_SUCCESS);
        break;

    /* ---------- File list (0x41) ---------- */
    case CMD_FILE_LIST: {
        CmdFileList *file_cmd = (CmdFileList *)f;
        if (file_cmd->control == FILE_LIST_READ) {
            TaskFileListRecord records[32];
            int count = task_file_list_query(records, 32);
            LOG_INFO("PROTO", "FILE LIST REQ: merged rows=%d", count);
            dbg_printf("[DBG][PROTO] RX 0x41 list count=%d\n", count);
            if (count >= 0) {
                int i;
                for (i = 0; i < count; ++i) {
                    LOG_INFO("FILE_LIST",
                             "task=%s file_index=%d file_type=%s ch=%d size=%" PRIu64
                             " start_sector=%" PRIu64 " sectors=%" PRIu64,
                             records[i].task_id,
                             records[i].file_index,
                             records[i].file_type,
                             records[i].channel_id,
                             records[i].file_size,
                             records[i].start_sector,
                             records[i].sector_count);
                }
                send_file_list_records(records, count);
            } else {
                proto_send_ack(cmd, ACK_FAILED);
            }
        } else if (file_cmd->control == FILE_LIST_SYNC_FLASH) {
            uint8_t sync_result;
            LOG_INFO("PROTO", "FILE LIST flash sync request received");
            sync_result = sync_filelist_db_to_flash();
            dbg_printf("[DBG][PROTO] RX 0x41 sync flash result=0x%02X\n", sync_result);
            if (sync_result == ACK_SUCCESS) {
                proto_send_file_list("", ACK_SUCCESS, 0u, 0u, 0u, 0u, 0u, 0u,
                                     FILE_LIST_FLAG_END, 0u);
            } else {
                proto_send_ack(cmd, sync_result);
            }
        } else if (file_cmd->control == FILE_LIST_CLEAR) {
            uint8_t clear_result;
            LOG_INFO("PROTO", "FILE LIST clear request received");
            clear_result = clear_file_list_records();
            dbg_printf("[DBG][PROTO] RX 0x41 clear result=0x%02X\n", clear_result);
            if (clear_result == ACK_SUCCESS) {
                proto_send_file_list("", ACK_SUCCESS, 0u, 0u, 0u, 0u, 0u, 0u,
                                     FILE_LIST_FLAG_END, 0u);
            } else {
                proto_send_ack(cmd, clear_result);
            }
        } else {
            LOG_WARN("PROTO", "FILE LIST invalid control: 0x%02X", file_cmd->control);
            proto_send_ack(cmd, ACK_INVALID_PARAM);
        }
        break;
    }

    /* ---------- File operation (0x51) ---------- */
    case CMD_FILE_OP: {
        CmdFileOp *file_op = (CmdFileOp *)f;
        uint8_t result;

        if (file_op->operation == FILE_OP_DELETE_ALL) {
            result = clear_file_list_records();
            dbg_printf("[DBG][PROTO] RX 0x51 delete all result=0x%02X\n", result);
        } else if (file_op->operation == FILE_OP_DELETE_ONE) {
            result = delete_file_from_command(file_op);
            dbg_printf("[DBG][PROTO] RX 0x51 delete one result=0x%02X\n", result);
        } else if (file_op->operation == FILE_OP_DOWNLOAD) {
            result = start_network_from_command(file_op);
            dbg_printf("[DBG][PROTO] RX 0x51 network start result=0x%02X\n", result);
        } else {
            LOG_WARN("FILE_OP", "Invalid operation: 0x%02X", file_op->operation);
            result = ACK_INVALID_PARAM;
        }
        proto_send_ack(cmd, result);
        break;
    }

    case CMD_STATUS:
    {
        uint8_t disk_result = get_status_query_result();
        dbg_printf("[DBG][PROTO] RX 0x61 disk_check result=0x%02X storage_state=%d transfer_state=%d\n",
                   disk_result, storage_state_summary(), transfer_task.state);
        proto_send_ack(cmd, disk_result);
        break;
    }

    case CMD_STOP_TRANSFER:
    {
        uint8_t result = ACK_SUCCESS;

        if (transfer_task.state == RUNNING) {
            LOG_INFO("NETWORK", "Stop network transfer requested");
            dbg_printf("[DBG][PROTO] RX 0x71 stop network pid=%d\n", transfer_task.pid);
            if (kill(transfer_task.pid, SIGTERM) != 0) {
                LOG_ERROR("NETWORK", "Stop network transfer signal failed: pid=%d errno=%d",
                          transfer_task.pid, errno);
                dbg_printf("[DBG][PROTO] RX 0x71 stop signal failed pid=%d errno=%d\n",
                           transfer_task.pid, errno);
                result = ACK_FAILED;
            } else {
                result = wait_network_worker_finished_after_stop();
            }
        } else {
            dbg_printf("[DBG][PROTO] RX 0x71 stop network idle\n");
        }
        proto_send_ack(cmd, result);
        break;
    }

    default:
        LOG_WARN("PROTO", "Unknown command: 0x%02X", cmd);
        proto_send_ack(cmd, ACK_INVALID_PARAM);
        break;
    }
}

/* ================== Main program ================== */

static void handle_storage_worker_signal(int signo)
{
    (void)signo;
    storage_write_request_stop();
}

static void install_storage_worker_signal_handlers(void)
{
    struct sigaction sa;

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = handle_storage_worker_signal;
    sigemptyset(&sa.sa_mask);
    (void)sigaction(SIGTERM, &sa, NULL);
    (void)sigaction(SIGINT, &sa, NULL);
}

static int storage_worker_event_fd(void)
{
    const char *text = getenv("SRC_REAL_STORAGE_EVENT_FD");
    char *end = NULL;
    long value;

    if (!text || text[0] == '\0') return -1;
    errno = 0;
    value = strtol(text, &end, 10);
    if (errno != 0 || end == text || *end != '\0' || value < 0 || value > INT_MAX)
        return -1;
    return (int)value;
}

static int run_storage_worker_main(int argc, char **argv, bool supervised)
{
    GlobalOptions gopt;
    ParsedArgs args;
    CommandType cmd;
    int rc;
    bool final_sent = false;
    bool event_channel_present = false;
    bool fatal_delivery_ok = true;
    WriteResult result;

    memset(&result, 0, sizeof(result));

    setenv("CCB_PROCESS_META_DIR", get_storage_meta_dir(), 0);
    if (parse_global_options(&argc, &argv, &gopt) != 0 || argc < 1) {
        dbg_printf("[DBG][WORKER] parse global options failed argc=%d\n", argc);
        usage();
        return 2;
    }
    if (parse_command_type(argv[0], &cmd) != 0 || (cmd != CMD_WRITE && cmd != CMD_STORAGE_WRITE)) {
        dbg_printf("[DBG][WORKER] invalid command argc=%d cmd=%s\n", argc, argc > 0 ? argv[0] : "");
        fprintf(stderr, "Storage worker only supports write/storage-write\n");
        usage();
        return 2;
    }
    if (parse_subcommand_args(argc, argv, &args) != 0 ||
        ((cmd == CMD_STORAGE_WRITE) ? validate_storage_write_args(&args) : validate_write_args(&args)) != 0) {
        dbg_printf("[DBG][WORKER] parse/validate write args failed\n");
        usage();
        return 2;
    }
    if (cmd == CMD_WRITE) {
        const ChannelConfig *cfg = find_channel(args.channel_id);
        args.has_proto_file_type = true;
        args.proto_file_type = cfg ? cfg->file_type : FILE_TYPE_LOW;
        args.has_calibration_type = true;
        args.calibration_type = 0u;
    }

    if (logger_init(LOG_DB_PATH) != 0) {
        fprintf(stderr, "Failed to initialize logger\n");
        return 1;
    }
    install_storage_worker_signal_handlers();

    dbg_printf("[DBG][WORKER] execute_write start ch=%d mode=%s size=%" PRIu64 " task=%s idx=%u dry=%u\n",
               args.channel_id,
               args.has_size ? "bounded" : "continuous",
               args.size_bytes,
               args.task_no,
               (unsigned)args.file_index,
               gopt.dry_run ? 1u : 0u);
    rc = execute_write_with_result_mode(&args, gopt, &result,
                                        supervised ? STORAGE_WRITE_SUPERVISED
                                                   : STORAGE_WRITE_STANDALONE);
    {
        StorageWorkerEvent event;
        int event_fd = storage_worker_event_fd();
        if (supervised && event_fd >= 0) {
            event_channel_present = true;
            if (rc == 0) {
                storage_ipc_make_event(&event, STORAGE_WORKER_DRAINED, (uint32_t)args.channel_id,
                                       0, result.dma_received_bytes, "drained");
                if (storage_ipc_write_event_deadline(
                        event_fd, &event,
                        storage_ipc_monotonic_us() + 1000000ull) != 0) {
                    rc = -1;
                    (void)snprintf(result.integrity_risk, sizeof(result.integrity_risk),
                                   "%s", "drained_event_send_failed");
                    result.integrity_ok = false;
                }
            }
            if (rc != 0) {
                if (!storage_write_fatal_event_sent()) {
                    storage_ipc_make_event(&event, STORAGE_WORKER_FATAL, (uint32_t)args.channel_id,
                                           rc, result.dma_received_bytes,
                                           result.integrity_risk[0] ? result.integrity_risk : "worker_failed");
                    fatal_delivery_ok = storage_ipc_write_event_deadline(
                        event_fd, &event,
                        storage_ipc_monotonic_us() + 1000000ull) == 0;
                } else {
                    fatal_delivery_ok = true;
                }
            }
            /* A failed FINAL is meaningful only after a delivered FATAL.  If
             * the critical FATAL pipe write itself failed, leave the pipe
             * without FINAL and exit non-zero; the supervisor will classify
             * the EOF explicitly instead of accepting an uncaused failure. */
            if (rc == 0 || fatal_delivery_ok) {
                storage_ipc_make_event(&event, STORAGE_WORKER_FINAL_RESULT,
                                       (uint32_t)args.channel_id, rc,
                                       result.dma_received_bytes,
                                       rc == 0 ? "final" : "failed");
                event.result = result;
                final_sent = storage_ipc_write_event_deadline(
                    event_fd, &event,
                    storage_ipc_monotonic_us() + 1000000ull) == 0;
            }
        }
    }
    if (final_sent) storage_write_flush_deferred_diag();
    if (system_env_flag_enabled("SRC_REAL_LEGACY_STORAGE_TEXT")) {
        printf("storage_worker_result task=%s channel=%d file_index=%u rc=%d"
               " data_persisted=%u integrity_ok=%u dma_stop_recovered=%u integrity_risk=%s\n",
               args.task_no,
               result.channel_id,
               (unsigned)result.file_index,
               rc,
               result.data_persisted ? 1u : 0u,
               result.integrity_ok ? 1u : 0u,
               result.dma_stop_recovered ? 1u : 0u,
               result.integrity_risk[0] != '\0' ? result.integrity_risk : "storage_error");
        fflush(stdout);
    }
    dbg_printf("[DBG][WORKER] execute_write done rc=%d task=%s idx=%u data_persisted=%u\n",
               rc,
               args.task_no,
               (unsigned)result.file_index,
               result.data_persisted ? 1u : 0u);
    logger_close();
    /* Standalone storage-write has no supervisor event pipe and retains its
     * local commit/return contract.  A supervised worker, however, must not
     * report success when its structured FINAL could not be delivered. */
    return (rc == 0 && (!supervised || (event_channel_present && final_sent))) ? 0 : 1;
}

static void handle_network_worker_signal(int signo)
{
    (void)signo;
    nvme_request_stop();
    tcp_transfer_request_stop();
}

static int run_network_worker_main(int argc, char **argv)
{
    GlobalOptions gopt;
    ParsedArgs args;
    CommandType cmd;
    int rc;

    setenv("CCB_PROCESS_META_DIR", get_storage_meta_dir(), 0);
    nvme_clear_stop_request();
    signal(SIGTERM, handle_network_worker_signal);
    signal(SIGINT, handle_network_worker_signal);

    if (parse_global_options(&argc, &argv, &gopt) != 0 || argc < 1) {
        usage();
        return 2;
    }
    if (parse_command_type(argv[0], &cmd) != 0 || cmd != CMD_NETWORK_SEND) {
        fprintf(stderr, "Network worker only supports network-send\n");
        usage();
        return 2;
    }
    if (parse_subcommand_args(argc, argv, &args) != 0 ||
        validate_network_send_args(&args) != 0) {
        usage();
        return 2;
    }
    if (logger_init(LOG_DB_PATH) != 0) {
        fprintf(stderr, "Failed to initialize logger\n");
        return 1;
    }
    if (file_list_init(FILELIST_DB_PATH) != 0) {
        logger_close();
        fprintf(stderr, "Failed to initialize file list database\n");
        return 1;
    }

    dbg_printf("[DBG][NET] network worker start task=%s idx=%u type=0x%02X dry=%u\n",
               args.task_no,
               (unsigned)args.file_index,
               (unsigned)args.proto_file_type,
               gopt.dry_run ? 1u : 0u);
    rc = network_send_existing_file(&args, gopt);
    dbg_printf("[DBG][NET] network worker done rc=%d task=%s idx=%u\n",
               rc, args.task_no, (unsigned)args.file_index);

    file_list_close();
    logger_close();
    return (rc == 0 || rc == -2) ? 0 : 1;
}

static int run_ddr_pattern_store_main(int argc, char **argv)
{
    GlobalOptions gopt;
    ParsedArgs args;
    CommandType cmd;
    WriteResult result;
    int rc;

    setenv("CCB_PROCESS_META_DIR", get_storage_meta_dir(), 0);
    memset(&result, 0, sizeof(result));

    if (parse_global_options(&argc, &argv, &gopt) != 0 || argc < 1) {
        usage();
        return 2;
    }
    if (parse_command_type(argv[0], &cmd) != 0 || cmd != CMD_DDR_PATTERN_STORE) {
        fprintf(stderr, "DDR pattern store mode only supports ddr-pattern-store\n");
        usage();
        return 2;
    }
    if (parse_subcommand_args(argc, argv, &args) != 0 ||
        validate_ddr_pattern_store_args(&args) != 0) {
        usage();
        return 2;
    }

    if (!args.has_channel) {
        args.has_channel = true;
        args.channel_all = false;
        args.channel_id = LOW_SPEED_CHANNEL_ID;
    }
    if (!args.has_lba) {
        args.has_lba = true;
        args.lba_auto = true;
        args.lba = 0u;
    }

    if (logger_init(LOG_DB_PATH) != 0) {
        fprintf(stderr, "Failed to initialize logger\n");
        return 1;
    }
    if (file_list_init(FILELIST_DB_PATH) != 0) {
        char cwd[PATH_MAX];
        logger_close();
        fprintf(stderr, "Failed to initialize file list database path=%s cwd=%s\n",
                FILELIST_DB_PATH,
                get_current_working_dir(cwd, sizeof(cwd)));
        return 1;
    }

    if (advance_duplicate_file_index_from_db(&args) != 0) {
        file_list_close();
        logger_close();
        fprintf(stderr, "Failed to allocate next file index for task=%s\n", args.task_no);
        return 1;
    }

    dbg_printf("[DBG][MAIN] ddr-pattern-store start ch=%d task=%s idx=%u type=0x%02X size=%" PRIu64 "\n",
               args.channel_id,
               args.task_no,
               (unsigned)args.file_index,
               (unsigned)args.proto_file_type,
               args.has_size ? args.size_bytes : (32ull * 1024ull * 1024ull));
    rc = execute_ddr_pattern_store_with_result(&args, gopt, &result);
    if (rc == 0 && record_storage_result_to_db(&args, &result) != 0) {
        (void)task_update_status(args.task_no, TASK_FAILED);
        rc = -1;
    }
    if (rc == 0) {
        (void)sync_filelist_db_to_flash();
    }
    dbg_printf("[DBG][MAIN] ddr-pattern-store done rc=%d task=%s idx=%u\n",
               rc,
               args.task_no,
               (unsigned)args.file_index);

    file_list_close();
    logger_close();
    return (rc == 0) ? 0 : 1;
}

static int run_ssd_lba_wrap_test_main(int argc, char **argv)
{
    GlobalOptions gopt;
    ParsedArgs args;
    CommandType cmd;
    int rc;

    if (parse_global_options(&argc, &argv, &gopt) != 0 || argc < 1) {
        usage();
        return 2;
    }
    if (parse_command_type(argv[0], &cmd) != 0 || cmd != CMD_SSD_LBA_WRAP_TEST) {
        fprintf(stderr, "SSD LBA wrap test mode only supports ssd-lba-wrap-test\n");
        usage();
        return 2;
    }
    if (parse_subcommand_args(argc, argv, &args) != 0 ||
        validate_ssd_lba_wrap_test_args(&args) != 0) {
        usage();
        return 2;
    }

    args.has_channel = true;
    args.channel_all = false;
    args.channel_id = LOW_SPEED_CHANNEL_ID;

    dbg_printf("[DBG][MAIN] ssd-lba-wrap-test start lba=0x%08" PRIx64
               " size=%" PRIu64 "\n",
               args.lba,
               args.has_size ? args.size_bytes : (4ull * 1024ull * 1024ull));
    rc = execute_ssd_lba_wrap_test(&args, gopt);
    dbg_printf("[DBG][MAIN] ssd-lba-wrap-test done rc=%d lba=0x%08" PRIx64 "\n",
               rc,
               args.lba);
    return (rc == 0) ? 0 : 1;
}

static int run_ssd_continuous_pattern_test_main(int argc, char **argv)
{
    GlobalOptions gopt;
    ParsedArgs args;
    CommandType cmd;
    int rc;

    if (parse_global_options(&argc, &argv, &gopt) != 0 || argc < 1) {
        usage();
        return 2;
    }
    if (parse_command_type(argv[0], &cmd) != 0 || cmd != CMD_SSD_CONTINUOUS_PATTERN_TEST) {
        fprintf(stderr, "SSD continuous pattern test mode only supports ssd-continuous-pattern-test\n");
        usage();
        return 2;
    }
    if (parse_subcommand_args(argc, argv, &args) != 0 ||
        validate_ssd_continuous_pattern_test_args(&args) != 0) {
        usage();
        return 2;
    }

    args.has_channel = true;
    args.channel_all = false;
    args.channel_id = LOW_SPEED_CHANNEL_ID;

    dbg_printf("[DBG][MAIN] ssd-continuous-pattern-test start lba=0x%08" PRIx64
               " size=%" PRIu64 "\n",
               args.lba,
               args.has_size ? args.size_bytes : (640ull * 1024ull * 1024ull));
    rc = execute_ssd_continuous_pattern_test(&args, gopt);
    dbg_printf("[DBG][MAIN] ssd-continuous-pattern-test done rc=%d lba=0x%08" PRIx64 "\n",
               rc,
               args.lba);
    return (rc == 0) ? 0 : 1;
}

static int run_dma_rx_benchmark_main(int argc, char **argv)
{
    GlobalOptions gopt;
    ParsedArgs args;
    CommandType cmd;

    if (parse_global_options(&argc, &argv, &gopt) != 0 || argc < 1 ||
        parse_command_type(argv[0], &cmd) != 0 || cmd != CMD_DMA_RX_BENCHMARK ||
        parse_subcommand_args(argc, argv, &args) != 0 ||
        !args.has_channel || args.channel_all || !args.has_duration_sec || !args.has_source) {
        usage();
        return 2;
    }
    return execute_dma_rx_benchmark(&args, gopt) == 0 ? 0 : 1;
}

int main(int argc, char **argv)
{
    uint8_t byte;
    const char *serial_dev = get_serial_device_path();
    g_program_path = (argc > 0 && argv[0]) ? argv[0] : "./src_real_app";
    (void)debug_uart_init();
    if (argc > 1) {
        dbg_verbose_printf("[DBG][MAIN] process start argc=%d program=%s\n", argc, g_program_path);
    } else {
        dbg_printf("[DBG][MAIN] process start argc=%d program=%s\n", argc, g_program_path);
    }

    if (argc > 1 && strcmp(argv[1], STORAGE_WORKER_ARG) == 0) {
        dbg_verbose_printf("[DBG][MAIN] entering storage worker mode\n");
        return run_storage_worker_main(argc - 1, argv + 1, true);
    }
    if (argc > 1 && strcmp(argv[1], STORAGE_WRITE_ARG) == 0) {
        dbg_printf("[DBG][MAIN] entering standalone storage-write mode\n");
        return run_storage_worker_main(argc, argv, false);
    }
    if (argc > 1 && strcmp(argv[1], DDR_PATTERN_STORE_ARG) == 0) {
        dbg_printf("[DBG][MAIN] entering ddr-pattern-store mode\n");
        return run_ddr_pattern_store_main(argc, argv);
    }
    if (argc > 1 && strcmp(argv[1], SSD_LBA_WRAP_TEST_ARG) == 0) {
        dbg_printf("[DBG][MAIN] entering ssd-lba-wrap-test mode\n");
        return run_ssd_lba_wrap_test_main(argc, argv);
    }
    if (argc > 1 && strcmp(argv[1], SSD_CONTINUOUS_PATTERN_TEST_ARG) == 0) {
        dbg_printf("[DBG][MAIN] entering ssd-continuous-pattern-test mode\n");
        return run_ssd_continuous_pattern_test_main(argc, argv);
    }
    if (argc > 1 && strcmp(argv[1], DMA_RX_BENCHMARK_ARG) == 0) {
        return run_dma_rx_benchmark_main(argc, argv);
    }
    if (argc > 1 && strcmp(argv[1], NETWORK_WORKER_ARG) == 0) {
        dbg_verbose_printf("[DBG][MAIN] entering network worker mode\n");
        return run_network_worker_main(argc - 1, argv + 1);
    }
    if (argc > 1 && strcmp(argv[1], NETWORK_SEND_ARG) == 0) {
        dbg_printf("[DBG][MAIN] entering standalone network-send mode\n");
        return run_network_worker_main(argc, argv);
    }

    (void)signal(SIGPIPE, SIG_IGN);
    setenv("CCB_PROCESS_META_DIR", get_storage_meta_dir(), 0);

    if (logger_init(LOG_DB_PATH) != 0) {
        fprintf(stderr, "Failed to initialize logger\n");
        dbg_printf("[DBG][MAIN] logger_init failed path=%s\n", LOG_DB_PATH);
        return -1;
    }

    {
        int deleted_count = logger_delete_old(30);
        if (deleted_count > 0) {
            LOG_INFO("SYSTEM", "Cleaned up %d old log entries", deleted_count);
        } else {
            LOG_DEBUG("SYSTEM", "No old logs to clean up");
        }
    }

    if (file_list_init(FILELIST_DB_PATH) != 0) {
        char cwd[PATH_MAX];
        LOG_ERROR("SYSTEM", "Failed to initialize file list database");
        dbg_printf("[DBG][MAIN] file_list_init failed path=%s\n", FILELIST_DB_PATH);
        fprintf(stderr, "Failed to initialize file list database path=%s cwd=%s\n",
                FILELIST_DB_PATH,
                get_current_working_dir(cwd, sizeof(cwd)));
        logger_close();
        return -1;
    }
    /* Retry only replica publication.  The outbox never replays metadata or
     * database inserts, so a restart cannot duplicate committed records. */
    storage_sync_outbox_retry();

    LOG_INFO("SYSTEM", "System starting up");
    LOG_INFO("SYSTEM", "UART device: %s", serial_dev);
    LOG_INFO("SYSTEM", "Log database: %s", LOG_DB_PATH);
    LOG_INFO("SYSTEM", "File list database: %s", FILELIST_DB_PATH);
    LOG_INFO("SYSTEM", "Storage metadata dir: %s", get_storage_meta_dir());
    dbg_printf("[DBG][MAIN] uart1=%s log_db=%s file_db=%s meta_dir=%s\n",
               serial_dev, LOG_DB_PATH, FILELIST_DB_PATH, get_storage_meta_dir());

    serial_fd = open(serial_dev, O_RDWR | O_NOCTTY);
    if (serial_fd < 0) {
        LOG_ERROR("SYSTEM", "Failed to open serial port: %s", serial_dev);
        perror("open serial");
        dbg_printf("[DBG][MAIN] open uart1 failed path=%s errno=%d\n", serial_dev, errno);
        file_list_close();
        logger_close();
        return -1;
    }

    if (set_serial(serial_fd) != 0) {
        LOG_ERROR("SYSTEM", "Serial configuration failed");
        dbg_printf("[DBG][MAIN] config uart1 failed path=%s errno=%d\n", serial_dev, errno);
        file_list_close();
        logger_close();
        close(serial_fd);
        return -1;
    }

    proto_init();
    proto_set_send(serial_send);
    proto_set_handler(handle_frame);
    LOG_INFO("SYSTEM", "Protocol module initialized");
    dbg_printf("[DBG][MAIN] protocol ready, waiting on uart1\n");

    while (1) {
        struct pollfd serial_poll = { .fd = serial_fd, .events = POLLIN, .revents = 0 };
        int poll_rc = poll(&serial_poll, 1u, 1);
        if (poll_rc > 0 && (serial_poll.revents & POLLIN) != 0) {
            int bytes_read = read(serial_fd, &byte, 1);
            if (bytes_read > 0) proto_input(byte);
        } else if (poll_rc < 0 && errno != EINTR) {
            dbg_printf("[DBG][MAIN] UART poll failed errno=%d\n", errno);
        }
        {
            size_t i;
            for (i = 0; i < STORAGE_TASK_COUNT; ++i) {
                check_task(&storage_tasks[i]);
            }
        }
        storage_service_pending_start();
        storage_service_pending_stop();
        check_task(&transfer_task);
    }

    /* Unreachable for current daemon-style loop. */
    close(serial_fd);
    file_list_close();
    logger_close();
    debug_uart_close();
    return 0;
}
