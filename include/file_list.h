#ifndef FILE_LIST_H
#define FILE_LIST_H

#include <stdint.h>
#include <time.h>

/* Data structures. */

/**
 * File record matching the file_list table.
 */
typedef struct {
    int id;                       /* Database row id. */
    char task_id[64];             /* Task id. */
    time_t overpass_time;         /* Overpass timestamp. */
    int file_index;               /* Database file index, starts from 1. */
    char file_type[32];           /* File type name. */
    int channel_id;               /* Hardware channel id: 0/1/2. */
    int proto_file_type_code;     /* Protocol file type code: 0/1/2/3. */
    int calibration_type;         /* Calibration type code. */
    uint64_t start_sector;        /* Start sector on SSD. */
    uint64_t sector_count;        /* Number of sectors used. */
    uint64_t file_size;           /* File size in bytes. */
    char filename[256];           /* Display filename. */
    time_t created_at;            /* Record creation timestamp. */
} FileRecord;

/**
 * Task information matching the task_info table.
 */
typedef struct {
    int id;                         /* Database row id. */
    char task_id[64];               /* Task id. */
    time_t overpass_time;           /* Overpass timestamp. */
    int total_files;                /* Total files for the task. */
    char task_status[32];           /* Task status string. */
    char description[512];          /* Task description JSON. */
    char task_payload_json[2048];   /* Full 0x11 payload snapshot. */
    time_t created_at;              /* Record creation timestamp. */
    time_t updated_at;              /* Last update timestamp. */
} TaskInfo;

/**
 * Joined task/file row returned by v_task_file_list.
 */
typedef struct {
    char task_id[64];
    time_t overpass_time;
    int total_files;
    char task_status[32];
    char description[512];
    char task_payload_json[2048];
    int file_index;
    char file_type[32];
    int channel_id;
    int proto_file_type_code;
    int calibration_type;
    uint64_t start_sector;
    uint64_t sector_count;
    uint64_t file_size;
    char filename[256];
    time_t file_created_at;
} TaskFileListRecord;

/**
 * Task status values used by task_info.
 */
typedef enum {
    TASK_PENDING = 0,             /* Waiting to run. */
    TASK_RUNNING,                 /* Currently running. */
    TASK_COMPLETED,               /* Completed successfully. */
    TASK_FAILED                   /* Failed. */
} TaskStatus;

/* Initialization and shutdown. */

/**
 * Initialize the file-list database.
 *
 * @param db_path Database path.
 * @return 0 on success, -1 on failure.
 */
int file_list_init(const char *db_path);

/**
 * Close the file-list database.
 */
void file_list_close(void);

/**
 * Save a consistent backup copy of the currently opened file-list database.
 *
 * @param backup_path Destination database path.
 * @return 0 on success, -1 on failure.
 */
int file_list_backup_to_path(const char *backup_path);

/* Task information APIs. */

/**
 * Create a task.
 *
 * @param task_id Task id.
 * @param overpass_time Overpass timestamp.
 * @param description Optional task description.
 * @return 0 on success, -1 on failure.
 */
int task_create(const char *task_id, time_t overpass_time, const char *description);

/**
 * Create a task with a full protocol payload snapshot.
 *
 * @param task_id Task id.
 * @param overpass_time Overpass timestamp.
 * @param description Optional task description.
 * @param task_payload_json Optional full task payload JSON.
 * @return 0 on success, -1 on failure.
 */
int task_create_with_payload(const char *task_id,
                             time_t overpass_time,
                             const char *description,
                             const char *task_payload_json);

/**
 * Update task status.
 *
 * @param task_id Task id.
 * @param status New task status.
 * @return 0 on success, -1 on failure.
 */
int task_update_status(const char *task_id, TaskStatus status);

/**
 * Update task file count.
 *
 * @param task_id Task id.
 * @param total_files New total file count.
 * @return 0 on success, -1 on failure.
 */
int task_update_total_files(const char *task_id, int total_files);

/**
 * Query one task.
 *
 * @param task_id Task id.
 * @param info Output task info.
 * @return 0 on success, -1 on failure.
 */
int task_query(const char *task_id, TaskInfo *info);

/**
 * Delete a task and its related file records.
 *
 * @param task_id Task id.
 * @return 0 on success, -1 on failure.
 */
int task_delete(const char *task_id);

/**
 * List tasks.
 *
 * @param tasks Output task array allocated by caller.
 * @param max_count Maximum row count.
 * @return Number of rows returned, or -1 on failure.
 */
int task_list_all(TaskInfo *tasks, int max_count);

/* File record APIs. */

/**
 * Add one file record.
 *
 * @param file File record.
 * @return 0 on success, -1 on failure.
 */
int file_add(const FileRecord *file);

/**
 * Add multiple file records.
 *
 * @param files File record array.
 * @param count Number of records.
 * @return Number of records added, or -1 on failure.
 */
int file_add_batch(const FileRecord *files, int count);

/**
 * Query files by task id.
 *
 * @param task_id Task id.
 * @param files Output file array allocated by caller.
 * @param max_count Maximum row count.
 * @return Number of rows returned, or -1 on failure.
 */
int file_query_by_task(const char *task_id, FileRecord *files, int max_count);

/**
 * Query one file by task id and file index.
 *
 * @param task_id Task id.
 * @param file_index Database file index.
 * @param file Output file record.
 * @return 0 on success, -1 on failure.
 */
int file_query_by_index(const char *task_id, int file_index, FileRecord *file);

/**
 * Query files by type name.
 *
 * @param file_type File type name.
 * @param files Output file array allocated by caller.
 * @param max_count Maximum row count.
 * @return Number of rows returned, or -1 on failure.
 */
int file_query_by_type(const char *file_type, FileRecord *files, int max_count);

/**
 * Delete one file record by database row id.
 *
 * @param file_id Database file row id.
 * @return 0 on success, -1 on failure.
 */
int file_delete(int file_id);

/**
 * Delete all file records for a task.
 *
 * @param task_id Task id.
 * @return 0 on success, -1 on failure.
 */
int file_delete_by_task(const char *task_id);

/**
 * Clear all file-list records and reset task file counts.
 *
 * @return 0 on success, -1 on failure.
 */
int file_clear_all(void);

/**
 * Count files for one task.
 *
 * @param task_id Task id.
 * @return File count, or -1 on failure.
 */
int file_count_by_task(const char *task_id);

/**
 * Query the next database file_index for a task.
 *
 * @param task_id Task id.
 * @param out_next_index Output next file index.
 * @return 0 on success, -1 on failure.
 */
int file_next_index_by_task(const char *task_id, int *out_next_index);

/**
 * Query the joined task/file view.
 *
 * @param records Output rows allocated by caller.
 * @param max_count Maximum row count.
 * @return Number of rows returned, or -1 on failure.
 */
int task_file_list_query(TaskFileListRecord *records, int max_count);

/**
 * Explicit database transaction helpers.
 */
int file_db_begin(void);
int file_db_commit(void);
int file_db_rollback(void);

/* Disk information APIs. */

/**
 * Disk information matching the disk_info table.
 */
typedef struct {
    int id;                       /* Database row id. */
    char disk_id[64];             /* Disk id. */
    char disk_description[256];   /* Disk description. */
    long total_sectors;           /* Total sectors. */
    long last_used_sector;        /* Last sector used. */
    long next_free_sector;        /* Next free sector. */
    long free_sectors;            /* Remaining free sectors. */
    time_t created_at;            /* Record creation timestamp. */
    time_t updated_at;            /* Last update timestamp. */
} DiskInfo;

/**
 * Initialize or replace one disk record.
 *
 * @param disk_id Disk id.
 * @param disk_description Disk description.
 * @param total_sectors Total sectors.
 * @return 0 on success, -1 on failure.
 */
int disk_init(const char *disk_id, const char *disk_description, long total_sectors);

/**
 * Query one disk.
 *
 * @param disk_id Disk id.
 * @param info Output disk info.
 * @return 0 on success, -1 on failure.
 */
int disk_query(const char *disk_id, DiskInfo *info);

/**
 * Update disk usage counters.
 *
 * @param disk_id Disk id.
 * @param used_sectors Used sector count.
 * @return 0 on success, -1 on failure.
 */
int disk_update_usage(const char *disk_id, long used_sectors);

/**
 * Allocate sectors sequentially.
 *
 * @param disk_id Disk id.
 * @param count Requested sector count.
 * @param start_sector Output start sector.
 * @return Allocated sector count, or -1 on failure.
 */
int disk_allocate_sectors(const char *disk_id, int count, long *start_sector);

/**
 * Return sectors to the disk record.
 *
 * @param disk_id Disk id.
 * @param sector_count Sector count to free.
 * @return 0 on success, -1 on failure.
 */
int disk_free_sectors(const char *disk_id, long sector_count);

/**
 * List disks.
 *
 * @param disks Output disk array allocated by caller.
 * @param max_count Maximum row count.
 * @return Number of rows returned, or -1 on failure.
 */
int disk_list_all(DiskInfo *disks, int max_count);

/**
 * Delete one disk record.
 *
 * @param disk_id Disk id.
 * @return 0 on success, -1 on failure.
 */
int disk_delete(const char *disk_id);

#endif
