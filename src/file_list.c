#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sqlite3.h>
#include "file_list.h"
#include "log_config.h"
#include "logger.h"
#include "storage_config.h"

/* Global state. */
static sqlite3 *file_db = NULL;

static uint64_t file_size_from_db(sqlite3_int64 file_size, uint64_t sector_count)
{
    if (file_size >= 0) {
        return (uint64_t)file_size;
    }
    if (sector_count <= UINT64_MAX / 512u) {
        LOG_WARN("FILE_DB",
                 "Recovering legacy negative file_size=%lld from sector_count=%llu",
                 (long long)file_size,
                 (unsigned long long)sector_count);
        return sector_count * 512u;
    }
    return 0u;
}

static int env_int_or_default(const char *name, int default_value, int min_value, int max_value)
{
    const char *value = storage_config_compat_getenv(name);
    char *end = NULL;
    long parsed;

    if (!value || value[0] == '\0') {
        return default_value;
    }
    parsed = strtol(value, &end, 0);
    if (end == value || *end != '\0' || parsed < min_value || parsed > max_value) {
        return default_value;
    }
    return (int)parsed;
}

static int table_has_column(sqlite3 *db, const char *table_name, const char *column_name)
{
    char sql[128];
    sqlite3_stmt *stmt = NULL;
    int found = 0;

    snprintf(sql, sizeof(sql), "PRAGMA table_info(%s);", table_name);
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        return 0;
    }
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *name = (const char *)sqlite3_column_text(stmt, 1);
        if (name && strcmp(name, column_name) == 0) {
            found = 1;
            break;
        }
    }
    sqlite3_finalize(stmt);
    return found;
}

static int ensure_column_exists(sqlite3 *db,
                                const char *table_name,
                                const char *column_name,
                                const char *column_def)
{
    char sql[256];
    char *err_msg = NULL;

    if (table_has_column(db, table_name, column_name)) {
        return 0;
    }

    snprintf(sql, sizeof(sql), "ALTER TABLE %s ADD COLUMN %s %s;", table_name, column_name, column_def);
    if (sqlite3_exec(db, sql, NULL, NULL, &err_msg) != SQLITE_OK) {
        LOG_ERROR("FILE_DB", "Add column failed %s.%s: %s", table_name, column_name, err_msg ? err_msg : "unknown");
        sqlite3_free(err_msg);
        return -1;
    }
    LOG_INFO("FILE_DB", "Column added: %s.%s", table_name, column_name);
    return 0;
}

static int recreate_task_file_view(sqlite3 *db)
{
    const char *drop_sql = "DROP VIEW IF EXISTS v_task_file_list;";
    const char *create_sql =
        "CREATE VIEW IF NOT EXISTS v_task_file_list AS "
        "SELECT "
        "t.task_id AS task_id, "
        "strftime('%s', t.overpass_time) AS task_overpass_time, "
        "t.total_files AS total_files, "
        "t.task_status AS task_status, "
        "t.description AS description, "
        "t.task_payload_json AS task_payload_json, "
        "f.id AS file_id, "
        "f.file_index AS file_index, "
        "f.file_type AS file_type, "
        "f.channel_id AS channel_id, "
        "f.proto_file_type_code AS proto_file_type_code, "
        "f.calibration_type AS calibration_type, "
        "f.start_sector AS start_sector, "
        "f.sector_count AS sector_count, "
        "f.file_size AS file_size, "
        "f.filename AS filename, "
        "strftime('%s', f.created_at) AS file_created_at "
        "FROM file_list f "
        "LEFT JOIN task_info t ON t.task_id = f.task_id "
        "ORDER BY f.created_at DESC, f.id DESC;";
    char *err_msg = NULL;

    if (sqlite3_exec(db, drop_sql, NULL, NULL, &err_msg) != SQLITE_OK) {
        LOG_ERROR("FILE_DB", "Drop view failed: %s", err_msg ? err_msg : "unknown");
        sqlite3_free(err_msg);
        return -1;
    }
    if (sqlite3_exec(db, create_sql, NULL, NULL, &err_msg) != SQLITE_OK) {
        LOG_ERROR("FILE_DB", "Create view failed: %s", err_msg ? err_msg : "unknown");
        sqlite3_free(err_msg);
        return -1;
    }
    LOG_INFO("FILE_DB", "View recreated: v_task_file_list");
    return 0;
}

/* Create database tables. */
static int create_tables(sqlite3 *db)
{
    char *err_msg = NULL;

    const char *task_sql =
        "CREATE TABLE IF NOT EXISTS task_info ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "task_id TEXT UNIQUE NOT NULL,"
        "overpass_time DATETIME NOT NULL,"
        "total_files INTEGER DEFAULT 0,"
        "task_status TEXT DEFAULT 'pending',"
        "description TEXT,"
        "task_payload_json TEXT DEFAULT '',"
        "created_at DATETIME DEFAULT CURRENT_TIMESTAMP,"
        "updated_at DATETIME DEFAULT CURRENT_TIMESTAMP"
        ");";

    const char *file_sql =
        "CREATE TABLE IF NOT EXISTS file_list ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "task_id TEXT NOT NULL,"
        "overpass_time DATETIME NOT NULL,"
        "file_index INTEGER NOT NULL,"
        "file_type TEXT NOT NULL,"
        "channel_id INTEGER DEFAULT -1,"
        "proto_file_type_code INTEGER DEFAULT -1,"
        "calibration_type INTEGER DEFAULT 0,"
        "start_sector INTEGER NOT NULL,"
        "sector_count INTEGER NOT NULL,"
        "file_size INTEGER NOT NULL,"
        "filename TEXT,"
        "created_at DATETIME DEFAULT CURRENT_TIMESTAMP,"
        "FOREIGN KEY (task_id) REFERENCES task_info(task_id)"
        ");";

    const char *disk_sql =
        "CREATE TABLE IF NOT EXISTS disk_info ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "disk_id TEXT UNIQUE NOT NULL,"
        "disk_description TEXT,"
        "total_sectors INTEGER NOT NULL,"
        "last_used_sector INTEGER DEFAULT 0,"
        "next_free_sector INTEGER DEFAULT 0,"
        "free_sectors INTEGER NOT NULL,"
        "created_at DATETIME DEFAULT CURRENT_TIMESTAMP,"
        "updated_at DATETIME DEFAULT CURRENT_TIMESTAMP"
        ");";

    const char *index_sql[] = {
        "CREATE INDEX IF NOT EXISTS idx_task_id ON file_list(task_id);",
        "CREATE INDEX IF NOT EXISTS idx_file_type ON file_list(file_type);",
        "CREATE INDEX IF NOT EXISTS idx_overpass_time ON file_list(overpass_time);",
        "CREATE INDEX IF NOT EXISTS idx_task_file_index ON file_list(task_id, file_index);",
        "CREATE INDEX IF NOT EXISTS idx_disk_id ON disk_info(disk_id);",
        NULL
    };

    if (sqlite3_exec(db, task_sql, NULL, NULL, &err_msg) != SQLITE_OK) {
        LOG_ERROR("FILE_DB", "Create task table failed: %s", err_msg ? err_msg : "unknown");
        sqlite3_free(err_msg);
        return -1;
    }
    if (sqlite3_exec(db, file_sql, NULL, NULL, &err_msg) != SQLITE_OK) {
        LOG_ERROR("FILE_DB", "Create file table failed: %s", err_msg ? err_msg : "unknown");
        sqlite3_free(err_msg);
        return -1;
    }
    if (sqlite3_exec(db, disk_sql, NULL, NULL, &err_msg) != SQLITE_OK) {
        LOG_ERROR("FILE_DB", "Create disk table failed: %s", err_msg ? err_msg : "unknown");
        sqlite3_free(err_msg);
        return -1;
    }

    if (ensure_column_exists(db, "task_info", "task_payload_json", "TEXT DEFAULT ''") != 0 ||
        ensure_column_exists(db, "file_list", "channel_id", "INTEGER DEFAULT -1") != 0 ||
        ensure_column_exists(db, "file_list", "proto_file_type_code", "INTEGER DEFAULT -1") != 0 ||
        ensure_column_exists(db, "file_list", "calibration_type", "INTEGER DEFAULT 0") != 0) {
        return -1;
    }

    for (int i = 0; index_sql[i] != NULL; i++) {
        if (sqlite3_exec(db, index_sql[i], NULL, NULL, &err_msg) != SQLITE_OK) {
            LOG_WARN("FILE_DB", "Create index failed but ignored: %s", err_msg ? err_msg : "unknown");
            sqlite3_free(err_msg);
            err_msg = NULL;
        }
    }

    if (recreate_task_file_view(db) != 0) {
        return -1;
    }

    LOG_INFO("FILE_DB", "Tables and view are ready");
    return 0;
}

/* Initialization and shutdown. */

int file_list_init(const char *db_path)
{
    int busy_timeout_ms;

    if (sqlite3_open(db_path, &file_db) != SQLITE_OK) {
        LOG_ERROR("FILE_DB", "Cannot open database: %s", db_path);
        return -1;
    }
    busy_timeout_ms = env_int_or_default("SRC_REAL_SQLITE_BUSY_TIMEOUT_MS", 10000, 1000, 60000);
    sqlite3_busy_timeout(file_db, busy_timeout_ms);

    if (create_tables(file_db) != 0) {
        sqlite3_close(file_db);
        file_db = NULL;
        return -1;
    }
    
    /* Enable foreign-key checks. */
    sqlite3_exec(file_db, "PRAGMA foreign_keys = ON;", NULL, NULL, NULL);
    
    LOG_INFO("FILE_DB", "Database initialized: %s", db_path);
    return 0;
}

void file_list_close(void)
{
    if (file_db) {
        sqlite3_close(file_db);
        file_db = NULL;
        LOG_INFO("FILE_DB", "Database closed");
    }
}

int file_list_backup_to_path(const char *backup_path)
{
    sqlite3 *dst_db = NULL;
    sqlite3_backup *backup = NULL;
    int rc;
    int step_rc;

    if (!file_db || !backup_path || backup_path[0] == '\0') {
        return -1;
    }

    rc = sqlite3_exec(file_db, "PRAGMA wal_checkpoint(FULL);", NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        LOG_WARN("FILE_DB", "WAL checkpoint before backup failed: %s", sqlite3_errmsg(file_db));
    }

    rc = sqlite3_open(backup_path, &dst_db);
    if (rc != SQLITE_OK) {
        LOG_ERROR("FILE_DB", "Cannot open backup database: %s", backup_path);
        if (dst_db) {
            sqlite3_close(dst_db);
        }
        return -1;
    }
    sqlite3_busy_timeout(dst_db, 5000);

    backup = sqlite3_backup_init(dst_db, "main", file_db, "main");
    if (!backup) {
        LOG_ERROR("FILE_DB", "Backup init failed: %s", sqlite3_errmsg(dst_db));
        sqlite3_close(dst_db);
        return -1;
    }

    do {
        step_rc = sqlite3_backup_step(backup, 128);
        if (step_rc == SQLITE_OK || step_rc == SQLITE_BUSY || step_rc == SQLITE_LOCKED) {
            sqlite3_sleep(25);
        }
    } while (step_rc == SQLITE_OK);

    if (step_rc == SQLITE_BUSY || step_rc == SQLITE_LOCKED) {
        LOG_ERROR("FILE_DB", "Backup source or destination is locked: %s", backup_path);
    }

    rc = sqlite3_backup_finish(backup);
    if (step_rc != SQLITE_DONE) {
        rc = step_rc;
    }
    if (rc == SQLITE_OK) {
        rc = sqlite3_exec(dst_db, "PRAGMA optimize;", NULL, NULL, NULL);
        if (rc != SQLITE_OK) {
            LOG_WARN("FILE_DB", "Backup optimize failed: %s", sqlite3_errmsg(dst_db));
        }
    }

    if (sqlite3_close(dst_db) != SQLITE_OK) {
        LOG_WARN("FILE_DB", "Backup database close returned an error");
    }

    if (rc != SQLITE_OK) {
        LOG_ERROR("FILE_DB", "Backup failed: %s", backup_path);
        return -1;
    }

    LOG_INFO("FILE_DB", "Backup saved: %s", backup_path);
    return 0;
}

/* Task information management. */

int task_create_with_payload(const char *task_id,
                             time_t overpass_time,
                             const char *description,
                             const char *task_payload_json)
{
    if (!file_db || !task_id) {
        return -1;
    }
    
    const char *sql = 
        "INSERT INTO task_info (task_id, overpass_time, task_status, description, task_payload_json) "
        "VALUES (?, datetime(?, 'unixepoch'), 'pending', ?, ?);";
    
    sqlite3_stmt *stmt;
    int rc = 0;
    
    if (sqlite3_prepare_v2(file_db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        LOG_ERROR("FILE_DB", "Prepare create task failed: %s sqlite_error=%s",
                  task_id, sqlite3_errmsg(file_db));
        return -1;
    }

    sqlite3_bind_text(stmt, 1, task_id, -1, SQLITE_STATIC);
    sqlite3_bind_int64(stmt, 2, (sqlite3_int64)overpass_time);
    sqlite3_bind_text(stmt, 3, description ? description : "", -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 4, task_payload_json ? task_payload_json : "", -1, SQLITE_STATIC);

    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    
    if (rc == SQLITE_DONE) {
        LOG_INFO("FILE_DB", "Task created: %s", task_id);
        return 0;
    }
    
    LOG_ERROR("FILE_DB", "Failed to create task: %s sqlite_rc=%d sqlite_error=%s",
              task_id, rc, sqlite3_errmsg(file_db));
    return -1;
}

int task_create(const char *task_id, time_t overpass_time, const char *description)
{
    return task_create_with_payload(task_id, overpass_time, description, "");
}

int task_update_status(const char *task_id, TaskStatus status)
{
    if (!file_db || !task_id) {
        return -1;
    }
    
    const char *status_str[] = {"pending", "running", "completed", "failed"};
    
    const char *sql = 
        "UPDATE task_info SET task_status = ?, updated_at = CURRENT_TIMESTAMP "
        "WHERE task_id = ?;";
    
    sqlite3_stmt *stmt;
    int rc = 0;
    
    if (sqlite3_prepare_v2(file_db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, status_str[status], -1, SQLITE_STATIC);
        sqlite3_bind_text(stmt, 2, task_id, -1, SQLITE_STATIC);
        
        rc = sqlite3_step(stmt);
        sqlite3_finalize(stmt);
    }
    
    return (rc == SQLITE_DONE) ? 0 : -1;
}

int task_update_total_files(const char *task_id, int total_files)
{
    if (!file_db || !task_id) {
        return -1;
    }
    
    const char *sql = 
        "UPDATE task_info SET total_files = ?, updated_at = CURRENT_TIMESTAMP "
        "WHERE task_id = ?;";
    
    sqlite3_stmt *stmt;
    int rc = 0;
    
    if (sqlite3_prepare_v2(file_db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_int(stmt, 1, total_files);
        sqlite3_bind_text(stmt, 2, task_id, -1, SQLITE_STATIC);
        
        rc = sqlite3_step(stmt);
        sqlite3_finalize(stmt);
    }
    
    return (rc == SQLITE_DONE) ? 0 : -1;
}

int task_query(const char *task_id, TaskInfo *info)
{
    if (!file_db || !task_id || !info) {
        return -1;
    }
    
    const char *sql = 
        "SELECT id, task_id, strftime('%s', overpass_time), total_files, "
        "task_status, description, task_payload_json, created_at, updated_at "
        "FROM task_info WHERE task_id = ?;";
    
    sqlite3_stmt *stmt;
    int rc = -1;
    
    if (sqlite3_prepare_v2(file_db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, task_id, -1, SQLITE_STATIC);
        
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            info->id = sqlite3_column_int(stmt, 0);
            strncpy(info->task_id, (const char*)sqlite3_column_text(stmt, 1), sizeof(info->task_id) - 1);
            info->overpass_time = (time_t)sqlite3_column_int64(stmt, 2);
            info->total_files = sqlite3_column_int(stmt, 3);
            strncpy(info->task_status, (const char*)sqlite3_column_text(stmt, 4), sizeof(info->task_status) - 1);
            strncpy(info->description, (const char*)sqlite3_column_text(stmt, 5), sizeof(info->description) - 1);
            {
                const char *payload = (const char *)sqlite3_column_text(stmt, 6);
                if (payload) {
                    strncpy(info->task_payload_json, payload, sizeof(info->task_payload_json) - 1);
                } else {
                    info->task_payload_json[0] = '\0';
                }
            }
            
            /* Parse creation timestamp. */
            const char *ts = (const char*)sqlite3_column_text(stmt, 7);
            if (ts) {
                struct tm tm_time;
                memset(&tm_time, 0, sizeof(tm_time));
                sscanf(ts, "%d-%d-%d %d:%d:%d",
                       &tm_time.tm_year, &tm_time.tm_mon, &tm_time.tm_mday,
                       &tm_time.tm_hour, &tm_time.tm_min, &tm_time.tm_sec);
                tm_time.tm_year -= 1900;
                tm_time.tm_mon -= 1;
                info->created_at = mktime(&tm_time);
            }
            
            /* Parse update timestamp. */
            ts = (const char*)sqlite3_column_text(stmt, 8);
            if (ts) {
                struct tm tm_time;
                memset(&tm_time, 0, sizeof(tm_time));
                sscanf(ts, "%d-%d-%d %d:%d:%d",
                       &tm_time.tm_year, &tm_time.tm_mon, &tm_time.tm_mday,
                       &tm_time.tm_hour, &tm_time.tm_min, &tm_time.tm_sec);
                tm_time.tm_year -= 1900;
                tm_time.tm_mon -= 1;
                info->updated_at = mktime(&tm_time);
            }
            
            rc = 0;
        }
        sqlite3_finalize(stmt);
    }
    
    return rc;
}

int task_delete(const char *task_id)
{
    if (!file_db || !task_id) {
        return -1;
    }
    
    /* Delete related file records first. */
    file_delete_by_task(task_id);
    
    const char *sql = "DELETE FROM task_info WHERE task_id = ?;";
    sqlite3_stmt *stmt;
    int rc = 0;
    
    if (sqlite3_prepare_v2(file_db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, task_id, -1, SQLITE_STATIC);
        rc = sqlite3_step(stmt);
        sqlite3_finalize(stmt);
    }
    
    if (rc == SQLITE_DONE) {
        LOG_INFO("FILE_DB", "Task deleted: %s", task_id);
        return 0;
    }
    
    return -1;
}

int task_list_all(TaskInfo *tasks, int max_count)
{
    if (!file_db || !tasks || max_count <= 0) {
        return -1;
    }
    
    const char *sql = 
        "SELECT id, task_id, strftime('%s', overpass_time), total_files, "
        "task_status, description, task_payload_json, created_at, updated_at "
        "FROM task_info ORDER BY created_at DESC LIMIT ?;";
    
    sqlite3_stmt *stmt;
    int count = 0;
    
    if (sqlite3_prepare_v2(file_db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_int(stmt, 1, max_count);
        
        while (sqlite3_step(stmt) == SQLITE_ROW && count < max_count) {
            tasks[count].id = sqlite3_column_int(stmt, 0);
            strncpy(tasks[count].task_id, (const char*)sqlite3_column_text(stmt, 1), sizeof(tasks[count].task_id) - 1);
            tasks[count].overpass_time = (time_t)sqlite3_column_int64(stmt, 2);
            tasks[count].total_files = sqlite3_column_int(stmt, 3);
            strncpy(tasks[count].task_status, (const char*)sqlite3_column_text(stmt, 4), sizeof(tasks[count].task_status) - 1);
            strncpy(tasks[count].description, (const char*)sqlite3_column_text(stmt, 5), sizeof(tasks[count].description) - 1);
            {
                const char *payload = (const char *)sqlite3_column_text(stmt, 6);
                if (payload) {
                    strncpy(tasks[count].task_payload_json, payload, sizeof(tasks[count].task_payload_json) - 1);
                } else {
                    tasks[count].task_payload_json[0] = '\0';
                }
            }
            
            count++;
        }
        sqlite3_finalize(stmt);
    }
    
    return count;
}

/* File record management. */

int file_add(const FileRecord *file)
{
    if (!file_db || !file) {
        return -1;
    }
    if (file->start_sector > (uint64_t)INT64_MAX ||
        file->sector_count > (uint64_t)INT64_MAX ||
        file->file_size > (uint64_t)INT64_MAX) {
        LOG_ERROR("FILE_DB", "File numeric field exceeds SQLite INTEGER range");
        return -1;
    }
    
    const char *sql = 
        "INSERT INTO file_list (task_id, overpass_time, file_index, file_type, "
        "channel_id, proto_file_type_code, calibration_type, start_sector, sector_count, file_size, filename) "
        "VALUES (?, datetime(?, 'unixepoch'), ?, ?, ?, ?, ?, ?, ?, ?, ?);";
    
    sqlite3_stmt *stmt;
    int rc = 0;
    
    if (sqlite3_prepare_v2(file_db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, file->task_id, -1, SQLITE_STATIC);
        sqlite3_bind_int64(stmt, 2, (sqlite3_int64)file->overpass_time);
        sqlite3_bind_int(stmt, 3, file->file_index);
        sqlite3_bind_text(stmt, 4, file->file_type, -1, SQLITE_STATIC);
        sqlite3_bind_int(stmt, 5, file->channel_id);
        sqlite3_bind_int(stmt, 6, file->proto_file_type_code);
        sqlite3_bind_int(stmt, 7, file->calibration_type);
        sqlite3_bind_int64(stmt, 8, (sqlite3_int64)file->start_sector);
        sqlite3_bind_int64(stmt, 9, (sqlite3_int64)file->sector_count);
        sqlite3_bind_int64(stmt, 10, (sqlite3_int64)file->file_size);
        sqlite3_bind_text(stmt, 11, file->filename, -1, SQLITE_STATIC);
        
        rc = sqlite3_step(stmt);
        sqlite3_finalize(stmt);
    }
    
    return (rc == SQLITE_DONE) ? 0 : -1;
}

int file_add_batch(const FileRecord *files, int count)
{
    if (!file_db || !files || count <= 0) {
        return -1;
    }
    
    /* Keep the older transaction behavior for conservative SQLite writes. */
    sqlite3_exec(file_db, "BEGIN TRANSACTION;", NULL, NULL, NULL);
    
    int success_count = 0;
    for (int i = 0; i < count; i++) {
        if (file_add(&files[i]) == 0) {
            success_count++;
        }
    }
    
    sqlite3_exec(file_db, "COMMIT;", NULL, NULL, NULL);
    
    LOG_INFO("FILE_DB", "Batch added %d files, %d succeeded", count, success_count);
    return success_count;
}

int file_query_by_task(const char *task_id, FileRecord *files, int max_count)
{
    if (!file_db || !task_id || !files || max_count <= 0) {
        return -1;
    }
    
    const char *sql = 
        "SELECT id, task_id, strftime('%s', overpass_time), file_index, file_type, "
        "channel_id, proto_file_type_code, calibration_type, start_sector, sector_count, file_size, filename, created_at "
        "FROM file_list WHERE task_id = ? ORDER BY file_index;";
    
    sqlite3_stmt *stmt;
    int count = 0;
    
    if (sqlite3_prepare_v2(file_db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, task_id, -1, SQLITE_STATIC);
        
        while (sqlite3_step(stmt) == SQLITE_ROW && count < max_count) {
            files[count].id = sqlite3_column_int(stmt, 0);
            strncpy(files[count].task_id, (const char*)sqlite3_column_text(stmt, 1), sizeof(files[count].task_id) - 1);
            files[count].overpass_time = (time_t)sqlite3_column_int64(stmt, 2);
            files[count].file_index = sqlite3_column_int(stmt, 3);
            strncpy(files[count].file_type, (const char*)sqlite3_column_text(stmt, 4), sizeof(files[count].file_type) - 1);
            files[count].channel_id = sqlite3_column_int(stmt, 5);
            files[count].proto_file_type_code = sqlite3_column_int(stmt, 6);
            files[count].calibration_type = sqlite3_column_int(stmt, 7);
            sqlite3_int64 start_sector = sqlite3_column_int64(stmt, 8);
            sqlite3_int64 sector_count = sqlite3_column_int64(stmt, 9);
            sqlite3_int64 file_size = sqlite3_column_int64(stmt, 10);
            files[count].start_sector = start_sector >= 0 ? (uint64_t)start_sector : 0u;
            files[count].sector_count = sector_count >= 0 ? (uint64_t)sector_count : 0u;
            files[count].file_size = file_size_from_db(file_size, files[count].sector_count);
            strncpy(files[count].filename, (const char*)sqlite3_column_text(stmt, 11), sizeof(files[count].filename) - 1);
            
            count++;
        }
        sqlite3_finalize(stmt);
    }
    
    return count;
}

int file_query_by_index(const char *task_id, int file_index, FileRecord *file)
{
    if (!file_db || !task_id || !file) {
        return -1;
    }
    
    const char *sql = 
        "SELECT id, task_id, strftime('%s', overpass_time), file_index, file_type, "
        "channel_id, proto_file_type_code, calibration_type, start_sector, sector_count, file_size, filename "
        "FROM file_list WHERE task_id = ? AND file_index = ?;";
    
    sqlite3_stmt *stmt;
    int rc = -1;
    
    if (sqlite3_prepare_v2(file_db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, task_id, -1, SQLITE_STATIC);
        sqlite3_bind_int(stmt, 2, file_index);
        
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            file->id = sqlite3_column_int(stmt, 0);
            strncpy(file->task_id, (const char*)sqlite3_column_text(stmt, 1), sizeof(file->task_id) - 1);
            file->overpass_time = (time_t)sqlite3_column_int64(stmt, 2);
            file->file_index = sqlite3_column_int(stmt, 3);
            strncpy(file->file_type, (const char*)sqlite3_column_text(stmt, 4), sizeof(file->file_type) - 1);
            file->channel_id = sqlite3_column_int(stmt, 5);
            file->proto_file_type_code = sqlite3_column_int(stmt, 6);
            file->calibration_type = sqlite3_column_int(stmt, 7);
            sqlite3_int64 start_sector = sqlite3_column_int64(stmt, 8);
            sqlite3_int64 sector_count = sqlite3_column_int64(stmt, 9);
            sqlite3_int64 file_size = sqlite3_column_int64(stmt, 10);
            file->start_sector = start_sector >= 0 ? (uint64_t)start_sector : 0u;
            file->sector_count = sector_count >= 0 ? (uint64_t)sector_count : 0u;
            file->file_size = file_size_from_db(file_size, file->sector_count);
            strncpy(file->filename, (const char*)sqlite3_column_text(stmt, 11), sizeof(file->filename) - 1);
            
            rc = 0;
        }
        sqlite3_finalize(stmt);
    }
    
    return rc;
}

int file_query_by_type(const char *file_type, FileRecord *files, int max_count)
{
    if (!file_db || !file_type || !files || max_count <= 0) {
        return -1;
    }
    
    const char *sql = 
        "SELECT id, task_id, strftime('%s', overpass_time), file_index, file_type, "
        "channel_id, proto_file_type_code, calibration_type, start_sector, sector_count, file_size, filename "
        "FROM file_list WHERE file_type = ? ORDER BY overpass_time DESC LIMIT ?;";
    
    sqlite3_stmt *stmt;
    int count = 0;
    
    if (sqlite3_prepare_v2(file_db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, file_type, -1, SQLITE_STATIC);
        sqlite3_bind_int(stmt, 2, max_count);
        
        while (sqlite3_step(stmt) == SQLITE_ROW && count < max_count) {
            files[count].id = sqlite3_column_int(stmt, 0);
            strncpy(files[count].task_id, (const char*)sqlite3_column_text(stmt, 1), sizeof(files[count].task_id) - 1);
            files[count].overpass_time = (time_t)sqlite3_column_int64(stmt, 2);
            files[count].file_index = sqlite3_column_int(stmt, 3);
            strncpy(files[count].file_type, (const char*)sqlite3_column_text(stmt, 4), sizeof(files[count].file_type) - 1);
            files[count].channel_id = sqlite3_column_int(stmt, 5);
            files[count].proto_file_type_code = sqlite3_column_int(stmt, 6);
            files[count].calibration_type = sqlite3_column_int(stmt, 7);
            sqlite3_int64 start_sector = sqlite3_column_int64(stmt, 8);
            sqlite3_int64 sector_count = sqlite3_column_int64(stmt, 9);
            sqlite3_int64 file_size = sqlite3_column_int64(stmt, 10);
            files[count].start_sector = start_sector >= 0 ? (uint64_t)start_sector : 0u;
            files[count].sector_count = sector_count >= 0 ? (uint64_t)sector_count : 0u;
            files[count].file_size = file_size_from_db(file_size, files[count].sector_count);
            strncpy(files[count].filename, (const char*)sqlite3_column_text(stmt, 11), sizeof(files[count].filename) - 1);
            
            count++;
        }
        sqlite3_finalize(stmt);
    }
    
    return count;
}

int file_delete(int file_id)
{
    if (!file_db) {
        return -1;
    }
    
    const char *sql = "DELETE FROM file_list WHERE id = ?;";
    sqlite3_stmt *stmt;
    int rc = 0;
    
    if (sqlite3_prepare_v2(file_db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_int(stmt, 1, file_id);
        rc = sqlite3_step(stmt);
        sqlite3_finalize(stmt);
    }
    
    return (rc == SQLITE_DONE) ? 0 : -1;
}

int file_delete_by_task(const char *task_id)
{
    if (!file_db || !task_id) {
        return -1;
    }
    
    const char *sql = "DELETE FROM file_list WHERE task_id = ?;";
    sqlite3_stmt *stmt;
    int rc = 0;
    
    if (sqlite3_prepare_v2(file_db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, task_id, -1, SQLITE_STATIC);
        rc = sqlite3_step(stmt);
        sqlite3_finalize(stmt);
    }
    
    return (rc == SQLITE_DONE) ? 0 : -1;
}

int file_clear_all(void)
{
    char *err_msg = NULL;

    if (!file_db) {
        return -1;
    }

    if (sqlite3_exec(file_db, "BEGIN TRANSACTION;", NULL, NULL, &err_msg) != SQLITE_OK) {
        LOG_ERROR("FILE_DB", "Clear begin failed: %s", err_msg ? err_msg : "unknown");
        sqlite3_free(err_msg);
        return -1;
    }

    if (sqlite3_exec(file_db, "DELETE FROM file_list;", NULL, NULL, &err_msg) != SQLITE_OK) {
        LOG_ERROR("FILE_DB", "Clear file_list failed: %s", err_msg ? err_msg : "unknown");
        sqlite3_free(err_msg);
        (void)sqlite3_exec(file_db, "ROLLBACK;", NULL, NULL, NULL);
        return -1;
    }

    if (sqlite3_exec(file_db, "DELETE FROM task_info;", NULL, NULL, &err_msg) != SQLITE_OK) {
        LOG_ERROR("FILE_DB", "Clear task_info failed: %s", err_msg ? err_msg : "unknown");
        sqlite3_free(err_msg);
        (void)sqlite3_exec(file_db, "ROLLBACK;", NULL, NULL, NULL);
        return -1;
    }

    if (sqlite3_exec(file_db, "COMMIT;", NULL, NULL, &err_msg) != SQLITE_OK) {
        LOG_ERROR("FILE_DB", "Clear commit failed: %s", err_msg ? err_msg : "unknown");
        sqlite3_free(err_msg);
        (void)sqlite3_exec(file_db, "ROLLBACK;", NULL, NULL, NULL);
        return -1;
    }

    LOG_INFO("FILE_DB", "All file_list and task_info records cleared");
    return 0;
}

int file_count_by_task(const char *task_id)
{
    if (!file_db || !task_id) {
        return -1;
    }
    
    const char *sql = "SELECT COUNT(*) FROM file_list WHERE task_id = ?;";
    sqlite3_stmt *stmt;
    int count = 0;
    
    if (sqlite3_prepare_v2(file_db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, task_id, -1, SQLITE_STATIC);
        
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            count = sqlite3_column_int(stmt, 0);
        }
        sqlite3_finalize(stmt);
    }
    
    return count;
}

int file_next_index_by_task(const char *task_id, int *out_next_index)
{
    const char *sql = "SELECT COALESCE(MAX(file_index), 0) + 1 FROM file_list WHERE task_id = ?;";
    sqlite3_stmt *stmt = NULL;
    int next_index = 1;

    if (!file_db || !task_id || !out_next_index) {
        return -1;
    }

    if (sqlite3_prepare_v2(file_db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, task_id, -1, SQLITE_STATIC);
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            next_index = sqlite3_column_int(stmt, 0);
            if (next_index <= 0) {
                next_index = 1;
            }
        }
        sqlite3_finalize(stmt);
    } else {
        return -1;
    }

    *out_next_index = next_index;
    return 0;
}

int task_file_list_query(TaskFileListRecord *records, int max_count)
{
    const char *sql =
        "SELECT task_id, task_overpass_time, total_files, task_status, description, task_payload_json, "
        "file_index, file_type, channel_id, proto_file_type_code, calibration_type, start_sector, sector_count, file_size, filename, file_created_at "
        "FROM v_task_file_list LIMIT ?;";
    sqlite3_stmt *stmt = NULL;
    int count = 0;

    if (!file_db || !records || max_count <= 0) {
        return -1;
    }

    if (sqlite3_prepare_v2(file_db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_int(stmt, 1, max_count);

    while (sqlite3_step(stmt) == SQLITE_ROW && count < max_count) {
        const char *task_id = (const char *)sqlite3_column_text(stmt, 0);
        const char *status = (const char *)sqlite3_column_text(stmt, 3);
        const char *desc = (const char *)sqlite3_column_text(stmt, 4);
        const char *payload = (const char *)sqlite3_column_text(stmt, 5);
        const char *file_type = (const char *)sqlite3_column_text(stmt, 7);
        const char *filename = (const char *)sqlite3_column_text(stmt, 14);

        memset(&records[count], 0, sizeof(records[count]));
        if (task_id) {
            strncpy(records[count].task_id, task_id, sizeof(records[count].task_id) - 1);
        }
        records[count].overpass_time = (time_t)sqlite3_column_int64(stmt, 1);
        records[count].total_files = sqlite3_column_int(stmt, 2);
        if (status) {
            strncpy(records[count].task_status, status, sizeof(records[count].task_status) - 1);
        }
        if (desc) {
            strncpy(records[count].description, desc, sizeof(records[count].description) - 1);
        }
        if (payload) {
            strncpy(records[count].task_payload_json, payload, sizeof(records[count].task_payload_json) - 1);
        }
        records[count].file_index = sqlite3_column_int(stmt, 6);
        if (file_type) {
            strncpy(records[count].file_type, file_type, sizeof(records[count].file_type) - 1);
        }
        records[count].channel_id = sqlite3_column_int(stmt, 8);
        records[count].proto_file_type_code = sqlite3_column_int(stmt, 9);
        records[count].calibration_type = sqlite3_column_int(stmt, 10);
        {
            sqlite3_int64 start_sector = sqlite3_column_int64(stmt, 11);
            sqlite3_int64 sector_count = sqlite3_column_int64(stmt, 12);
            sqlite3_int64 file_size = sqlite3_column_int64(stmt, 13);
            records[count].start_sector = start_sector >= 0 ? (uint64_t)start_sector : 0u;
            records[count].sector_count = sector_count >= 0 ? (uint64_t)sector_count : 0u;
            records[count].file_size = file_size_from_db(file_size, records[count].sector_count);
        }
        if (filename) {
            strncpy(records[count].filename, filename, sizeof(records[count].filename) - 1);
        }
        records[count].file_created_at = (time_t)sqlite3_column_int64(stmt, 15);
        ++count;
    }

    sqlite3_finalize(stmt);
    return count;
}

int file_db_begin(void)
{
    char *err_msg = NULL;
    if (!file_db) {
        return -1;
    }
    if (sqlite3_exec(file_db, "BEGIN TRANSACTION;", NULL, NULL, &err_msg) != SQLITE_OK) {
        LOG_ERROR("FILE_DB", "BEGIN failed: %s", err_msg ? err_msg : "unknown");
        sqlite3_free(err_msg);
        return -1;
    }
    return 0;
}

int file_db_commit(void)
{
    char *err_msg = NULL;
    if (!file_db) {
        return -1;
    }
    if (sqlite3_exec(file_db, "COMMIT;", NULL, NULL, &err_msg) != SQLITE_OK) {
        LOG_ERROR("FILE_DB", "COMMIT failed: %s", err_msg ? err_msg : "unknown");
        sqlite3_free(err_msg);
        return -1;
    }
    return 0;
}

int file_db_rollback(void)
{
    char *err_msg = NULL;
    if (!file_db) {
        return -1;
    }
    if (sqlite3_exec(file_db, "ROLLBACK;", NULL, NULL, &err_msg) != SQLITE_OK) {
        LOG_ERROR("FILE_DB", "ROLLBACK failed: %s", err_msg ? err_msg : "unknown");
        sqlite3_free(err_msg);
        return -1;
    }
    return 0;
}

/* Disk information management. */

int disk_init(const char *disk_id, const char *disk_description, long total_sectors)
{
    if (!file_db || !disk_id || total_sectors <= 0) {
        return -1;
    }
    
    const char *sql = 
        "INSERT OR REPLACE INTO disk_info "
        "(disk_id, disk_description, total_sectors, last_used_sector, next_free_sector, free_sectors) "
        "VALUES (?, ?, ?, 0, 0, ?);";
    
    sqlite3_stmt *stmt;
    int rc = 0;
    
    if (sqlite3_prepare_v2(file_db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, disk_id, -1, SQLITE_STATIC);
        sqlite3_bind_text(stmt, 2, disk_description ? disk_description : "", -1, SQLITE_STATIC);
        sqlite3_bind_int64(stmt, 3, (sqlite3_int64)total_sectors);
        sqlite3_bind_int64(stmt, 4, (sqlite3_int64)total_sectors);
        
        rc = sqlite3_step(stmt);
        sqlite3_finalize(stmt);
    }
    
    if (rc == SQLITE_DONE) {
        LOG_INFO("FILE_DB", "Disk initialized: %s (%s, total sectors: %ld)", 
                 disk_id, disk_description ? disk_description : "N/A", total_sectors);
        return 0;
    }
    
    LOG_ERROR("FILE_DB", "Failed to initialize disk: %s", disk_id);
    return -1;
}

int disk_query(const char *disk_id, DiskInfo *info)
{
    if (!file_db || !disk_id || !info) {
        return -1;
    }
    
    const char *sql = 
        "SELECT id, disk_id, disk_description, total_sectors, last_used_sector, "
        "next_free_sector, free_sectors, created_at, updated_at "
        "FROM disk_info WHERE disk_id = ?;";
    
    sqlite3_stmt *stmt;
    int rc = -1;
    
    if (sqlite3_prepare_v2(file_db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, disk_id, -1, SQLITE_STATIC);
        
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            info->id = sqlite3_column_int(stmt, 0);
            strncpy(info->disk_id, (const char*)sqlite3_column_text(stmt, 1), sizeof(info->disk_id) - 1);
            strncpy(info->disk_description, (const char*)sqlite3_column_text(stmt, 2), sizeof(info->disk_description) - 1);
            info->total_sectors = (long)sqlite3_column_int64(stmt, 3);
            info->last_used_sector = (long)sqlite3_column_int64(stmt, 4);
            info->next_free_sector = (long)sqlite3_column_int64(stmt, 5);
            info->free_sectors = (long)sqlite3_column_int64(stmt, 6);
            
            /* Timestamp parsing is intentionally simplified here. */
            info->created_at = time(NULL);
            info->updated_at = time(NULL);
            
            rc = 0;
        }
        sqlite3_finalize(stmt);
    }
    
    return rc;
}

int disk_update_usage(const char *disk_id, long used_sectors)
{
    if (!file_db || !disk_id || used_sectors < 0) {
        return -1;
    }
    
    DiskInfo info;
    if (disk_query(disk_id, &info) != 0) {
        return -1;  /* Disk does not exist. */
    }
    
    if (used_sectors > info.total_sectors) {
        LOG_ERROR("FILE_DB", "Used sectors exceed total capacity");
        return -1;
    }
    
    const char *sql = 
        "UPDATE disk_info SET "
        "last_used_sector = ?, "
        "next_free_sector = ?, "
        "free_sectors = ?, "
        "updated_at = CURRENT_TIMESTAMP "
        "WHERE disk_id = ?;";
    
    sqlite3_stmt *stmt;
    int rc = 0;
    long next_free = used_sectors;
    long free = info.total_sectors - used_sectors;
    
    if (sqlite3_prepare_v2(file_db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_int64(stmt, 1, (sqlite3_int64)used_sectors);
        sqlite3_bind_int64(stmt, 2, (sqlite3_int64)next_free);
        sqlite3_bind_int64(stmt, 3, (sqlite3_int64)free);
        sqlite3_bind_text(stmt, 4, disk_id, -1, SQLITE_STATIC);
        
        rc = sqlite3_step(stmt);
        sqlite3_finalize(stmt);
    }
    
    return (rc == SQLITE_DONE) ? 0 : -1;
}

int disk_allocate_sectors(const char *disk_id, int count, long *start_sector)
{
    if (!file_db || !disk_id || !start_sector || count <= 0) {
        return -1;
    }
    
    DiskInfo info;
    if (disk_query(disk_id, &info) != 0) {
        return -1;
    }
    
    /* Check whether enough free sectors are available. */
    if (count > info.free_sectors) {
        LOG_WARN("FILE_DB", "Not enough free sectors on disk %s", disk_id);
        count = info.free_sectors;  /* Return all remaining free sectors. */
        if (count <= 0) {
            return -1;
        }
    }
    
    *start_sector = info.next_free_sector;
    long new_next_free = info.next_free_sector + count;
    long new_free = info.free_sectors - count;
    
    const char *sql = 
        "UPDATE disk_info SET "
        "last_used_sector = ?, "
        "next_free_sector = ?, "
        "free_sectors = ?, "
        "updated_at = CURRENT_TIMESTAMP "
        "WHERE disk_id = ?;";
    
    sqlite3_stmt *stmt;
    int rc = 0;
    
    if (sqlite3_prepare_v2(file_db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_int64(stmt, 1, (sqlite3_int64)new_next_free);
        sqlite3_bind_int64(stmt, 2, (sqlite3_int64)new_next_free);
        sqlite3_bind_int64(stmt, 3, (sqlite3_int64)new_free);
        sqlite3_bind_text(stmt, 4, disk_id, -1, SQLITE_STATIC);
        
        rc = sqlite3_step(stmt);
        sqlite3_finalize(stmt);
    }
    
    if (rc == SQLITE_DONE) {
        LOG_DEBUG("FILE_DB", "Allocated %d sectors on disk %s, start: %ld", count, disk_id, *start_sector);
        return count;
    }
    
    return -1;
}

int disk_free_sectors(const char *disk_id, long sector_count)
{
    if (!file_db || !disk_id || sector_count <= 0) {
        return -1;
    }
    
    DiskInfo info;
    if (disk_query(disk_id, &info) != 0) {
        return -1;
    }
    
    long new_free = info.free_sectors + sector_count;
    if (new_free > info.total_sectors) {
        LOG_WARN("FILE_DB", "Free sectors exceed total capacity, capping at %ld", info.total_sectors);
        new_free = info.total_sectors;
    }
    
    const char *sql = 
        "UPDATE disk_info SET "
        "free_sectors = ?, "
        "updated_at = CURRENT_TIMESTAMP "
        "WHERE disk_id = ?;";
    
    sqlite3_stmt *stmt;
    int rc = 0;
    
    if (sqlite3_prepare_v2(file_db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_int64(stmt, 1, (sqlite3_int64)new_free);
        sqlite3_bind_text(stmt, 2, disk_id, -1, SQLITE_STATIC);
        
        rc = sqlite3_step(stmt);
        sqlite3_finalize(stmt);
    }
    
    return (rc == SQLITE_DONE) ? 0 : -1;
}

int disk_list_all(DiskInfo *disks, int max_count)
{
    if (!file_db || !disks || max_count <= 0) {
        return -1;
    }
    
    const char *sql = 
        "SELECT id, disk_id, disk_description, total_sectors, last_used_sector, "
        "next_free_sector, free_sectors, created_at, updated_at "
        "FROM disk_info ORDER BY created_at DESC LIMIT ?;";
    
    sqlite3_stmt *stmt;
    int count = 0;
    
    if (sqlite3_prepare_v2(file_db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_int(stmt, 1, max_count);
        
        while (sqlite3_step(stmt) == SQLITE_ROW && count < max_count) {
            disks[count].id = sqlite3_column_int(stmt, 0);
            strncpy(disks[count].disk_id, (const char*)sqlite3_column_text(stmt, 1), sizeof(disks[count].disk_id) - 1);
            strncpy(disks[count].disk_description, (const char*)sqlite3_column_text(stmt, 2), sizeof(disks[count].disk_description) - 1);
            disks[count].total_sectors = (long)sqlite3_column_int64(stmt, 3);
            disks[count].last_used_sector = (long)sqlite3_column_int64(stmt, 4);
            disks[count].next_free_sector = (long)sqlite3_column_int64(stmt, 5);
            disks[count].free_sectors = (long)sqlite3_column_int64(stmt, 6);
            
            count++;
        }
        sqlite3_finalize(stmt);
    }
    
    return count;
}

int disk_delete(const char *disk_id)
{
    if (!file_db || !disk_id) {
        return -1;
    }
    
    const char *sql = "DELETE FROM disk_info WHERE disk_id = ?;";
    sqlite3_stmt *stmt;
    int rc = 0;
    
    if (sqlite3_prepare_v2(file_db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, disk_id, -1, SQLITE_STATIC);
        rc = sqlite3_step(stmt);
        sqlite3_finalize(stmt);
    }
    
    return (rc == SQLITE_DONE) ? 0 : -1;
}
