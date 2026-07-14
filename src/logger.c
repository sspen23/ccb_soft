#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <sqlite3.h>
#include <pthread.h>
#include <time.h>
#include "logger.h"
#include "log_config.h"
#include "storage_config.h"

/* Global state. */
static sqlite3 *db = NULL;
static pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;

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

static int logger_runtime_level(void)
{
    const char *value = storage_config_compat_getenv("SRC_REAL_LOG_LEVEL");

    if (!value || value[0] == '\0' || strcmp(value, "quiet") == 0) {
        return 0;
    }
    if (strcmp(value, "summary") == 0) {
        return 1;
    }
    if (strcmp(value, "debug") == 0) {
        return 2;
    }
    if (strcmp(value, "trace") == 0) {
        return 3;
    }
    return 0;
}

static int logger_should_drop_message(LogLevel level, const char *message)
{
    int runtime_level = logger_runtime_level();

    if (runtime_level <= 1 && level < LOG_WARN) {
        return 1;
    }
    if (level >= LOG_WARN) {
        return 0;
    }
    if (runtime_level < 3 && message &&
        (strstr(message, "storage_pipeline") != NULL ||
         strstr(message, "slot_write_perf") != NULL ||
         strstr(message, "nvme_perf_calc") != NULL ||
         strstr(message, "slot_sw_timing") != NULL)) {
        return 1;
    }
    return 0;
}

#if ASYNC_LOG_ENABLED
/* Async writer state. */
typedef struct {
    LogLevel level;
    char module[LOG_MODULE_MAX_LEN];
    char message[LOG_MESSAGE_MAX_LEN];
} LogQueueItem;

static LogQueueItem *async_queue = NULL;
static int queue_head = 0;
static int queue_tail = 0;
static int queue_size = 0;
static pthread_t async_thread;
static int async_running = 0;
#endif

/* Create the log database table. */
static int create_log_table(sqlite3 *database)
{
    const char *sql = 
        "CREATE TABLE IF NOT EXISTS system_logs ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,"
        "level INTEGER NOT NULL,"
        "module TEXT NOT NULL,"
        "message TEXT NOT NULL"
        ");";
    
    char *err_msg = NULL;
    if (sqlite3_exec(database, sql, NULL, NULL, &err_msg) != SQLITE_OK) {
        fprintf(stderr, "SQL error creating table: %s\n", err_msg);
        sqlite3_free(err_msg);
        return -1;
    }
    
    /* Create an index for faster timestamp queries. */
    const char *index_sql = 
        "CREATE INDEX IF NOT EXISTS idx_timestamp ON system_logs(timestamp);";
    if (sqlite3_exec(database, index_sql, NULL, NULL, &err_msg) != SQLITE_OK) {
        sqlite3_free(err_msg);
        /* Index creation failure is non-fatal. */
    }
    
    return 0;
}

#if ASYNC_LOG_ENABLED
/* Async writer thread. */
static void *async_writer_thread(void *arg)
{
    (void)arg;
    
    while (async_running) {
        pthread_mutex_lock(&log_mutex);
        
        if (queue_size > 0) {
            LogQueueItem item = async_queue[queue_head];
            queue_head = (queue_head + 1) % ASYNC_QUEUE_MAX_LEN;
            queue_size--;
            
            pthread_mutex_unlock(&log_mutex);
            
            /* Write the pending item to the database. */
            const char *sql = 
                "INSERT INTO system_logs (level, module, message) VALUES (?, ?, ?);";
            sqlite3_stmt *stmt;
            
            if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
                sqlite3_bind_int(stmt, 1, item.level);
                sqlite3_bind_text(stmt, 2, item.module, -1, SQLITE_STATIC);
                sqlite3_bind_text(stmt, 3, item.message, -1, SQLITE_STATIC);
                
                sqlite3_step(stmt);
                sqlite3_finalize(stmt);
            }
        } else {
            pthread_mutex_unlock(&log_mutex);
            usleep(10000); /* Sleep 10 ms to avoid busy waiting. */
        }
    }
    
    return NULL;
}

/* Initialize the async queue. */
static int init_async_queue(void)
{
    async_queue = (LogQueueItem *)malloc(sizeof(LogQueueItem) * ASYNC_QUEUE_MAX_LEN);
    if (!async_queue) {
        return -1;
    }
    
    async_running = 1;
    if (pthread_create(&async_thread, NULL, async_writer_thread, NULL) != 0) {
        free(async_queue);
        async_running = 0;
        return -1;
    }
    
    return 0;
}

/* Clean up the async queue. */
static void cleanup_async_queue(void)
{
    if (async_running) {
        async_running = 0;
        pthread_join(async_thread, NULL);
    }
    
    if (async_queue) {
        free(async_queue);
        async_queue = NULL;
    }
}
#endif

/* Initialize the logging system. */
int logger_init(const char *db_path)
{
    int busy_timeout_ms;

    if (sqlite3_open(db_path, &db) != SQLITE_OK) {
        fprintf(stderr, "Cannot open database: %s\n", sqlite3_errmsg(db));
        return -1;
    }
    busy_timeout_ms = env_int_or_default("SRC_REAL_SQLITE_BUSY_TIMEOUT_MS", 10000, 1000, 60000);
    sqlite3_busy_timeout(db, busy_timeout_ms);
    
    if (create_log_table(db) != 0) {
        sqlite3_close(db);
        db = NULL;
        return -1;
    }
    
#if ASYNC_LOG_ENABLED
    if (init_async_queue() != 0) {
        fprintf(stderr, "Warning: Async queue init failed, using sync mode\n");
    }
#endif
    
    return 0;
}

/* Close the logging system. */
void logger_close(void)
{
#if ASYNC_LOG_ENABLED
    cleanup_async_queue();
#endif
    
    if (db) {
        sqlite3_close(db);
        db = NULL;
    }
}

/* Write one log entry. */
int logger_write(LogLevel level, const char *module, const char *format, ...)
{
    if (level < CURRENT_LOG_LEVEL || !db) {
        return 0;
    }
    
    /* Format the message. */
    char buffer[LOG_BUFFER_SIZE];
    va_list args;
    va_start(args, format);
    vsnprintf(buffer, sizeof(buffer), format, args);
    va_end(args);

    if (logger_should_drop_message(level, buffer)) {
        return 0;
    }
    
#if ASYNC_LOG_ENABLED
    /* Async mode: enqueue the item. */
    if (async_queue) {
        pthread_mutex_lock(&log_mutex);
        
        if (queue_size >= ASYNC_QUEUE_MAX_LEN) {
            /* Queue is full; drop the oldest item. */
            queue_head = (queue_head + 1) % ASYNC_QUEUE_MAX_LEN;
            queue_size--;
        }
        
        async_queue[queue_tail].level = level;
        strncpy(async_queue[queue_tail].module, module, LOG_MODULE_MAX_LEN - 1);
        async_queue[queue_tail].module[LOG_MODULE_MAX_LEN - 1] = '\0';
        strncpy(async_queue[queue_tail].message, buffer, LOG_MESSAGE_MAX_LEN - 1);
        async_queue[queue_tail].message[LOG_MESSAGE_MAX_LEN - 1] = '\0';
        
        queue_tail = (queue_tail + 1) % ASYNC_QUEUE_MAX_LEN;
        queue_size++;
        
        pthread_mutex_unlock(&log_mutex);
        return 0;
    }
#endif
    
    /* Sync mode: write directly. */
    pthread_mutex_lock(&log_mutex);
    
    const char *sql = 
        "INSERT INTO system_logs (level, module, message) VALUES (?, ?, ?);";
    sqlite3_stmt *stmt;
    
    int rc = 0;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_int(stmt, 1, level);
        sqlite3_bind_text(stmt, 2, module, -1, SQLITE_STATIC);
        sqlite3_bind_text(stmt, 3, buffer, -1, SQLITE_STATIC);
        
        rc = sqlite3_step(stmt);
        sqlite3_finalize(stmt);
    }
    
    pthread_mutex_unlock(&log_mutex);
    
    return (rc == SQLITE_DONE) ? 0 : -1;
}

/* Query logs. */
int logger_query(int limit, LogEntry *entries)
{
    if (!db || !entries || limit <= 0) {
        return -1;
    }
    
    char sql[256];
    snprintf(sql, sizeof(sql), 
             "SELECT id, timestamp, level, module, message FROM system_logs ORDER BY id DESC LIMIT %d;", 
             limit);
    
    sqlite3_stmt *stmt;
    int count = 0;
    
    pthread_mutex_lock(&log_mutex);
    
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        while (sqlite3_step(stmt) == SQLITE_ROW && count < limit) {
            entries[count].id = sqlite3_column_int(stmt, 0);
            
            /* Parse the timestamp. */
            const char *ts_str = (const char *)sqlite3_column_text(stmt, 1);
            if (ts_str) {
                struct tm tm_time;
                memset(&tm_time, 0, sizeof(tm_time));
                sscanf(ts_str, "%d-%d-%d %d:%d:%d",
                       &tm_time.tm_year, &tm_time.tm_mon, &tm_time.tm_mday,
                       &tm_time.tm_hour, &tm_time.tm_min, &tm_time.tm_sec);
                tm_time.tm_year -= 1900;
                tm_time.tm_mon -= 1;
                entries[count].timestamp = mktime(&tm_time);
            } else {
                entries[count].timestamp = time(NULL);
            }
            
            entries[count].level = (LogLevel)sqlite3_column_int(stmt, 2);
            
            const char *module_str = (const char *)sqlite3_column_text(stmt, 3);
            if (module_str) {
                strncpy(entries[count].module, module_str, sizeof(entries[count].module) - 1);
                entries[count].module[sizeof(entries[count].module) - 1] = '\0';
            }
            
            const char *msg_str = (const char *)sqlite3_column_text(stmt, 4);
            if (msg_str) {
                strncpy(entries[count].message, msg_str, sizeof(entries[count].message) - 1);
                entries[count].message[sizeof(entries[count].message) - 1] = '\0';
            }
            
            count++;
        }
        sqlite3_finalize(stmt);
    }
    
    pthread_mutex_unlock(&log_mutex);
    
    return count;
}

/* Delete old logs. */
int logger_delete_old(int days)
{
    if (!db) {
        return -1;
    }
    
    char sql[256];
    if (days > 0) {
        snprintf(sql, sizeof(sql),
                 "DELETE FROM system_logs WHERE timestamp < datetime('now', '-%d days');",
                 days);
    } else {
        snprintf(sql, sizeof(sql),
                 "DELETE FROM system_logs;");
    }
    
    char *err_msg = NULL;
    int rc = sqlite3_exec(db, sql, NULL, NULL, &err_msg);
    
    if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL error deleting old logs: %s\n", err_msg);
        sqlite3_free(err_msg);
        return -1;
    }
    
    /* Return the affected row count. */
    return sqlite3_changes(db);
}
