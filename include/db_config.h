#ifndef DB_CONFIG_H
#define DB_CONFIG_H

/* ==================== Database storage paths ==================== */

/*
 * Database file directory.
 * Current default: the program working directory.
 * Other possible values:
 *   - "./data"
 *   - "/opt/storage_app/db"
 *   - "/var/lib/storageapp"
 */
#define DB_STORAGE_DIR    "."

/*
 * Runtime log database.
 */
#define LOG_DB_NAME       "logs.db"
#define LOG_DB_PATH       DB_STORAGE_DIR "/" LOG_DB_NAME

/*
 * File list database for task_info and file_list records.
 */
#define FILELIST_DB_NAME  "filelist.db"
#define FILELIST_DB_PATH  DB_STORAGE_DIR "/" FILELIST_DB_NAME

/*
 * Persistent copy on the SPI1-mounted flash filesystem.
 * Override at runtime with SRC_REAL_FLASH_FILELIST_DB_PATH when the board uses
 * a different mount point.
 */
#define FLASH_FILELIST_DB_PATH "/mnt/spi1/filelist.db"

/* ==================== UART and process-test settings ==================== */

/* UART command device. Current default uses uart1 (/dev/ttyUL1). */
#define UART_DEV_PATH            "/dev/ttyUL1"

/* Backward-compatible alias for older code. */
#ifndef SERIAL_DEV_PATH
#define SERIAL_DEV_PATH          UART_DEV_PATH
#endif

/* Real storage metadata directory; keeps the existing meta_chN.bin layout. */
#define STORAGE_META_DIR         "/run/ccb_nvme_process_test"


/* Low-speed AD data width. */
#define LOW_SPEED_AD_WIDTH_BITS  16

/* ==================== Database backup settings ==================== */

/* Enable database backup. */
#define DB_BACKUP_ENABLED     0

/* Backup directory. */
#define DB_BACKUP_DIR         DB_STORAGE_DIR "/backup"

/* Backup retention days. 0 means no automatic cleanup. */
#define DB_BACKUP_RETAIN_DAYS 7

/* ==================== Helper macros ==================== */

/* Get database paths. */
#define GET_LOG_DB_PATH()       LOG_DB_PATH
#define GET_FILELIST_DB_PATH()  FILELIST_DB_PATH

#endif /* DB_CONFIG_H */
