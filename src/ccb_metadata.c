#include "ccb_metadata.h"

#include "ccb_hw.h"
#include "storage_config.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <sys/stat.h>
#include <sys/types.h>

#ifdef _WIN32
#include <direct.h>
#endif

/* File-list backend: same 32-byte entries and meta_chN.bin files as process_test. */
static const char *metadata_dir(void) {
    const char *env = storage_config_compat_getenv("CCB_PROCESS_META_DIR");
    if (env && env[0] != '\0') {
        return env;
    }
    return PROCESS_META_DIR_DEFAULT;
}

static int mkdir_one(const char *path) {
#ifdef _WIN32
    if (_mkdir(path) == 0 || errno == EEXIST) {
        return 0;
    }
#else
    if (mkdir(path, 0755) == 0 || errno == EEXIST) {
        return 0;
    }
#endif
    return -1;
}

static int ensure_metadata_dir(const char *path) {
    if (mkdir_one(path) == 0) {
        return 0;
    }
    fprintf(stderr, "Failed to create metadata directory %s: errno=%d (%s)\n", path, errno, strerror(errno));
    return -1;
}

static int build_metadata_path(const char *dir, int ch, char *path, size_t path_size) {
    int n = snprintf(path, path_size, "%s%smeta_ch%d.bin", dir, PATH_SEPARATOR, ch);
    return (n > 0 && (size_t)n < path_size) ? 0 : -1;
}

static int read_metadata_from_file(int ch, FileEntry *table) {
    const char *dir = metadata_dir();
    char path[512];
    FILE *fp;
    size_t n;

    if (ensure_metadata_dir(dir) != 0) {
        return -1;
    }
    if (build_metadata_path(dir, ch, path, sizeof(path)) != 0) {
        fprintf(stderr, "metadata path is too long\n");
        return -1;
    }

    fp = fopen(path, "rb");
    if (!fp) {
        /* Missing file is treated as an empty file list. */
        memset(table, 0, MAX_FILES_TOTAL * sizeof(FileEntry));
        return 0;
    }
    n = fread(table, sizeof(FileEntry), MAX_FILES_TOTAL, fp);
    if (ferror(fp)) {
        fprintf(stderr, "Failed to read %s\n", path);
        fclose(fp);
        return -1;
    }
    fclose(fp);
    if (n < MAX_FILES_TOTAL) {
        memset(table + n, 0, (MAX_FILES_TOTAL - n) * sizeof(FileEntry));
    }
    return 0;
}

static int write_metadata_to_file(int ch, const FileEntry *table) {
    const char *dir = metadata_dir();
    char path[512];
    FILE *fp;
    size_t n;

    if (ensure_metadata_dir(dir) != 0) {
        return -1;
    }
    if (build_metadata_path(dir, ch, path, sizeof(path)) != 0) {
        fprintf(stderr, "metadata path is too long\n");
        return -1;
    }

    fp = fopen(path, "wb");
    if (!fp) {
        fprintf(stderr, "Failed to write %s: errno=%d (%s)\n", path, errno, strerror(errno));
        return -1;
    }
    n = fwrite(table, sizeof(FileEntry), MAX_FILES_TOTAL, fp);
    fclose(fp);
    return (n == MAX_FILES_TOTAL) ? 0 : -1;
}

int metadata_read(ChannelRuntime *rt, FileEntry *table) {
    return read_metadata_from_file(rt->cfg->id, table);
}

int metadata_write(ChannelRuntime *rt, const FileEntry *table) {
    return write_metadata_to_file(rt->cfg->id, table);
}

int metadata_alloc_slot_and_lba(const FileEntry *table, int *out_slot, uint64_t *out_lba, uint32_t *out_valid_count) {
    int slot = -1;
    uint64_t next_lba = DATA_START_LBA;
    uint32_t valid_count = 0u;
    uint32_t i;

    /*
     * Allocation policy:
     * - metadata slot: first invalid entry
     * - data area: append after the highest end_lba among valid entries
     */
    for (i = 0; i < MAX_FILES_TOTAL; ++i) {
        if (table[i].valid == 1u) {
            uint64_t end_lba;
            ++valid_count;
            end_lba = table[i].start_lba + (uint64_t)table[i].sector_count;
            if (end_lba > next_lba) {
                next_lba = end_lba;
            }
        } else if (slot < 0) {
            slot = (int)i;
        }
    }

    if (slot < 0) {
        return -1;
    }
    *out_slot = slot;
    *out_lba = next_lba;
    *out_valid_count = valid_count;
    return 0;
}

static int range_overlaps(uint64_t start_a, uint64_t sectors_a, uint64_t start_b, uint64_t sectors_b) {
    uint64_t end_a = start_a + sectors_a;
    uint64_t end_b = start_b + sectors_b;
    return start_a < end_b && start_b < end_a;
}

int metadata_check_lba_overlap(const FileEntry *table, uint64_t start_lba, uint64_t sectors) {
    uint32_t i;
    if (UINT64_MAX - start_lba < sectors) {
        fprintf(stderr, "Requested LBA range overflows uint64\n");
        return -1;
    }
    for (i = 0; i < MAX_FILES_TOTAL; ++i) {
        if (table[i].valid != 1u) {
            continue;
        }
        if (UINT64_MAX - table[i].start_lba < (uint64_t)table[i].sector_count) {
            fprintf(stderr, "Existing metadata entry has invalid LBA range at slot %u\n", (unsigned)i);
            return -1;
        }
        if (range_overlaps(start_lba, sectors, table[i].start_lba, (uint64_t)table[i].sector_count)) {
            fprintf(stderr,
                    "Requested LBA range overlaps channel entry slot=%u start_lba=0x%08" PRIx64 " sectors=%u\n",
                    (unsigned)i,
                    table[i].start_lba,
                    (unsigned)table[i].sector_count);
            return -1;
        }
    }
    return 0;
}

static void task_string(const FileEntry *entry, char *out, size_t out_size) {
    memset(out, 0, out_size);
    memcpy(out, entry->task_no, 11u);
}

static int task_matches(const FileEntry *entry, const char *task_no) {
    char stored[12];
    task_string(entry, stored, sizeof(stored));
    return strcmp(stored, task_no) == 0;
}

int metadata_find_by_task(const FileEntry *table, const char *task_no, uint32_t file_index, int *out_slot, FileEntry *out) {
    uint32_t i;
    for (i = 0; i < MAX_FILES_TOTAL; ++i) {
        if (table[i].valid != 1u) {
            continue;
        }
        if (table[i].file_index != file_index) {
            continue;
        }
        if (!task_matches(&table[i], task_no)) {
            continue;
        }
        *out_slot = (int)i;
        *out = table[i];
        return 0;
    }
    return -1;
}

int metadata_resolve_file_index(const FileEntry *table,
                                const char *task_no,
                                uint32_t requested_index,
                                uint32_t *out_index) {
    uint32_t max_index = 0u;
    bool task_exists = false;
    uint32_t i;

    if (!table || !task_no || !out_index || requested_index > UINT16_MAX) {
        return -1;
    }
    for (i = 0u; i < MAX_FILES_TOTAL; ++i) {
        if (table[i].valid != 1u || !task_matches(&table[i], task_no)) {
            continue;
        }
        task_exists = true;
        if (table[i].file_index > max_index) {
            max_index = table[i].file_index;
        }
    }
    if (!task_exists || requested_index > max_index) {
        *out_index = requested_index;
        return 0;
    }
    if (max_index >= UINT16_MAX) {
        return -1;
    }
    *out_index = max_index + 1u;
    return 0;
}

void print_entry(int ch, int slot, const FileEntry *e) {
    char task[12];
    uint64_t effective_size;
    /* task_no in metadata may not be null-terminated, so copy into local buffer. */
    task_string(e, task, sizeof(task));
    effective_size = e->sector_count != 0u
                         ? (uint64_t)e->sector_count * 512u
                         : (uint64_t)e->file_size_bytes;
    printf("channel=%d slot=%d task=%s type=%u file_index=%u size=%" PRIu64
           " metadata_size_saturated=%u start_lba=0x%08" PRIx64 " sectors=%u valid=%u\n",
           ch,
           slot,
           task,
           (unsigned)e->file_type,
           (unsigned)e->file_index,
           effective_size,
           e->file_size_bytes == UINT32_MAX ? 1u : 0u,
           e->start_lba,
           (unsigned)e->sector_count,
           (unsigned)e->valid);
}
