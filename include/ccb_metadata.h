#ifndef CCB_METADATA_H
#define CCB_METADATA_H

#include "ccb_types.h"

/* Read/write full metadata table from SSD (or dry-run file backend). */
int metadata_read(ChannelRuntime *rt, FileEntry *table);
int metadata_write(ChannelRuntime *rt, const FileEntry *table);

/* Allocate first free metadata slot and compute next append LBA. */
int metadata_alloc_slot_and_lba(const FileEntry *table, int *out_slot, uint64_t *out_lba, uint32_t *out_valid_count);

/* Reject explicit LBA ranges that overlap existing valid metadata entries. */
int metadata_check_lba_overlap(const FileEntry *table, uint64_t start_lba, uint64_t sectors);

/* Locate one file by (task_no, file_index). */
int metadata_find_by_task(const FileEntry *table, const char *task_no, uint32_t file_index, int *out_slot, FileEntry *out);
/* Keep a new file index above all existing entries for the same task. */
int metadata_resolve_file_index(const FileEntry *table,
                                const char *task_no,
                                uint32_t requested_index,
                                uint32_t *out_index);

/* Human-readable entry printer used by list command. */
void print_entry(int ch, int slot, const FileEntry *e);

#endif
