#ifndef CCB_STORAGE_DIAG_H
#define CCB_STORAGE_DIAG_H

#include <stdint.h>
#include <stdatomic.h>

typedef enum {
    STORAGE_EVENT_DMA_BD_COMPLETED = 1, STORAGE_EVENT_DMA_BD_LOW,
    STORAGE_EVENT_DMA_BD_EXHAUSTED, STORAGE_EVENT_DMA_ERROR,
    STORAGE_EVENT_DESCRIPTOR_ERROR, STORAGE_EVENT_SLOT_STATE_ERROR,
    STORAGE_EVENT_NVME_SUBMIT_STALL, STORAGE_EVENT_NVME_CQ_STALL,
    STORAGE_EVENT_WRITER_SCHEDULE_GAP, STORAGE_EVENT_START_READY,
    STORAGE_EVENT_START_ARMED, STORAGE_EVENT_START_RUNNING,
    STORAGE_EVENT_WORKER_FATAL, STORAGE_EVENT_STOP_DRAINED
} StorageEventId;

typedef struct {
    uint64_t timestamp_us; uint32_t event_id; uint16_t channel; uint16_t flags;
    uint64_t arg0; uint64_t arg1; uint64_t sequence;
} StorageEventRecord;

typedef struct {
    StorageEventRecord *records; uint32_t capacity;
    _Atomic uint64_t next_sequence; _Atomic uint64_t fatal_sequence;
    StorageEventRecord fatal;
} StorageEventRing;

int storage_event_ring_init(StorageEventRing *ring, uint32_t capacity);
void storage_event_ring_destroy(StorageEventRing *ring);
void storage_event_ring_push(StorageEventRing *ring, const StorageEventRecord *record, int fatal);
uint32_t storage_event_ring_copy(StorageEventRing *ring, StorageEventRecord *out, uint32_t max);

#endif
