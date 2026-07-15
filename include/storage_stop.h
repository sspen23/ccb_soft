#ifndef STORAGE_STOP_H
#define STORAGE_STOP_H

#include <stdbool.h>
#include <stdint.h>

typedef enum {
    STORAGE_STOP_NONE = 0,
    STORAGE_STOP_REQUESTED,
    STORAGE_STOP_WAIT_BOUNDARY,
    STORAGE_STOP_DMA_QUIESCING,
    STORAGE_STOP_HARVESTING,
    STORAGE_STOP_PRODUCER_DONE,
    STORAGE_STOP_WRITER_DRAINING,
    STORAGE_STOP_FINALIZING,
    STORAGE_STOP_FINISHED,
    STORAGE_STOP_FAILED
} StorageStopPhase;

typedef int (*StorageDmaQuiesceFn)(void *opaque);

typedef struct {
    uint64_t drain_epoch;
    bool started;
    bool finished;
    int result;
} StorageDmaQuiesceState;

typedef struct {
    StorageStopPhase state;
    uint64_t deadline_us;
    StorageDmaQuiesceState quiesce;
} StorageStopState;

typedef struct {
    uint32_t consecutive_empty_scans;
    uint64_t empty_since_us;
} StorageStopHarvestState;

typedef enum {
    STORAGE_INPUT_IDLE_NO_CHANGE = 0,
    STORAGE_INPUT_IDLE_CANDIDATE,
    STORAGE_INPUT_ACTIVE
} StorageInputIdleEvent;

typedef struct {
    bool first_data_seen;
    bool candidate;
    uint64_t last_dma_activity_us;
    uint64_t dma_observed_bytes;
    uint64_t completed_descriptor_count;
    uint64_t idle_since_us;
    uint32_t idle_scan_count;
} StorageInputIdleState;

typedef enum {
    STORAGE_STOP_TAIL_QUEUE = 0,
    STORAGE_STOP_TAIL_DEFER_UNALIGNED,
    STORAGE_STOP_TAIL_DEFER_LATE
} StorageStopTailDisposition;

void storage_stop_state_init(StorageStopState *state);
uint64_t storage_dma_quiesce_wait_us(uint64_t configured_timeout_us);
int storage_dma_quiesce_once(StorageDmaQuiesceState *state,
                             uint64_t drain_epoch,
                             StorageDmaQuiesceFn quiesce,
                             void *opaque);
bool storage_stop_state_latch(StorageStopState *state, uint64_t deadline_us);
int storage_stop_state_advance(StorageStopState *state, StorageStopPhase next);
void storage_stop_state_fail(StorageStopState *state);
bool storage_stop_state_expired(const StorageStopState *state, uint64_t now_us);
bool storage_stop_boundary_should_quiesce(const StorageStopState *state,
                                          bool rx_packet_open,
                                          uint64_t now_us);
void storage_stop_harvest_state_init(StorageStopHarvestState *state);
void storage_input_idle_init(StorageInputIdleState *state);
StorageInputIdleEvent storage_input_idle_observe(
    StorageInputIdleState *state,
    uint64_t now_us,
    uint64_t dma_observed_bytes,
    uint64_t completed_descriptor_count,
    bool rx_packet_open,
    bool dma_error,
    uint64_t required_idle_us,
    uint32_t required_scans);
bool storage_stop_harvest_observe(StorageStopHarvestState *state,
                                  bool dma_quiesced,
                                  uint32_t harvested_count,
                                  uint32_t completed_unharvested,
                                  bool rx_packet_open,
                                  uint64_t now_us,
                                  uint32_t required_empty_scans,
                                  uint64_t stable_window_us);
StorageStopTailDisposition storage_stop_tail_disposition(bool stop_active,
                                                         bool tail_already_seen,
                                                         uint64_t payload_bytes,
                                                         uint64_t media_bytes,
                                                         bool padding_coherent);

#endif
