#include "ccb_storage_pipeline.h"

#include <stdlib.h>
#include <string.h>

uint32_t storage_ring_pressure_level(uint32_t occupied_slots,
                                     uint32_t total_slots,
                                     uint32_t warning_percent,
                                     uint32_t critical_percent)
{
    uint64_t occupied_percent;

    if (total_slots == 0u || warning_percent == 0u ||
        warning_percent >= critical_percent || critical_percent > 100u)
        return 0u;
    if (occupied_slots >= total_slots) return 3u;
    occupied_percent = (uint64_t)occupied_slots * 100u / total_slots;
    if (occupied_percent >= critical_percent) return 2u;
    if (occupied_percent >= warning_percent) return 1u;
    return 0u;
}

uint32_t storage_writer_budget_for_pressure(uint32_t base_budget_us,
                                            uint32_t pressure_level)
{
    uint64_t scaled = base_budget_us;

    if (pressure_level >= 2u) scaled *= 2u;
    else if (pressure_level == 1u) scaled += (scaled + 1u) / 2u;
    return scaled > UINT32_MAX ? UINT32_MAX : (uint32_t)scaled;
}

bool storage_ring_pressure_should_stop(uint32_t pressure_level,
                                       uint64_t critical_since_us,
                                       uint64_t now_us,
                                       uint64_t critical_duration_us,
                                       bool stop_enabled)
{
    return stop_enabled && pressure_level >= 2u && critical_since_us != 0u &&
           now_us >= critical_since_us &&
           now_us - critical_since_us >= critical_duration_us;
}

bool storage_ring_pressure_requires_degraded_drain(uint32_t pressure_level,
                                                   bool drain_requested)
{
    return pressure_level >= 2u && !drain_requested;
}

bool storage_receive_failure_allows_drain(bool safe_discard,
                                          bool pressure_drain,
                                          bool dma_error,
                                          bool descriptor_error)
{
    return (safe_discard || pressure_drain) && !dma_error &&
           !descriptor_error;
}

StorageFirstDmaDeadlineOutcome storage_first_dma_deadline_outcome(
    bool deadline_due, bool saw_dma_data, bool stop_requested,
    int harvest_rc, uint32_t harvest_count)
{
    if (harvest_rc != 0) return STORAGE_FIRST_DMA_DEADLINE_HARVEST_FAILED;
    if (harvest_count != 0u || saw_dma_data)
        return STORAGE_FIRST_DMA_DEADLINE_DATA;
    if (deadline_due && !stop_requested)
        return STORAGE_FIRST_DMA_DEADLINE_EXPIRED;
    return STORAGE_FIRST_DMA_DEADLINE_WAIT;
}

uint32_t storage_dma_harvest_batch_limit(uint32_t base_limit,
                                         uint32_t total_slots,
                                         uint32_t completed_unharvested,
                                         uint32_t available_limit)
{
    uint32_t limit = base_limit;
    uint32_t quarter;
    uint32_t half;

    if (base_limit == 0u || total_slots == 0u || available_limit == 0u)
        return 0u;
    quarter = (total_slots + 3u) / 4u;
    half = (total_slots + 1u) / 2u;
    if (completed_unharvested >= half && limit < 64u) limit = 64u;
    else if (completed_unharvested >= quarter && limit < 32u) limit = 32u;
    if (limit > available_limit) limit = available_limit;
    return limit;
}

bool storage_dma_emergency_harvest(uint32_t dma_writable,
                                   uint32_t completed_unharvested,
                                   uint32_t total_slots)
{
    uint32_t half;

    if (total_slots == 0u) return false;
    half = (total_slots + 1u) / 2u;
    return dma_writable == 0u || completed_unharvested >= half;
}

bool storage_dma_producer_may_idle(uint32_t harvested,
                                   uint32_t completed_unharvested)
{
    return harvested == 0u && completed_unharvested == 0u;
}

int storage_pipeline_counts_valid(const StoragePipeline *p)
{
    if (!p) return 0;
    return p->count <= p->slots.capacity &&
           storage_slot_table_valid(&p->slots);
}

uint32_t storage_harvest_limit_for_remaining(uint64_t remaining_bytes,
                                             uint32_t dma_desc_bytes,
                                             uint32_t max_batch)
{
    uint64_t needed;
    if (remaining_bytes == 0u || dma_desc_bytes == 0u || max_batch == 0u) return 0u;
    needed = remaining_bytes / dma_desc_bytes;
    if ((remaining_bytes % dma_desc_bytes) != 0u) ++needed;
    return needed < max_batch ? (uint32_t)needed : max_batch;
}

int storage_pipeline_init(StoragePipeline *p, uint32_t slots)
{
    if (!p || slots == 0u) return -1;
    memset(p, 0, sizeof(*p));
    p->items = calloc(slots, sizeof(*p->items));
    if (!p->items || storage_slot_table_init(&p->slots, slots) != 0 ||
        pthread_mutex_init(&p->lock, NULL) != 0 ||
        pthread_cond_init(&p->not_empty, NULL) != 0) {
        free(p->items);
        storage_slot_table_destroy(&p->slots);
        memset(p, 0, sizeof(*p));
        return -1;
    }
    return 0;
}

void storage_pipeline_destroy(StoragePipeline *p)
{
    if (!p) return;
    (void)pthread_cond_destroy(&p->not_empty);
    (void)pthread_mutex_destroy(&p->lock);
    free(p->items);
    storage_slot_table_destroy(&p->slots);
    memset(p, 0, sizeof(*p));
}

int storage_pipeline_mark_completed(StoragePipeline *p, uint32_t slot)
{
    int rc;
    if (!p) return -1;
    pthread_mutex_lock(&p->lock);
    rc = storage_slot_transition(&p->slots, slot, STORAGE_SLOT_DMA_WRITABLE,
                                 STORAGE_SLOT_DMA_COMPLETED_UNHARVESTED);
    if (rc != 0) p->error = 1;
    pthread_mutex_unlock(&p->lock);
    return rc;
}

int storage_queue_push_batch(StoragePipeline *p, const StoragePipelineItem *items,
                             uint32_t item_count)
{
    uint32_t i, j, tail;
    if (!p || !items || item_count == 0u) return -1;
    pthread_mutex_lock(&p->lock);
    if (p->error || p->count > p->slots.capacity || !storage_pipeline_counts_valid(p) ||
        item_count > p->slots.capacity - p->count ||
        item_count > p->slots.counts.completed_unharvested) goto bad;
    for (i = 0u; i < item_count; ++i) {
        uint64_t expected_sectors;
        if (items[i].bytes == 0u || items[i].sectors == 0u ||
            items[i].slot >= p->slots.capacity ||
            storage_slot_state(&p->slots, items[i].slot) !=
                STORAGE_SLOT_DMA_COMPLETED_UNHARVESTED)
            goto bad;
        expected_sectors = items[i].bytes / 512u;
        if ((items[i].bytes % 512u) != 0u) ++expected_sectors;
        if (items[i].sectors != expected_sectors) goto bad;
        for (j = 0u; j < i; ++j) if (items[j].slot == items[i].slot) goto bad;
    }
    /* Commit below only performs assignments proven safe by the validation above. */
    tail = p->tail;
    for (i = 0u; i < item_count; ++i) {
        p->items[tail] = items[i];
        tail = (tail + 1u) % p->slots.capacity;
        if (storage_slot_transition(&p->slots, items[i].slot,
                                    STORAGE_SLOT_DMA_COMPLETED_UNHARVESTED,
                                    STORAGE_SLOT_READY_FOR_NVME) != 0)
            goto bad;
    }
    p->tail = tail;
    p->count += item_count;
    pthread_cond_signal(&p->not_empty);
    pthread_mutex_unlock(&p->lock);
    return 0;
bad:
    p->error = 1;
    pthread_cond_broadcast(&p->not_empty);
    pthread_mutex_unlock(&p->lock);
    return -1;
}

int storage_pipeline_pop(StoragePipeline *p, StoragePipelineItem *out, int wait)
{
    int rc = 0;
    if (!p || !out) return -1;
    pthread_mutex_lock(&p->lock);
    while (wait && !p->error && p->count == 0u) pthread_cond_wait(&p->not_empty, &p->lock);
    if (p->error) rc = -1;
    else if (p->count == 0u) rc = 1;
    else {
        *out = p->items[p->head];
        p->head = (p->head + 1u) % p->slots.capacity;
        --p->count;
        if (storage_slot_transition(&p->slots, out->slot,
                                    STORAGE_SLOT_READY_FOR_NVME,
                                    STORAGE_SLOT_NVME_BUSY) != 0) {
            p->error = 1;
            rc = -1;
        }
    }
    pthread_mutex_unlock(&p->lock);
    return rc;
}

int storage_pipeline_complete(StoragePipeline *p, uint32_t slot, int requeue)
{
    int rc;
    if (!p) return -1;
    pthread_mutex_lock(&p->lock);
    rc = storage_slot_transition(&p->slots, slot, STORAGE_SLOT_NVME_BUSY,
                                 requeue ? STORAGE_SLOT_REQUEUE_PENDING
                                         : STORAGE_SLOT_FREE);
    if (rc != 0) p->error = 1;
    pthread_mutex_unlock(&p->lock);
    return rc;
}
