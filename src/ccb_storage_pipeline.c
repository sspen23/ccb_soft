#include "ccb_storage_pipeline.h"

#include <stdlib.h>
#include <string.h>

static uint32_t *storage_count_for_state(StorageSlotCounts *c, StorageSlotState state)
{
    switch (state) {
    case STORAGE_SLOT_DMA_WRITABLE: return &c->dma_writable;
    case STORAGE_SLOT_DMA_COMPLETED_UNHARVESTED: return &c->completed_unharvested;
    case STORAGE_SLOT_READY_FOR_NVME: return &c->ready;
    case STORAGE_SLOT_NVME_BUSY: return &c->nvme_busy;
    case STORAGE_SLOT_REQUEUE_PENDING: return &c->requeue_pending;
    case STORAGE_SLOT_FREE: return &c->free_count;
    default: return NULL;
    }
}

int storage_pipeline_counts_valid(const StoragePipeline *p)
{
    const StorageSlotCounts *c;
    if (!p) return 0;
    c = &p->counts;
    return c->total == c->dma_writable + c->completed_unharvested + c->ready +
                       c->nvme_busy + c->requeue_pending + c->free_count;
}

int storage_pipeline_init(StoragePipeline *p, uint32_t slots)
{
    if (!p || slots == 0u) return -1;
    memset(p, 0, sizeof(*p));
    p->items = calloc(slots, sizeof(*p->items));
    p->states = calloc(slots, sizeof(*p->states));
    if (!p->items || !p->states || pthread_mutex_init(&p->lock, NULL) != 0 ||
        pthread_cond_init(&p->not_empty, NULL) != 0) {
        free(p->items); free(p->states); memset(p, 0, sizeof(*p)); return -1;
    }
    p->capacity = slots;
    p->counts.total = slots;
    p->counts.dma_writable = slots;
    return 0;
}

void storage_pipeline_destroy(StoragePipeline *p)
{
    if (!p) return;
    (void)pthread_cond_destroy(&p->not_empty);
    (void)pthread_mutex_destroy(&p->lock);
    free(p->items); free(p->states); memset(p, 0, sizeof(*p));
}

int storage_slot_transition_locked(StoragePipeline *p, uint32_t slot,
                                   StorageSlotState expected, StorageSlotState next)
{
    uint32_t *old_count;
    uint32_t *new_count;
    if (!p || slot >= p->capacity || p->states[slot] != expected || !storage_pipeline_counts_valid(p)) {
        if (p) p->error = 1;
        return -1;
    }
    old_count = storage_count_for_state(&p->counts, expected);
    new_count = storage_count_for_state(&p->counts, next);
    if (!old_count || !new_count || *old_count == 0u) { p->error = 1; return -1; }
    --*old_count; ++*new_count; p->states[slot] = next;
    if (!storage_pipeline_counts_valid(p)) { p->error = 1; return -1; }
    return 0;
}

int storage_pipeline_mark_completed(StoragePipeline *p, uint32_t slot)
{
    int rc;
    if (!p) return -1;
    pthread_mutex_lock(&p->lock);
    rc = storage_slot_transition_locked(p, slot, STORAGE_SLOT_DMA_WRITABLE,
                                        STORAGE_SLOT_DMA_COMPLETED_UNHARVESTED);
    pthread_mutex_unlock(&p->lock);
    return rc;
}

int storage_queue_push_batch(StoragePipeline *p, const StoragePipelineItem *items,
                             uint32_t item_count)
{
    uint32_t i, j;
    if (!p || !items || item_count == 0u) return -1;
    pthread_mutex_lock(&p->lock);
    if (p->error || item_count > p->capacity - p->count) goto bad;
    for (i = 0u; i < item_count; ++i) {
        if (items[i].slot >= p->capacity || p->states[items[i].slot] != STORAGE_SLOT_DMA_COMPLETED_UNHARVESTED)
            goto bad;
        for (j = 0u; j < i; ++j) if (items[j].slot == items[i].slot) goto bad;
    }
    for (i = 0u; i < item_count; ++i) {
        p->items[p->tail] = items[i];
        p->tail = (p->tail + 1u) % p->capacity;
        ++p->count;
        if (storage_slot_transition_locked(p, items[i].slot,
                                           STORAGE_SLOT_DMA_COMPLETED_UNHARVESTED,
                                           STORAGE_SLOT_READY_FOR_NVME) != 0) goto bad;
    }
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
        *out = p->items[p->head]; p->head = (p->head + 1u) % p->capacity; --p->count;
        if (storage_slot_transition_locked(p, out->slot, STORAGE_SLOT_READY_FOR_NVME,
                                           STORAGE_SLOT_NVME_BUSY) != 0) rc = -1;
    }
    pthread_mutex_unlock(&p->lock);
    return rc;
}

int storage_pipeline_complete(StoragePipeline *p, uint32_t slot, int requeue)
{
    int rc;
    if (!p) return -1;
    pthread_mutex_lock(&p->lock);
    rc = storage_slot_transition_locked(p, slot, STORAGE_SLOT_NVME_BUSY,
                                        requeue ? STORAGE_SLOT_REQUEUE_PENDING : STORAGE_SLOT_FREE);
    pthread_mutex_unlock(&p->lock);
    return rc;
}
