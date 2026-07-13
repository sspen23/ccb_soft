#include "ccb_storage_diag.h"
#include <stdlib.h>
#include <string.h>

int storage_event_ring_init(StorageEventRing *ring, uint32_t capacity)
{
    if (!ring || capacity == 0u) return -1;
    memset(ring, 0, sizeof(*ring));
    ring->records = calloc(capacity, sizeof(*ring->records));
    if (!ring->records) return -1;
    ring->capacity = capacity;
    return 0;
}
void storage_event_ring_destroy(StorageEventRing *ring)
{ if (ring) { free(ring->records); memset(ring, 0, sizeof(*ring)); } }
void storage_event_ring_push(StorageEventRing *ring, const StorageEventRecord *record, int fatal)
{
    StorageEventRecord copy;
    uint64_t seq;
    if (!ring || !ring->records || !record) return;
    seq = atomic_fetch_add_explicit(&ring->next_sequence, 1u, memory_order_relaxed) + 1u;
    if (seq > ring->capacity) {
        (void)atomic_fetch_add_explicit(&ring->overwrite_count, 1u, memory_order_relaxed);
    }
    copy = *record; copy.sequence = 0u;
    ring->records[seq % ring->capacity] = copy;
    atomic_thread_fence(memory_order_release);
    ring->records[seq % ring->capacity].sequence = seq;
    if (fatal) {
        if (atomic_load_explicit(&ring->fatal_sequence, memory_order_relaxed) == 0u) {
            ring->fatal = copy;
            ring->fatal.sequence = seq;
            atomic_thread_fence(memory_order_release);
            atomic_store_explicit(&ring->fatal_sequence, seq, memory_order_release);
        }
    }
}
uint32_t storage_event_ring_copy(StorageEventRing *ring, StorageEventRecord *out, uint32_t max)
{
    uint64_t last, first, seq; uint32_t n = 0u;
    if (!ring || !out || max == 0u) return 0u;
    last = atomic_load_explicit(&ring->next_sequence, memory_order_acquire);
    first = last > ring->capacity ? last - ring->capacity + 1u : 1u;
    for (seq = first; seq <= last && n < max; ++seq) {
        StorageEventRecord r = ring->records[seq % ring->capacity];
        atomic_thread_fence(memory_order_acquire);
        if (r.sequence == seq) out[n++] = r;
    }
    if (atomic_load_explicit(&ring->fatal_sequence, memory_order_acquire) != 0u && n < max) {
        uint32_t i;
        bool present = false;
        for (i = 0u; i < n; ++i) if (out[i].sequence == ring->fatal.sequence) present = true;
        if (!present) out[n++] = ring->fatal;
    }
    {
        uint32_t i;
        for (i = 1u; i < n; ++i) {
            StorageEventRecord value = out[i];
            uint32_t j = i;
            while (j > 0u && out[j - 1u].sequence > value.sequence) {
                out[j] = out[j - 1u];
                --j;
            }
            out[j] = value;
        }
    }
    return n;
}

bool storage_event_ring_should_dump(bool is_error, bool stopped,
                                    bool dump_on_error, bool dump_on_stop)
{
    return (is_error && dump_on_error) || (stopped && dump_on_stop);
}
