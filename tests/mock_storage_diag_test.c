#include <assert.h>
#include <stdio.h>
#include <string.h>
#include "ccb_storage_diag.h"

int main(void)
{
    StorageEventRing r; StorageEventRecord e, out[4]; uint32_t n;
    memset(&e, 0, sizeof(e)); e.event_id = STORAGE_EVENT_DMA_BD_COMPLETED;
    assert(storage_event_ring_init(&r, 2u) == 0);
    storage_event_ring_push(&r, &e, 0); e.arg0 = 1; storage_event_ring_push(&r, &e, 1);
    e.arg0 = 2; storage_event_ring_push(&r, &e, 0);
    n = storage_event_ring_copy(&r, out, 4u);
    assert(n == 2u && out[0].sequence + 1u == out[1].sequence);
    assert(r.fatal_sequence != 0u && r.fatal.arg0 == 1u);
    storage_event_ring_destroy(&r); puts("mock_storage_diag_test: ok"); return 0;
}
