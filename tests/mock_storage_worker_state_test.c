#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "storage_worker.h"

static void test_normal_lifecycle(void)
{
    StorageWorkerState state = WORKER_INIT;

    assert(storage_worker_transition(&state, WORKER_INIT, WORKER_READY) == 0);
    assert(storage_worker_transition(&state, WORKER_READY, WORKER_ARMED) == 0);
    assert(storage_worker_transition(&state, WORKER_ARMED, WORKER_RUNNING) == 0);
    assert(storage_worker_can_requeue(state));
    assert(storage_worker_transition(&state, WORKER_RUNNING,
                                     WORKER_WAIT_BOUNDARY) == 0);
    assert(storage_worker_stop_latched(state));
    assert(!storage_worker_can_requeue(state));
    assert(storage_worker_transition(&state, WORKER_WAIT_BOUNDARY,
                                     WORKER_DMA_QUIESCING) == 0);
    assert(storage_worker_transition(&state, WORKER_DMA_QUIESCING,
                                     WORKER_HARVESTING) == 0);
    assert(storage_worker_transition(&state, WORKER_HARVESTING,
                                     WORKER_DRAINING) == 0);
    assert(storage_worker_producer_done(state));
    assert(storage_worker_transition(&state, WORKER_DRAINING,
                                     WORKER_DRAINED_WAIT_STOP) == 0);
    assert(storage_worker_transition(&state, WORKER_DRAINED_WAIT_STOP,
                                     WORKER_FINALIZING) == 0);
    assert(storage_worker_transition(&state, WORKER_FINALIZING,
                                     WORKER_DONE) == 0);
    assert(strcmp(storage_worker_state_name(state), "done") == 0);
}

static void test_invalid_and_failure_transitions(void)
{
    StorageWorkerState state = WORKER_INIT;

    assert(storage_worker_transition(&state, WORKER_INIT, WORKER_RUNNING) != 0);
    assert(state == WORKER_INIT);
    assert(storage_worker_transition(&state, WORKER_INIT, WORKER_READY) == 0);
    assert(storage_worker_transition(&state, WORKER_READY,
                                     WORKER_FAILED) == 0);
    assert(state == WORKER_FAILED);
    assert(storage_worker_transition(&state, WORKER_FAILED,
                                     WORKER_FAILED) != 0);
    assert(storage_worker_transition(&state, WORKER_FAILED, WORKER_DONE) != 0);
    assert(storage_worker_stop_latched(state));
    assert(storage_worker_producer_done(state));
    assert(!storage_worker_can_requeue(state));
}

int main(void)
{
    test_normal_lifecycle();
    test_invalid_and_failure_transitions();
    puts("mock_storage_worker_state_test: ok");
    return 0;
}
