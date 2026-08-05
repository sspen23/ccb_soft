#include <assert.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>

#include "storage_queue.h"

typedef struct {
    StorageRequeueGate gate;
    pthread_mutex_t lock;
    pthread_cond_t cond;
    bool action_started;
    bool release_action;
    bool stop_done;
    uint32_t action_calls;
    uint32_t stop_calls;
    StorageRequeueResult result;
} Fixture;

static int slow_requeue(void *opaque)
{
    Fixture *fixture = opaque;

    pthread_mutex_lock(&fixture->lock);
    ++fixture->action_calls;
    fixture->action_started = true;
    pthread_cond_broadcast(&fixture->cond);
    while (!fixture->release_action)
        pthread_cond_wait(&fixture->cond, &fixture->lock);
    pthread_mutex_unlock(&fixture->lock);
    return 0;
}

static void stop_action(void *opaque)
{
    Fixture *fixture = opaque;
    ++fixture->stop_calls;
}

static void *run_requeue(void *opaque)
{
    Fixture *fixture = opaque;
    fixture->result = storage_requeue_gate_run(&fixture->gate,
                                               slow_requeue, fixture);
    return NULL;
}

static void *run_stop(void *opaque)
{
    Fixture *fixture = opaque;
    (void)storage_requeue_gate_latch_stop(&fixture->gate, 77u,
                                          stop_action, fixture);
    pthread_mutex_lock(&fixture->lock);
    fixture->stop_done = true;
    pthread_cond_broadcast(&fixture->cond);
    pthread_mutex_unlock(&fixture->lock);
    return NULL;
}

static void fixture_init(Fixture *fixture)
{
    *fixture = (Fixture){0};
    assert(storage_requeue_gate_init(&fixture->gate) == 0);
    assert(pthread_mutex_init(&fixture->lock, NULL) == 0);
    assert(pthread_cond_init(&fixture->cond, NULL) == 0);
}

static void fixture_destroy(Fixture *fixture)
{
    storage_requeue_gate_destroy(&fixture->gate);
    assert(pthread_cond_destroy(&fixture->cond) == 0);
    assert(pthread_mutex_destroy(&fixture->lock) == 0);
}

static void test_stop_linearizes_first(void)
{
    Fixture fixture;

    fixture_init(&fixture);
    assert(storage_requeue_gate_latch_stop(&fixture.gate, 42u,
                                           stop_action, &fixture));
    assert(!storage_requeue_gate_latch_stop(&fixture.gate, 43u,
                                            stop_action, &fixture));
    assert(storage_requeue_gate_run(&fixture.gate, slow_requeue, &fixture) ==
           STORAGE_REQUEUE_STOPPED);
    assert(fixture.action_calls == 0u);
    assert(fixture.stop_calls == 1u);
    assert(fixture.gate.stop_epoch == 42u);
    fixture_destroy(&fixture);
}

static void test_requeue_linearizes_first(void)
{
    Fixture fixture;
    pthread_t requeue_thread;
    pthread_t stop_thread;
    pthread_mutex_t simulated_queue_lock = PTHREAD_MUTEX_INITIALIZER;

    fixture_init(&fixture);
    assert(pthread_create(&requeue_thread, NULL, run_requeue, &fixture) == 0);
    pthread_mutex_lock(&fixture.lock);
    while (!fixture.action_started)
        pthread_cond_wait(&fixture.cond, &fixture.lock);
    pthread_mutex_unlock(&fixture.lock);

    assert(pthread_create(&stop_thread, NULL, run_stop, &fixture) == 0);
    /* A slow DMA action owns only the requeue gate.  Ready-queue producers
     * can still acquire their independent queue lock immediately. */
    assert(pthread_mutex_trylock(&simulated_queue_lock) == 0);
    pthread_mutex_unlock(&simulated_queue_lock);

    pthread_mutex_lock(&fixture.lock);
    assert(!fixture.stop_done);
    fixture.release_action = true;
    pthread_cond_broadcast(&fixture.cond);
    pthread_mutex_unlock(&fixture.lock);

    assert(pthread_join(requeue_thread, NULL) == 0);
    assert(pthread_join(stop_thread, NULL) == 0);
    assert(fixture.result == STORAGE_REQUEUE_EXECUTED);
    assert(fixture.action_calls == 1u && fixture.stop_calls == 1u);
    assert(fixture.stop_done && fixture.gate.stop_epoch == 77u);
    assert(storage_requeue_gate_run(&fixture.gate, slow_requeue, &fixture) ==
           STORAGE_REQUEUE_STOPPED);
    assert(pthread_mutex_destroy(&simulated_queue_lock) == 0);
    fixture_destroy(&fixture);
}

int main(void)
{
    test_stop_linearizes_first();
    test_requeue_linearizes_first();
    puts("mock_storage_requeue_test: ok");
    return 0;
}
