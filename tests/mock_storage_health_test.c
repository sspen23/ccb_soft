#include <assert.h>
#include <stdatomic.h>
#include <stdio.h>
#include <time.h>

#include "storage_health.h"

static atomic_uint g_calls;

static StorageErrorCode fake_probe(uint32_t channel, void *ctx,
                                   StorageHealthSnapshot *snapshot)
{
    (void)ctx;
    atomic_fetch_add(&g_calls, 1u);
    snapshot->channel = channel;
    snapshot->pcie_link = true;
    snapshot->nvme_ready = channel != 1u;
    snapshot->capacity_valid = channel != 1u;
    return channel == 1u ? STORAGE_ERR_NVME_PROBE : STORAGE_ERR_NONE;
}

int main(void)
{
    struct timespec pause = {0, 1000000L};
    struct timespec before;
    struct timespec after;
    StorageHealthSnapshot snapshots[3];
    StorageHealthResult health = STORAGE_HEALTH_RETRYING;
    unsigned attempts;

    assert(storage_health_start(fake_probe, NULL, 1000u) == 0);
    assert(storage_health_query(1000000u, snapshots) ==
           STORAGE_HEALTH_RETRYING);
    for (attempts = 0u; attempts < 1000u && atomic_load(&g_calls) < 3u;
         ++attempts)
        (void)nanosleep(&pause, NULL);
    assert(atomic_load(&g_calls) >= 3u);
    for (attempts = 0u; attempts < 1000u && health != STORAGE_HEALTH_FAILED;
         ++attempts) {
        health = storage_health_query(1000000u, snapshots);
        if (health != STORAGE_HEALTH_FAILED) (void)nanosleep(&pause, NULL);
    }
    assert(health == STORAGE_HEALTH_FAILED);
    (void)clock_gettime(CLOCK_MONOTONIC, &before);
    assert(storage_health_query(1000000u, snapshots) ==
           STORAGE_HEALTH_FAILED);
    (void)clock_gettime(CLOCK_MONOTONIC, &after);
    assert((after.tv_sec - before.tv_sec) * 1000000000L +
               after.tv_nsec - before.tv_nsec < 100000000L);
    assert(snapshots[0].nvme_ready);
    assert(!snapshots[1].nvme_ready);
    storage_health_stop();
    puts("mock_storage_health_test: ok");
    return 0;
}
