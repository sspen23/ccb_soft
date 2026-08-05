#define _POSIX_C_SOURCE 200809L
#include "storage_health.h"

#include <errno.h>
#include <pthread.h>
#include <string.h>
#include <time.h>

#define STORAGE_HEALTH_CHANNELS 3u

typedef struct {
    pthread_t thread;
    pthread_mutex_t lock;
    pthread_cond_t wake;
    StorageHealthProbeFn probe;
    void *ctx;
    StorageHealthSnapshot snapshots[STORAGE_HEALTH_CHANNELS];
    uint32_t refresh_interval_ms;
    bool initialized;
    bool running;
    bool stop;
    bool busy;
    bool refresh_requested;
    bool abort_refresh;
} StorageHealthService;

static StorageHealthService g_health;

static uint64_t monotonic_us(void)
{
    struct timespec ts;

    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) return 0u;
    return (uint64_t)ts.tv_sec * 1000000ull +
           (uint64_t)ts.tv_nsec / 1000ull;
}

static struct timespec realtime_deadline(uint32_t delay_ms)
{
    struct timespec deadline;
    uint64_t nanoseconds;

    (void)clock_gettime(CLOCK_REALTIME, &deadline);
    nanoseconds = (uint64_t)deadline.tv_nsec +
                  (uint64_t)delay_ms * 1000000ull;
    deadline.tv_sec += (time_t)(nanoseconds / 1000000000ull);
    deadline.tv_nsec = (long)(nanoseconds % 1000000000ull);
    return deadline;
}

static void *storage_health_thread(void *unused)
{
    (void)unused;
    for (;;) {
        StorageHealthSnapshot refreshed[STORAGE_HEALTH_CHANNELS];
        uint32_t channel;
        bool busy;
        bool aborted = false;

        pthread_mutex_lock(&g_health.lock);
        if (g_health.stop) {
            pthread_mutex_unlock(&g_health.lock);
            break;
        }
        busy = g_health.busy;
        g_health.refresh_requested = false;
        g_health.abort_refresh = false;
        pthread_mutex_unlock(&g_health.lock);

        if (!busy) {
            memset(refreshed, 0, sizeof(refreshed));
            for (channel = 0u; channel < STORAGE_HEALTH_CHANNELS; ++channel) {
                StorageErrorCode error;

                pthread_mutex_lock(&g_health.lock);
                aborted = g_health.stop || g_health.busy ||
                          g_health.abort_refresh;
                pthread_mutex_unlock(&g_health.lock);
                if (aborted) break;

                refreshed[channel].channel = channel;
                error = g_health.probe(channel, g_health.ctx,
                                       &refreshed[channel]);
                refreshed[channel].error = error;
                refreshed[channel].checked_us = monotonic_us();

                pthread_mutex_lock(&g_health.lock);
                aborted = g_health.stop || g_health.busy ||
                          g_health.abort_refresh;
                pthread_mutex_unlock(&g_health.lock);
                if (aborted) break;
            }
            if (!aborted) {
                pthread_mutex_lock(&g_health.lock);
                memcpy(g_health.snapshots, refreshed, sizeof(refreshed));
                pthread_mutex_unlock(&g_health.lock);
            }
        }

        pthread_mutex_lock(&g_health.lock);
        while (!g_health.stop && !g_health.refresh_requested) {
            struct timespec deadline = realtime_deadline(
                g_health.refresh_interval_ms);
            int rc = pthread_cond_timedwait(&g_health.wake, &g_health.lock,
                                            &deadline);
            if (rc == ETIMEDOUT) break;
        }
        pthread_mutex_unlock(&g_health.lock);
    }
    return NULL;
}

int storage_health_start(StorageHealthProbeFn probe, void *ctx,
                         uint32_t refresh_interval_ms)
{
    uint32_t channel;

    if (!probe || refresh_interval_ms == 0u || g_health.initialized) return -1;
    memset(&g_health, 0, sizeof(g_health));
    if (pthread_mutex_init(&g_health.lock, NULL) != 0) return -1;
    if (pthread_cond_init(&g_health.wake, NULL) != 0) {
        (void)pthread_mutex_destroy(&g_health.lock);
        return -1;
    }
    for (channel = 0u; channel < STORAGE_HEALTH_CHANNELS; ++channel)
        g_health.snapshots[channel].channel = channel;
    g_health.probe = probe;
    g_health.ctx = ctx;
    g_health.refresh_interval_ms = refresh_interval_ms;
    g_health.initialized = true;
    g_health.running = true;
    if (pthread_create(&g_health.thread, NULL, storage_health_thread, NULL) != 0) {
        g_health.running = false;
        g_health.initialized = false;
        (void)pthread_cond_destroy(&g_health.wake);
        (void)pthread_mutex_destroy(&g_health.lock);
        return -1;
    }
    return 0;
}

void storage_health_stop(void)
{
    if (!g_health.initialized) return;
    pthread_mutex_lock(&g_health.lock);
    g_health.stop = true;
    pthread_cond_broadcast(&g_health.wake);
    pthread_mutex_unlock(&g_health.lock);
    if (g_health.running) (void)pthread_join(g_health.thread, NULL);
    (void)pthread_cond_destroy(&g_health.wake);
    (void)pthread_mutex_destroy(&g_health.lock);
    memset(&g_health, 0, sizeof(g_health));
}

void storage_health_set_busy(bool busy)
{
    bool was_busy;

    if (!g_health.initialized) return;
    pthread_mutex_lock(&g_health.lock);
    was_busy = g_health.busy;
    g_health.busy = busy;
    if (busy) {
        g_health.abort_refresh = true;
    }
    if (was_busy && !busy) {
        g_health.refresh_requested = true;
        pthread_cond_signal(&g_health.wake);
    }
    pthread_mutex_unlock(&g_health.lock);
}

void storage_health_request_refresh(void)
{
    if (!g_health.initialized) return;
    pthread_mutex_lock(&g_health.lock);
    g_health.refresh_requested = true;
    pthread_cond_signal(&g_health.wake);
    pthread_mutex_unlock(&g_health.lock);
}

void storage_health_abort_refresh(void)
{
    if (!g_health.initialized) return;
    pthread_mutex_lock(&g_health.lock);
    g_health.abort_refresh = true;
    pthread_cond_signal(&g_health.wake);
    pthread_mutex_unlock(&g_health.lock);
}

StorageHealthResult storage_health_query(uint64_t max_age_us,
                                         StorageHealthSnapshot snapshots[3])
{
    bool stale = false;
    bool failed = false;
    uint64_t now_us = monotonic_us();
    uint32_t channel;

    if (!g_health.initialized || max_age_us == 0u)
        return STORAGE_HEALTH_RETRYING;
    pthread_mutex_lock(&g_health.lock);
    if (snapshots)
        memcpy(snapshots, g_health.snapshots, sizeof(g_health.snapshots));
    for (channel = 0u; channel < STORAGE_HEALTH_CHANNELS; ++channel) {
        const StorageHealthSnapshot *snapshot = &g_health.snapshots[channel];

        if (snapshot->checked_us == 0u || now_us < snapshot->checked_us ||
            now_us - snapshot->checked_us > max_age_us) {
            stale = true;
            g_health.refresh_requested = true;
            continue;
        }
        if (snapshot->error != STORAGE_ERR_NONE || !snapshot->pcie_link ||
            !snapshot->nvme_ready || !snapshot->capacity_valid)
            failed = true;
    }
    if (g_health.refresh_requested) pthread_cond_signal(&g_health.wake);
    pthread_mutex_unlock(&g_health.lock);
    if (stale) return STORAGE_HEALTH_RETRYING;
    return failed ? STORAGE_HEALTH_FAILED : STORAGE_HEALTH_OK;
}
