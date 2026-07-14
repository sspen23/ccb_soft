#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include "ccb_storage_ipc.h"

int main(void)
{
    int p[2]; StorageControlMessage c, out; StorageWorkerEvent e, eo; _Atomic uint64_t dropped = 0u, dropped_diag = 0u;
    assert(pipe(p) == 0);
    storage_ipc_make_control(&c, STORAGE_CTRL_ARM, 1u);
    assert(storage_ipc_write_control(p[1], &c) == 0);
    assert(storage_ipc_read_control(p[0], &out) == 0 && out.type == STORAGE_CTRL_ARM);
    c.magic = 0u; assert(storage_ipc_write_control(p[1], &c) != 0);
    storage_ipc_make_event(&e, STORAGE_WORKER_READY, 0u, 0, 0u, "ready");
    assert(storage_ipc_write_event(p[1], &e) == 0);
    assert(storage_ipc_read_event(p[0], &eo) == 0 && eo.type == STORAGE_WORKER_READY);
    e.version++; assert(storage_ipc_write_event(p[1], &e) != 0);
    assert(write(p[1], &e, sizeof(e)) == (ssize_t)sizeof(e));
    assert(storage_ipc_read_event_raw(p[0], &eo) == 0);
    assert(!storage_ipc_validate_event(&eo));
    close(p[1]); assert(storage_ipc_read_control(p[0], &out) == 1); close(p[0]);
    assert(pipe(p) == 0);
    storage_ipc_make_event(&e, STORAGE_WORKER_READY, 0u, 0, 0u, "partial");
    assert(write(p[1], &e, sizeof(e) / 2u) == (ssize_t)(sizeof(e) / 2u));
    close(p[1]);
    assert(storage_ipc_read_event_raw(p[0], &eo) == -1);
    close(p[0]);
    assert(pipe(p) == 0);
    assert(fcntl(p[1], F_SETFL, fcntl(p[1], F_GETFL, 0) | O_NONBLOCK) == 0);
    storage_ipc_make_event(&e, STORAGE_WORKER_PERF_SAMPLE, 0u, 0, 0u, "perf");
    while (write(p[1], &c, sizeof(c)) > 0) { }
    assert(storage_ipc_try_write_perf(p[1], &e, &dropped) == 1);
    assert(atomic_load(&dropped) == 1u);
    storage_ipc_make_event(&e, STORAGE_WORKER_DIAG_EVENT, 0u, 0, 0u, "diag");
    assert(storage_ipc_try_write_diag(p[1], &e, &dropped_diag) == 1);
    assert(atomic_load(&dropped_diag) == 1u);
    close(p[0]); close(p[1]);
    /* ARM/RUN reader preserves a partial nonblocking control frame across a
     * deadline instead of losing the consumed prefix. */
    assert(pipe(p) == 0);
    assert(fcntl(p[0], F_SETFL, fcntl(p[0], F_GETFL, 0) | O_NONBLOCK) == 0);
    storage_ipc_make_control(&c, STORAGE_CTRL_RUN, 7u);
    {
        StorageControlReader reader;
        storage_ipc_control_reader_init(&reader);
        assert(write(p[1], &c, sizeof(c) / 2u) == (ssize_t)(sizeof(c) / 2u));
        assert(storage_ipc_read_control_deadline(p[0], &reader, &out,
                                                 storage_ipc_monotonic_us()) != 0);
        assert(write(p[1], (const uint8_t *)&c + sizeof(c) / 2u,
                     sizeof(c) - sizeof(c) / 2u) ==
               (ssize_t)(sizeof(c) - sizeof(c) / 2u));
        assert(storage_ipc_read_control_deadline(p[0], &reader, &out,
                                                 storage_ipc_monotonic_us() + 100000u) == 0);
        assert(out.type == STORAGE_CTRL_RUN);
    }
    close(p[0]); close(p[1]);
    assert(pipe(p) == 0);
    storage_ipc_make_event(&e, STORAGE_WORKER_FATAL, 0u, -1, 1u, "fatal");
    assert(storage_ipc_write_event_deadline(p[1], &e, storage_ipc_monotonic_us() + 100000u) == 0);
    storage_ipc_make_event(&e, STORAGE_WORKER_FINAL_RESULT, 0u, 0, 1u, "final");
    assert(storage_ipc_write_event_deadline(p[1], &e, storage_ipc_monotonic_us() + 100000u) == 0);
    storage_ipc_make_event(&e, STORAGE_WORKER_DIAG_EVENT, 0u, 0, 0u, "diag");
    assert(storage_ipc_try_write_diag(p[1], &e, &dropped_diag) == 0);
    assert(storage_ipc_read_event(p[0], &eo) == 0 && eo.type == STORAGE_WORKER_FATAL);
    assert(storage_ipc_read_event(p[0], &eo) == 0 && eo.type == STORAGE_WORKER_FINAL_RESULT);
    assert(storage_ipc_read_event(p[0], &eo) == 0 && eo.type == STORAGE_WORKER_DIAG_EVENT);
    close(p[0]); close(p[1]);
    assert(pipe(p) == 0);
    assert(fcntl(p[1], F_SETFL, fcntl(p[1], F_GETFL, 0) | O_NONBLOCK) == 0);
    storage_ipc_make_event(&e, STORAGE_WORKER_FATAL, 0u, -1, 0u, "fatal_full_pipe");
    while (write(p[1], &c, sizeof(c)) > 0) { }
    assert(storage_ipc_write_event_deadline(
               p[1], &e, storage_ipc_monotonic_us()) != 0);
    close(p[0]); close(p[1]);
    {
        StorageParentStopTimeoutConfig stop_config;

        unsetenv("SRC_REAL_STORAGE_PARENT_STOP_TIMEOUT_US");
        unsetenv("SRC_REAL_STORAGE_PARENT_STOP_TIMEOUT_MS");
        setenv("SRC_REAL_DMA_QUIESCE_TIMEOUT_US", "100000", 1);
        setenv("SRC_REAL_STOP_HARVEST_TIMEOUT_US", "100000", 1);
        setenv("SRC_REAL_WRITER_DRAIN_TIMEOUT_US", "100000", 1);
        setenv("SRC_REAL_NVME_ABORT_TIMEOUT_US", "100000", 1);
        storage_ipc_parent_stop_timeout_config(&stop_config, 5000000u);
        assert(stop_config.source == STORAGE_PARENT_STOP_TIMEOUT_CALCULATED);
        assert(stop_config.stage_total_us == 400000u);
        assert(stop_config.margin_us == 5000000u);
        assert(stop_config.parent_timeout_us == 5400000u);
        /* A worker still inside its writer-drain phase is not reaped. */
        assert(!storage_ipc_parent_stop_should_force_reap(
            true, false, 300000u, stop_config.parent_timeout_us));

        setenv("SRC_REAL_STORAGE_PARENT_STOP_TIMEOUT_MS", "321", 1);
        setenv("SRC_REAL_STORAGE_PARENT_STOP_TIMEOUT_US", "123456", 1);
        storage_ipc_parent_stop_timeout_config(&stop_config, 5000000u);
        assert(stop_config.source == STORAGE_PARENT_STOP_TIMEOUT_EXPLICIT_US);
        assert(stop_config.parent_timeout_us == 123456u);
        unsetenv("SRC_REAL_STORAGE_PARENT_STOP_TIMEOUT_US");
        storage_ipc_parent_stop_timeout_config(&stop_config, 5000000u);
        assert(stop_config.source == STORAGE_PARENT_STOP_TIMEOUT_EXPLICIT_MS);
        assert(stop_config.parent_timeout_us == 321000u);
        unsetenv("SRC_REAL_STORAGE_PARENT_STOP_TIMEOUT_MS");

        setenv("SRC_REAL_DMA_QUIESCE_TIMEOUT_US", "18446744073709551615", 1);
        setenv("SRC_REAL_STOP_HARVEST_TIMEOUT_US", "18446744073709551615", 1);
        setenv("SRC_REAL_WRITER_DRAIN_TIMEOUT_US", "18446744073709551615", 1);
        setenv("SRC_REAL_NVME_ABORT_TIMEOUT_US", "18446744073709551615", 1);
        storage_ipc_parent_stop_timeout_config(&stop_config, 5000000u);
        assert(stop_config.stage_total_us == UINT64_MAX);
        assert(stop_config.parent_timeout_us == UINT64_MAX);
        assert(storage_ipc_saturating_add_u64(UINT64_MAX - 1u, 100u) == UINT64_MAX);

        /* A reaped worker never escalates; a live worker crosses the parent
         * deadline once, then the pending STOP latch suppresses repeats. */
        assert(!storage_ipc_parent_stop_should_force_reap(false, false, 10u, 1u));
        assert(!storage_ipc_parent_stop_should_force_reap(true, false, 9u, 10u));
        assert(storage_ipc_parent_stop_should_force_reap(true, false, 10u, 10u));
        assert(!storage_ipc_parent_stop_should_force_reap(true, true, 11u, 10u));

        unsetenv("SRC_REAL_DMA_QUIESCE_TIMEOUT_US");
        unsetenv("SRC_REAL_STOP_HARVEST_TIMEOUT_US");
        unsetenv("SRC_REAL_WRITER_DRAIN_TIMEOUT_US");
        unsetenv("SRC_REAL_NVME_ABORT_TIMEOUT_US");
    }
    puts("mock_storage_ipc_test: ok"); return 0;
}
