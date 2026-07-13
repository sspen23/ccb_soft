#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdatomic.h>
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
    puts("mock_storage_ipc_test: ok"); return 0;
}
