#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>
#include "ccb_storage_ipc.h"

int main(void)
{
    int p[2]; StorageControlMessage c, out; StorageWorkerEvent e, eo;
    assert(pipe(p) == 0);
    storage_ipc_make_control(&c, STORAGE_CTRL_ARM, 1u);
    assert(storage_ipc_write_control(p[1], &c) == 0);
    assert(storage_ipc_read_control(p[0], &out) == 0 && out.type == STORAGE_CTRL_ARM);
    c.magic = 0u; assert(storage_ipc_write_control(p[1], &c) != 0);
    storage_ipc_make_event(&e, STORAGE_WORKER_READY, 0u, 0, 0u, "ready");
    assert(storage_ipc_write_event(p[1], &e) == 0);
    assert(storage_ipc_read_event(p[0], &eo) == 0 && eo.type == STORAGE_WORKER_READY);
    e.version++; assert(storage_ipc_write_event(p[1], &e) != 0);
    close(p[1]); assert(storage_ipc_read_control(p[0], &out) == 1); close(p[0]);
    puts("mock_storage_ipc_test: ok"); return 0;
}
