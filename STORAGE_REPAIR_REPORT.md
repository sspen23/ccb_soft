# Storage repair verification report

## Scope and baseline

- Verification baseline: `a7323fa5299b0db9e1d59f2a393fabdb9347753b`.
- This report covers host verification only. It does not claim Vitis, bitstream, DMA, PCIe, NVMe media, scheduler-permission, or serial-board validation.
- No SQLite header or live database is required by the storage host tests. The commit test uses `StorageCommitOps` mocks.

## Host coverage map

| Area | Mock coverage |
| --- | --- |
| IPC | control/event validation, EOF, malformed event, nonblocking PERF/DIAG drop, reliable FATAL/FINAL ordering |
| Supervisor | READY/ARMED/RUNNING order, channel ownership, fatal fan-out, STOP retry state, duplicate FINAL, all-target aggregate, worker EOF/exit without FINAL |
| START/STOP | monotonic capture states, idempotent STOP latch, writer/producers ready before RUNNING, DMA quiesce and completed-BD drain |
| Commit | three-channel success, duplicate/rollback/error paths, mismatch rejection, parent-only completed status, standalone/supervised mode separation |
| Queue/bounded DMA | harvest limits at beginning/middle/end boundaries, partial descriptor, duplicate/state/capacity failures, atomic rollback, continuous chunk/LBA order, six-state conservation |
| Diagnostics | SPSC ring wrap/order, overwrite count, first-fatal latch, error/STOP dump policy, nonblocking PERF/DIAG and event-type log formatting |
| Cross-slot NVMe | multiple active slots, interleaved/out-of-order CQE, duplicate/unknown/status-error CID failures, capacity/SQ full, budget continuation, abort drain/reset, timeout, callback failure |
| Lifecycle | all task fd close/reset paths and child inherited-write-end closure |

## Static review checklist

The following properties were checked by targeted source inspection and host compilation:

- `storage_task_close_fds()` and runtime reset set every task fd to `-1`; the task mock covers normal/error exit variants and inherited writer closure.
- Worker events are structurally validated, bound to the expected event pipe channel, and the supervisor tracks terminal masks. FINAL is accepted once and aggregate emission is guarded once.
- Supervisor STOP remains pending until the parent marks the control write sent; retry failure remains pending and signal fallback is preserved in the parent lifecycle path.
- Cross-slot destruction is reached only after `nvme_cross_slot_engine_is_quiesced()`; failure requests abort then drains/resets before queue/runtime cleanup.
- Producer batch admission performs no `not_full` wait. Queue capacity/state failure is terminal rather than a producer wait loop.
- `STORAGE_WRITE_SUPERVISED` does not locally publish final metadata/DB/task completion. Parent aggregate commit owns `TASK_COMPLETED`/`TASK_FAILED`.
- `finalize_storage_task()` consumes structured final state only; there is no active stdout result parser in the default path.
- DIAG uses a single nonblocking IPC write and is deferred until after reliable FINAL; a DIAG drop is non-authoritative.
- Queue and pipeline snapshots use maintained six-state counts and validate their sum against capacity; descriptor inspection remains only in diagnostic/hardware snapshots.
- IPC terminal deadlines use `CLOCK_MONOTONIC`; DMA/NVMe ordering and existing barrier/MMIO code were not changed in this verification stage.
- Existing LBA overlap/range checks remain in the write path.

## Commands and real results

Executed from `src_real`:

```sh
git status --short
git diff --check
git log --oneline --decorate -12
gcc -std=c11 -Wall -Wextra -Wformat=2 -D_DEFAULT_SOURCE -Iinclude \
  -fsyntax-only src/ccb_storage_ipc.c src/ccb_storage_diag.c \
  src/ccb_storage_pipeline.c src/ccb_storage_supervisor.c \
  src/ccb_storage_task.c src/ccb_storage_perf.c src/ccb_storage_commit.c \
  src/ccb_commands.c src/ccb_hw.c
make storage-host-tests
make mock-bd-test
```

Results:

- `git diff --check`: passed.
- SQLite-free static syntax check with `-std=c11 -Wall -Wextra -Wformat=2`: passed.
- `make storage-host-tests`: passed all commit, IPC, pipeline, diagnostic, supervisor, task, performance, configuration, cross-slot, and DMA-harvest mocks.
- `make mock-bd-test`: passed. The expected injected `DMA requeue failed ...` diagnostic is part of the negative requeue case; the test reports `mock_bd_ring_test: ok`.
- No `make all`, Vitis compilation, real hardware access, SQLite integration test, or board test was run.

## Residual risk

Host mocks cannot prove AXI DMA halt timing under continuous Aurora input, NVMe controller reset behavior, real-time scheduling permissions, PCIe recovery, flash durability, or SSD data integrity. Run the board plan before declaring deployment-ready.
