# Storage pipeline V2

## Architecture

The parent retains serial-protocol ownership and launches one worker per
selected channel.  Each worker contains the DMA producer and NVMe writer.
Parent/worker control is a fixed binary pipe protocol; stdout is retained only
as a compatibility diagnostic channel.

## Two-phase start and IPC

`0x11` creates stdout, control, and event pipes.  The worker completes DMA
ring/NVMe/metadata/queue/writer initialization then sends `READY`.  `0x21`
sends `ARM` to all selected workers; each starts its halted S2MM ring and
reports `ARMED`.  After every target is armed, the parent broadcasts `RUN`
with one monotonic timestamp; workers report `RUNNING` and only then enable
RT scheduling.

`StorageControlMessage` and `StorageWorkerEvent` have magic `0x53544732`,
version 1, explicit size and fixed records smaller than `PIPE_BUF`.  Reads and
writes retry `EINTR`, validate headers, distinguish EOF, and retain the
control pipe through STOP/exit.  Events include READY, ARMED, RUNNING, FATAL,
DRAINED, FINAL_RESULT and PERF_SAMPLE.

## Scheduling

Default producer and writer policy is `SCHED_RR` priority 60 for ch0/ch1/ch2.
The defaults are controlled by `SRC_REAL_PRODUCER_RT_POLICY` and
`SRC_REAL_WRITER_RT_POLICY`; per-channel priority overrides remain supported.
New producer idle settings are `SRC_REAL_CHx_PRODUCER_IDLE_SLEEP_US` (3/3/20
us) and `SRC_REAL_PRODUCER_BUSY_POLL_US=20`.  Producer and writer time budgets
are `SRC_REAL_PRODUCER_BUDGET_US=100` and `SRC_REAL_WRITER_BUDGET_US=300`.
Legacy `SRC_REAL_CHx_DMA_POLL_SLEEP_US` remains a documented compatibility
fallback for deployments that have not moved to the new name.

## Data integrity

Slots follow six states: DMA_WRITABLE, DMA_COMPLETED_UNHARVESTED,
READY_FOR_NVME, NVME_BUSY, REQUEUE_PENDING, FREE.  The independent pipeline
module checks each transition and preserves the sum of state counters.  Batch
enqueue verifies every item, duplicate, range, state and capacity before one
commit; a full queue is fatal, not a producer wait.

DMA harvest uses `dma_harvest_batch()`, which emits ordered completed BDs,
stops at the first incomplete descriptor, and can return valid prior entries
alongside a fatal error.  `dma_harvest_one()` remains a compatibility wrapper.
The requeue sequence remains descriptor status clear, barrier, TAILDESC/MSB,
barrier, DMA status check, hardware count increment, then software writable.

ch0/ch1 default to cross-slot QD (8) with a batch of 4; ch2 defaults to the
single-slot path.  The cross-slot writer owns a persistent CID/slot engine:
each 300 us step drains a CQ batch, retires only fully completed slots, then
refills SQ without discarding inflight state.  Overrides are
`SRC_REAL_NVME_CROSS_SLOT_QD_CHx` and `SRC_REAL_NVME_CROSS_SLOT_BATCH_CHx`,
with legacy global values retained.

## Diagnostics and completion

`SRC_REAL_CONSOLE_LOG_LEVEL` defaults to `critical`; parent stdout only allows
DDR exhaustion, aggregate successful completion, and startup-fatal text.
`ccb_storage_diag` supplies a bounded overwrite event ring with a separate
fatal latch and release/acquire sequence publication, so dumps never consume a
partially written entry.  Performance logging is designed to be append-only
and non-blocking; its defaults are `SRC_REAL_PERF_LOG_ENABLE=1`,
`SRC_REAL_PERF_LOG_FILE=/tmp/storage_perf.log`, interval 5 seconds, no fsync.

On FATAL the parent latches the first event and broadcasts STOP to all active
workers.  The worker drains NVMe-complete slots before FINAL_RESULT.  Each
worker sends exactly one structured final WriteResult; parent aggregation uses
that result, including DMA/NVMe/file byte equality and ch0/ch1 equality.

## Vitis source list

Add these new C sources: `src/ccb_storage_ipc.c`, `src/ccb_storage_diag.c`,
`src/ccb_storage_pipeline.c`.  Add the matching headers under `include/`.
The independent host tests do not include SQLite, `system.c`, or `file_list.c`.
