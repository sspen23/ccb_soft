# Storage pipeline V2 implementation plan

## Current structure

Each selected channel has one forked storage worker.  The worker owns the AXI
DMA S2MM ring and one NVMe writer thread.  The parent currently observes
worker stdout to find `storage_ready`, `storage_started`, and final result
text.  The producer harvests DMA descriptors and the writer returns completed
slots to the DMA tail pointer.

## Target structure and invariants

The parent retains three independent workers (ch0, ch1, ch2).  It creates a
control pipe and an event pipe for each worker.  The control protocol is
ARM/RUN/STOP; the event protocol is READY/ARMED/RUNNING/FATAL/DRAINED/FINAL.
Both pipes use fixed versioned records smaller than `PIPE_BUF`, validated for
magic, version and size.  stdout remains a non-authoritative compatibility
diagnostic channel only.

Worker phases are `READY -> ARMED -> RUNNING -> (FATAL | DRAINED) -> FINAL`.
Before RUN, producer and writer remain `SCHED_OTHER`.  RUN is released after
all selected workers report ARMED.  Any FATAL latches the first error in the
parent and broadcasts STOP to every target worker.

Slot ownership is strictly:

`DMA_WRITABLE -> DMA_COMPLETED_UNHARVESTED -> READY_FOR_NVME -> NVME_BUSY ->
REQUEUE_PENDING -> DMA_WRITABLE`.

After stop, completed NVMe slots may instead move to `FREE`.  All transitions
use one checked transition entry point; the six state counters must sum to the
ring size at every transition.  A full ready queue is fatal, never a producer
wait condition.

## Data-path policy

DMA harvest is batched in descriptor order (ch0/ch1 default 16, ch2 default
4) and stops at the first non-complete descriptor, the item limit, or a time
budget.  Normal flow uses software counts; full BD snapshots are rate-limited
to 100 ms and forced only for low-water, stop, finalization, or error paths.

ch0/ch1 default to cross-slot NVMe scheduling (QD 8, batch 4); ch2 retains
the single-slot default.  A persistent writer engine must drain CQ in batches,
then refill SQ, preserving CID-to-slot ownership across writer budgets.
DMA requeue remains ordered: all NVMe commands complete, mark
REQUEUE_PENDING, clear descriptor status, barrier, write TAILDESC/MSB,
barrier, check DMA, increment hardware-owned count, then DMA_WRITABLE.

## Scheduling and diagnostics

The defaults are RR/60 for every producer and writer, applied only after RUN.
Producer budget is 100 us, writer budget 300 us; an idle producer sleeps after
at most 20 us busy poll (3 us high channels, 20 us ch2).  A user-configured
higher producer priority uses nanosleep at its budget boundary, not only
`sched_yield`.

Console output is parent-owned and defaults to `critical`; performance records
go to append-only diagnostics and fixed event rings.  Producer/writer do not
format or synchronously write diagnostics in their hot loops.  FINAL is
emitted exactly once per worker after DMA/NVMe/file byte equality checks.

## Files and tests

New independent modules: `ccb_storage_ipc`, `ccb_storage_diag`, and
`ccb_storage_pipeline`.  Tests cover IPC framing, event ring integrity, slot
state/batch queue behavior, and DMA batch harvesting.  Existing command and
system code is migrated to use the structured IPC and batch APIs without
altering the serial protocol, FileEntry format, metadata, or SQLite logic.
