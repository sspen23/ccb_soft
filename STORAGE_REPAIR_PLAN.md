# Storage pipeline repair plan

## Verification update (2026-07-13)

- Verification baseline: `a7323fa fix: minimize storage logging and preserve compatibility`.
- Implemented phases: deterministic START/STOP and event ownership; parent-only aggregate commit; cross-slot CID abort/drain/reset; bounded DMA batch/atomic queue/O(1) counters; and nonblocking diagnostics/logging compatibility.
- The current host suite is SQLite-free and covers IPC/event validation, supervisor sequencing and terminal aggregation, fd cleanup, commit rollback, DMA descriptor quiesce/harvest, bounded queue invariants, event-ring fatal retention, performance/DIAG serialization, and persistent cross-slot completion/abort paths.
- Remaining verification requiring target hardware is intentionally retained in `BOARD_THREE_NVME_TEST_PLAN.md`; no Vitis build is part of host verification.

## Audit baseline and scope

- Branch: `main`
- HEAD: `5b5a063a3f22b003d404a568bd1d2dc30f478101`
- Recent baseline: `5b5a063 storage: complete cross-slot scheduling policy`
- Pre-audit worktree: only untracked `filelist.db` and `logs.db`; they are not part of this plan or commit.
- `git diff --check` passed before the audit.
- Scope reviewed: `src/system.c`, `src/ccb_commands.c`, `src/ccb_hw.c`, storage IPC/supervisor/task/diag/perf/pipeline modules, public headers, mock tests, and `Makefile`.
- This stage changes documentation only. It does not change C behavior, SQLite data, hardware addresses, NVMe registers, DMA requeue order, metadata format, or serial protocol.

## Findings

### 1. STOP can wait forever for natural DMA idle

- Location: `src/ccb_commands.c`, `execute_write_with_result()` around the capture loop and `stop_idle_polls` handling (lines 2938-3227).
- Call chain: parent `request_storage_stop_all()` -> control pipe `STORAGE_CTRL_STOP` -> `storage_control_stop_requested()` -> `execute_write_with_result()` -> `dma_harvest_batch()`.
- Root cause: STOP only exits after `STORAGE_STOP_DRAIN_POLLS_DEFAULT` consecutive empty harvests. `stop_idle_polls` increments only when `harvest_count == 0` and is reset when data is harvested. A continuously active input therefore keeps DMA accepting data and prevents the loop from reaching the drain condition.
- Consequence: STOP timeout escalates to process signals even though the worker is healthy, and the final byte boundary is not controlled by the STOP state machine.

### 2. RUNNING is emitted before writer and both RT roles are ready

- Location: `src/ccb_commands.c`, `storage_wait_start_gate()` (lines 353-390), `storage_nvme_writer_thread()` / `storage_nvme_cross_slot_writer_thread()` (lines 2118-2252), and `execute_write_with_result()` (lines 2904-2935).
- Call chain: writer thread is created -> writer blocks in `storage_queue_wait_run()` -> worker receives RUN and emits `STORAGE_WORKER_RUNNING` inside `storage_wait_start_gate()` -> caller enables the queue and applies producer RT -> writer wakes and only then calls `storage_apply_writer_rt()`.
- Root cause: the externally visible RUNNING event is tied to receipt of the parent RUN message, not to an acknowledged local run barrier. There is no writer-created/writer-RT-ready/writer-running handshake.
- Consequence: the parent may start upstream traffic while the writer is still parked and before producer/writer scheduling policy is established.

### 3. Worker publishes metadata and database completion too early

- Location: `src/ccb_commands.c`, `execute_write_with_result()` calls `metadata_write()` (lines 3388-3415); `src/system.c`, `run_storage_worker_main()` calls `record_storage_result_to_db()` (lines 4337-4345); `record_storage_result_to_db()` commits SQLite and sets `TASK_COMPLETED` (lines 2359-2411). `wait_storage_workers_finished_after_stop()` also sets `TASK_COMPLETED` (lines 1230-1292).
- Call chain: one channel drains -> worker writes that channel metadata -> worker opens SQLite transaction, inserts one file, commits, sets task completed -> only afterwards emits DRAINED/FINAL -> parent supervisor later aggregates all target channels.
- Root cause: persistence publication is channel-local, while correctness is task-wide. FINAL and supervisor aggregate are not the commit authority.
- Consequence: another channel can fail after a successful channel has become visible; SQLite, metadata files, and task status can describe a partially completed task.

### 4. Cross-slot fatal can leave NVMe commands inflight

- Location: `src/ccb_hw.c`, `cross_slot_fail()`, `nvme_cross_slot_engine_step()`, and `nvme_cross_slot_engine_destroy()` (lines 2196-2512); `src/ccb_commands.c`, `storage_nvme_cross_slot_writer_thread()` (lines 2192-2252).
- Call chain: completion/submit/invariant error -> `cross_slot_fail()` -> writer marks error and breaks -> `nvme_cross_slot_engine_destroy()` only calls `free()` -> outer cleanup stops DMA and closes runtime.
- Root cause: the engine has no fatal state transition that blocks new submission, drains known pending CIDs with a deadline, or resets the queue/controller before freeing CID/context state.
- Consequence: outstanding hardware commands may complete after their software ownership has been discarded or after mappings begin teardown. Slot ownership and byte accounting are then unknowable.

### 5. Bounded batch can over-harvest, drop descriptors, and skip chunk indexes

- Location: `src/ccb_hw.c`, `dma_harvest_batch()` (lines 3338-3385); `src/ccb_commands.c`, multi-item branch in `execute_write_with_result()` (lines 3174-3223).
- Call chain: `dma_harvest_batch()` advances `next_harvest_bd` and decrements `dma_hw_desc_count` for the whole returned batch -> producer iterates returned descriptors -> bounded remaining bytes may reach zero before the returned batch is consumed -> error exits without queueing/requeueing later harvested descriptors.
- Root cause: harvesting mutates ownership before the bounded prefix is selected. The producer also assigns `chunk_index = dma_desc_completed_count + i` while `storage_stats_record_dma_desc()` increments `dma_desc_completed_count` inside the same loop, producing indexes such as 0, 2, 4 rather than a contiguous sequence.
- Consequence: descriptors can disappear from both DMA and queue ownership, and metadata/diagnostics can report non-contiguous chunks.

### 6. Batch queue mutation is not atomic on mid-batch failure

- Location: `src/ccb_commands.c`, `storage_local_queue_push_batch()` (lines 1300-1335); mirrored host model in `src/ccb_storage_pipeline.c`, `storage_queue_push_batch()`.
- Call chain: validate batch -> mutate tail/count/busy/bytes and transition each slot -> a later transition fails -> `goto bad` sets error without restoring earlier mutations.
- Root cause: validation and commit are separate, but the commit phase still invokes fallible transitions and has no transaction snapshot or rollback.
- Consequence: queue indices, slot states, counters, and buffered byte totals can disagree, making later cleanup and diagnostics unreliable.

### 7. Old and new cross-slot environment variables are not fully compatible

- Location: `src/ccb_commands.c`, `storage_channel_env_u32()` and config construction (lines 544-559 and 2666-2700); `src/ccb_hw.c`, `nvme_configure_runtime()` (lines 494-629).
- Current behavior:
  - Mode still accepts `SRC_REAL_NVME_CROSS_SLOT_QD[_CHn]`.
  - The old `SRC_REAL_NVME_CROSS_SLOT_BATCH[_CHn]` is parsed and printed but only stored in `StorageWriteQueue.cross_slot_batch`; it does not configure `NvmeCrossSlotConfig.max_active_slots` or `cq_batch`.
  - New per-channel values use `SRC_REAL_CHn_NVME_CROSS_SLOT_*`, while older channel conventions use the suffix form `..._CHn`.
  - Runtime values from `SRC_REAL_NVME_QD[_CHn]`, `SRC_REAL_NVME_CQ_POP_BATCH[_CHn]`, `SRC_REAL_NVME_BUSY_POLL_US[_CHn]`, and `SRC_REAL_NVME_POLL_SLEEP_US[_CHn]` are not consistently inherited by the explicit cross-slot config.
- Root cause: configuration was added beside the legacy parser without one precedence/alias table.
- Consequence: deployed settings can be silently ignored or produce a log value different from actual engine behavior.

### 8. DIAG dumping can delay FATAL and FINAL

- Location: `src/ccb_commands.c`, `storage_emit_diag_event()`, `storage_dump_event_ring()`, and `storage_maybe_dump_event_rings()` (lines 313-350); cleanup calls at lines 3370 and 3756; terminal events are emitted later by `run_storage_worker_main()` in `src/system.c` (lines 4350-4369).
- Call chain: DMA stops -> writer joins -> cleanup dumps up to both ring capacities -> every DIAG uses an independent 100 ms reliable deadline -> `execute_write_with_result()` returns -> worker emits FATAL/DRAINED/FINAL.
- Root cause: best-effort diagnostics are serialized ahead of reliable terminal events, with a fresh deadline per record. Default two 1024-entry rings can impose a very large aggregate delay when the pipe is full.
- Consequence: supervisor may report event EOF/worker exit or STOP timeout before the real terminal result is sent.

### 9. Event channel is not bound to its pipe owner

- Location: `src/system.c`, `drain_storage_events()` (lines 1586-1627); `src/ccb_storage_supervisor.c`, `storage_supervisor_handle_event()` (lines 36-73).
- Call chain: read event from `task->event_fd` -> log using `event.channel` -> supervisor accepts any channel in `target_channel_mask` -> update the current `Task` flags regardless of whether `event.channel` equals `task->planned_file.channel_id`.
- Root cause: IPC validation checks event shape and supervisor target membership, but not the expected channel associated with the dedicated pipe.
- Consequence: a corrupted or misrouted event can mark another channel FINAL while updating the wrong local Task object.

### 10. O(1) slot counters are not used for queue snapshots

- Location: `src/ccb_commands.c`, `StorageWriteQueue.slot_counts`, `storage_local_slot_transition_locked()` (lines 1117-1145), and `storage_queue_snapshot()` (lines 1392-1442).
- Root cause: every state transition maintains `StorageSlotCounts`, but snapshot still scans all `capacity` entries in `slot_state` under the queue mutex.
- Consequence: periodic snapshot time and lock hold time scale with ring size. The counters and scan can also diverge without the snapshot detecting the invariant explicitly.
- Note: `src/ccb_hw.c:dma_get_bd_snapshot()` still needs hardware descriptor inspection to distinguish newly completed DMA descriptors; this finding concerns the software queue-state part, not removal of necessary MMIO/descriptor observation.

### 11. Queue-empty wait is counted as writer schedule gap

- Location: `src/ccb_commands.c`, `storage_nvme_writer_thread()` (lines 2132-2147) and `storage_nvme_cross_slot_writer_thread()` (lines 2213-2223).
- Call chain: writer timestamps before blocking `storage_queue_pop(..., true)` -> producer eventually enqueues -> elapsed condition-variable wait >= 1 ms -> increments `writer_schedule_gap_count`.
- Root cause: voluntary queue-empty idle and involuntary failure to schedule while work is pending share the same timer.
- Consequence: performance logs overstate scheduler stalls during normal input gaps.

### 12. Quiet default still emits substantial legacy text

- Location: `src/ccb_commands.c`, unconditional `storage_emit_line()` / `printf()` calls for prepared/config/started/result/receive/failure records (notably lines 2832-2935 and 3598-3880); `src/ccb_hw.c`, unconditional probe/config/timing `printf()` calls (notably lines 313-332, 450, and 612-635).
- Root cause: `SRC_REAL_LOG_LEVEL=quiet` gates periodic helpers but does not gate most legacy/config/result lines. Structured IPC is authoritative, yet the worker still builds and writes several large text records by default.
- Consequence: stdout pipe traffic and formatting work remain high and can interfere with critical event handling.

### 13. `finalize_storage_task()` retains unreachable stdout parsing

- Location: `src/system.c`, `finalize_storage_task()` (lines 1990 onward), especially the unconditional returns before the comment at line 2064.
- Root cause: both `!final_result_seen` and `final_result_seen` branches return, leaving the old token parser and database reconstruction path unreachable.
- Consequence: dead code obscures the actual authoritative path, keeps obsolete parsing assumptions alive, and makes future lifecycle changes risky.

### 14. Cross-slot submit hot path performs redundant timing/log work

- Location: `src/ccb_hw.c`, `nvme_cross_slot_engine_step()` (lines 2355-2512) and `nvme_submit_command_async()` (lines 1096-1184).
- Call chain: each submit is timed before/after the ops call by the engine -> the real submit function independently calls `wall_time_us()` multiple times and evaluates verbose per-command logging -> completion handling also takes multiple timestamps per CQ item even when detailed diagnostics are not requested.
- Root cause: scheduling deadlines, timeout enforcement, aggregate metrics, and detailed diagnostics use overlapping clocks rather than a sampled/conditional timing policy.
- Consequence: ordinary cross-slot operation pays avoidable `clock_gettime` and formatting/check overhead per command/completion.

## Target START/STOP state machine

### START

Worker states will be explicit and monotonic:

1. `PREPARING`: open runtime, probe NVMe, read metadata for allocation only, prepare DMA ring, create queue and writer thread. No DMA ingress and no public metadata change.
2. `READY`: writer thread exists and is parked at a local barrier. The worker emits READY only after all resources needed for ARM exist.
3. `ARMING`: on ARM, writer applies its requested RT policy and acknowledges `writer_rt_ready`; producer applies its RT policy; failures are terminal. DMA is started only after both acknowledgements.
4. `ARMED`: DMA ring is ready/running, writer is still gated, and producer is ready to enter its capture loop. Emit ARMED only at this point.
5. `STARTING`: on RUN, set the queue run gate and wait for the writer to acknowledge `writer_running`.
6. `RUNNING`: record the common start timestamp and emit RUNNING only after writer enable, writer RT readiness, producer RT readiness, and the producer capture loop are all true. The parent starts upstream traffic only after every target channel is RUNNING.

The writer barrier will expose `created`, `rt_ready`, `run_enabled`, and `running` flags under the existing queue mutex/condition variable. Standalone mode follows the same local sequence without parent messages.

### STOP

STOP remains permanently latched and will not depend on empty harvest polls:

1. `STOP_LATCHED`: the signal/control path atomically latches STOP and rejects any later RUN/ARM transition.
2. `INGRESS_QUIESCE`: producer immediately stops requesting new work and calls a bounded DMA halt/quiesce operation. This defines the capture boundary even if upstream is continuous.
3. `FINAL_HARVEST`: after DMA is halted, inspect and commit only descriptors completed before the halt. Handle an incomplete tail using the existing integrity policy; do not re-arm descriptors.
4. `PRODUCER_DONE`: atomically publish the final queue batch, mark producer done, and wake writer.
5. `NVME_DRAIN`: writer submits no new work after the accepted queue is drained, then waits for known CIDs. A fatal path uses the drain/reset procedure below.
6. `WRITER_JOINED`: writer has exited and no NVMe command can access slot memory.
7. `TERMINAL_EVENTS`: emit FATAL when applicable, then DRAINED for a clean drain, then exactly one FINAL. DIAG is emitted afterwards with a total bounded/nonblocking policy.
8. `WORKER_EXIT`: close fds and exit only after terminal event attempts complete.

Parent STOP timeout escalation remains: retry control pipe, then SIGTERM for a permanently failed pipe or expired graceful deadline, and SIGKILL only for final process recovery.

## NVMe fatal drain/reset design

- Add an engine terminal state: `RUNNING`, `DRAINING`, `FAILED_DRAINING`, `RESET_REQUIRED`, `QUIESCED`. The first fatal reason is immutable.
- On any engine fatal, stop refill immediately. Preserve contexts, pending CID entries, global inflight count, and the original fatal reason.
- Drain only known pending CIDs until an absolute `fatal_drain_deadline_us`. Valid completions retire their exact CID. No new command is submitted, no failed slot is requeued, and task success callbacks are suppressed after task failure is latched.
- If all known CIDs retire, validate `pending == inflight == context inflight == 0`, snapshot diagnostics, and enter `QUIESCED` without reset.
- If the deadline expires, completion identity/status makes accounting untrustworthy, or hardware reports queue failure, invoke a narrow queue/controller recovery operation using existing MMIO definitions and initialization code. The recovery must stop command issue, reset/reinitialize queue state, invalidate all software CIDs, clear active QD, and confirm the hardware is no longer accessing DDR before runtime unmap.
- `nvme_cross_slot_engine_destroy()` will reject or loudly diagnose destruction with inflight commands; normal callers must call the new quiesce/drain API first.
- Return both `primary_reason` and `recovery_reason` so reset failure does not overwrite the original error.
- Host mock ops gain explicit `quiesce/reset` callbacks. No NVMe register address or command layout changes are planned.

## Metadata and database whole-task commit design

- Workers stop calling `metadata_write()`, `record_storage_result_to_db()`, and `task_update_status(..., TASK_COMPLETED)`. A successful worker FINAL reports an unpublished commit candidate: channel, slot/index, LBA, sectors, byte counts, type, and integrity result.
- Allocation remains read-only during PREPARING. The parent validates all candidates after the supervisor has FINAL from every target channel and aggregate integrity succeeds.
- The parent prepares one task transaction:
  1. Build staged metadata images for every affected channel without replacing live files.
  2. Write a small transaction manifest containing task id, old/new metadata checksums, target paths, and phase.
  3. Begin one SQLite transaction and insert/update all file rows and totals, but do not set TASK_COMPLETED yet.
  4. Publish all metadata images using temp-file plus atomic rename, recording progress in the manifest.
  5. Commit SQLite, then set TASK_COMPLETED in the same database transaction where supported; otherwise include the status update before the single commit.
  6. Mark the manifest committed and remove backups/staging.
- Before SQLite commit, any error restores metadata backups and rolls back SQLite. Startup recovery uses the manifest to finish or compensate an interrupted publish deterministically.
- On aggregate failure, discard all staged metadata/DB candidates and set TASK_FAILED once in the parent. NVMe data may remain physically written but is not made discoverable.
- Existing metadata layout and SQLite schema should remain unchanged unless crash recovery proves impossible without a version marker; any schema change requires a separate explicit approval.

## Bounded batch handling design

- Split DMA harvest into observe/select/commit semantics. A peek operation reads a contiguous completed prefix without advancing `next_harvest_bd` or decrementing `dma_hw_desc_count`; commit advances ownership for exactly the selected prefix.
- Before selection, obtain queue capacity/reservation and the bounded remaining-byte limit. Stop the selected prefix at the descriptor containing the requested boundary. Descriptors after that boundary remain owned and visible until ingress is halted; they are never silently dropped.
- If the final descriptor contains more bytes than requested, record source overflow/tail policy explicitly. Queue only the accepted bytes while retaining the descriptor's actual-byte accounting for receive integrity.
- Assign `chunk_index` from a dedicated `next_chunk_index` exactly once per committed queue item. Increment DMA statistics after commit, not while constructing a possibly rejected batch.
- On a descriptor error after valid earlier descriptors, commit the valid prefix, latch the fatal descriptor separately, halt ingress, and then enter the normal fatal drain path.

## Atomic batch queue design

- Under the queue mutex, validate the entire batch: capacity, unique slots, exact expected states, byte/sectors fields, counter conservation, and ring index bounds.
- Build all resulting tail/count/counter/state values in local variables. The commit phase performs only assignments and cannot call a fallible transition helper.
- Publish tail/count and signal the writer only after every item/state/counter has been committed.
- If validation fails, queue state is unchanged; only the first error reason/fatal latch changes.
- Apply the same algorithm to `src/ccb_storage_pipeline.c` so host tests exercise the production semantics rather than a weaker model.
- Add invariant checks at API boundaries, not per item in the hot path.

## Repair phases

### Phase 1: lifecycle, IPC ownership, and terminal priority

Files:

- `src/system.c`
- `src/ccb_commands.c`
- `src/ccb_storage_ipc.c`
- `src/ccb_storage_supervisor.c`
- `src/ccb_storage_task.c`
- `include/ccb_storage_ipc.h`
- `include/ccb_storage_supervisor.h`
- `include/ccb_storage_task.h`
- `tests/mock_storage_ipc_test.c`
- `tests/mock_storage_supervisor_test.c`
- `tests/mock_storage_task_test.c`
- `Makefile`

Changes:

- Implement local writer/producer START acknowledgements and the explicit STOP quiesce sequence.
- Validate every event against the channel bound to its pipe before logging or supervisor mutation.
- Send reliable terminal events before diagnostics; give DIAG one total bounded/nonblocking budget.
- Remove the unreachable stdout parser from `finalize_storage_task()` and keep structured FINAL authoritative.
- Gate legacy worker text behind an explicit compatibility switch; keep parent aggregate and critical errors.

Host tests:

- RUNNING is impossible before writer created, writer RT ready, producer RT ready, and run gate acknowledged.
- Continuous synthetic DMA input still reaches STOP quiesced/drained state within a bound.
- STOP before READY, during ARM, during RUN, and repeated STOP are monotonic/idempotent.
- Wrong-channel READY/FATAL/FINAL on a task pipe is rejected with `event_channel_mismatch` and cannot complete another channel.
- Full event pipe cannot let DIAG delay FATAL/FINAL beyond the terminal deadline.
- FINAL is emitted once; parent aggregate remains once-only; all fds end at `-1`.

### Phase 2: bounded harvest, atomic queue, O(1) snapshot, and stall classification

Files:

- `src/ccb_commands.c`
- `src/ccb_hw.c`
- `src/ccb_storage_pipeline.c`
- `include/ccb_hw.h`
- `include/ccb_storage_pipeline.h`
- `include/ccb_storage_ipc.h`
- `src/ccb_storage_perf.c`
- `tests/mock_dma_harvest_batch_test.c`
- `tests/mock_storage_pipeline_test.c`
- `tests/mock_storage_perf_test.c`
- `Makefile`

Changes:

- Add observe/select/commit bounded harvest and contiguous chunk numbering.
- Make production and mock batch queue commits atomic.
- Populate queue snapshots from `StorageSlotCounts` in O(1), with an invariant assertion/failure reason.
- Separate `writer_queue_empty_wait` from `writer_schedule_gap`; schedule gap is counted only while runnable work or inflight engine work exists.

Host tests:

- Bounded size ending at the first/middle/last descriptor of a batch consumes no later descriptor.
- Final-descriptor trimming, descriptor error after a valid prefix, and queue capacity smaller than harvest prefix preserve ownership.
- Chunk indexes remain contiguous across batch sizes and budget interruptions.
- Injected failure at every batch item leaves head/tail/count/states/counters/bytes unchanged.
- O(1) snapshot equals a test-only full scan for every slot transition.
- Empty condition-variable wait increments only queue-empty wait; delayed writer with nonempty queue increments schedule gap.

### Phase 3: cross-slot fatal closure, config compatibility, and hot-path cost

Files:

- `src/ccb_commands.c`
- `src/ccb_hw.c`
- `include/ccb_hw.h`
- `include/ccb_types.h` if runtime recovery state is required
- `tests/mock_nvme_cross_slot_test.c`
- `Makefile`

Changes:

- Add fatal quiesce/drain/reset API and forbid engine destruction with inflight CIDs.
- Centralize config precedence in `ccb_commands.c`; pass one resolved `NvmeCrossSlotConfig` into the engine.
- Preserve legacy aliases with a single startup warning when aliases conflict.
- Remove redundant per-command clocks and logs in normal mode; reuse batch timestamps and enable detailed timing only through the existing diagnostic flag/sampling policy.

Host tests:

- Fatal with zero, one, and many inflight CIDs; orderly drain; drain timeout; reset success/failure.
- No submit after first fatal, no callback/requeue for failed task, and no destroy while inflight.
- Late, duplicate, unknown, interleaved, and status-error completions retain the first reason and choose the correct recovery path.
- Table-driven environment precedence tests for new per-channel, new global, legacy per-channel, legacy global, and defaults; ch2 remains legacy single-slot by default.
- Clock/log mock counters show normal mode does not time or format every command while deadlines and aggregate stats remain correct.

### Phase 4: whole-task metadata and SQLite commit

Files:

- `src/system.c`
- `src/ccb_commands.c`
- `src/ccb_metadata.c`
- `src/file_list.c` or the existing SQLite transaction implementation
- relevant headers under `include/`
- new focused host tests under `tests/mock_*` using temporary files and an in-memory/test SQLite database
- `Makefile`

Changes:

- Make worker FINAL carry an unpublished candidate and move publication authority to parent aggregate success.
- Stage and recover metadata images with a transaction manifest.
- Commit all SQLite file rows, totals, and TASK_COMPLETED as one task operation.
- Remove duplicate worker/STOP-wait status updates; parent sets terminal status once.

Host tests:

- Two-channel success publishes both metadata files and DB rows together.
- Failure before/after each stage or rename leaves either the complete old state or complete new state after recovery.
- One worker failure publishes no metadata/DB row and sets TASK_FAILED once.
- Duplicate FINAL/replayed commit is idempotent and cannot duplicate DB records.
- Process restart at every manifest phase completes or compensates deterministically.

## Compatibility risks

- Environment precedence must be documented and stable. Proposed order is new per-channel `SRC_REAL_CHn_NVME_CROSS_SLOT_*`, new global, legacy suffix `_CHn`, legacy global, then channel default. `SRC_REAL_NVME_QD[_CHn]`, CQ batch, busy poll, and sleep aliases must map only where semantics match.
- Legacy `SRC_REAL_NVME_CROSS_SLOT_BATCH` historically meant active slots, not CQ completion batch. It should alias `max_active_slots`; mapping it to `cq_batch` would silently change behavior.
- Moving metadata/SQLite publication to the parent changes when files become visible and changes the meaning of `data_persisted`. IPC versioning or an explicit `nvme_data_complete`/`metadata_published` distinction may be required.
- Quieting stdout can affect external scripts that still parse legacy text even though stdout is documented as non-authoritative. Keep a temporary opt-in legacy switch and emit one deprecation warning outside the hot path.
- Immediate DMA quiesce on STOP changes the exact accepted tail compared with natural-idle draining. The final boundary and incomplete descriptor policy must be visible in FINAL.
- NVMe reset affects all commands sharing the same hardware queue. Confirm each worker/channel owns an independent queue before enabling automatic reset.
- Transaction manifests add crash-recovery files. Their directory, durability expectations on ramfs, flash synchronization order, and cleanup policy need explicit operational documentation.

## Vitis and board-only validation

Do not run these as part of host implementation or CI; they require the target bitstream/platform and are not substitutes for host tests:

- Confirm the exact AXI DMA halt behavior under continuous Aurora input: whether a descriptor can complete after halt request, how RXSOF/RXEOF and partial tail status behave, and whether upstream data loss must be reported.
- Measure ARM/RUN skew across ch0/ch1 after the new RT acknowledgements, including effective `SCHED_RR 60/60` permissions and fallback behavior.
- Verify NVMe host-core queue reset/quiesce sequencing, late CQ behavior, interrupt/status clearing, and that DDR is no longer accessed before unmap/reuse.
- Inject SQ full, CQ stall, completion status errors, unknown/duplicate CID observations, and reset timeout on real hardware without changing register definitions.
- Validate that ch0/ch1 cross-slot defaults and all legacy environment aliases produce the intended actual QD/active-slot/CQ-batch values; confirm ch2 remains on single-slot legacy path.
- Measure STOP latency and accepted-byte boundary under sustained full-rate input; verify no descriptor or chunk index is lost.
- Measure hot-path CPU cost and throughput with detailed timing/logging disabled and enabled, including `clock_gettime`, stdout, and event-pipe pressure.
- Power-cycle at each metadata/SQLite manifest phase and verify recovery from ramfs/flash behavior on the deployed filesystem.
- Verify serial output remains limited to required protocol/aggregate records and `/tmp/storage_perf.log` receives PERF/FATAL/FINAL/DIAG without delaying terminal control.

## Completion criteria

- All four phases pass their focused host tests, `make storage-host-tests`, `make mock-bd-test`, and `git diff --check`.
- STOP terminates under continuous input without SIGKILL in the healthy case.
- RUNNING means both RT roles and the writer run gate are ready.
- No engine/runtime teardown occurs with unknown NVMe inflight ownership.
- A task is visible in metadata and SQLite only after successful all-channel aggregate, and TASK_COMPLETED is written once.
- Bounded harvest and batch queue failures preserve descriptor, slot, queue, CID, and chunk-index invariants.
