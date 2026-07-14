# Three-NVMe board verification plan

## Common setup

- Use the production bitstream/platform already approved for the board. This plan does not authorize a Vitis rebuild or register/address change.
- Start from a clean task id and record the current metadata/DB state. Keep `SRC_REAL_PERF_LOG_ENABLE=1`, `SRC_REAL_PERF_LOG_INTERVAL_SEC=5`, and leave `SRC_REAL_PERF_LOG_FSYNC` unset unless durability timing is specifically under test.
- Default cross-slot policy is ch0/ch1 enabled with `target_qd=8`, `max_active_slots=4`, `cq_batch=32`; ch2 is legacy single-slot with QD 4. Do not set compatibility variables unless the step asks for them.
- Capture serial output, parent stdout, `/tmp/storage_perf.log`, task status, metadata and file hashes. The serial binary ACK protocol is unchanged and is authoritative for command acknowledgement; worker stdout is not.
- A normal terminal record is exactly one `storage_capture_complete`. Per-channel detail is taken from structured FINAL/PERF/DIAG records in the performance log.
- Pending hardware assumptions: a legacy submit that reports `submit_accept_timeout_after_stop` or `submit_accept_timeout` must drain its matching CQ before its CID/DDR slot is reused. The current Host Core integration has no documented queue/controller reset or CQ-clear sequence that proves PRP ownership ended; `nvme_queue_reset_unavailable` is therefore a safe failure. Board validation must confirm reset/disable ownership and CQ FIFO semantics before enabling any recovery that releases unresolved DDR.

## 1. ch2 single-channel baseline

- Environment variables: `SRC_REAL_NVME_CROSS_SLOT_QD_CH2=0`, `SRC_REAL_NVME_CROSS_SLOT_TARGET_QD_CH2=4`, `SRC_REAL_STORAGE_TIMEOUT_US=5000000`.
- Serial expectation: accepted start ACK, then accepted STOP/completion ACK; no protocol framing change.
- Performance log: `storage_perf` for channel 2 shows active QD appropriate to legacy execution, monotonic DMA/NVMe deltas, six slot states summing to capacity, and zero fatal reason.
- Success: one FINAL, parent `storage_capture_complete ... status=success`, `dma_bytes == nvme_bytes == file_bytes`, metadata/DB entry visible only after aggregate commit.
- Failure: FATAL/failed aggregate, byte mismatch, illegal slot sum, or unexpected cross-slot active QD.
- Recovery: stop task, wait for FINAL/worker exit; if absent use parent STOP escalation, collect DIAG, then reset/reinitialize only through the existing operational recovery procedure.

## 2. ch0 single-channel cross-slot baseline

- Environment variables: `SRC_REAL_NVME_CROSS_SLOT_QD_CH0=1`, `SRC_REAL_NVME_CROSS_SLOT_MAX_ACTIVE_CH0=4`, `SRC_REAL_NVME_CROSS_SLOT_TARGET_QD_CH0=8`, `SRC_REAL_NVME_CROSS_SLOT_CQ_BATCH_CH0=32`.
- Serial expectation: normal start/completion ACKs; no worker `storage_result` text is required.
- Performance log: active QD rises toward 8, SQ/CQ/MMIO/completion counters advance, scheduler gap is distinct from queue-empty wait, and integrity fields remain one.
- Success: one FINAL and successful aggregate; no duplicate CID/unknown CID/abort DIAG; resulting file hash matches source.
- Failure: FATAL, abort/reset event, active QD beyond target, nonconserved slot count, or byte/hash mismatch.
- Recovery: issue STOP, preserve `/tmp/storage_perf.log`, verify engine quiesce before reusing DDR, then restart with the same default configuration.

## 3. ch1 single-channel cross-slot baseline

- Environment variables: `SRC_REAL_NVME_CROSS_SLOT_QD_CH1=1`, `SRC_REAL_NVME_CROSS_SLOT_MAX_ACTIVE_CH1=4`, `SRC_REAL_NVME_CROSS_SLOT_TARGET_QD_CH1=8`, `SRC_REAL_NVME_CROSS_SLOT_CQ_BATCH_CH1=32`.
- Serial expectation: the same protocol ACK sequence as ch0; channel identity must be ch1 in structured events.
- Performance log: channel 1 only; observe DMA/NVMe deltas, active QD, SQ-full/CQ-empty counts, no-progress sleeps, and all six slot counters.
- Success: successful aggregate with byte equality and content hash match; no records assigned to ch0/ch2.
- Failure: event-channel mismatch, any terminal reason, persistent CQ stall, or metadata/DB record for a wrong channel.
- Recovery: STOP ch1 through parent control, retain final/DIAG evidence, verify task is failed rather than partially completed, then re-run after cleanup.

## 4. ch0/ch1 parallel capture

- Environment variables: ch0/ch1 values from steps 2/3; `SRC_REAL_CH0_WRITER_RT_PRIO=60`, `SRC_REAL_CH1_WRITER_RT_PRIO=60`, `SRC_REAL_CH0_PRODUCER_RT_PRIO=60`, `SRC_REAL_CH1_PRODUCER_RT_PRIO=60` when permissions permit.
- Serial expectation: both start ACKs occur only after parent sees both RUNNING; one terminal aggregate ACK/report for the task.
- Performance log: interleaved channel 0/1 samples; each has its own active QD and slot counters. Compare `writer_schedule_gap_*` against `queue_empty_wait_us` rather than treating queue idle as a stall.
- Success: ch0/ch1 FINAL byte counts equal each other when split capture requires it, both integrity fields are one, and parent commits both records atomically.
- Failure: one channel fatal, ch0/ch1 byte mismatch, duplicate FINAL, or aggregate before both terminal channels are known.
- Recovery: parent broadcasts STOP to both; verify pending STOP retry/signal fallback reaches both workers; rollback must leave no partial normal record.

## 5. three-drive parallel capture

- Environment variables: retain ch0/ch1 cross-slot defaults and `SRC_REAL_NVME_CROSS_SLOT_QD_CH2=0`; use a common `SRC_REAL_STORAGE_TIMEOUT_US` sized for the run.
- Serial expectation: three accepted starts, no change to binary ACK framing, exactly one final task aggregate.
- Performance log: all three channel streams every five seconds; ch2 remains legacy while ch0/ch1 expose cross-slot active QD and stall counters.
- Success: all FINAL results are successful, aggregate commits three metadata/DB candidates together, byte equality rules hold, and no channel shows unreconciled slots/CIDs.
- Failure: any channel fatal, missing FINAL, unexpected event channel, partial database/metadata visibility, or aggregate duplication.
- Recovery: stop all target channels, await quiesce/drain/reset as needed, inspect ring DIAG after writer join, mark task failed once, and restart as a new task id.

## 6. operator-initiated STOP

- Environment variables: `SRC_REAL_STORAGE_STOP_TIMEOUT_US=5000000`; optionally `SRC_REAL_DUMP_EVENT_RING_ON_STOP=1` for this test only.
- Serial expectation: STOP ACK is prompt and idempotent if sent twice; no requirement for the legacy idle timer.
- Performance log: terminal window shows stop-related DIAG only after FINAL, accepted byte boundary, queue drain to zero, and no post-stop submission growth.
- Success: repeated STOP produces one terminal aggregate, DMA quiesces, completed descriptors drain, writer joins, and final byte/integrity policy is explicit.
- Failure: wait for natural five-second idle, continued DMA ingress after quiesce, duplicated FINAL, or STOP timeout in a healthy path.
- Recovery: use normal parent STOP retry; for permanent control-pipe failure verify SIGTERM fallback, reserve SIGKILL for final timeout recovery, then inspect DIAG.

## 7. STOP under sustained input

- Environment variables: same as step 6 plus a controlled continuous source; set `SRC_REAL_DUMP_EVENT_RING_ON_ERROR=1` to retain failure diagnostics if the tail is incomplete.
- Serial expectation: STOP is acknowledged while input remains active; no upstream-idle prerequisite appears on serial.
- Performance log: quiesce boundary, completed-BD harvest, tail-incomplete state if applicable, no descriptor reuse before writer completion, and final integrity reason.
- Success: bounded STOP latency, no dropped ownership state, six slot counters conserved, and either successful exact boundary or explicit failed tail policy.
- Failure: indefinite harvest, descriptor loss, chunk/LBA discontinuity, or writer accepting data after producer_done.
- Recovery: halt source, use parent escalation only after the monotonic deadline, preserve DDR/DIAG evidence, then reinitialize DMA/NVMe through existing recovery paths.

## 8. injected NVMe failure

- Environment variables: enable the existing board-supported NVMe fault injection only; keep `SRC_REAL_DUMP_EVENT_RING_ON_ERROR=1` and do not invent register writes.
- Serial expectation: normal command ACK followed by failed aggregate notification; no false successful terminal ACK.
- Performance log: `storage_fatal` before `storage_final`, then DIAG; first fatal reason remains stable, inflight CIDs drain or a confirmed reset is recorded, and no new submit follows abort.
- Success: failed slot has no success callback/requeue, other inflight commands are drained/reset safely, task is `TASK_FAILED`, and no partial metadata/DB record is committed.
- Failure: engine destruction with inflight commands, callback after failure, duplicate reason overwrite, or TASK_COMPLETED.
- Recovery: wait for drain/reset confirmation; if reset fails, isolate the channel using the platform procedure and do not reuse its DDR region until hardware recovery is confirmed.

## 9. long-duration capture

- Environment variables: production defaults; `SRC_REAL_PERF_LOG_ENABLE=1`, `SRC_REAL_PERF_LOG_INTERVAL_SEC=5`, `SRC_REAL_DUMP_EVENT_RING_ON_STOP=0`, and optional `SRC_REAL_PERF_LOG_FSYNC=1` only for a separate durability run.
- Serial expectation: only start/critical/aggregate output, without periodic legacy `storage_stats`/per-command text unless an explicit debug/legacy switch is set.
- Performance log: five-second samples retain DMA/NVMe deltas, six-state counts, QD, SQ/CQ/MMIO/completion, queue-empty wait, scheduler gap, no-progress sleep, dropped PERF/DIAG, and integrity fields.
- Success: stable throughput, no growing slot leak, no unexpected dropped terminal event, bounded DIAG drops under pressure, and clean STOP/final aggregate at the end.
- Failure: monotonically growing busy/inflight state, recurring no-progress timeout, missing samples, log-open failure spam, or degraded integrity.
- Recovery: stop cleanly, archive log and task state, compare before/after metadata, then restart only after engine/runtime quiesce is confirmed.

## 10. byte and file-content verification

- Environment variables: use the production capture settings; set `SRC_REAL_EXPECTED_BYTES_CHn` only when the source byte count is known and strict validation is intended; set `SRC_REAL_STRICT_END_TO_END=1` for that controlled run.
- Serial expectation: command ACKs only; final success is the parent aggregate, not worker text.
- Performance log: FINAL records report equal DMA/NVMe/file bytes and integrity fields; PERF deltas sum to final totals within the accepted capture boundary.
- Success: `dma_received_bytes == nvme_completed_bytes == file_bytes` per channel, required ch0/ch1 equality holds, metadata LBA/sector range is valid, and independent readback hash/checksum equals the captured source.
- Failure: any byte mismatch, unexpected sector count/LBA overlap, content hash mismatch, or metadata/DB visibility after a failed aggregate.
- Recovery: mark task failed through parent aggregation, do not publish partial candidates, retain source/hash/log evidence, and use a new metadata slot/task id for the retry after root-cause analysis.
