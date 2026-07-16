# Storage performance A/B procedure

Use the same bitstream, SSDs, source, capture duration, CPU frequency, log
level and UART command order for every run.  Each case must be repeated at
least five times.  The script prints the profile environment and the commit
that defines the code stage; it never checks out a revision or controls the
board.

```sh
eval "$(sh tools/storage_ab_env.sh C)"
/etc/init.d/storage restart
# Send the unchanged UART start/data/STOP sequence from the host controller.
cp /tmp/storage_perf.log storage-perf-C-run1.log
awk -v wanted_task=TASK_ID -f tools/storage_perf_summary.awk \
    storage-perf-C-run1.log
```

Cases A through G correspond to current cross-slot 256 KiB, legacy 256 KiB,
legacy 512 KiB, queue-lock split, producer backlog policy, periodic metrics,
and optimized cross-slot 512 KiB.  A result is invalid unless every channel
has exactly one FINAL with `ready_count=0`, `active_count=0`,
`global_inflight=0`, `completed_unharvested=0`, and equal submit/completion
counts.  Parser exit status 2 means the run must not enter the performance
average.

If a log contains `nvme_queue_reset_unavailable`, `ownership unresolved`,
`submit_accept_timeout`, or `writer_abort_timeout`, restart or power-cycle the
affected board/SSD according to the platform procedure before continuing.
Do not count an immediate rerun as valid recovery evidence.
