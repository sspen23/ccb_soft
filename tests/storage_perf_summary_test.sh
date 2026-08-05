#!/bin/sh
set -eu

output=$(awk -v wanted_task=t1 -f tools/storage_perf_summary.awk <<'EOF'
storage_perf task=t1 channel=0 ts_us=1 window_start_us=0 window_end_us=1000000 dma_bytes_delta=1048576 nvme_bytes_delta=1048576 dma_writable=7 completed_unharvested=1 ready_slots=2 nvme_busy_slots=1 requeue_pending=0 free_slots=0 active_qd=7 active_qd_max=8 submit_stall_count=1 submit_stall_max_us=11 cq_empty_wait_count=2 cq_empty_wait_max_us=12 submit_mmio_count=4 submit_mmio_max_us=13 completion_process_count=3 completion_process_max_us=14 writer_schedule_gap_count=1 writer_schedule_gap_max_us=15 no_progress_sleep_count=2
storage_perf task=t1 channel=0 ts_us=2 window_start_us=1000000 window_end_us=2000000 dma_bytes_delta=1048576 nvme_bytes_delta=1048576 dma_writable=6 completed_unharvested=3 ready_slots=4 nvme_busy_slots=1 requeue_pending=0 free_slots=0 active_qd=8 active_qd_max=8 submit_stall_count=3 submit_stall_max_us=21 cq_empty_wait_count=5 cq_empty_wait_max_us=22 submit_mmio_count=8 submit_mmio_max_us=23 completion_process_count=7 completion_process_max_us=24 writer_schedule_gap_count=2 writer_schedule_gap_max_us=25 no_progress_sleep_count=4
storage_final task=t1 channel=0 ts_us=3 error=0 dma_bytes=2097152 nvme_bytes=2097152 tail_unqueued_bytes=0 file_bytes=2097152 stop_request_us=2000000 final_us=2250000 ready_count=0 active_count=0 global_inflight=0 submit_count=4 completion_count=4 completed_unharvested=0 persisted=1 integrity_ok=1
EOF
)

case "$output" in
    *"0,2,1.000,1.000,7.500,8,4,3,6,3,21,5,22,8,23,7,24,2,25,4,4,4,0,2097152,250.000,1,0"*"aggregate,1,1.000,1.000"*) ;;
    *)
        echo "$output" >&2
        exit 1
        ;;
esac
echo "storage_perf_summary_test: ok"
