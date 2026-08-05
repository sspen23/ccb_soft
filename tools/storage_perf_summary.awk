# Usage: awk -v wanted_task=TASK -f tools/storage_perf_summary.awk storage_perf.log

function parse_fields(    i, pair, count) {
    delete value
    for (i = 1; i <= NF; ++i) {
        count = split($i, pair, "=")
        if (count >= 2) value[pair[1]] = substr($i, length(pair[1]) + 2)
    }
}

function selected() {
    return wanted_task == "" || value["task"] == wanted_task
}

$1 == "storage_perf" {
    parse_fields()
    if (!selected()) next
    ch = value["channel"] + 0
    channels[ch] = 1
    samples[ch]++
    window_us[ch] += value["window_end_us"] - value["window_start_us"]
    dma_bytes[ch] += value["dma_bytes_delta"]
    nvme_bytes[ch] += value["nvme_bytes_delta"]
    active_qd_sum[ch] += value["active_qd"]
    if (value["active_qd_max"] > active_qd_max[ch])
        active_qd_max[ch] = value["active_qd_max"]
    if (value["ready_slots"] > ready_max[ch])
        ready_max[ch] = value["ready_slots"]
    if (value["completed_unharvested"] > completed_max[ch])
        completed_max[ch] = value["completed_unharvested"]
    if (!(ch in writable_min) || value["dma_writable"] < writable_min[ch])
        writable_min[ch] = value["dma_writable"]
    if (value["submit_stall_count"] > submit_stall_count[ch])
        submit_stall_count[ch] = value["submit_stall_count"]
    if (value["submit_stall_max_us"] > submit_stall_max_us[ch])
        submit_stall_max_us[ch] = value["submit_stall_max_us"]
    if (value["cq_empty_wait_count"] > cq_empty_wait_count[ch])
        cq_empty_wait_count[ch] = value["cq_empty_wait_count"]
    if (value["cq_empty_wait_max_us"] > cq_empty_wait_max_us[ch])
        cq_empty_wait_max_us[ch] = value["cq_empty_wait_max_us"]
    if (value["submit_mmio_count"] > submit_mmio_count[ch])
        submit_mmio_count[ch] = value["submit_mmio_count"]
    if (value["submit_mmio_max_us"] > submit_mmio_max_us[ch])
        submit_mmio_max_us[ch] = value["submit_mmio_max_us"]
    if (value["completion_process_count"] > completion_process_count[ch])
        completion_process_count[ch] = value["completion_process_count"]
    if (value["completion_process_max_us"] > completion_process_max_us[ch])
        completion_process_max_us[ch] = value["completion_process_max_us"]
    if (value["writer_schedule_gap_count"] > writer_gap_count[ch])
        writer_gap_count[ch] = value["writer_schedule_gap_count"]
    if (value["writer_schedule_gap_max_us"] > writer_gap_max_us[ch])
        writer_gap_max_us[ch] = value["writer_schedule_gap_max_us"]
    if (value["no_progress_sleep_count"] > no_progress_sleep_count[ch])
        no_progress_sleep_count[ch] = value["no_progress_sleep_count"]
    next
}

$1 == "storage_final" {
    parse_fields()
    if (!selected()) next
    ch = value["channel"] + 0
    channels[ch] = 1
    finals[ch]++
    persisted[ch] = value["nvme_bytes"] + 0
    submit_count[ch] = value["submit_count"] + 0
    completion_count[ch] = value["completion_count"] + 0
    tail_unqueued_bytes[ch] = value["tail_unqueued_bytes"] + 0
    stop_ms[ch] = value["final_us"] >= value["stop_request_us"] ?
                  (value["final_us"] - value["stop_request_us"]) / 1000.0 : 0
    final_ok[ch] = value["error"] == 0 && value["persisted"] == 1 &&
                   value["integrity_ok"] == 1 && value["ready_count"] == 0 &&
                   value["active_count"] == 0 && value["global_inflight"] == 0 &&
                   value["completed_unharvested"] == 0 &&
                   value["submit_count"] == value["completion_count"]
    next
}

/unknown.*CID|unknown_completion_cid|ownership unresolved|submit_accept_timeout|writer_abort_timeout|nvme_queue_reset_unavailable/ {
    parse_fields()
    if (selected()) reset_required = 1
}

END {
    print "channel,samples,capture_window_mib_s,nvme_wall_mib_s,active_qd_avg,active_qd_max,ready_q_max,completed_unharvested_max,dma_writable_min,submit_stall_count,submit_stall_max_us,cq_empty_wait_count,cq_empty_wait_max_us,submit_mmio_count,submit_mmio_max_us,completion_process_count,completion_process_max_us,writer_schedule_gap_count,writer_schedule_gap_max_us,no_progress_sleep_count,submit_count,completion_count,tail_unqueued_bytes,persisted_bytes,stop_ms,final_ok,reset_required"
    aggregate_capture = 0
    aggregate_nvme = 0
    aggregate_persisted = 0
    channel_count = 0
    bad = 0
    for (ch in channels) {
        capture_rate = window_us[ch] > 0 ? dma_bytes[ch] * 1000000.0 / window_us[ch] / 1048576.0 : 0
        nvme_rate = window_us[ch] > 0 ? nvme_bytes[ch] * 1000000.0 / window_us[ch] / 1048576.0 : 0
        qd_avg = samples[ch] > 0 ? active_qd_sum[ch] / samples[ch] : 0
        ok = finals[ch] == 1 && final_ok[ch]
        printf "%d,%d,%.3f,%.3f,%.3f,%d,%d,%d,%d,%.0f,%.0f,%.0f,%.0f,%.0f,%.0f,%.0f,%.0f,%.0f,%.0f,%.0f,%.0f,%.0f,%.0f,%.0f,%.3f,%d,%d\n", ch,
               samples[ch], capture_rate, nvme_rate, qd_avg, active_qd_max[ch],
               ready_max[ch], completed_max[ch], writable_min[ch],
               submit_stall_count[ch], submit_stall_max_us[ch],
               cq_empty_wait_count[ch], cq_empty_wait_max_us[ch],
               submit_mmio_count[ch], submit_mmio_max_us[ch],
               completion_process_count[ch], completion_process_max_us[ch],
               writer_gap_count[ch], writer_gap_max_us[ch],
               no_progress_sleep_count[ch], submit_count[ch],
               completion_count[ch], tail_unqueued_bytes[ch], persisted[ch],
               stop_ms[ch], ok ? 1 : 0, reset_required ? 1 : 0
        aggregate_capture += capture_rate
        aggregate_nvme += nvme_rate
        aggregate_persisted += persisted[ch]
        ++channel_count
        if (!ok) bad = 1
    }
    printf "aggregate,%d,%.3f,%.3f,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,%.0f,0,%d,%d\n",
           channel_count, aggregate_capture, aggregate_nvme,
           aggregate_persisted, bad ? 0 : 1, reset_required ? 1 : 0
    if (channel_count == 0 || bad || reset_required) exit 2
}
