#include "ccb_storage_supervisor.h"
#include <string.h>
#include <stdio.h>

static uint32_t bit(uint32_t ch) { return ch < NUM_CHANNELS ? 1u << ch : 0u; }
static void fail(StorageTaskSupervisor *s, uint32_t ch, const char *reason)
{ if (!s->first_fatal) { s->first_fatal = true; s->fatal_channel = ch; snprintf(s->fatal_reason, sizeof(s->fatal_reason), "%s", reason); } s->result_known_failed = true; s->stop_requested_mask |= s->target_channel_mask & ~s->final_seen_mask; }
void storage_supervisor_init(StorageTaskSupervisor *s, uint32_t target_mask)
{ memset(s, 0, sizeof(*s)); s->target_channel_mask = target_mask; s->terminal = STORAGE_TASK_ACTIVE; }
int storage_supervisor_handle_event(StorageTaskSupervisor *s, const StorageWorkerEvent *e)
{
    uint32_t b;
    if (!s || !e || !(b = bit(e->channel)) || !(s->target_channel_mask & b)) return -1;
    switch (e->type) {
    case STORAGE_WORKER_READY: if (s->ready_mask & b) { fail(s,e->channel,"duplicate_ready"); return -1; } s->ready_mask |= b; break;
    case STORAGE_WORKER_ARMED: if (!(s->ready_mask & b) || (s->armed_mask & b)) { fail(s,e->channel,"invalid_armed"); return -1; } s->armed_mask |= b; break;
    case STORAGE_WORKER_RUNNING: if (!(s->armed_mask & b) || (s->running_mask & b)) { fail(s,e->channel,"invalid_running"); return -1; } s->running_mask |= b; break;
    case STORAGE_WORKER_DRAINED: s->drained_mask |= b; break;
    case STORAGE_WORKER_FATAL: fail(s,e->channel,e->reason); break;
    case STORAGE_WORKER_FINAL_RESULT:
        if (s->final_seen_mask & b) { fail(s,e->channel,"duplicate_final"); return -1; }
        s->final_seen_mask |= b; s->final_result[e->channel] = e->result; break;
    default: break;
    }
    (void)storage_supervisor_result_status(s); return 0;
}
int storage_supervisor_handle_worker_eof(StorageTaskSupervisor *s, uint32_t ch)
{ if (!s || !(s->final_seen_mask & bit(ch))) { if (s) { fail(s,ch,"event_eof_without_final"); s->unavailable_mask |= bit(ch); (void)storage_supervisor_result_status(s); } return -1; } return 0; }
int storage_supervisor_handle_worker_exit(StorageTaskSupervisor *s, uint32_t ch, int code)
{ if (code != 0) { fail(s,ch,"worker_exit"); return -1; } return storage_supervisor_handle_worker_eof(s,ch); }
uint32_t storage_supervisor_stop_mask(const StorageTaskSupervisor *s) { return s ? s->stop_requested_mask : 0u; }
uint32_t storage_supervisor_take_stop_mask(StorageTaskSupervisor *s)
{ uint32_t m = s ? s->stop_requested_mask & ~s->stop_sent_mask : 0u; if (s) s->stop_sent_mask |= m; return m; }
bool storage_supervisor_is_terminal(const StorageTaskSupervisor *s) { return s && s->terminal != STORAGE_TASK_ACTIVE; }
StorageTaskTerminal storage_supervisor_result_status(StorageTaskSupervisor *s)
{
    uint32_t i; if (!s || s->terminal != STORAGE_TASK_ACTIVE) return s ? s->terminal : STORAGE_TASK_FAILED;
    if ((s->final_seen_mask | s->unavailable_mask) != s->target_channel_mask) return STORAGE_TASK_ACTIVE;
    s->aggregate_ready = true;
    if (s->result_known_failed) return s->terminal = STORAGE_TASK_FAILED;
    for (i=0;i<NUM_CHANNELS;i++) if (s->target_channel_mask & bit(i)) { WriteResult *r=&s->final_result[i]; if (!r->data_persisted || !r->receive_integrity_ok || !r->storage_integrity_ok || !r->integrity_ok || r->dma_received_bytes != r->nvme_completed_bytes || r->nvme_completed_bytes != r->file_bytes) return s->terminal=STORAGE_TASK_FAILED; }
    if ((s->target_channel_mask & 3u) == 3u && s->final_result[0].dma_received_bytes != s->final_result[1].dma_received_bytes) return s->terminal=STORAGE_TASK_FAILED;
    return s->terminal=STORAGE_TASK_SUCCESS;
}
