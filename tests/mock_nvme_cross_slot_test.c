#include <assert.h>
#include <stdio.h>
#include <string.h>
#include "ccb_hw.h"
typedef struct { uint16_t cid[8]; unsigned n, p; uint64_t now; int unknown; } Mock;
static int submit(void *o,uint16_t c,uint64_t l,uint32_t s,uint64_t a){ Mock*m=o;(void)l;(void)s;(void)a;m->cid[m->n++]=c;return 0; }
static int poll(void *o,NvmeCompletion *c){ Mock*m=o; if(m->p>=m->n)return 0; memset(c,0,sizeof(*c)); c->cid=m->unknown ? 0xffffu : m->cid[m->n-1u-m->p++]; return 1; }
static uint64_t now(void *o){ return ((Mock*)o)->now++; }
static void slp(void*o,uint32_t u){(void)o;(void)u;}
static int done(void*o,const NvmeWriteSlotReq*r){ unsigned *n=o; (void)r; ++*n; return 0; }
int main(void){ ChannelRuntime rt; Mock m; NvmeCrossSlotOps ops={submit,poll,now,slp}; NvmeWriteSlotReq a={.slot=1,.start_lba=1,.sectors=1,.hw_addr=1,.bytes=512},b={.slot=2,.start_lba=2,.sectors=1,.hw_addr=2,.bytes=512}; NvmeCrossSlotEngine*e; unsigned complete=0; memset(&rt,0,sizeof(rt));memset(&m,0,sizeof(m));rt.nvme_qd_effective=2;rt.nvme_cmd_sectors=1;e=nvme_cross_slot_engine_create_with_ops(&rt,&ops,&m);assert(e);assert(nvme_cross_slot_engine_can_accept(e));assert(!nvme_cross_slot_engine_add(e,&a));assert(!nvme_cross_slot_engine_add(e,&b));assert(nvme_cross_slot_engine_active(e)==2);assert(!nvme_cross_slot_engine_step(e,100,done,&complete));assert(m.n==2);assert(nvme_cross_slot_engine_active(e)<=2);nvme_cross_slot_engine_destroy(e);puts("mock_nvme_cross_slot_test: ok");}
