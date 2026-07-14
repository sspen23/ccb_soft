#include "ccb_tcp_transfer.h"

#include "ccb_config.h"
#include "debug_uart.h"
#include "storage_config.h"

#include <errno.h>
#include <inttypes.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>

#ifdef __linux__
#include <sys/mman.h>
#define TCP_HAVE_MMAP 1
#else
#define TCP_HAVE_MMAP 0
#ifndef MAP_FAILED
#define MAP_FAILED ((void *)-1)
#endif
#endif

#define TCP_MM2S_DMACR               0x00u
#define TCP_MM2S_DMASR               0x04u
#define TCP_MM2S_CURDESC             0x08u
#define TCP_MM2S_CURDESC_MSB         0x0Cu
#define TCP_MM2S_TAILDESC            0x10u
#define TCP_MM2S_TAILDESC_MSB        0x14u
#define TCP_S2MM_DMACR               0x30u
#define TCP_S2MM_DMASR               0x34u
#define TCP_DMA_CR_RS_BIT            (1u << 0)
#define TCP_DMA_CR_RESET_BIT         (1u << 2)
#define TCP_DMA_CR_IOC_IRQ_EN        (1u << 12)

#define TCP_DMA_SR_HALTED            (1u << 0)
#define TCP_DMA_SR_IDLE              (1u << 1)
#define TCP_DMA_SR_DMA_INT_ERR       (1u << 4)
#define TCP_DMA_SR_DMA_SLV_ERR       (1u << 5)
#define TCP_DMA_SR_DMA_DEC_ERR       (1u << 6)
#define TCP_DMA_SR_SG_INT_ERR        (1u << 8)
#define TCP_DMA_SR_SG_SLV_ERR        (1u << 9)
#define TCP_DMA_SR_SG_DEC_ERR        (1u << 10)
#define TCP_DMA_SR_IRQ_STATUS_MASK   0x00007000u
#define TCP_DMA_SR_ERR_IRQ           (1u << 14)

#define TCP_DMA_SR_ERROR_MASK        (TCP_DMA_SR_DMA_INT_ERR | \
                                      TCP_DMA_SR_DMA_SLV_ERR | \
                                      TCP_DMA_SR_DMA_DEC_ERR | \
                                      TCP_DMA_SR_SG_INT_ERR | \
                                      TCP_DMA_SR_SG_SLV_ERR | \
                                      TCP_DMA_SR_SG_DEC_ERR | \
                                      TCP_DMA_SR_ERR_IRQ)

#define TCP_DESC_CTRL_SOF            (1u << 27)
#define TCP_DESC_CTRL_EOF            (1u << 26)
#define TCP_DESC_STS_CMPLT           (1u << 31)

#define TCP_AXIS_SWITCH_CTRL         0x0000u
#define TCP_AXIS_SWITCH_MI0_MUX      0x0040u
#define TCP_AXIS_SWITCH_UPDATE       (1u << 1)
#define TCP_POLL_SLEEP_US            10u
#define TCP_WAIT_LOG_US              5000000u

#define PAGE_ALIGN_DOWN(v, p) ((v) & ~((uint64_t)((p) - 1u)))
#define PAGE_ALIGN_UP(v, p) (((v) + ((p) - 1u)) & ~((uint64_t)((p) - 1u)))

static volatile sig_atomic_t g_tcp_stop_requested = 0;

static int tcp_env_flag_enabled(const char *name)
{
    const char *value = storage_config_compat_getenv(name);

    if (!value || value[0] == '\0') {
        return 0;
    }
    if (strcmp(value, "0") == 0 ||
        strcmp(value, "false") == 0 ||
        strcmp(value, "FALSE") == 0 ||
        strcmp(value, "no") == 0 ||
        strcmp(value, "NO") == 0) {
        return 0;
    }
    return 1;
}

static inline uint32_t mmio_read32(const MappedRegion *r, uint32_t off)
{
    return *(volatile uint32_t *)(r->virt + off);
}

static inline void mmio_write32(const MappedRegion *r, uint32_t off, uint32_t value)
{
    *(volatile uint32_t *)(r->virt + off) = value;
}

static int map_region(int fd, uint64_t phys, size_t size, MappedRegion *out)
{
#if TCP_HAVE_MMAP
    long page = sysconf(_SC_PAGE_SIZE);
    uint64_t aligned;
    size_t off;
    size_t map_len;
    void *base;

    memset(out, 0, sizeof(*out));
    if (page <= 0) {
        page = 4096;
    }
    aligned = PAGE_ALIGN_DOWN(phys, (uint64_t)page);
    off = (size_t)(phys - aligned);
    map_len = (size_t)PAGE_ALIGN_UP(size + off, (uint64_t)page);
    base = mmap(0, map_len, PROT_READ | PROT_WRITE, MAP_SHARED, fd, (off_t)aligned);
    if (base == MAP_FAILED) {
        fprintf(stderr, "tcp mmap failed: phys=0x%016" PRIx64 " size=0x%zx errno=%d (%s)\n",
                phys, size, errno, strerror(errno));
        return -1;
    }
    out->fd = fd;
    out->map_base = base;
    out->map_len = map_len;
    out->map_off = off;
    out->virt = (volatile uint8_t *)base + off;
    out->phys = phys;
    out->size = size;
    out->valid = true;
    return 0;
#else
    (void)fd;
    (void)phys;
    (void)size;
    (void)out;
    fprintf(stderr, "tcp transfer requires Linux mmap support\n");
    return -1;
#endif
}

static void unmap_region(MappedRegion *r)
{
#if TCP_HAVE_MMAP
    if (r->valid && r->map_base && r->map_len > 0u) {
        munmap(r->map_base, r->map_len);
    }
#endif
    memset(r, 0, sizeof(*r));
}

static int wait_bits_common(const MappedRegion *r,
                            uint32_t off,
                            uint32_t mask,
                            uint32_t expected,
                            uint32_t timeout_us,
                            int abort_on_stop)
{
    uint32_t waited = 0u;
    while (waited < timeout_us) {
        uint32_t value = mmio_read32(r, off);
        if ((value & mask) == expected) {
            return 0;
        }
        if (abort_on_stop && g_tcp_stop_requested) {
            return -2;
        }
        usleep(TCP_POLL_SLEEP_US);
        waited += TCP_POLL_SLEEP_US;
    }
    return -1;
}

static int wait_bits(const MappedRegion *r, uint32_t off, uint32_t mask, uint32_t expected, uint32_t timeout_us)
{
    return wait_bits_common(r, off, mask, expected, timeout_us, 1);
}

static int wait_bits_no_stop(const MappedRegion *r, uint32_t off, uint32_t mask, uint32_t expected, uint32_t timeout_us)
{
    return wait_bits_common(r, off, mask, expected, timeout_us, 0);
}

static void tcp_log_mm2s_state(const char *prefix,
                               const TcpTransferConfig *cfg,
                               const MappedRegion *dma,
                               const DmaSgDesc *desc,
                               uint32_t desc_count,
                               uint32_t next_desc,
                               uint32_t completed,
                               uint32_t waited_us,
                               int poll_dmasr,
                               uint32_t last_dmasr)
{
    uint32_t dmasr = dma && dma->valid ? mmio_read32(dma, TCP_MM2S_DMASR) : 0u;
    uint32_t curdesc = dma && dma->valid ? mmio_read32(dma, TCP_MM2S_CURDESC) : 0u;
    uint32_t taildesc = dma && dma->valid ? mmio_read32(dma, TCP_MM2S_TAILDESC) : 0u;
    uint32_t desc_control = 0u;
    uint32_t desc_status = 0u;
    uint32_t desc_buf = 0u;

    if (desc && desc_count > 0u && next_desc < desc_count) {
        desc_control = desc[next_desc].control;
        desc_status = desc[next_desc].status;
        desc_buf = desc[next_desc].buffer_addr;
    }

    fprintf(stderr,
            "%s bytes=%" PRIu64 " dma=0x%08" PRIx64 " ddr_dma=0x%08" PRIx64
            " desc=%u completed=%u/%u desc_control=0x%08x desc_status=0x%08x"
            " desc_buf=0x%08x dmasr=0x%08x curdesc=0x%08x taildesc=0x%08x"
            " waited_us=%u poll_dmasr=%d last_dmasr=0x%08x\n",
            prefix ? prefix : "tcp MM2S state",
            cfg ? cfg->transfer_bytes : 0u,
            cfg ? cfg->dma_base : 0u,
            cfg ? cfg->ddr_dma_base : 0u,
            (unsigned)next_desc,
            (unsigned)completed,
            (unsigned)desc_count,
            desc_control,
            desc_status,
            desc_buf,
            dmasr,
            curdesc,
            taildesc,
            (unsigned)waited_us,
            poll_dmasr,
            last_dmasr);
    dbg_printf("[DBG][TCP] %s bytes=%" PRIu64 " dma=0x%08" PRIx64
               " ddr_dma=0x%08" PRIx64 " desc=%u completed=%u/%u"
               " desc_control=0x%08x desc_status=0x%08x desc_buf=0x%08x"
               " dmasr=0x%08x curdesc=0x%08x taildesc=0x%08x"
               " waited_us=%u poll_dmasr=%d last_dmasr=0x%08x\n",
               prefix ? prefix : "tcp MM2S state",
               cfg ? cfg->transfer_bytes : 0u,
               cfg ? cfg->dma_base : 0u,
               cfg ? cfg->ddr_dma_base : 0u,
               (unsigned)next_desc,
               (unsigned)completed,
               (unsigned)desc_count,
               desc_control,
               desc_status,
               desc_buf,
               dmasr,
               curdesc,
               taildesc,
               (unsigned)waited_us,
               poll_dmasr,
               last_dmasr);
}

static int tcp_open_regions(const TcpTransferConfig *cfg, int *fd_out, MappedRegion *dma, MappedRegion *sw, MappedRegion *desc)
{
    int fd;
    uint32_t desc_count = (uint32_t)((cfg->transfer_bytes + TCP_MAX_BYTES_PER_DESC - 1u) / TCP_MAX_BYTES_PER_DESC);
    size_t desc_bytes = (size_t)desc_count * sizeof(DmaSgDesc);

    *fd_out = -1;
    if (desc_count == 0u) {
        return -1;
    }
    fd = open("/dev/mem", O_RDWR | O_SYNC);
    if (fd < 0) {
        fprintf(stderr, "tcp open /dev/mem failed errno=%d (%s)\n", errno, strerror(errno));
        return -1;
    }
    if (map_region(fd, cfg->dma_base, 0x10000u, dma) != 0 ||
        map_region(fd, cfg->switch_base, 0x10000u, sw) != 0 ||
        map_region(fd, cfg->desc_cpu_base, desc_bytes, desc) != 0) {
        unmap_region(desc);
        unmap_region(sw);
        unmap_region(dma);
        close(fd);
        return -1;
    }
    *fd_out = fd;
    return 0;
}

static void tcp_close_regions(int fd, MappedRegion *dma, MappedRegion *sw, MappedRegion *desc)
{
    unmap_region(desc);
    unmap_region(sw);
    unmap_region(dma);
    if (fd >= 0) {
        close(fd);
    }
}

static int tcp_route_switch(const TcpTransferConfig *cfg, const MappedRegion *sw)
{
    int rc;

    dbg_verbose_printf("[DBG][TCP] route switch begin switch=0x%08" PRIx64 " input=%u\n",
                       cfg->switch_base,
                       (unsigned)cfg->switch_input_select);
    mmio_write32(sw, TCP_AXIS_SWITCH_MI0_MUX, cfg->switch_input_select & 0x0Fu);
    mmio_write32(sw, TCP_AXIS_SWITCH_CTRL, TCP_AXIS_SWITCH_UPDATE);
    rc = wait_bits(sw, TCP_AXIS_SWITCH_CTRL, TCP_AXIS_SWITCH_UPDATE, 0u, cfg->timeout_us);
    if (rc != 0) {
        fprintf(stderr, "tcp switch route timeout: switch=0x%08" PRIx64 " input=%u rc=%d\n",
                cfg->switch_base,
                (unsigned)cfg->switch_input_select,
                rc);
        dbg_printf("[DBG][TCP] route switch failed switch=0x%08" PRIx64
                   " input=%u rc=%d ctrl=0x%08x\n",
                   cfg->switch_base,
                   (unsigned)cfg->switch_input_select,
                   rc,
                   mmio_read32(sw, TCP_AXIS_SWITCH_CTRL));
        return rc;
    }
    dbg_verbose_printf("[DBG][TCP] route switch done switch=0x%08" PRIx64 " input=%u ctrl=0x%08x\n",
                       cfg->switch_base,
                       (unsigned)cfg->switch_input_select,
                       mmio_read32(sw, TCP_AXIS_SWITCH_CTRL));
    return 0;
}

static int tcp_reset_dma_mapped(const TcpTransferConfig *cfg, const MappedRegion *dma)
{
    int mm2s_ok;
    int s2mm_ok;
    uint32_t mm2s_cr_before;
    uint32_t s2mm_cr_before;

    dbg_verbose_printf("[DBG][TCP] dma reset begin dma=0x%08" PRIx64 "\n", cfg->dma_base);
    mm2s_cr_before = mmio_read32(dma, TCP_MM2S_DMACR);
    s2mm_cr_before = mmio_read32(dma, TCP_S2MM_DMACR);
    dbg_verbose_printf("[DBG][TCP] dma reset state dma=0x%08" PRIx64
                       " mm2s_cr=0x%08x mm2s_sr=0x%08x"
                       " s2mm_cr=0x%08x s2mm_sr=0x%08x\n",
                       cfg->dma_base,
                       mm2s_cr_before,
                       mmio_read32(dma, TCP_MM2S_DMASR),
                       s2mm_cr_before,
                       mmio_read32(dma, TCP_S2MM_DMASR));

    if (((mm2s_cr_before | s2mm_cr_before) & TCP_DMA_CR_RESET_BIT) != 0u) {
        fprintf(stderr,
                "tcp DMA reset rejected: reset already in progress"
                " mm2s_cr=0x%08x s2mm_cr=0x%08x\n",
                mm2s_cr_before,
                s2mm_cr_before);
        return -1;
    }

    /* PG021: either channel Reset bit resets the entire AXI DMA core. */
    mmio_write32(dma, TCP_MM2S_DMACR, TCP_DMA_CR_RESET_BIT);

    mm2s_ok = (wait_bits_no_stop(dma, TCP_MM2S_DMACR, TCP_DMA_CR_RESET_BIT, 0u, cfg->timeout_us) == 0);
    s2mm_ok = (wait_bits_no_stop(dma, TCP_S2MM_DMACR, TCP_DMA_CR_RESET_BIT, 0u, cfg->timeout_us) == 0);

    dbg_verbose_printf("[DBG][TCP] dma reset done dma=0x%08" PRIx64
                       " mm2s_ok=%d s2mm_ok=%d mm2s_cr=0x%08x mm2s_sr=0x%08x"
                       " s2mm_cr=0x%08x s2mm_sr=0x%08x\n",
                       cfg->dma_base,
                       mm2s_ok,
                       s2mm_ok,
                       mmio_read32(dma, TCP_MM2S_DMACR),
                       mmio_read32(dma, TCP_MM2S_DMASR),
                       mmio_read32(dma, TCP_S2MM_DMACR),
                       mmio_read32(dma, TCP_S2MM_DMASR));

    if (!mm2s_ok || !s2mm_ok) {
        fprintf(stderr,
                "tcp DMA reset timeout: mm2s_ok=%d s2mm_ok=%d"
                " mm2s_cr=0x%08x s2mm_cr=0x%08x mm2s_sr=0x%08x s2mm_sr=0x%08x\n",
                mm2s_ok,
                s2mm_ok,
                mmio_read32(dma, TCP_MM2S_DMACR),
                mmio_read32(dma, TCP_S2MM_DMACR),
                mmio_read32(dma, TCP_MM2S_DMASR),
                mmio_read32(dma, TCP_S2MM_DMASR));
        return -1;
    }
    return 0;
}

static int tcp_halt_mm2s_mapped(const TcpTransferConfig *cfg,
                                const MappedRegion *dma,
                                const char *reason)
{
    uint32_t control = mmio_read32(dma, TCP_MM2S_DMACR);
    uint32_t status = mmio_read32(dma, TCP_MM2S_DMASR);

    if ((control & TCP_DMA_CR_RESET_BIT) != 0u) {
        fprintf(stderr,
                "tcp MM2S halt rejected: reset in progress reason=%s"
                " dmacr=0x%08x dmasr=0x%08x\n",
                reason ? reason : "none",
                control,
                status);
        return -1;
    }
    if ((status & TCP_DMA_SR_HALTED) != 0u) {
        return 0;
    }

    mmio_write32(dma, TCP_MM2S_DMACR, control & ~TCP_DMA_CR_RS_BIT);
    if (wait_bits_no_stop(dma,
                          TCP_MM2S_DMASR,
                          TCP_DMA_SR_HALTED,
                          TCP_DMA_SR_HALTED,
                          cfg->timeout_us) != 0) {
        fprintf(stderr,
                "tcp MM2S halt timeout reason=%s dmacr=0x%08x dmasr=0x%08x\n",
                reason ? reason : "none",
                mmio_read32(dma, TCP_MM2S_DMACR),
                mmio_read32(dma, TCP_MM2S_DMASR));
        return -1;
    }
    dbg_verbose_printf("[DBG][TCP] MM2S halted reason=%s dma=0x%08" PRIx64
                       " dmacr=0x%08x dmasr=0x%08x\n",
                       reason ? reason : "none",
                       cfg->dma_base,
                       mmio_read32(dma, TCP_MM2S_DMACR),
                       mmio_read32(dma, TCP_MM2S_DMASR));
    return 0;
}

static int tcp_prepare_mm2s_mapped(const TcpTransferConfig *cfg, const MappedRegion *dma)
{
    uint32_t mm2s_control = mmio_read32(dma, TCP_MM2S_DMACR);
    uint32_t s2mm_control = mmio_read32(dma, TCP_S2MM_DMACR);
    uint32_t mm2s_status = mmio_read32(dma, TCP_MM2S_DMASR);

    if (((mm2s_control | s2mm_control) & TCP_DMA_CR_RESET_BIT) != 0u) {
        fprintf(stderr,
                "tcp MM2S prepare rejected: DMA reset stuck"
                " mm2s_cr=0x%08x s2mm_cr=0x%08x mm2s_sr=0x%08x\n",
                mm2s_control,
                s2mm_control,
                mm2s_status);
        return -1;
    }
    if ((mm2s_status & TCP_DMA_SR_ERROR_MASK) != 0u) {
        dbg_printf("[DBG][TCP] MM2S error recovery reset dma=0x%08" PRIx64
                   " dmasr=0x%08x\n",
                   cfg->dma_base,
                   mm2s_status);
        if (tcp_reset_dma_mapped(cfg, dma) != 0) {
            return -1;
        }
    } else if (tcp_halt_mm2s_mapped(cfg, dma, "prepare") != 0) {
        return -1;
    }

    mmio_write32(dma, TCP_MM2S_DMASR, TCP_DMA_SR_IRQ_STATUS_MASK);
    return 0;
}

static int tcp_cleanup_mm2s_mapped(const TcpTransferConfig *cfg,
                                   const MappedRegion *dma,
                                   const char *reason)
{
    uint32_t status = mmio_read32(dma, TCP_MM2S_DMASR);

    if ((status & TCP_DMA_SR_ERROR_MASK) != 0u) {
        dbg_printf("[DBG][TCP] MM2S cleanup error reset reason=%s dma=0x%08" PRIx64
                   " dmasr=0x%08x\n",
                   reason ? reason : "none",
                   cfg->dma_base,
                   status);
        return tcp_reset_dma_mapped(cfg, dma);
    }
    return tcp_halt_mm2s_mapped(cfg, dma, reason);
}

static uint32_t tcp_prepare_desc(const TcpTransferConfig *cfg, const MappedRegion *desc_region)
{
    uint32_t desc_count = (uint32_t)((cfg->transfer_bytes + TCP_MAX_BYTES_PER_DESC - 1u) / TCP_MAX_BYTES_PER_DESC);
    DmaSgDesc *desc = (DmaSgDesc *)(void *)desc_region->virt;
    uint64_t remaining = cfg->transfer_bytes;
    uint64_t ddr_addr = cfg->ddr_dma_base;
    uint32_t i;

    for (i = 0u; i < desc_count; ++i) {
        uint32_t next = (i + 1u) % desc_count;
        uint32_t chunk = (remaining > TCP_MAX_BYTES_PER_DESC) ? TCP_MAX_BYTES_PER_DESC : (uint32_t)remaining;
        uint32_t control = chunk;

        if (i == 0u) {
            control |= TCP_DESC_CTRL_SOF;
        }
        if (i == (desc_count - 1u)) {
            control |= TCP_DESC_CTRL_EOF;
        }

        memset(&desc[i], 0, sizeof(desc[i]));
        desc[i].next_desc = (uint32_t)(cfg->desc_dma_base + (uint64_t)next * sizeof(DmaSgDesc));
        desc[i].buffer_addr = (uint32_t)ddr_addr;
        desc[i].control = control;

        ddr_addr += chunk;
        remaining -= chunk;
    }
    return desc_count;
}

void tcp_transfer_default_config(TcpTransferConfig *cfg, uint64_t transfer_bytes, GlobalOptions gopt)
{
    memset(cfg, 0, sizeof(*cfg));
    cfg->switch_base = TCP_SWITCH_BASE_DEFAULT;
    cfg->switch_input_select = TCP_SWITCH_INPUT_DEFAULT;
    cfg->dma_base = TCP_DMA_BASE_DEFAULT;
    cfg->desc_cpu_base = TCP_DESC_CPU_BASE_DEFAULT;
    cfg->desc_dma_base = TCP_DESC_DMA_BASE_DEFAULT;
    cfg->ddr_dma_base = TCP_DDR_DMA_BASE_DEFAULT;
    cfg->transfer_bytes = transfer_bytes;
    cfg->timeout_us = gopt.timeout_us ? gopt.timeout_us : DEFAULT_TIMEOUT_US;
    cfg->dry_run = gopt.dry_run;
}

void tcp_transfer_request_stop(void)
{
    g_tcp_stop_requested = 1;
}

int tcp_transfer_reset(const TcpTransferConfig *cfg)
{
    int fd = -1;
    int rc;
    MappedRegion dma;

    memset(&dma, 0, sizeof(dma));
    if (!cfg) {
        return -1;
    }
    if (cfg->dry_run) {
        printf("network_reset_done dry_run=1\n");
        return 0;
    }
    fd = open("/dev/mem", O_RDWR | O_SYNC);
    if (fd < 0) {
        fprintf(stderr, "tcp reset open /dev/mem failed errno=%d (%s)\n", errno, strerror(errno));
        return -1;
    }
    if (map_region(fd, cfg->dma_base, 0x10000u, &dma) != 0) {
        close(fd);
        return -1;
    }
    rc = tcp_reset_dma_mapped(cfg, &dma);
    unmap_region(&dma);
    close(fd);
    if (rc == 0) {
        printf("network_reset_done dma=0x%08" PRIx64 "\n", cfg->dma_base);
    }
    return rc;
}

int tcp_transfer_send(const TcpTransferConfig *cfg)
{
    int fd = -1;
    int rc = -1;
    uint32_t desc_count;
    uint32_t completed = 0u;
    uint32_t next_desc = 0u;
    uint32_t idle_waited_us = 0u;
    uint32_t last_dmasr = 0u;
    int poll_dmasr;
    int dma_started = 0;
    MappedRegion dma;
    MappedRegion sw;
    MappedRegion desc_region;
    DmaSgDesc *desc;

    memset(&dma, 0, sizeof(dma));
    memset(&sw, 0, sizeof(sw));
    memset(&desc_region, 0, sizeof(desc_region));
    if (!cfg || cfg->transfer_bytes == 0u) {
        return -1;
    }
    if (cfg->transfer_bytes > ((uint64_t)TCP_MAX_BYTES_PER_DESC * (uint64_t)DMA_DESC_COUNT_MAX)) {
        fprintf(stderr, "tcp transfer too large: bytes=%" PRIu64 "\n", cfg->transfer_bytes);
        return -1;
    }
    if (cfg->dry_run) {
        printf("network_send_done dry_run=1 bytes=%" PRIu64 " dma=0x%08" PRIx64
               " switch=0x%08" PRIx64 " input=%u desc_cpu=0x%08" PRIx64
               " desc_dma=0x%08" PRIx64 " ddr_dma=0x%08" PRIx64 "\n",
               cfg->transfer_bytes,
               cfg->dma_base,
               cfg->switch_base,
               (unsigned)cfg->switch_input_select,
               cfg->desc_cpu_base,
               cfg->desc_dma_base,
               cfg->ddr_dma_base);
        return 0;
    }

    if (g_tcp_stop_requested) {
        fprintf(stderr, "tcp transfer stop requested before start\n");
        return -2;
    }
    if (tcp_open_regions(cfg, &fd, &dma, &sw, &desc_region) != 0) {
        return -1;
    }
    desc = (DmaSgDesc *)(void *)desc_region.virt;
    poll_dmasr = tcp_env_flag_enabled("SRC_REAL_TCP_POLL_DMASR");
    dbg_printf("[DBG][TCP] start bytes=%" PRIu64 " dma=0x%08" PRIx64
               " input=%u poll_dmasr=%d\n",
               cfg->transfer_bytes,
               cfg->dma_base,
               (unsigned)cfg->switch_input_select,
               poll_dmasr);
    dbg_verbose_printf("[DBG][TCP] route detail switch=0x%08" PRIx64
                       " desc_cpu=0x%08" PRIx64 " desc_dma=0x%08" PRIx64
                       " ddr_dma=0x%08" PRIx64 "\n",
                       cfg->switch_base,
                       cfg->desc_cpu_base,
                       cfg->desc_dma_base,
                       cfg->ddr_dma_base);

    if (tcp_route_switch(cfg, &sw) != 0) {
        goto out;
    }
    desc_count = tcp_prepare_desc(cfg, &desc_region);
    dbg_verbose_printf("[DBG][TCP] desc prepared bytes=%" PRIu64 " desc_count=%u"
                       " first_ctrl=0x%08x first_buf=0x%08x tail_desc=0x%08" PRIx64 "\n",
                       cfg->transfer_bytes,
                       (unsigned)desc_count,
                       desc[0].control,
                       desc[0].buffer_addr,
                       cfg->desc_dma_base + (uint64_t)(desc_count - 1u) * sizeof(DmaSgDesc));
    if (tcp_prepare_mm2s_mapped(cfg, &dma) != 0) {
        goto out;
    }

    dbg_verbose_printf("[DBG][TCP] dma start begin curdesc=0x%08" PRIx64 "\n",
                       cfg->desc_dma_base);
    mmio_write32(&dma, TCP_MM2S_CURDESC, (uint32_t)cfg->desc_dma_base);
    mmio_write32(&dma, TCP_MM2S_CURDESC_MSB, 0u);
    mmio_write32(&dma, TCP_MM2S_DMACR, TCP_DMA_CR_RS_BIT | TCP_DMA_CR_IOC_IRQ_EN);
    dma_started = 1;
    if (wait_bits(&dma, TCP_MM2S_DMASR, TCP_DMA_SR_HALTED, 0u, cfg->timeout_us) != 0) {
        fprintf(stderr, "tcp MM2S halt clear timeout dmasr=0x%08x\n",
                mmio_read32(&dma, TCP_MM2S_DMASR));
        dbg_printf("[DBG][TCP] dma start failed dma=0x%08" PRIx64 " dmacr=0x%08x dmasr=0x%08x\n",
                   cfg->dma_base,
                   mmio_read32(&dma, TCP_MM2S_DMACR),
                   mmio_read32(&dma, TCP_MM2S_DMASR));
        goto out;
    }
    dbg_verbose_printf("[DBG][TCP] dma start done dma=0x%08" PRIx64 " dmacr=0x%08x dmasr=0x%08x\n",
                       cfg->dma_base,
                       mmio_read32(&dma, TCP_MM2S_DMACR),
                       mmio_read32(&dma, TCP_MM2S_DMASR));

    dbg_verbose_printf("[DBG][TCP] tail write begin tail=0x%08" PRIx64 "\n",
                       cfg->desc_dma_base + (uint64_t)(desc_count - 1u) * sizeof(DmaSgDesc));
    mmio_write32(&dma, TCP_MM2S_TAILDESC, (uint32_t)(cfg->desc_dma_base + (uint64_t)(desc_count - 1u) * sizeof(DmaSgDesc)));
    mmio_write32(&dma, TCP_MM2S_TAILDESC_MSB, 0u);
    dbg_printf("[DBG][TCP] wait completion desc_count=%u\n", (unsigned)desc_count);
    dbg_verbose_printf("[DBG][TCP] wait completion detail desc0_status=0x%08x\n",
                       desc[0].status);

    while (completed < desc_count) {
        uint32_t status;

        if (g_tcp_stop_requested) {
            tcp_log_mm2s_state("tcp transfer stop requested",
                               cfg,
                               &dma,
                               desc,
                               desc_count,
                               next_desc,
                               completed,
                               idle_waited_us,
                               poll_dmasr,
                               last_dmasr);
            rc = -2;
            goto out;
        }

        status = desc[next_desc].status;
        if ((status & TCP_DESC_STS_CMPLT) != 0u) {
            dbg_verbose_printf("[DBG][TCP] desc complete idx=%u status=0x%08x completed=%u/%u\n",
                               next_desc,
                               status,
                               completed + 1u,
                               desc_count);
            desc[next_desc].status = 0u;
            ++completed;
            next_desc = (next_desc + 1u) % desc_count;
            idle_waited_us = 0u;
            continue;
        }

        if (poll_dmasr) {
            last_dmasr = mmio_read32(&dma, TCP_MM2S_DMASR);
            if ((last_dmasr & TCP_DMA_SR_ERROR_MASK) != 0u) {
                fprintf(stderr, "tcp MM2S error dmasr=0x%08x desc=%u completed=%u/%u\n",
                        last_dmasr, next_desc, completed, desc_count);
                goto out;
            }
        }
        usleep(TCP_POLL_SLEEP_US);
        if (idle_waited_us < UINT32_MAX - TCP_POLL_SLEEP_US) {
            idle_waited_us += TCP_POLL_SLEEP_US;
        }
        if (idle_waited_us >= cfg->timeout_us) {
            tcp_log_mm2s_state("tcp MM2S completion timeout",
                               cfg,
                               &dma,
                               desc,
                               desc_count,
                               next_desc,
                               completed,
                               idle_waited_us,
                               poll_dmasr,
                               last_dmasr);
            goto out;
        }
        if (idle_waited_us > 0u && (idle_waited_us % TCP_WAIT_LOG_US) == 0u) {
            dbg_printf("[DBG][TCP] waiting desc=%u completed=%u/%u waited_us=%u"
                       " desc_status=0x%08x poll_dmasr=%d last_dmasr=0x%08x\n",
                       next_desc,
                       completed,
                       desc_count,
                       idle_waited_us,
                       desc[next_desc].status,
                       poll_dmasr,
                       last_dmasr);
        }
    }

    printf("network_send_done bytes=%" PRIu64 " desc_count=%u ddr_dma=0x%08" PRIx64 "\n",
           cfg->transfer_bytes, desc_count, cfg->ddr_dma_base);
    rc = 0;

out:
    if (dma.valid && dma_started) {
        int cleanup_rc = tcp_cleanup_mm2s_mapped(cfg,
                                                 &dma,
                                                 rc == 0 ? "send_complete" : "send_exit_error");
        if (cleanup_rc != 0 && rc == 0) {
            rc = -1;
        }
    }
    tcp_close_regions(fd, &dma, &sw, &desc_region);
    return rc;
}
