#include <errno.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <signal.h>
#include <unistd.h>

#define SSD_PCIE_RST_GPIO_BASE 0x40010000ull
#define AXI_GPIO_MAP_SIZE      0x10000u
#define AXI_GPIO_DATA_OFFSET   0x00u
#define AXI_GPIO_TRI_OFFSET    0x04u
#define SSD_PCIE_RST_BIT       0u

#define DEFAULT_HOLD_MS        100u
#define DEFAULT_SETTLE_MS      1000u
#define STORAGE_PIDFILE        "/var/run/storage.elf.pid"

typedef struct {
    int fd;
    void *map_base;
    size_t map_len;
    uintptr_t page_offset;
} MmioMap;

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s [--force] assert\n"
            "  %s release\n"
            "  %s [--force] pulse [hold_ms] [settle_ms]\n"
            "  %s status\n"
            "\n"
            "AXI GPIO base: 0x%08" PRIx64 ", bit: %u, active low.\n"
            "assert  : drive pcie_rstn low (SSD in reset)\n"
            "release : drive pcie_rstn high (SSD out of reset)\n"
            "pulse   : assert then release, defaults hold=%u ms settle=%u ms\n",
            prog,
            prog,
            prog,
            prog,
            (uint64_t)SSD_PCIE_RST_GPIO_BASE,
            SSD_PCIE_RST_BIT,
            DEFAULT_HOLD_MS,
            DEFAULT_SETTLE_MS);
}

static int storage_service_running(void)
{
    FILE *fp;
    long pid;

    fp = fopen(STORAGE_PIDFILE, "r");
    if (!fp) {
        return 0;
    }
    if (fscanf(fp, "%ld", &pid) != 1 || pid <= 0) {
        fclose(fp);
        return 0;
    }
    fclose(fp);
    return kill((pid_t)pid, 0) == 0;
}

static int refuse_if_storage_running(const char *cmd, bool force)
{
    if (force || !storage_service_running()) {
        return 0;
    }
    fprintf(stderr,
            "Refuse to %s SSD reset while storage service is running. "
            "Stop /etc/init.d/storage first, or pass --force.\n",
            cmd);
    return -1;
}

static int parse_u32(const char *text, uint32_t *out)
{
    char *end = NULL;
    unsigned long value;

    if (!text || !out) {
        return -1;
    }

    errno = 0;
    value = strtoul(text, &end, 0);
    if (errno != 0 || end == text || *end != '\0' || value > UINT32_MAX) {
        return -1;
    }

    *out = (uint32_t)value;
    return 0;
}

static int mmio_open(uint64_t phys, size_t size, MmioMap *mmio)
{
    long page_size_l;
    uint64_t aligned;
    size_t map_len;

    if (!mmio || size == 0u) {
        return -1;
    }
    memset(mmio, 0, sizeof(*mmio));
    mmio->fd = -1;

    page_size_l = sysconf(_SC_PAGESIZE);
    if (page_size_l <= 0) {
        fprintf(stderr, "sysconf(_SC_PAGESIZE) failed\n");
        return -1;
    }

    aligned = phys & ~((uint64_t)page_size_l - 1ull);
    mmio->page_offset = (uintptr_t)(phys - aligned);
    map_len = mmio->page_offset + size;

    mmio->fd = open("/dev/mem", O_RDWR | O_SYNC);
    if (mmio->fd < 0) {
        fprintf(stderr, "open /dev/mem failed: errno=%d (%s)\n", errno, strerror(errno));
        return -1;
    }

    mmio->map_base = mmap(NULL, map_len, PROT_READ | PROT_WRITE, MAP_SHARED, mmio->fd, (off_t)aligned);
    if (mmio->map_base == MAP_FAILED) {
        fprintf(stderr,
                "mmap failed: phys=0x%08" PRIx64 " size=0x%zx errno=%d (%s)\n",
                phys,
                size,
                errno,
                strerror(errno));
        close(mmio->fd);
        mmio->fd = -1;
        mmio->map_base = NULL;
        return -1;
    }

    mmio->map_len = map_len;
    return 0;
}

static void mmio_close(MmioMap *mmio)
{
    if (!mmio) {
        return;
    }
    if (mmio->map_base && mmio->map_base != MAP_FAILED) {
        munmap(mmio->map_base, mmio->map_len);
    }
    if (mmio->fd >= 0) {
        close(mmio->fd);
    }
    memset(mmio, 0, sizeof(*mmio));
    mmio->fd = -1;
}

static volatile uint32_t *mmio_reg(MmioMap *mmio, uint32_t offset)
{
    return (volatile uint32_t *)((uint8_t *)mmio->map_base + mmio->page_offset + offset);
}

static uint32_t gpio_read(MmioMap *mmio, uint32_t offset)
{
    return *mmio_reg(mmio, offset);
}

static void gpio_write(MmioMap *mmio, uint32_t offset, uint32_t value)
{
    volatile uint32_t *reg = mmio_reg(mmio, offset);
    *reg = value;
}

static void gpio_set_output(MmioMap *mmio, uint32_t bit)
{
    (void)bit;
    gpio_write(mmio, AXI_GPIO_TRI_OFFSET, 0u);
}

static void gpio_drive_bit(MmioMap *mmio, uint32_t bit, bool high)
{
    gpio_write(mmio, AXI_GPIO_DATA_OFFSET, high ? (1u << bit) : 0u);
}

static void print_status(MmioMap *mmio)
{
    uint32_t data = gpio_read(mmio, AXI_GPIO_DATA_OFFSET);
    uint32_t tri = gpio_read(mmio, AXI_GPIO_TRI_OFFSET);
    uint32_t bit_value = (data >> SSD_PCIE_RST_BIT) & 1u;
    uint32_t bit_tri = (tri >> SSD_PCIE_RST_BIT) & 1u;

    printf("ssd_pcie_reset_status base=0x%08" PRIx64
           " data=0x%08x tri=0x%08x bit%u=%u direction=%s reset_state=%s\n",
           (uint64_t)SSD_PCIE_RST_GPIO_BASE,
           data,
           tri,
           SSD_PCIE_RST_BIT,
           bit_value,
           bit_tri ? "input" : "output",
           bit_value ? "released" : "asserted");
}

int main(int argc, char **argv)
{
    const char *cmd;
    MmioMap mmio;
    bool force = false;
    int rc = 1;

    if (argc < 2) {
        usage(argv[0]);
        return 2;
    }

    if (strcmp(argv[1], "--force") == 0) {
        force = true;
        --argc;
        ++argv;
        if (argc < 2) {
            usage(argv[0]);
            return 2;
        }
    }

    cmd = argv[1];
    if (mmio_open(SSD_PCIE_RST_GPIO_BASE, AXI_GPIO_MAP_SIZE, &mmio) != 0) {
        return 1;
    }

    if (strcmp(cmd, "assert") == 0 || strcmp(cmd, "reset") == 0) {
        if (refuse_if_storage_running(cmd, force) != 0) {
            goto out;
        }
        gpio_set_output(&mmio, SSD_PCIE_RST_BIT);
        gpio_drive_bit(&mmio, SSD_PCIE_RST_BIT, false);
        printf("ssd_pcie_reset_asserted base=0x%08" PRIx64 " bit=%u value=0\n",
               (uint64_t)SSD_PCIE_RST_GPIO_BASE,
               SSD_PCIE_RST_BIT);
        rc = 0;
    } else if (strcmp(cmd, "release") == 0) {
        gpio_set_output(&mmio, SSD_PCIE_RST_BIT);
        gpio_drive_bit(&mmio, SSD_PCIE_RST_BIT, true);
        printf("ssd_pcie_reset_released base=0x%08" PRIx64 " bit=%u value=1\n",
               (uint64_t)SSD_PCIE_RST_GPIO_BASE,
               SSD_PCIE_RST_BIT);
        rc = 0;
    } else if (strcmp(cmd, "pulse") == 0) {
        uint32_t hold_ms = DEFAULT_HOLD_MS;
        uint32_t settle_ms = DEFAULT_SETTLE_MS;

        if (argc >= 3 && parse_u32(argv[2], &hold_ms) != 0) {
            fprintf(stderr, "Invalid hold_ms: %s\n", argv[2]);
            goto out;
        }
        if (argc >= 4 && parse_u32(argv[3], &settle_ms) != 0) {
            fprintf(stderr, "Invalid settle_ms: %s\n", argv[3]);
            goto out;
        }
        if (argc > 4) {
            usage(argv[0]);
            rc = 2;
            goto out;
        }

        if (refuse_if_storage_running(cmd, force) != 0) {
            goto out;
        }
        gpio_set_output(&mmio, SSD_PCIE_RST_BIT);
        gpio_drive_bit(&mmio, SSD_PCIE_RST_BIT, false);
        printf("ssd_pcie_reset_asserted base=0x%08" PRIx64 " bit=%u hold_ms=%u\n",
               (uint64_t)SSD_PCIE_RST_GPIO_BASE,
               SSD_PCIE_RST_BIT,
               hold_ms);
        fflush(stdout);
        usleep((useconds_t)hold_ms * 1000u);

        gpio_drive_bit(&mmio, SSD_PCIE_RST_BIT, true);
        printf("ssd_pcie_reset_released base=0x%08" PRIx64 " bit=%u settle_ms=%u\n",
               (uint64_t)SSD_PCIE_RST_GPIO_BASE,
               SSD_PCIE_RST_BIT,
               settle_ms);
        fflush(stdout);
        usleep((useconds_t)settle_ms * 1000u);
        rc = 0;
    } else if (strcmp(cmd, "status") == 0) {
        print_status(&mmio);
        rc = 0;
    } else {
        usage(argv[0]);
        rc = 2;
    }

out:
    mmio_close(&mmio);
    return rc;
}
