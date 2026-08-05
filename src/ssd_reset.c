#define _POSIX_C_SOURCE 200809L
#include "ssd_reset.h"

#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#ifdef __linux__
#include <sys/mman.h>
#endif

static uint32_t ssd_reset_read(volatile uint32_t *reg)
{
    return *reg;
}

static void ssd_reset_write(volatile uint32_t *reg, uint32_t value)
{
    *reg = value;
    __sync_synchronize();
}

static int ssd_reset_delay_ms(uint32_t delay_ms, void *ctx)
{
    struct timespec remaining;

    (void)ctx;
    remaining.tv_sec = (time_t)(delay_ms / 1000u);
    remaining.tv_nsec = (long)(delay_ms % 1000u) * 1000000L;
    while (nanosleep(&remaining, &remaining) != 0) {
        if (errno != EINTR) return -1;
    }
    return 0;
}

static int ssd_reset_drive(volatile uint32_t *data_reg, bool high)
{
    const uint32_t mask = 1u << SSD_RESET_GPIO_BIT;
    uint32_t value = ssd_reset_read(data_reg);

    value = high ? value | mask : value & ~mask;
    ssd_reset_write(data_reg, value);
    value = ssd_reset_read(data_reg);
    return ((value & mask) != 0u) == high ? 0 : -1;
}

SsdResetError ssd_reset_pulse_fixed_output_registers(
    volatile uint32_t *data_reg, uint32_t hold_ms, uint32_t settle_ms,
    SsdResetDelayFn delay_fn, void *delay_ctx)
{
    SsdResetError error = SSD_RESET_OK;

    if (!data_reg || !delay_fn) return SSD_RESET_ERR_ARGUMENT;

    if (ssd_reset_drive(data_reg, false) != 0)
        error = SSD_RESET_ERR_ASSERT;
    else if (delay_fn(hold_ms, delay_ctx) != 0)
        error = SSD_RESET_ERR_HOLD_DELAY;

    /* Never leave the SSD and the connected data channels held in reset. */
    if (ssd_reset_drive(data_reg, true) != 0)
        return SSD_RESET_ERR_RELEASE;
    if (error != SSD_RESET_OK) return error;
    if (delay_fn(settle_ms, delay_ctx) != 0)
        return SSD_RESET_ERR_SETTLE_DELAY;
    return SSD_RESET_OK;
}

SsdResetError ssd_reset_pulse_registers(volatile uint32_t *data_reg,
                                        volatile uint32_t *tri_reg,
                                        uint32_t hold_ms,
                                        uint32_t settle_ms,
                                        SsdResetDelayFn delay_fn,
                                        void *delay_ctx)
{
    const uint32_t mask = 1u << SSD_RESET_GPIO_BIT;
    uint32_t direction;
    SsdResetError error = SSD_RESET_OK;

    if (!data_reg || !tri_reg || !delay_fn) return SSD_RESET_ERR_ARGUMENT;

    direction = ssd_reset_read(tri_reg) & ~mask;
    ssd_reset_write(tri_reg, direction);
    if ((ssd_reset_read(tri_reg) & mask) != 0u) {
        (void)ssd_reset_drive(data_reg, true);
        return SSD_RESET_ERR_DIRECTION;
    }

    if (ssd_reset_drive(data_reg, false) != 0)
        error = SSD_RESET_ERR_ASSERT;
    else if (delay_fn(hold_ms, delay_ctx) != 0)
        error = SSD_RESET_ERR_HOLD_DELAY;

    /* Never leave the SSD and the connected data channels held in reset. */
    if (ssd_reset_drive(data_reg, true) != 0)
        return SSD_RESET_ERR_RELEASE;
    if (error != SSD_RESET_OK) return error;
    if (delay_fn(settle_ms, delay_ctx) != 0)
        return SSD_RESET_ERR_SETTLE_DELAY;
    return SSD_RESET_OK;
}

SsdResetError ssd_reset_gpio_pulse(uint32_t hold_ms, uint32_t settle_ms)
{
#ifdef __linux__
    long page_size;
    uint64_t aligned;
    size_t page_offset;
    size_t map_len;
    void *map_base;
    volatile uint8_t *gpio;
    int fd;
    SsdResetError error;

    page_size = sysconf(_SC_PAGESIZE);
    if (page_size <= 0) return SSD_RESET_ERR_MAP;
    aligned = SSD_RESET_GPIO_BASE & ~((uint64_t)page_size - 1ull);
    page_offset = (size_t)(SSD_RESET_GPIO_BASE - aligned);
    map_len = page_offset + SSD_RESET_GPIO_MAP_SIZE;

    fd = open("/dev/mem", O_RDWR | O_SYNC);
    if (fd < 0) return SSD_RESET_ERR_OPEN;
    map_base = mmap(NULL, map_len, PROT_READ | PROT_WRITE, MAP_SHARED, fd,
                    (off_t)aligned);
    if (map_base == MAP_FAILED) {
        close(fd);
        return SSD_RESET_ERR_MAP;
    }

    gpio = (volatile uint8_t *)map_base + page_offset;
    /* axi_gpio_ssd_rstn is synthesized with C_ALL_OUTPUTS=1. */
    error = ssd_reset_pulse_fixed_output_registers(
        (volatile uint32_t *)(gpio + SSD_RESET_GPIO_DATA_OFFSET),
        hold_ms, settle_ms, ssd_reset_delay_ms, NULL);

    (void)munmap(map_base, map_len);
    (void)close(fd);
    return error;
#else
    (void)hold_ms;
    (void)settle_ms;
    return SSD_RESET_ERR_MAP;
#endif
}

const char *ssd_reset_error_string(SsdResetError error)
{
    switch (error) {
    case SSD_RESET_OK: return "none";
    case SSD_RESET_ERR_ARGUMENT: return "argument";
    case SSD_RESET_ERR_OPEN: return "open_devmem";
    case SSD_RESET_ERR_MAP: return "mmap_gpio";
    case SSD_RESET_ERR_DIRECTION: return "gpio_direction";
    case SSD_RESET_ERR_ASSERT: return "gpio_assert";
    case SSD_RESET_ERR_HOLD_DELAY: return "hold_delay";
    case SSD_RESET_ERR_RELEASE: return "gpio_release";
    case SSD_RESET_ERR_SETTLE_DELAY: return "settle_delay";
    default: return "unknown";
    }
}
