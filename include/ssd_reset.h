#ifndef SSD_RESET_H
#define SSD_RESET_H

#include <stdint.h>

#define SSD_RESET_GPIO_BASE 0x40010000ull
#define SSD_RESET_GPIO_MAP_SIZE 0x10000u
#define SSD_RESET_GPIO_DATA_OFFSET 0x00u
#define SSD_RESET_GPIO_TRI_OFFSET 0x04u
#define SSD_RESET_GPIO_BIT 0u

#define SSD_RESET_DEFAULT_HOLD_MS 100u
#define SSD_RESET_DEFAULT_SETTLE_MS 3000u

typedef enum {
    SSD_RESET_OK = 0,
    SSD_RESET_ERR_ARGUMENT,
    SSD_RESET_ERR_OPEN,
    SSD_RESET_ERR_MAP,
    SSD_RESET_ERR_DIRECTION,
    SSD_RESET_ERR_ASSERT,
    SSD_RESET_ERR_HOLD_DELAY,
    SSD_RESET_ERR_RELEASE,
    SSD_RESET_ERR_SETTLE_DELAY
} SsdResetError;

typedef int (*SsdResetDelayFn)(uint32_t delay_ms, void *ctx);

/* Pulse a GPIO bit implemented as a hardware-fixed output. */
SsdResetError ssd_reset_pulse_fixed_output_registers(
    volatile uint32_t *data_reg, uint32_t hold_ms, uint32_t settle_ms,
    SsdResetDelayFn delay_fn, void *delay_ctx);

/* Testable register-level sequence. The GPIO is active-low. */
SsdResetError ssd_reset_pulse_registers(volatile uint32_t *data_reg,
                                        volatile uint32_t *tri_reg,
                                        uint32_t hold_ms,
                                        uint32_t settle_ms,
                                        SsdResetDelayFn delay_fn,
                                        void *delay_ctx);

/* Map the board AXI GPIO and issue one low/high reset pulse. */
SsdResetError ssd_reset_gpio_pulse(uint32_t hold_ms, uint32_t settle_ms);
const char *ssd_reset_error_string(SsdResetError error);

#endif
