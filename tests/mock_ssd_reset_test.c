#include <assert.h>
#include <stdint.h>
#include <stdio.h>

#include "ssd_reset.h"

typedef struct {
    volatile uint32_t *data;
    uint32_t calls;
    int fail_call;
} FakeDelay;

static int fake_delay(uint32_t delay_ms, void *opaque)
{
    FakeDelay *delay = opaque;

    if (delay->calls == 0u) {
        assert(delay_ms == SSD_RESET_DEFAULT_HOLD_MS);
        assert((*delay->data & 1u) == 0u);
    } else {
        assert(delay_ms == SSD_RESET_DEFAULT_SETTLE_MS);
        assert((*delay->data & 1u) == 1u);
    }
    ++delay->calls;
    return delay->fail_call == (int)delay->calls ? -1 : 0;
}

int main(void)
{
    volatile uint32_t data = 0xffffffffu;
    volatile uint32_t tri = 0xffffffffu;
    FakeDelay delay = {&data, 0u, 0};

    assert(ssd_reset_pulse_registers(
               &data, &tri, SSD_RESET_DEFAULT_HOLD_MS,
               SSD_RESET_DEFAULT_SETTLE_MS, fake_delay, &delay) ==
           SSD_RESET_OK);
    assert((tri & 1u) == 0u);
    assert((data & 1u) == 1u);
    assert((tri & ~1u) == 0xfffffffeu);
    assert((data & ~1u) == 0xfffffffeu);
    assert(delay.calls == 2u);

    data = 0xffffffffu;
    delay.calls = 0u;
    delay.fail_call = 0;
    assert(ssd_reset_pulse_fixed_output_registers(
               &data, SSD_RESET_DEFAULT_HOLD_MS,
               SSD_RESET_DEFAULT_SETTLE_MS, fake_delay, &delay) ==
           SSD_RESET_OK);
    assert((data & 1u) == 1u);
    assert(delay.calls == 2u);

    data = 0xffffffffu;
    tri = 0xffffffffu;
    delay.calls = 0u;
    delay.fail_call = 1;
    assert(ssd_reset_pulse_registers(
               &data, &tri, SSD_RESET_DEFAULT_HOLD_MS,
               SSD_RESET_DEFAULT_SETTLE_MS, fake_delay, &delay) ==
           SSD_RESET_ERR_HOLD_DELAY);
    assert((data & 1u) == 1u);
    assert(delay.calls == 1u);

    puts("mock_ssd_reset_test: ok");
    return 0;
}
