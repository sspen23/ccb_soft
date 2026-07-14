#include "serial_proto.h"
#include "debug_uart.h"
#include "storage_config.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#define FRAME_HEAD 0x55
#define FRAME_TAIL 0xAA
#define DEVICE_ID  0xCC

static uint8_t buf[128];
static int idx = 0;
static int expected_len = 0;

/* Registered callbacks. */
static int (*send_cb)(uint8_t *, int) = NULL;
static void (*handler_cb)(uint8_t *) = NULL;

/* Internal helpers. */

static int env_flag_enabled(const char *name)
{
    const char *value = storage_config_compat_getenv(name);

    if (!value || value[0] == '\0') {
        return 0;
    }
    if (strcmp(value, "0") == 0 ||
        strcmp(value, "false") == 0 ||
        strcmp(value, "FALSE") == 0 ||
        strcmp(value, "off") == 0 ||
        strcmp(value, "OFF") == 0 ||
        strcmp(value, "no") == 0 ||
        strcmp(value, "NO") == 0) {
        return 0;
    }
    return 1;
}

static int proto_hex_enabled(void)
{
    static int cached = -1;

    if (cached < 0) {
        cached = env_flag_enabled("SRC_REAL_DEBUG_HEX") ||
                 env_flag_enabled("CCB_DEBUG_HEX") ||
                 env_flag_enabled("SRC_REAL_DEBUG_VERBOSE") ||
                 env_flag_enabled("CCB_DEBUG_VERBOSE");
    }
    return cached != 0;
}

static int get_len(uint8_t cmd)
{
    switch(cmd) {
        case 0x11: return 64;
        case 0x21: return 16;
        case 0x31: return 16;
        case 0x41: return 16;
        case 0x51: return 32;
        case 0x61: return 16;
        case 0x71: return 16;
        default: return 0;
    }
}

static void dump_hex_line(const char *tag, const uint8_t *data, int len)
{
    int i;

    printf("[RXHEX] %s len=%d:", tag, len);
    for (i = 0; i < len; ++i) {
        printf(" %02X", (unsigned)data[i]);
    }
    printf("\n");
    fflush(stdout);
}

static void dump_rx_line(const char *tag, const uint8_t *data, int len)
{
    if (proto_hex_enabled()) {
        dump_hex_line(tag, data, len);
        return;
    }
    if (!dbg_category_enabled("PROTO")) {
        return;
    }
    printf("[RX] %s cmd=0x%02X len=%d\n",
           tag,
           (len > 2) ? (unsigned)data[2] : 0u,
           len);
    fflush(stdout);
}

static void dump_tx_hex_line(const char *tag, const uint8_t *data, int len)
{
    int i;

    printf("[TXHEX] %s len=%d:", tag, len);
    for (i = 0; i < len; ++i) {
        printf(" %02X", (unsigned)data[i]);
    }
    printf("\n");
    fflush(stdout);
}

static void dump_tx_line(const char *tag, const uint8_t *data, int len)
{
    if (proto_hex_enabled()) {
        dump_tx_hex_line(tag, data, len);
        return;
    }
    if (!dbg_category_enabled("PROTO")) {
        return;
    }
    printf("[TX] %s cmd=0x%02X result=0x%02X len=%d\n",
           tag,
           (len > 2) ? (unsigned)data[2] : 0u,
           (len > 3) ? (unsigned)data[3] : 0u,
           len);
    fflush(stdout);
}

static void store_be32(uint8_t out[4], uint32_t value)
{
    out[0] = (uint8_t)((value >> 24) & 0xFFu);
    out[1] = (uint8_t)((value >> 16) & 0xFFu);
    out[2] = (uint8_t)((value >> 8) & 0xFFu);
    out[3] = (uint8_t)(value & 0xFFu);
}

static void store_be64(uint8_t out[8], uint64_t value)
{
    out[0] = (uint8_t)((value >> 56) & 0xFFu);
    out[1] = (uint8_t)((value >> 48) & 0xFFu);
    out[2] = (uint8_t)((value >> 40) & 0xFFu);
    out[3] = (uint8_t)((value >> 32) & 0xFFu);
    out[4] = (uint8_t)((value >> 24) & 0xFFu);
    out[5] = (uint8_t)((value >> 16) & 0xFFu);
    out[6] = (uint8_t)((value >> 8) & 0xFFu);
    out[7] = (uint8_t)(value & 0xFFu);
}

static void send_ack(uint8_t cmd, uint8_t result)
{
    uint8_t ack[16] = {0};

    ack[0] = FRAME_HEAD;
    ack[1] = DEVICE_ID;
    ack[2] = cmd;
    ack[3] = result;
    ack[15] = FRAME_TAIL;

    dump_tx_line("ack", ack, 16);
    if (send_cb) send_cb(ack, 16);
}

static void send_acq_ack(uint8_t result, uint8_t acq_type, uint8_t failure_type)
{
    uint8_t ack[16] = {0};

    ack[0] = FRAME_HEAD;
    ack[1] = DEVICE_ID;
    ack[2] = CMD_ACQ_CTRL;
    ack[3] = result;
    ack[4] = acq_type;
    ack[5] = failure_type;
    ack[15] = FRAME_TAIL;

    dump_tx_line("acq_ack", ack, 16);
    if (send_cb) send_cb(ack, 16);
}

/* Public API. */

void proto_init()
{
    idx = 0;
    expected_len = 0;
}

void proto_set_send(int (*func)(uint8_t *, int))
{
    send_cb = func;
}

void proto_set_handler(void (*handler)(uint8_t *))
{
    handler_cb = handler;
}

void proto_input(uint8_t byte)
{
    if (idx == 0) {
        if (byte != FRAME_HEAD) return;
        buf[idx++] = byte;
        return;
    }

    if (idx == 1) {
        if (byte != DEVICE_ID) {
            buf[idx++] = byte;
            dump_rx_line("bad_device", buf, idx);
            idx = 0;
            return;
        }
        buf[idx++] = byte;
        return;
    }

    if (idx == 2) {
        buf[idx++] = byte;
        expected_len = get_len(byte);
        if (expected_len == 0) {
            dump_rx_line("unknown_cmd", buf, idx);
            idx = 0;
        }
        return;
    }

    buf[idx++] = byte;

    if (idx >= expected_len) {
        if (buf[expected_len - 1] == FRAME_TAIL) {
            dump_rx_line("frame_ok", buf, expected_len);

            if (handler_cb) {
                handler_cb(buf);
            } else {
                send_ack(buf[2], 0x00);
            }
        } else {
            printf("[RX] frame_tail_error cmd=0x%02X expected_len=%d actual_tail=%02X\n",
                   (unsigned)buf[2],
                   expected_len,
                   (unsigned)buf[expected_len - 1]);
            if (proto_hex_enabled()) {
                dump_hex_line("frame_tail_error_data", buf, expected_len);
            }
        }

        idx = 0;
    }
}

void proto_send_ack(uint8_t cmd, uint8_t result)
{
    send_ack(cmd, result);
}

void proto_send_acq_ack(uint8_t result, uint8_t acq_type, uint8_t failure_type)
{
    send_acq_ack(result, acq_type, failure_type);
}

void proto_send_file_list(const char *task_id,
                          uint8_t result,
                          uint8_t file_count,
                          uint8_t file_index,
                          uint8_t file_type,
                          uint64_t file_size,
                          uint64_t start_sector,
                          uint32_t sector_count,
                          uint8_t flag,
                          uint8_t calibration_type)
{
    AckFileList ack;

    memset(&ack, 0, sizeof(ack));
    ack.frame_head = FRAME_HEAD;
    ack.device_id = DEVICE_ID;
    ack.cmd_type = CMD_FILE_LIST;
    ack.result = result;
    if (task_id) {
        size_t task_len = strlen(task_id);
        if (task_len > 11u) {
            task_len = 11u;
        }
        memcpy(ack.task_id, task_id, task_len);
    }
    ack.file_count = file_count;
    ack.file_index = file_index;
    ack.file_type = file_type;
    store_be64(ack.file_size, file_size);
    store_be64(ack.start_sector, start_sector);
    store_be32(ack.sector_count, sector_count);
    ack.flag = flag;
    ack.calibration_type = calibration_type;
    ack.frame_tail = FRAME_TAIL;

    dump_tx_line("file_list", (const uint8_t *)&ack, (int)sizeof(ack));
    if (send_cb) {
        send_cb((uint8_t *)&ack, (int)sizeof(ack));
    }
}
