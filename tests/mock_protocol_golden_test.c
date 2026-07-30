#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "serial_proto.h"

#define UART_LOG_BYTES 4096u

static uint8_t sent_frame[sizeof(ResponsePkt)];
static int sent_length;
static uint8_t handled_frame[sizeof(CommandPkt)];
static int handled_count;
static char uart_log[UART_LOG_BYTES];
static size_t uart_log_bytes;

void debug_uart_write(const char *data, size_t length)
{
    size_t available;

    if (!data || length == 0u || uart_log_bytes >= sizeof(uart_log) - 1u) {
        return;
    }
    available = sizeof(uart_log) - 1u - uart_log_bytes;
    if (length > available) {
        length = available;
    }
    memcpy(uart_log + uart_log_bytes, data, length);
    uart_log_bytes += length;
    uart_log[uart_log_bytes] = '\0';
}

static void reset_uart_log(void)
{
    memset(uart_log, 0, sizeof(uart_log));
    uart_log_bytes = 0u;
}

static void expect_uart_log(const char *expected)
{
    assert(expected != NULL);
    assert(strcmp(uart_log, expected) == 0);
}

static void feed_frame(const void *frame, size_t length)
{
    const uint8_t *bytes = frame;
    size_t i;

    proto_init();
    for (i = 0u; i < length; ++i) {
        proto_input(bytes[i]);
    }
}

static int capture_send(uint8_t *frame, int length)
{
    assert(length >= 0);
    assert((size_t)length <= sizeof(sent_frame));
    memcpy(sent_frame, frame, (size_t)length);
    sent_length = length;
    return length;
}

static void capture_handler(uint8_t *frame)
{
    int length = frame[2] == CMD_TASK_INFO ? (int)sizeof(CmdTaskInfo) :
                 frame[2] == CMD_FILE_OP ? (int)sizeof(CmdFileOp) :
                 (int)sizeof(CmdStatusQuery);

    memcpy(handled_frame, frame, (size_t)length);
    ++handled_count;
}

static void expect_bytes(const uint8_t *expected, size_t length)
{
    assert(sent_length == (int)length);
    assert(memcmp(sent_frame, expected, length) == 0);
}

static void test_command_framing(void)
{
    uint8_t status[sizeof(CmdStatusQuery)] = {0};
    size_t i;

    status[0] = FRAME_HEAD;
    status[1] = DEVICE_ID;
    status[2] = CMD_STATUS;
    status[sizeof(status) - 1u] = FRAME_TAIL;

    proto_init();
    handled_count = 0;
    for (i = 0u; i < sizeof(status); ++i) proto_input(status[i]);
    assert(handled_count == 1);
    assert(memcmp(handled_frame, status, sizeof(status)) == 0);

    status[sizeof(status) - 1u] = 0u;
    for (i = 0u; i < sizeof(status); ++i) proto_input(status[i]);
    assert(handled_count == 1);

    proto_input(FRAME_HEAD);
    proto_input(0u);
    status[sizeof(status) - 1u] = FRAME_TAIL;
    for (i = 0u; i < sizeof(status); ++i) proto_input(status[i]);
    assert(handled_count == 2);
}

static void test_rx_debug_field_offsets(void)
{
    static const char task_id[11] = {
        'R', '2', '5', '0', '9', '1', '0', '0', '0', '0', '0'
    };
    CmdTaskInfo task_info;
    CmdFileOp file_op;
    CmdAcqCtrl acq_ctrl;
    CmdUsbTransfer usb_transfer;
    CmdFileList file_list;
    CmdStatusQuery status;
    CmdStopTransfer stop_transfer;

    memset(&task_info, 0, sizeof(task_info));
    task_info.frame_head = FRAME_HEAD;
    task_info.device_id = DEVICE_ID;
    task_info.cmd_type = CMD_TASK_INFO;
    memcpy(task_info.task_id, task_id, sizeof(task_info.task_id));
    task_info.frame_tail = FRAME_TAIL;
    reset_uart_log();
    feed_frame(&task_info, sizeof(task_info));
    expect_uart_log("[UART RX] frame_ok cmd=0x11 task_no=R2509100000 len=64\n");

    memset(&file_op, 0, sizeof(file_op));
    file_op.frame_head = FRAME_HEAD;
    file_op.device_id = DEVICE_ID;
    file_op.cmd_type = CMD_FILE_OP;
    file_op.operation = FILE_OP_DOWNLOAD;
    memcpy(file_op.task_id, task_id, sizeof(file_op.task_id));
    file_op.frame_tail = FRAME_TAIL;
    reset_uart_log();
    feed_frame(&file_op, sizeof(file_op));
    expect_uart_log("[UART RX] frame_ok cmd=0x51 task_no=R2509100000 len=32\n");
    assert(memcmp(((const CmdFileOp *)(const void *)handled_frame)->task_id,
                  task_id, sizeof(task_id)) == 0);

    memset(&acq_ctrl, 0, sizeof(acq_ctrl));
    acq_ctrl.frame_head = FRAME_HEAD;
    acq_ctrl.device_id = DEVICE_ID;
    acq_ctrl.cmd_type = CMD_ACQ_CTRL;
    acq_ctrl.switch_flag = SWITCH_ON;
    acq_ctrl.frame_tail = FRAME_TAIL;
    reset_uart_log();
    feed_frame(&acq_ctrl, sizeof(acq_ctrl));
    expect_uart_log("[UART RX] frame_ok cmd=0x21 arg=0x11 len=16\n");

    memset(&usb_transfer, 0, sizeof(usb_transfer));
    usb_transfer.frame_head = FRAME_HEAD;
    usb_transfer.device_id = DEVICE_ID;
    usb_transfer.cmd_type = CMD_USB_TRANSFER;
    usb_transfer.switch_flag = SWITCH_OFF;
    usb_transfer.frame_tail = FRAME_TAIL;
    reset_uart_log();
    feed_frame(&usb_transfer, sizeof(usb_transfer));
    expect_uart_log("[UART RX] frame_ok cmd=0x31 arg=0xFF len=16\n");

    memset(&file_list, 0, sizeof(file_list));
    file_list.frame_head = FRAME_HEAD;
    file_list.device_id = DEVICE_ID;
    file_list.cmd_type = CMD_FILE_LIST;
    file_list.control = FILE_LIST_SYNC_FLASH;
    file_list.frame_tail = FRAME_TAIL;
    reset_uart_log();
    feed_frame(&file_list, sizeof(file_list));
    expect_uart_log("[UART RX] frame_ok cmd=0x41 arg=0x22 len=16\n");

    memset(&status, 0, sizeof(status));
    status.frame_head = FRAME_HEAD;
    status.device_id = DEVICE_ID;
    status.cmd_type = CMD_STATUS;
    status.frame_tail = FRAME_TAIL;
    reset_uart_log();
    feed_frame(&status, sizeof(status));
    expect_uart_log("[UART RX] frame_ok cmd=0x61 len=16\n");

    memset(&stop_transfer, 0, sizeof(stop_transfer));
    stop_transfer.frame_head = FRAME_HEAD;
    stop_transfer.device_id = DEVICE_ID;
    stop_transfer.cmd_type = CMD_STOP_TRANSFER;
    stop_transfer.frame_tail = FRAME_TAIL;
    reset_uart_log();
    feed_frame(&stop_transfer, sizeof(stop_transfer));
    expect_uart_log("[UART RX] frame_ok cmd=0x71 len=16\n");
}

static void test_common_and_acquisition_ack(void)
{
    uint8_t expected[sizeof(AckCommon)] = {0};

    expected[0] = FRAME_HEAD;
    expected[1] = DEVICE_ID;
    expected[2] = CMD_TASK_INFO;
    expected[3] = ACK_RETRYING;
    expected[sizeof(expected) - 1u] = FRAME_TAIL;
    reset_uart_log();
    proto_send_ack(CMD_TASK_INFO, ACK_RETRYING);
    expect_bytes(expected, sizeof(expected));
    expect_uart_log("[UART TX] ack cmd=0x11 result=0x55 len=16\n");

    memset(expected, 0, sizeof(expected));
    expected[0] = FRAME_HEAD;
    expected[1] = DEVICE_ID;
    expected[2] = CMD_ACQ_CTRL;
    expected[3] = ACK_FAILED;
    expected[4] = ACQ_TYPE_HIGH_I | ACQ_TYPE_HIGH_Q;
    expected[5] = FAIL_TYPE_HIGH_Q;
    expected[sizeof(expected) - 1u] = FRAME_TAIL;
    reset_uart_log();
    proto_send_acq_ack(ACK_FAILED, ACQ_TYPE_HIGH_I | ACQ_TYPE_HIGH_Q,
                       FAIL_TYPE_HIGH_Q);
    expect_bytes(expected, sizeof(expected));
    expect_uart_log("[UART TX] acq_ack cmd=0x21 result=0xFF len=16\n");
}

static void test_file_list_ack(void)
{
    static const uint8_t expected[] = {
        0x55, 0xcc, 0x41, 0x11,
        'R', '2', '6', '0', '7', '1', '4', '0', '0', '0', '1',
        0x03, 0x02, 0x02,
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
        0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
        0x21, 0x22, 0x23, 0x24,
        0xff, 0x7a, 0xaa
    };

    _Static_assert(sizeof(expected) == sizeof(AckFileList),
                   "file-list golden frame size");
    reset_uart_log();
    proto_send_file_list("R2607140001", ACK_SUCCESS, 3u, 2u, FILE_TYPE_Q,
                         UINT64_C(0x0102030405060708),
                         UINT64_C(0x1112131415161718),
                         UINT32_C(0x21222324), FILE_LIST_FLAG_END, 0x7au);
    expect_bytes(expected, sizeof(expected));
    expect_uart_log("[UART TX] file_list cmd=0x41 result=0x11 len=41\n");
}

int main(void)
{
    unsetenv("CCB_LOG_LEVEL");
    unsetenv("SRC_REAL_LOG_LEVEL");
    proto_set_send(capture_send);
    proto_set_handler(capture_handler);
    test_command_framing();
    test_rx_debug_field_offsets();
    test_common_and_acquisition_ack();
    test_file_list_ack();
    puts("mock_protocol_golden_test: ok");
    return 0;
}
