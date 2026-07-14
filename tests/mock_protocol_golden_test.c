#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "serial_proto.h"

static uint8_t sent_frame[sizeof(ResponsePkt)];
static int sent_length;
static uint8_t handled_frame[sizeof(CommandPkt)];
static int handled_count;

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

static void test_common_and_acquisition_ack(void)
{
    uint8_t expected[sizeof(AckCommon)] = {0};

    expected[0] = FRAME_HEAD;
    expected[1] = DEVICE_ID;
    expected[2] = CMD_TASK_INFO;
    expected[3] = ACK_RETRYING;
    expected[sizeof(expected) - 1u] = FRAME_TAIL;
    proto_send_ack(CMD_TASK_INFO, ACK_RETRYING);
    expect_bytes(expected, sizeof(expected));

    memset(expected, 0, sizeof(expected));
    expected[0] = FRAME_HEAD;
    expected[1] = DEVICE_ID;
    expected[2] = CMD_ACQ_CTRL;
    expected[3] = ACK_FAILED;
    expected[4] = ACQ_TYPE_HIGH_I | ACQ_TYPE_HIGH_Q;
    expected[5] = FAIL_TYPE_HIGH_Q;
    expected[sizeof(expected) - 1u] = FRAME_TAIL;
    proto_send_acq_ack(ACK_FAILED, ACQ_TYPE_HIGH_I | ACQ_TYPE_HIGH_Q,
                       FAIL_TYPE_HIGH_Q);
    expect_bytes(expected, sizeof(expected));
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
    proto_send_file_list("R2607140001", ACK_SUCCESS, 3u, 2u, FILE_TYPE_Q,
                         UINT64_C(0x0102030405060708),
                         UINT64_C(0x1112131415161718),
                         UINT32_C(0x21222324), FILE_LIST_FLAG_END, 0x7au);
    expect_bytes(expected, sizeof(expected));
}

int main(void)
{
    proto_set_send(capture_send);
    proto_set_handler(capture_handler);
    test_command_framing();
    test_common_and_acquisition_ack();
    test_file_list_ack();
    puts("mock_protocol_golden_test: ok");
    return 0;
}
