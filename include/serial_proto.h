#ifndef SERIAL_PROTO_H
#define SERIAL_PROTO_H

#include <stddef.h>
#include <stdint.h>

/*
 * UART protocol note:
 * The current upper-computer program sends and parses all multi-byte protocol
 * fields as big-endian. Packet structs stay byte-packed, and code converts
 * multi-byte fields explicitly instead of relying on CPU endianness.
 */

#define FRAME_HEAD        0x55
#define FRAME_TAIL        0xAA
#define DEVICE_ID         0xCC

#define CMD_TASK_INFO     0x11
#define CMD_ACQ_CTRL      0x21
#define CMD_USB_TRANSFER  0x31
#define CMD_FILE_LIST     0x41
#define CMD_FILE_OP       0x51
#define CMD_STATUS        0x61
#define CMD_STOP_TRANSFER 0x71

#define ACK_SUCCESS       0x11
#define ACK_FAILED        0xFF
#define ACK_INVALID_PARAM 0x00
#define ACK_RETRYING      0x55

#define FAIL_TYPE_NONE    0x00
#define FAIL_TYPE_LOW     0x01
#define FAIL_TYPE_HIGH_I  0x02
#define FAIL_TYPE_HIGH_Q  0x04
#define FAIL_TYPE_USB     0x10

#define SWITCH_ON         0x11
#define SWITCH_OFF        0xFF

#define FILE_OP_DOWNLOAD       0x11
#define FILE_OP_DELETE_ONE     0x22
#define FILE_OP_DELETE_ALL     0xFF

#define FILE_LIST_READ         0x11
#define FILE_LIST_SYNC_FLASH   0x22
#define FILE_LIST_CLEAR        0xFF

#define FILE_LIST_FLAG_CONTINUE 0x11
#define FILE_LIST_FLAG_END      0xFF

#define ACQ_TYPE_ENVELOPE   0x01
#define ACQ_TYPE_HIGH_I     0x02
#define ACQ_TYPE_HIGH_Q     0x04
#define ACQ_TYPE_CALIB      0x08

#define FILE_TYPE_LOW     0x00
#define FILE_TYPE_I       0x01
#define FILE_TYPE_Q       0x02
#define FILE_TYPE_CALIB   0x03

#define TASK_FILE_MODE_CALIB_ONLY 0x11
#define TASK_FILE_MODE_LOW_ONLY   0x22
#define TASK_FILE_MODE_HIGH_ONLY  0x33
#define TASK_FILE_MODE_ALL        0xAA

#pragma pack(1)
typedef struct {
    uint8_t  frame_head;
    uint8_t  device_id;
    uint8_t  cmd_type;
    char     task_id[11];
    char     overpass_time[14];
    uint8_t  work_mode;
    uint8_t  rcs_value;
    uint8_t  delay_setting;
    uint8_t  envelope_clock;
    uint8_t  iq_clock;
    uint8_t  rx_bandwidth;
    uint8_t  azimuth_angle[2];    /* big-endian uint16 on UART */
    uint8_t  elevation_angle[2];  /* big-endian int16 on UART */
    uint8_t  envelope_duration[2];/* big-endian uint16 on UART */
    uint8_t  iq_duration;
    uint8_t  lo_select;
    uint8_t  freq_select;
    uint8_t  bandwidth_setting;
    uint8_t  pulse_width;
    uint8_t  period_setting[2];   /* big-endian uint16 on UART */
    uint8_t  task_file_mode;
    uint8_t  usb_transfer_enable;
    uint8_t  calibration_type;
    uint8_t  reserved[13];
    uint8_t  frame_tail;
} CmdTaskInfo;
#pragma pack()

#pragma pack(1)
typedef struct {
    uint8_t  frame_head;
    uint8_t  device_id;
    uint8_t  cmd_type;
    uint8_t  switch_flag;
    uint8_t  reserved[11];
    uint8_t  frame_tail;
} CmdAcqCtrl;
#pragma pack()

#pragma pack(1)
typedef struct {
    uint8_t  frame_head;
    uint8_t  device_id;
    uint8_t  cmd_type;
    uint8_t  switch_flag;
    uint8_t  reserved[11];
    uint8_t  frame_tail;
} CmdUsbTransfer;
#pragma pack()

#pragma pack(1)
typedef struct {
    uint8_t  frame_head;
    uint8_t  device_id;
    uint8_t  cmd_type;
    uint8_t  control;
    uint8_t  reserved[11];
    uint8_t  frame_tail;
} CmdFileList;
#pragma pack()

#pragma pack(1)
typedef struct {
    uint8_t  frame_head;
    uint8_t  device_id;
    uint8_t  cmd_type;
    uint8_t  operation;
    char     task_id[11];
    uint8_t  file_index;
    uint8_t  file_type;
    uint8_t  calibration_type;
    uint8_t  reserved[13];
    uint8_t  frame_tail;
} CmdFileOp;
#pragma pack()

#pragma pack(1)
typedef struct {
    uint8_t  frame_head;
    uint8_t  device_id;
    uint8_t  cmd_type;
    uint8_t  reserved[12];
    uint8_t  frame_tail;
} CmdStatusQuery;
#pragma pack()

#pragma pack(1)
typedef struct {
    uint8_t  frame_head;
    uint8_t  device_id;
    uint8_t  cmd_type;
    uint8_t  result;
    uint8_t  reserved[11];
    uint8_t  frame_tail;
} AckCommon;
#pragma pack()

#pragma pack(1)
typedef struct {
    uint8_t  frame_head;
    uint8_t  device_id;
    uint8_t  cmd_type;
    uint8_t  result;
    uint8_t  acq_type;
    uint8_t  failure_type;
    uint8_t  reserved[9];
    uint8_t  frame_tail;
} AckAcqCtrl;
#pragma pack()

#pragma pack(1)
typedef struct {
    uint8_t  frame_head;
    uint8_t  device_id;
    uint8_t  cmd_type;
    uint8_t  result;
    char     task_id[11];
    uint8_t  file_count;
    uint8_t  file_index;
    uint8_t  file_type;
    uint8_t  file_size[8];       /* big-endian uint64 on UART */
    uint8_t  start_sector[8];    /* big-endian uint64 on UART */
    uint8_t  sector_count[4];    /* big-endian uint32 on UART */
    uint8_t  flag;
    uint8_t  calibration_type;
    uint8_t  frame_tail;
} AckFileList;
#pragma pack()

_Static_assert(sizeof(CmdTaskInfo) == 64u, "CmdTaskInfo must be 64 bytes");
_Static_assert(offsetof(CmdTaskInfo, azimuth_angle) == 34u, "CmdTaskInfo azimuth offset mismatch");
_Static_assert(offsetof(CmdTaskInfo, elevation_angle) == 36u, "CmdTaskInfo elevation offset mismatch");
_Static_assert(offsetof(CmdTaskInfo, envelope_duration) == 38u, "CmdTaskInfo duration offset mismatch");
_Static_assert(offsetof(CmdTaskInfo, period_setting) == 45u, "CmdTaskInfo period offset mismatch");
_Static_assert(offsetof(CmdTaskInfo, frame_tail) == 63u, "CmdTaskInfo tail offset mismatch");
_Static_assert(sizeof(CmdAcqCtrl) == 16u, "CmdAcqCtrl must be 16 bytes");
_Static_assert(sizeof(CmdUsbTransfer) == 16u, "CmdUsbTransfer must be 16 bytes");
_Static_assert(sizeof(CmdFileList) == 16u, "CmdFileList must be 16 bytes");
_Static_assert(sizeof(CmdFileOp) == 32u, "CmdFileOp must be 32 bytes");
_Static_assert(sizeof(CmdStatusQuery) == 16u, "CmdStatusQuery must be 16 bytes");
_Static_assert(sizeof(AckCommon) == 16u, "AckCommon must be 16 bytes");
_Static_assert(sizeof(AckAcqCtrl) == 16u, "AckAcqCtrl must be 16 bytes");
_Static_assert(sizeof(AckFileList) == 41u, "AckFileList must be 41 bytes");

typedef union {
    uint8_t        raw[64];
    CmdTaskInfo    task_info;
    CmdAcqCtrl     acq_ctrl;
    CmdUsbTransfer usb_transfer;
    CmdFileList    file_list_cmd;
    CmdFileOp      file_op;
    CmdStatusQuery status_query;
} CommandPkt;

typedef union {
    uint8_t        raw[64];
    AckCommon      common;
    AckAcqCtrl     acq_ack;
    AckFileList    file_list_ack;
} ResponsePkt;

void proto_init(void);
void proto_input(uint8_t byte);
void proto_set_send(int (*send_func)(uint8_t *buf, int len));
void proto_set_handler(void (*handler)(uint8_t *frame));

void proto_send_ack(uint8_t cmd, uint8_t result);
void proto_send_acq_ack(uint8_t result, uint8_t acq_type, uint8_t failure_type);
void proto_send_file_list(const char *task_id,
                          uint8_t result,
                          uint8_t file_count,
                          uint8_t file_index,
                          uint8_t file_type,
                          uint64_t file_size,
                          uint64_t start_sector,
                          uint32_t sector_count,
                          uint8_t flag,
                          uint8_t calibration_type);

#endif
