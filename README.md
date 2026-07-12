下面按“软件功能、实现方式、和硬件关系”总结当前 `src_real` 工程。

**整体定位**

当前软件是运行在板卡 Linux 上的集成控制程序，主程序是 `storage.elf` / `src_real_app` 这一类形态。它通过串口接收上位机协议命令，控制数据采集存盘、文件列表管理、NVMe 读盘、TCP 网络发送、状态查询和停止任务。

核心代码在：

- [system.c]：串口协议、任务调度、子进程管理、数据库记录、网络发送入口。
- [ccb_commands.c]：存盘、读盘、文件列表、metadata 操作。
- [ccb_hw.c]：DMA、AXIS switch、NVMe host core、DDR `/dev/mem` 映射。
- [ccb_tcp_transfer.c]：TCP 发送方向的 MM2S DMA 配置。
- [ccb_config.c]：ch0/ch1/ch2 硬件地址表。

**主要功能**

当前支持的串口命令大致是：

- `0x11` 数据任务发送：解析任务号、过境时间、文件模式等信息，并提前启动存盘 worker，准备 DMA/NVMe。
- `0x21` 采集控制：`0x11` 表示开始采集，`0xFF` 表示停止采集。停止时会给存盘 worker 发信号，让它停止收数、排空已完成 descriptor、写完 metadata。
- `0x41` 文件列表：读取当前文件列表，也支持同步 flash DB、清空文件列表等操作。
- `0x51` 文件操作：主要用于网络下载已有文件，也支持删除单个文件或删除全部。
- `0x61` 状态查询：探测 ch0/ch1/ch2 NVMe 状态，不只是看软件 worker 状态。
- `0x71` 停止网络传输：停止 TCP 网络发送 worker，并通知 NVMe 读盘流程退出。

软件内部把长时间任务拆成 worker 子进程：

- 主进程负责串口协议和 ACK。
- 存盘 worker 负责 S2MM DMA + NVMe write。
- 网络 worker 负责 NVMe read + TCP MM2S DMA。

这样做的好处是串口协议不会被长时间 DMA/NVMe 操作阻塞。

**存盘实现方式**

当前存盘路径是：

```text
数据源
  -> AXIS switch
  -> AXI DMA S2MM
  -> 数据通路 DDR ring
  -> NVMe host core
  -> SSD
```

软件不搬运数据内容。CPU 只做控制：

1. 配 AXIS switch，选择输入数据源。
2. 配 S2MM SG descriptor ring。
3. 等待 DMA descriptor 完成。
4. 对完成的 DDR slot 发 NVMe write。
5. NVMe 写完后清 descriptor status、推进 `TAILDESC`，再把 descriptor 交还 DMA。
6. 停止时继续 drain 已完成的 descriptor，最后记录文件 metadata。

对应逻辑在 [ccb_commands.c](ccb_commands.c:287) 附近：

- `dma_init_s2mm_ring()` 初始化 DMA ring。
- `dma_harvest_one()` 获取已完成的 slot。
- `nvme_rw(..., true, ...)` 把该 slot 写入 NVMe。
- `dma_requeue_one()` 把 slot 重新交还给 DMA。

AXI DMA 开启了 Scatter Gather，但软件没有开启 DMACR 的 Cyclic BD bit。
当前按 PG021 Tail Pointer Mode 管理 BD：`TAILDESC` 是硬件 pause pointer。
软件使用 `dma_hw_desc_count` 区分 HW-owned BD，并使用 `slot_busy[]` 保护已完成
DMA、尚未写完 NVMe 的 DDR slot。硬件重新获取 BD 前，软件必须先清除
`Completed`，否则 PG021 规定会触发 `SGIntErr`。

当前每个 DMA descriptor 默认 16MiB，和你的一帧大小一致：

```c
#define DMA_DESC_BYTES_CH0_DEFAULT (16u * 1024u * 1024u)
#define DMA_DESC_BYTES_CH2_DEFAULT (16u * 1024u * 1024u)
```

位置在 [ccb_types.h](ccb_types.h:21)。

**DDR 缓冲关系**

这是当前工程里比较关键的设计点。

你的硬件是：

- CPU 每个数据通路 DDR 只接入 64MiB。
- DMA 和 NVMe 使用硬件本地 DDR offset。
- ch0/ch1 数据通路 DDR 硬件容量是 2GiB，但当前软件强制只使用低 1GiB；
  即使 `SRC_REAL_STORAGE_RING_BYTES_CH0` / `SRC_REAL_STORAGE_RING_BYTES_CH1`
  请求更大值，也会 warning 并 clamp 回 1GiB。
- ch2 数据通路 DDR 是 512MiB。

所以软件里把两个概念分开：

```c
.ddr_cpu_size = CHANNEL_CPU_DDR_BYTES,
.dma_ring_bytes = CHANNEL0_DDR_BYTES,
.dma_ring_bytes_max = CHANNEL0_DDR_BYTES_MAX,
```

位置在 [ccb_config.c](ccb_config.c:30)。

含义是：

- `ddr_cpu_size`：CPU 可 mmap 的窗口，只有 64MiB。
- `dma_ring_bytes`：DMA/NVMe 默认使用的 DDR ring，ch0/ch1 1GiB，ch2 512MiB。
- `dma_ring_bytes_max`：硬件描述仍保留最大窗口；运行时 ch0/ch1 会 clamp 到默认
  1GiB，ch2 保持 512MiB。

因此存盘缓冲能力是：

```text
ch0/ch1 storage-write 默认: 1GiB / 8MiB = 128 slot
ch2: 512MiB / 16MiB = 32 帧
```

CPU 不会访问 64MiB 之外的 DDR，但 DMA/NVMe 可以用硬件地址访问完整 DDR。这样可以让 DDR 承担突发缓存作用：输入瞬时带宽高于 NVMe 写盘带宽时，数据先积累在 DDR ring 里，软件慢慢写盘。

前提是平均输入带宽不能长期超过 NVMe 写盘带宽。DDR 只能吸收突发，不能无限堆积。

slot 全部占用时会限频打印：

```text
storage_ring_warning channel=2 ring_full_count=1 no_free=1 buffered=536870912
busy_slots=32 total_slots=32 captured_bytes=...
```

此时 DMA 会通过 AXI-Stream `TREADY` 反压 AXIS FIFO 和 channel switch，但
当前 Aurora IP 配置为 `RX-only Simplex`、`flow_mode=None`、`c_nfc=false`，
并且 Aurora RX 接口生成参数为 `HAS_TREADY=0`。综合后的顶层中，channel
switch 对 Aurora 输入产生的 `tready` 接到未连接网络，不能反馈给 Aurora
发送端。因此 ring 满后 Aurora 仍可能继续输出，数据会在 DMA 前丢失。

当前硬件若要无损长时间采集，必须满足以下至少一项：发送端主动限速，使平均
输入带宽不超过 NVMe 写盘带宽；增加独立的接收端即将满流控信号；或把 Aurora
改为支持返回流控的双向方案并在两端实现 flow control。软件无法仅靠调整
`TAILDESC` 修复无反压链路上的数据丢失。

完成日志中的 `max_ddr_busy_slots` 和 `max_ddr_buffered_bytes` 用于判断本次
采集是否逼近或达到 DDR ring 容量。

**descriptor BRAM 校验**

DMA descriptor 存放在 BRAM，不在数据 DDR 里。当前代码做了容量校验。

在 [ccb_hw.c](ccb_hw.c:731)：

```c
desc_count = dma_ring_bytes / dma_desc_bytes;
desc_capacity = desc_cpu_size / sizeof(DmaSgDesc);

if (desc_count == 0u || desc_capacity == 0u || desc_count > desc_capacity) {
    return -1;
}
```

当前配置：

```text
DmaSgDesc = 64 bytes

ch0/ch1:
  ring = 1GiB runtime clamp
  storage-write descriptor size = 8MiB
  需要 descriptor = 128
  BRAM = 0x4000 = 16KiB
  BRAM 可放 = 256
  通过

ch2:
  ring = 512MiB
  descriptor size = 16MiB
  需要 descriptor = 32
  BRAM = 0x4000 = 16KiB
  BRAM 可放 = 256
  通过
```

如果以后改 descriptor 大小，代码也会重新校验，不满足就不会启动 DMA。

**NVMe 实现方式**

NVMe 不是走 Linux block 设备，而是直接访问自定义 NVMe host core 的寄存器。

主要逻辑在 [ccb_hw.c](ccb_hw.c:597) 和 [ccb_hw.c](ccb_hw.c:622)：

- `nvme_probe()`：读 link 状态、block size、max_lba。
- `nvme_rw()`：按 LBA、sector 数和 DDR 硬件地址发读写命令。
- 每个 NVMe 命令大小可通过 `SRC_REAL_NVME_CMD_KIB` 配置为 256、512、1024、
  2048 或 4096KiB；也支持按通道覆盖：`SRC_REAL_NVME_CMD_KIB_CH0`、
  `SRC_REAL_NVME_CMD_KIB_CH1`、`SRC_REAL_NVME_CMD_KIB_CH2`。
- 默认值按通道区分：ch0/ch1高速通道默认请求1024KiB，ch2低速通道默认请求
  256KiB。
- 实际 command size 不会超过代码4MiB上限和NVMe Host/SSD报告的`max_dts`；
  超限或非法配置会打印warning并回退到安全值。
- NVMe write支持`SRC_REAL_NVME_QD=1/2/4/8/16/32`，也支持按通道覆盖：
  `SRC_REAL_NVME_QD_CH0`、`SRC_REAL_NVME_QD_CH1`、`SRC_REAL_NVME_QD_CH2`。
  默认值按通道区分：ch0/ch1默认8，ch2默认4；单writer线程在一个DDR slot内
  提交多条command，并按CQ返回的CID匹配pending command。
- 软件QD安全上限为32。Host Core启用了tags（`MAX_NUM_TAGS=255`），但SQ深度
  没有通过软件寄存器直接暴露，因此提交时仍以`SQ_FIFO_FULL`作为运行时保护。
- `SRC_REAL_NVME_BUSY_POLL_US` 默认 0，`SRC_REAL_NVME_POLL_SLEEP_US` 默认 10；
  用于配置等待 `CMD_PENDING`、SQ full retry 和 CQ completion 时的 poll/backoff。
  也支持按通道覆盖：`SRC_REAL_NVME_BUSY_POLL_US_CH0/CH1/CH2`、
  `SRC_REAL_NVME_POLL_SLEEP_US_CH0/CH1/CH2`。
- `SRC_REAL_NVME_FEED_MODE` 默认 `legacy`，可设为 `tight` 启用 fixed-window
  tight feeder；也支持按通道覆盖：
  `SRC_REAL_NVME_FEED_MODE_CH0/CH1/CH2`。tight模式先提交到effective QD，
  之后每pop一个CQ completion就立即补提交一条replacement command。
- `SRC_REAL_NVME_CQ_POP_BATCH` 默认1，tight模式默认每次只pop一个completion
  后回到refill loop；可临时设为2或4做A/B。
- `SRC_REAL_NVME_DIAG_TIMING` 默认0。为0时tight inner loop不做per-command
  timing；设为1后输出submit/CQ拆分耗时，便于诊断但会增加软件开销。
- `SRC_REAL_NVME_SKIP_CONST_CTX` 默认0。设为1后tight submit会跳过不变的
  `CmdCTX0`写入；最后一条partial command sectors变化时仍会重新写CTX0。
- 默认 `SRC_REAL_NVME_CROSS_SLOT_QD=0` 时，NVMe write在单个DDR slot内先尽量
  提交到配置QD，再每处理一个CQ completion就补提交下一条命令。
- 设置 `SRC_REAL_NVME_CROSS_SLOT_QD=1` 后，单个writer线程会按batch跨多个DDR
  slot做全局QD调度；`SRC_REAL_NVME_CROSS_SLOT_BATCH` 默认8。slot只有在其所有
  NVMe command完成后才归还给DMA。
- 采集存盘默认仍走legacy兼容路径：主进程为每个channel fork独立worker，每个
  worker内部用采集producer和单个NVMe writer线程流水处理DDR slot。新增
  `storage_pipeline_config` 和 `storage_pipeline` 只用于观测，不会改变默认
  slot ownership。
- `SRC_REAL_PIPELINE_MODE=threaded`、`SRC_REAL_CH0_FAST_PIPELINE=1`、
  `SRC_REAL_CH1_FAST_PIPELINE=1`、`SRC_REAL_CH2_FAST_PIPELINE=1` 当前用于显式标记
  pipeline测试配置；legacy回退路径保持可用。
- `SRC_REAL_READY_QUEUE_DEPTH` 默认使用通道slot数量；harvest batch 已实际控制
  producer 连续收割循环，ch0/ch1/ch2 默认分别为32/32/4，原有
  `SRC_REAL_HARVEST_BATCH_MAX` 继续作为全局 fallback。
- 最终`storage_worker_done`会输出`nvme_max_dts_bytes`、`nvme_cmd_size_bytes`、
  `nvme_active_qd_max`、`nvme_active_qd_avg`、`ready_q_max`、`writer_idle_ms`、
  `writer_active_ms`和DMA harvest batch统计，用于判断实际并发深度、command size
  clamp、writer等待数据还是数据等待writer。
- worker内部的`storage_transfer_done`使用`final_ready_q_max`、
  `final_writer_idle_ms`等字段；主控进程会解析后在最终`storage_worker_done`
  里输出不带`final_`前缀的字段，避免和每秒统计字段混淆。
- NVMe read保持同步CQ completion，避免改变TCP下载等现有流程。

存盘时 NVMe PRP 地址使用的是 DDR 硬件地址：

```text
hw_addr = ddr_hw_base + slot_offset
```

不是 CPU 虚拟地址，也不是 CPU mmap 地址。

**网络发送实现方式**

网络下载路径是：

```text
SSD
  -> NVMe read
  -> DDR offset 0
  -> TCP MM2S DMA
  -> TCP/IP 硬件发送通路
```

当前网络发送按 16MiB 一帧发送，不再做 64MiB 拼包，也不做 16MiB 尾部补零。这样更贴近你说的“一帧一帧，每帧有 last 信号”。

相关逻辑在 [system.c](system.c:1624) 附近。

网络下载仍然只使用 DDR 的低 16MiB/64MiB 区域，因为它可能启用 CPU verify，CPU 只能看 64MiB。完整 DDR ring 主要用于收数存盘，不用于网络发送缓存。

**文件列表和 metadata**

当前有两套记录关系：

- 文件 metadata：记录每个文件的 task、index、size、start_lba、sector_count。
- SQLite/log DB：记录任务和文件列表，供串口文件列表、网络下载查找使用。

重要常量在 [ccb_types.h](D:/Project_sspen/NVMe/ccb_soft_linux/src_real/include/ccb_types.h:37)：

```c
MAX_FILES_TOTAL = 128
DATA_START_LBA = 1000
METADATA_START_LBA = 0
```

数据文件从 LBA 1000 后开始自动追加，避免覆盖 metadata 区。SQLite、网络下载和
串口文件列表使用64位文件大小。为兼容现有32字节裸机metadata格式，其中的
`file_size_bytes`在文件超过4GiB时写`UINT32_MAX`，完整范围由`sector_count`记录，
完整字节数由`filelist.db`保存。单文件软件上限由32位sector count决定，约2TiB。

**和硬件地址的关系**

当前 ch0/ch1/ch2 的硬件地址表在 [ccb_config.c](ccb_config.c:16)。

ch0：

```text
DMA base        0x41e00000
AXIS switch     0x44a10000
NVMe base       0x44a00000
desc CPU base   0x20000000
desc DMA base   0x10000000
DDR CPU base    0x10000000
DDR HW base     0x00000000
CPU DDR window  64MiB
DMA ring        1GiB runtime clamp
```

ch1：

```text
DMA base        0xa0060000
AXIS switch     0xa0070000
NVMe base       0xa0080000
desc CPU base   0x30000000
desc DMA base   0x10000000
DDR CPU base    0xd0000000
DDR HW base     0x00000000
CPU DDR window  64MiB
DMA ring        1GiB runtime clamp
```

ch2：

```text
DMA base        0x00030000
AXIS switch     0x00040000
NVMe base       0x00010000
desc CPU base   0x20004000
desc DMA base   0x10000000
DDR CPU base    0xc0000000
DDR HW base     0x00000000
CPU DDR window  64MiB
DMA ring        512MiB
```

这里有一个重要区别：

- descriptor BRAM 的 CPU 访问地址是 `desc_cpu_base`。
- AXI DMA 看到 descriptor 的地址是 `desc_dma_base`。
- 数据 DDR 的 CPU 地址是 `ddr_cpu_base`，但 CPU 只看 64MiB。
- 数据 DDR 的 DMA/NVMe 地址从 `ddr_hw_base = 0` 开始，覆盖完整 ring。

**当前稳定性策略**

现在软件偏保守：

- 不再做 NVMe 软复位。
- 不再做 GPIO 数据通路复位。
- NVMe write走CID匹配的有界异步QD，read仍走同步CQ completion。
- 0x71 停止网络时会通知 NVMe read 和 TCP transfer 停止。
- 存盘停止时不是立刻杀进程，而是让 worker drain 已完成 DMA descriptor，再写 metadata。
- CPU 不 mmap 完整数据 DDR，避免访问未接入地址导致 `Bus error`。

整体上，当前工程的设计目标是：让 DMA/NVMe 尽量直通数据，CPU 只做控制面；用完整数据 DDR 做存盘突发缓存；同时避免 CPU 访问硬件没有接入的 DDR 区域。



# src_real README

`src_real` is the integrated Linux program for the storage board.

It provides:

- UART1 command receive and ACK.
- Real unknown-length capture to SSD through AXI DMA S2MM and the NVMe host IP.
- Metadata, `filelist.db`, and `task_info` updates.
- Network send of an existing stored file through the hardware MM2S path.
- Debug output through the Linux console/UART0 path.

All runtime print and log messages in the program are ASCII English to avoid
garbled UART output.

## Build

Build on the board Linux SDK/sysroot environment:

```sh
cd src_real
make
```

Output:

```text
src_real_app
```

This program uses Linux headers, `/dev/mem`, `termios`, and `sqlite3`, so a
normal Windows MinGW build is not enough.

## Default Paths

Command UART:

```text
/dev/ttyUL1
```

Debug output:

```text
/dev/console
```

Databases in the runtime directory:

```text
logs.db
filelist.db
```

Persistent database and NVMe metadata copies on the SPI1 user flash mount:

```text
/mnt/spi1/filelist.db
/mnt/spi1/meta_ch0.bin
/mnt/spi1/meta_ch2.bin
```

On top-level `storage.elf` startup, standalone `storage-write`, standalone
`network-send`, and `ddr-pattern-store`, the program restores this flash copy
to the runtime `filelist.db` and restores the channel metadata files before
opening SQLite. Internal worker subprocesses do not restore from flash, so an
active daemon task cannot overwrite its runtime state while it is forking
workers. Startup fails if the configured flash parent directory is not a real
mount point; this prevents accidental writes into the root filesystem.

The flash copies are updated together by `0x41 control=0x22` and by successful
`ddr-pattern-store`. Keeping metadata with the database is required so the next
automatic SSD LBA allocation cannot reuse an existing file range after reboot.

The PetaLinux init script auto-detects the MTD partition named `spi1-user` and
mounts it as JFFS2 at `/mnt/spi1` before starting `storage.elf`. A new or erased
flash must be formatted once with `flash_eraseall -j /dev/mtdN`, where `mtdN` is
the `spi1-user` entry from `/proc/mtd`. The init script never formats flash
automatically.

Override the flash database path when your board mounts SPI1 elsewhere:

```sh
export SRC_REAL_FLASH_FILELIST_DB_PATH=/mnt/your_spi1_mount/filelist.db
```

Metadata directory:

```text
/run/ccb_nvme_process_test
```

Useful overrides:

```sh
export UART_DEV_PATH=/dev/ttyUL1
export CCB_PROCESS_META_DIR=/run/ccb_nvme_process_test
```

## Hardware Address Map

The Linux program uses the address table in `src/ccb_config.c`. The values
below are the current software view checked against the FPGA/PetaLinux address
map; do not use an old exported `system.dts` as the only authority.

Supported storage channels:

```text
ch0 HIGH_I
  NVMe host      0x44a00000
  AXI DMA        0x41e00000
  AXIS switch    0x44a10000
  desc CPU       0x20000000
  desc DMA       0x10000000
  desc size      0x4000
  storage-write DMA desc bytes 0x00800000
  storage-write DMA desc count 128 default
  DDR CPU        0x10000000
  DDR size       0x04000000
  DDR ring       0x40000000 default
  DDR ring max   0x80000000
  DDR HW offset  0x00000000

ch1 HIGH_Q
  NVMe host      0xa0080000
  AXI DMA        0xa0060000
  AXIS switch    0xa0070000
  desc CPU       0x30000000
  desc DMA       0x10000000
  desc size      0x4000
  storage-write DMA desc bytes 0x00800000
  storage-write DMA desc count 128 default
  DDR CPU        0xd0000000
  DDR size       0x04000000
  DDR ring       0x40000000 default
  DDR ring max   0x80000000
  DDR HW offset  0x00000000

ch2 LOW_SPEED/CALIB
  NVMe host      0x00010000
  AXI DMA        0x00030000
  AXIS switch    0x00040000
  desc CPU       0x20004000
  desc DMA       0x10000000
  desc size      0x4000
  DMA desc bytes 0x01000000
  DMA desc count 32
  DDR CPU        0xc0000000
  DDR size       0x04000000
  DDR ring       0x20000000
  DDR HW offset  0x00000000
```

The old bare-metal test code has different descriptor and DDR CPU addresses in
some places. For `src_real`, use the device-tree values above.

The descriptor CPU address comes from `system.dts`. The descriptor DMA-view
address stays at the old hardware value `0x10000000`.

The default S2MM descriptor payload size for `storage-write` is 8 MiB on
ch0/ch1 and 16 MiB on ch2. The CPU maps only a 64 MiB window. The DMA/NVMe
ring is ch0/ch1 1 GiB and ch2 512 MiB at runtime; larger ch0/ch1 environment
requests are rejected with `storage_ring_config_error` rather than silently
running with a smaller ring.

Expected ch0/ch1 1 GiB startup line:

```text
storage_pipeline_config channel=0 ... requested_ring_bytes=1073741824 effective_ring_bytes=1073741824 slot_bytes=8388608 total_slots=128 ring_clamp_reason=none hw_ring_base=0x00000000 hw_ring_end=0x40000000 hw_ddr_span_bytes=1073741824 dma_bd_count=128 ...
```

If `requested_ring_bytes` and `effective_ring_bytes` differ, or if
`total_slots` would exceed descriptor BRAM capacity, the worker prints
`storage_ring_config_error` and fails prestart instead of continuing.

The DMA/NVMe data-buffer address written into S2MM descriptors and NVMe PRP is
still channel-local offset `0x00000000`, matching the bare-metal storage flow.

## Serial Behavior

Endian rule:

- The upper-computer program sends and parses UART protocol multi-byte fields as
  big-endian. This build follows that behavior for command parsing and response
  packets, even if older document text says little-endian.
- Single-byte result fields such as `0x11`, `0x21`, `0x51`, `0x61`, and `0x71`
  ACK results have no endian conversion.
- Current multi-byte UART fields are:
  `0x11.azimuth_angle`, `0x11.elevation_angle`,
  `0x11.envelope_duration`, `0x11.period_setting`,
  `0x41.file_size`, `0x41.start_sector`, and `0x41.sector_count`.
  All of them are encoded or decoded explicitly as big-endian.

`0x11` task info:

- Parses task id, overpass time, file mode, and calibration type.
- Parses 16-bit task parameters as big-endian, including azimuth angle,
  elevation angle, envelope duration, and period setting.
- Writes or updates `task_info`.
- Starts storage worker(s) immediately, before `0x21`.
- Does not need a known data size. Workers keep receiving until `0x21` stop.
- For `0xAA`, launches ch2, ch0, and ch1 workers first, then waits until all
  print `storage_ready`; `0x11` returns success only after all planned channels
  are ready.

Supported file modes:

```text
0x11  calib only       -> ch2, file type 3
0x22  low speed only   -> ch2, file type 0
0x33  high only        -> ch0 file type 1 and ch1 file type 2
0xAA  low + high       -> ch2 file type 0, ch0 file type 1, and ch1 file type 2
```

ch1 is the high-Q storage path and is included in high-only and all-channel
capture modes.

`0x21` acquisition control:

- `SWITCH_ON` returns success immediately. It does not check or wait for the
  storage worker state.
- `SWITCH_OFF` sends a graceful stop request to storage worker(s), then waits
  for all planned workers to exit before sending the 0x21 ACK.
- After stop, each worker drains completed DMA descriptors, writes the final SSD
  chunks, writes metadata, and inserts `filelist.db`.
- In UART mode, the parent process keeps `task_info.task_status` as `running`
  until all planned storage workers have exited, then marks the task
  `completed`; if any worker fails, the parent requests the other workers to
  stop and marks the task `failed`.
- A successful `SWITCH_OFF` ACK means metadata and `filelist.db` updates have
  completed. Failure means at least one storage worker failed or stop waiting
  timed out.
- Does not start storage.
- File-list records are created only after the worker exits successfully.

`0x41` file list:

- `byte 3 = 0x11`: reads `v_task_file_list` from `filelist.db`.
- `byte 3 = 0x22`: synchronizes the current `filelist.db` and channel NVMe
  metadata files to the SPI1 user flash.
- `byte 3 = 0xFF`: clears file-list records and supported-channel metadata.
- At top-level startup, daemon mode and standalone commands restore the SPI1
  flash copies to the runtime database and metadata directory before use.
- `0x22` sync success uses the same empty 41-byte `0x41` success response as
  clear success: `result=0x11`, `flag=0xFF`.
- Returns the 41-byte network-version response from the docx 2.4 section.
- Database fields used by `0x41` must match the response fields below.

```text
0x41 response, 41 bytes
  byte 0      frame head       0x55
  byte 1      device id        0xCC
  byte 2      command          0x41
  byte 3      result           0x11 success, 0xFF failed, 0x00 invalid
  byte 4-14   task_id          11 ASCII bytes
  byte 15     file_count       total files for this task
  byte 16     file_index       protocol index, starts from 0
  byte 17     file_type        0 low, 1 I, 2 Q, 3 calibration
  byte 18-25  file_size        big-endian uint64, bytes actually stored
  byte 26-33  start_sector     big-endian uint64
  byte 34-37  sector_count     big-endian uint32
  byte 38     flag             0x11 continue, 0xFF end
  byte 39     calibration_type  docx calibration code
  byte 40     frame tail       0xAA
```

Database mapping:

```text
task_info.task_id              -> response task_id
task_info.total_files          -> response file_count
file_list.file_index           -> database index, starts from 1
file_list.file_index - 1       -> response file_index
file_list.proto_file_type_code -> response file_type
file_list.file_size            -> response file_size
file_list.start_sector         -> response start_sector
file_list.sector_count         -> response sector_count
file_list.calibration_type     -> response calibration_type
```

`0x51` file operation:

```text
byte 4 = 0x11  start network send of an existing file
byte 4 = 0x22  delete one file by task_id, file_index, file_type, calibration_type
byte 4 = 0xFF  delete all file records and clear supported-channel metadata
```

`0x61` disk status:

```text
0x11  ch0/ch1/ch2 NVMe disks are detected and capabilities are valid
0xFF  at least one ch0/ch1/ch2 NVMe disk detection failed
```

The check reuses the standalone self-test style logic: open the channel runtime,
run `nvme_probe()`, then validate `block_size`, `max_dts_bytes`, and `max_lba`.
ch0, ch1, and ch2 are checked.

Optional timeout override:

```sh
export SRC_REAL_STATUS_TIMEOUT_US=5000000
```

`0x71` stop network send:

- Sends SIGTERM to the network worker.
- The worker requests MM2S stop and resets the MM2S DMA.

## Standalone Storage Tests

Continuous storage, unknown size:

```sh
./src_real_app storage-write \
  --channel 2 \
  --task-no R2509100001 \
  --file-index 1 \
  --ssd-lba auto \
  --source transfer \
  --proto-file-type 0
```

Stop it with `Ctrl+C` for standalone testing. The signal is handled as a
graceful stop request, so the program writes metadata and `filelist.db` before
exiting. In daemon mode, `0x21 SWITCH_OFF` sends the same stop request.

Fixed-size storage is still useful for bench tests:

Low-speed ch2:

```sh
./src_real_app storage-write \
  --channel 2 \
  --size 1048576 \
  --task-no R2509100001 \
  --file-index 1 \
  --ssd-lba auto \
  --source transfer \
  --proto-file-type 0
```

Calibration ch2:

```sh
./src_real_app storage-write \
  --channel 2 \
  --size 1048576 \
  --task-no R2509100002 \
  --file-index 1 \
  --ssd-lba auto \
  --source transfer \
  --proto-file-type 3 \
  --calibration-type 1
```

High-I ch0:

```sh
./src_real_app storage-write \
  --channel 0 \
  --size 1048576 \
  --task-no R2509100003 \
  --file-index 1 \
  --ssd-lba auto \
  --source transfer \
  --proto-file-type 1
```

Dry-run:

```sh
./src_real_app --dry-run storage-write \
  --channel 0 \
  --size 1048576 \
  --task-no R2509100004 \
  --file-index 1 \
  --ssd-lba auto \
  --source transfer \
  --proto-file-type 1
```

On success, standalone storage performs:

- Real DMA capture and NVMe write.
- Metadata update.
- `filelist.db.file_list` insert.
- `task_info` create/update.
- `total_files` and task status update.

For continuous storage, `file_size` and `sector_count` are the actual amount
stored when the stop request is handled.

## DDR Pattern Store Test

Generate a 32MiB test pattern in ch2 DDR and store it to SSD without waiting
for external DMA input:

```sh
./src_real_app ddr-pattern-store \
  --task-no R2509100100 \
  --file-index 1 \
  --proto-file-type 3
```

Optional size override:

```sh
./src_real_app ddr-pattern-store \
  --size 33554432 \
  --task-no R2509100100 \
  --file-index 1 \
  --proto-file-type 3 \
  --calibration-type 1
```

This mode writes CPU-view DDR `0xc0000000`, which is ch2 DMA/NVMe view
`0x00000000`. Each 32-bit word is:

```text
word[n] = ((n & 0xffff) << 16) | (n & 0xffff)
```

The command writes SSD metadata and `filelist.db`, then syncs both to flash
when possible. It does not send TCP itself; use the existing serial file
download command or standalone `network-send` afterwards.

## Standalone Network Send

Send an existing file by database task id and 1-based file index:

```sh
./src_real_app network-send \
  --task-no R2509100001 \
  --file-index 1 \
  --proto-file-type 0
```

Dry-run:

```sh
./src_real_app --dry-run network-send \
  --task-no R2509100001 \
  --file-index 1 \
  --proto-file-type 0
```

Network send flow:

1. Query the current working directory `./filelist.db`.
2. Read SSD data to channel DDR at hardware offset `0x00000000`.
3. Clear the final padded DDR tail if the last network descriptor is not full.
4. Select the TCP switch input and MM2S DMA from the protocol file type.
5. Send full MM2S descriptors through the TCP hardware stream.

Network MM2S route:

```text
ch0 HIGH_I file
  TCP switch base    0xa0040000
  TCP switch input   0
  TCP MM2S DMA       0x41e00000
  desc CPU base      0x20000000
  desc DMA base      0x10000000
  DDR DMA/NVMe addr  0x00000000
  TCP desc bytes     16 MiB

ch1 HIGH_Q file
  TCP switch base    0xa0040000
  TCP switch input   1
  TCP MM2S DMA       0xa0060000
  desc CPU base      0x30000000
  desc DMA base      0x10000000
  DDR DMA/NVMe addr  0x00000000
  TCP desc bytes     16 MiB

ch2 LOW_SPEED/CALIB file
  TCP switch base    0xa0040000
  TCP switch input   2
  TCP MM2S DMA       0x00030000
  desc CPU base      0x20004000
  desc DMA base      0x10000000
  DDR DMA/NVMe addr  0x00000000
  TCP desc bytes     16 MiB
```

`FILE_TYPE_LOW` (`0x00`) and `FILE_TYPE_CALIB` (`0x03`) both route to ch2.
They use the same NVMe read-back address and the same TCP MM2S path.
`FILE_TYPE_Q` (`0x02`) routes to ch1.

## Standalone SSD PCIe Reset

The SSD PCIe reset helper is a small `/dev/mem` tool under `tools/`.

Source and build:

```sh
cd tools
make
```

Runtime commands:

```sh
# Drive pcie_rstn high and release SSD reset.
./ssd_pcie_reset release

# Read AXI GPIO DATA/TRI status without changing direction.
./ssd_pcie_reset status

# Assert then release. Defaults: hold 100 ms, settle 1000 ms.
./ssd_pcie_reset pulse

# Custom pulse: hold 200 ms, settle 3000 ms.
./ssd_pcie_reset pulse 200 3000

# Drive pcie_rstn low and keep SSD in reset.
./ssd_pcie_reset assert
```

Safe reset sequence on the board:

```sh
# 1. Stop the storage service first. Do not reset SSD while NVMe is in use.
/etc/init.d/storage stop

# 2. Make sure the reset output is released before checking status.
/usr/bin/ssd_rst.elf release
/usr/bin/ssd_rst.elf status

# 3. If a reset pulse is needed, start with a short pulse.
/usr/bin/ssd_rst.elf --force pulse 10 0

# 4. Wait for the PCIe/NVMe side to recover, then start the service again.
sleep 2
/etc/init.d/storage start
```

Rules for using SSD reset:

- `pcie_rstn` is active low: `0` holds SSD in reset, `1` releases reset.
- Do not run `pulse` or `assert` while `storage.elf` is doing NVMe probe,
  read, write, TCP send, or capture stop. Resetting the SSD during an active
  NVMe transaction can leave the NVMe host/IP or AXI path waiting indefinitely.
- Newer `ssd_rst.elf` builds refuse `assert` and `pulse` when
  `/var/run/storage.elf.pid` points to a running service. Use `--force` only
  after stopping the service or when intentionally recovering a stuck board.
- If `assert` or `pulse` makes the board unresponsive, verify that the FPGA
  signal only resets the SSD device. It must not reset the NVMe host core, AXI
  interconnect, MicroBlaze, or shared clocks.
- For normal boot, leave SSD reset released. Use `pulse` only as a recovery or
  controlled re-enumeration action.

Hardware mapping:

```text
pcie_rstn AXI GPIO base  0x40010000
AXI GPIO DATA offset     0x00
AXI GPIO TRI offset      0x04
bit                      0
active level             low
```

In the PetaLinux `myapp` recipe this helper can be packaged as
`/usr/bin/ssd_rst.elf`.

## Debug Environment Variables

```sh
export SRC_REAL_DEBUG=write,nvme
export SRC_REAL_DEBUG_VERBOSE=1
export SRC_REAL_DEBUG_HEX=1
export SRC_REAL_STORAGE_EVENTS=1

export SRC_REAL_STORAGE_DRY_RUN=1
export SRC_REAL_STORAGE_SKIP_LINK_CHECK=1
export SRC_REAL_STORAGE_TIMEOUT_US=5000000
export SRC_REAL_STORAGE_PREP_TIMEOUT_US=5000000
export SRC_REAL_STORAGE_STOP_TIMEOUT_MS=120000
export SRC_REAL_STORAGE_IDLE_NOTICE_MS=5000
export SRC_REAL_LOG_LEVEL=summary
export SRC_REAL_PIPELINE_STATS_SEC=5
export SRC_REAL_ENABLE_STORAGE_STATS=0
export SRC_REAL_SLOT_WRITE_PERF=0
export SRC_REAL_SLOT_WRITE_PERF_SAMPLE=0
export SRC_REAL_PRINT_ZERO_STATS=0
export SRC_REAL_ECHO_WORKER_OUTPUT=0
export SRC_REAL_STORAGE_POLL_SLEEP_US=100
export SRC_REAL_STORAGE_HIGH_WATERMARK_POLL_US=10
export SRC_REAL_STORAGE_CRITICAL_WATERMARK_POLL_US=0
export SRC_REAL_NVME_CMD_KIB=256
export SRC_REAL_NVME_QD=16
export SRC_REAL_NVME_BUSY_POLL_US=0
export SRC_REAL_NVME_POLL_SLEEP_US=10
export SRC_REAL_NVME_CROSS_SLOT_QD=0
export SRC_REAL_NVME_CROSS_SLOT_BATCH=8
export SRC_REAL_PCIE_BRIDGE_BASE_CH0=0xb0000000
export SRC_REAL_PCIE_BRIDGE_BASE_CH1=0xd0000000
# export SRC_REAL_PCIE_BRIDGE_BASE_CH2=0x...

export SRC_REAL_NETWORK_DRY_RUN=1
export SRC_REAL_NETWORK_SKIP_LINK_CHECK=1
export SRC_REAL_NETWORK_TIMEOUT_US=5000000
export SRC_REAL_NETWORK_TASK_TIMEOUT_SECONDS=0
export SRC_REAL_NETWORK_LIMIT_MB_S=0
export SRC_REAL_NETWORK_VERIFY_DDR_READ=1
```

Regular `[DBG]` and UART RX/TX banner output is off by default. Enable only the
categories needed for the current test:

```text
SRC_REAL_DEBUG=write,nvme
  Enables selected `[DBG][...]` categories. Common values are `proto`,
  `write`, `nvme`, `dma`, `storage`, `net`, `tcp`, `pattern`, `disk`, `flash`.
  Use comma-separated values, or `all` for every normal debug category.

SRC_REAL_DEBUG_VERBOSE=1
  Enables detailed internal progress logs, including DMA/NVMe/TCP register
  details, NVMe submit/complete traces, worker fork/ready messages, and per
  network-chunk progress.

SRC_REAL_DEBUG_HEX=1
  Enables full UART RX/TX frame hex dumps.

SRC_REAL_STORAGE_EVENTS=1
  Enables high-frequency storage watermarks and ring-full event logs. Keep it
  off during throughput tests unless those events are being diagnosed.

SRC_REAL_LOG_LEVEL=quiet|summary|debug|trace
  Controls storage stdout/log verbosity. The default is `quiet`: startup
  summary, warnings/errors, stop/drain, `storage_transfer_done`, and
  `storage_worker_done`. `summary` enables periodic `storage_pipeline`.
  `debug` allows sampled slot diagnostics. `trace` allows detailed diagnostic
  output and can materially reduce write throughput.

SRC_REAL_PIPELINE_STATS_SEC=5
  Period for `storage_pipeline` when `SRC_REAL_LOG_LEVEL=summary` or higher.
  `0` disables periodic pipeline stats. Repeated zero-value periods are not
  printed by default.

SRC_REAL_ENABLE_STORAGE_STATS=1
  Re-enables the legacy `storage_stats` line. The default is `0`; throughput
  tests should normally use only `storage_pipeline`.

SRC_REAL_SLOT_WRITE_PERF=1
  Enables sampled per-slot NVMe write diagnostics for `storage-write`.
  Diagnostics are written to `/tmp/storage_slot_perf.log` or
  `SRC_REAL_SLOT_WRITE_PERF_LOG`; they are not echoed to UART/stdout.

SRC_REAL_SLOT_WRITE_PERF_SAMPLE=N
  Controls sampling. `N=16` logs every sixteenth slot. `N=1` logs every slot
  only when `SRC_REAL_LOG_LEVEL=trace`. Default `0` disables slot diagnostics.

  Each sampled slot writes compact lines:

  - `storage_nvme_param_compare`: confirms `storage-write` and raw store use
    the same runtime NVMe parameters such as cmd size, QD, feed mode,
    busy/poll sleep, and max DTS.
  - `slot_write_perf`: slot bytes, write ms, MiB/s, command count, active QD.
  - `nvme_perf_calc`: Host Core PERF_CALC result for that slot.
  - `slot_sw_timing`: software submit/CQ wait/CQ pop delta for that slot.

SRC_REAL_ECHO_WORKER_OUTPUT=1
  Echoes all worker stdout/stderr to the parent stdout. Default `0` still
  drains worker pipes but only echoes warning/error/final lines, avoiding pipe
  backpressure and UART overhead.

PCIe bridge link status:
  The helper reads the Xilinx/AMD AXI Bridge for PCIe Gen3 PHY Status/Control
  register at `pcie_bridge_base + 0x144` and prints one line per
  channel/reason during runtime open and after NVMe probe:

  ```text
  pcie_link_status channel=0 reason=startup bridge_base=0xb0000000 phy_reg=0x0000.... link_up=1 speed=Gen3_8GT width=x4 ltssm=0x..
  ```

  The bridge control base must be the CPU-visible AXI Bridge for PCIe Gen3
  AXI-Lite control base, not the NVMe Host Core base such as `0x44a00000`.
  The current defaults are ch0=`0xb0000000`, ch1=`0xd0000000`, and ch2 unset.
  Override them per channel with `SRC_REAL_PCIE_BRIDGE_BASE_CH0`,
  `SRC_REAL_PCIE_BRIDGE_BASE_CH1`, or `SRC_REAL_PCIE_BRIDGE_BASE_CH2`.
  If no base is available the line contains `error=no_bridge_base`; if the
  optional MMIO read cannot be performed it contains `error=read_failed`.

  Speed decode: `down`, `Gen1_2p5GT`, `Gen2_5GT`, or `Gen3_8GT`.
  Width decode: `x1`, `x2`, `x4`, `x8`, or `x16`.

Recommended throughput-test configuration:

```sh
export SRC_REAL_LOG_LEVEL=summary
export SRC_REAL_PIPELINE_STATS_SEC=5
export SRC_REAL_ENABLE_STORAGE_STATS=0
export SRC_REAL_SLOT_WRITE_PERF=0
export SRC_REAL_ECHO_WORKER_OUTPUT=0
export SRC_REAL_DMA_IDLE_DONE_MS=5000
export SRC_REAL_WRITER_BACKLOG_MODE=1
```

Minimum-disturbance configuration:

```sh
export SRC_REAL_LOG_LEVEL=quiet
export SRC_REAL_PIPELINE_STATS_SEC=0
export SRC_REAL_ENABLE_STORAGE_STATS=0
export SRC_REAL_SLOT_WRITE_PERF=0
export SRC_REAL_ECHO_WORKER_OUTPUT=0
export SRC_REAL_DMA_IDLE_DONE_MS=5000
```

Single-slot diagnosis:

```sh
export SRC_REAL_LOG_LEVEL=trace
export SRC_REAL_SLOT_WRITE_PERF=1
export SRC_REAL_SLOT_WRITE_PERF_SAMPLE=16
export SRC_REAL_ECHO_WORKER_OUTPUT=1
```

Do not use `trace` or per-slot logs as throughput-test evidence; they add
substantial stdout/file I/O and timing overhead.

SRC_REAL_DMA_IDLE_DONE_MS=5000
  In continuous `storage-write`, after at least one DMA completed descriptor
  has been received, 5 seconds without new DMA completions is treated as input
  end. The worker prints `storage_dma_idle_done`, stops requeueing new DMA BDs,
  drains ready/NVMe-busy slots, finalizes metadata/db, and prints
  `storage_transfer_done` or `storage_transfer_failed`. Set `0` to disable.

SRC_REAL_CH0_WRITER_RT_PRIO / SRC_REAL_CH1_WRITER_RT_PRIO / SRC_REAL_CH2_WRITER_RT_PRIO
  Attempts to set the storage writer thread to `SCHED_FIFO` with the requested
  priority. Failure prints one warning and does not abort the task.

SRC_REAL_WRITER_BACKLOG_MODE=1
  Enables backlog-aware writer statistics and keeps the writer in drain mode
  while ready queue has backlog. It does not change NVMe command size, QD, or
  DDR slot ownership. Set `0` for legacy-compatible behavior.

`ring_full_count > 0` means the real DMA BD writable count reached zero. The
worker can still drain data already in DDR, but final `integrity_ok` will be
0 with `integrity_risk=dma_bd_exhausted_no_upstream_backpressure`; stop responses
must fail even when `data_persisted=1` reports that partial data reached SSD.

Performance durations use `CLOCK_MONOTONIC_RAW` with a `CLOCK_MONOTONIC`
fallback. `nvme_active_mib_s` covers writer-active NVMe time, while
`nvme_wall_mib_s` covers the first successful submit through the last
completion. Summary mode emits at most one `storage_pipeline` line per channel
and window, using `window_ms`, `rx_mib_s`, and `nvme_complete_mib_s`.

Stop command handling:
  If workers are still running, `0x21=0xFF` requests stop, waits for drain and
  finalization, then prints `serial_cmd_result`. If a worker already completed
  through DMA idle auto-done, stop returns the cached completed worker state
  after `check_task()` updates it.

Cross-slot QD remains a planned optimization for ch0/ch1. Proposed per-channel
env names are `SRC_REAL_NVME_CROSS_SLOT_QD_CH0` and
`SRC_REAL_NVME_CROSS_SLOT_QD_CH1`; they are not required for the current
single-slot legacy writer path. Slot ownership must still obey “all NVMe
completions before `dma_requeue_one()`”.

Successful protocol storage tasks always print a final `storage_worker_done`
line. The line includes the final NVMe command count, effective queue depth,
maximum and average active queue depth, DDR ring-full count, and maximum DDR
ring occupancy. It also includes ready-queue and writer idle/active counters.
Use `ring_full_count > 0` or `max_ddr_busy_slots` equal to the channel slot
count as a data-integrity warning when the upstream stream has no backpressure.

SRC_REAL_NETWORK_TASK_TIMEOUT_SECONDS=0
  Disables the parent network worker total timeout. Non-zero values restore a
  whole-task timeout in seconds.

SRC_REAL_NETWORK_LIMIT_MB_S=0
  Disables TCP transfer throttling. Non-zero values add average per-chunk
  throttling; for example, 10 limits the transfer to roughly 10 MiB/s.

SRC_REAL_NETWORK_VERIFY_DDR_READ=1
  Before each network NVMe read, fills the TCP DDR window with 0xA5 and then
  samples it after the read. If every sampled byte is still 0xA5, the program
  stops before TCP send and reports that the SSD read did not overwrite DDR.

When `SRC_REAL_DEBUG` includes `write`, `dma`, or `storage`, selected worker
debug output can still be forwarded. For complete worker stdout/stderr echo,
set `SRC_REAL_ECHO_WORKER_OUTPUT=1`.

Single-channel real-capture storage test example:

```sh
rm -f /tmp/ch0_slot_perf.log /tmp/ch0_capture.log
sudo env \
  DEBUG_CONSOLE_PATH=/proc/self/fd/1 \
  SRC_REAL_DEBUG=MAIN,PROTO,CAPTURE,STORAGE \
  SRC_REAL_LOG_LEVEL=summary \
  SRC_REAL_PIPELINE_STATS_SEC=5 \
  SRC_REAL_ENABLE_STORAGE_STATS=0 \
  SRC_REAL_STORAGE_EVENTS=0 \
  SRC_REAL_SLOT_WRITE_PERF=1 \
  SRC_REAL_SLOT_WRITE_PERF_LOG=/tmp/ch0_slot_perf.log \
  SRC_REAL_SLOT_WRITE_PERF_SAMPLE=16 \
  SRC_REAL_ECHO_WORKER_OUTPUT=0 \
  SRC_REAL_STORAGE_RING_BYTES_CH0=1073741824 \
  SRC_REAL_NVME_QD_CH0=8 \
  SRC_REAL_NVME_CMD_KIB_CH0=256 \
  SRC_REAL_NVME_BUSY_POLL_US=50 \
  SRC_REAL_NVME_POLL_SLEEP_US=1 \
  /tmp/storage.elf 2>&1 | tee /tmp/ch0_capture.log
```

Then send `0x11` with `task_file_mode=0x33` for high-only or use your existing
single-channel source mode if the upstream only drives ch0, send `0x21=0x11`
to start, and `0x21=0xFF` to stop. Inspect:

```sh
grep -E "storage_worker_done|ring_full|CQ timeout|write failed" /tmp/ch0_capture.log
grep -E "storage_nvme_param_compare|slot_write_perf|nvme_perf_calc|slot_sw_timing" /tmp/ch0_slot_perf.log
```
```

`CCB_DEBUG=...`, `CCB_DEBUG_VERBOSE=1` and `CCB_DEBUG_HEX=1` are also accepted
aliases.

## Database Settings

The file-list and log databases use SQLite's default journal and sync behavior.
This is slower than the earlier WAL/NORMAL tuning, but it is more conservative
for board debugging.

Runtime override:

```sh
export SRC_REAL_SQLITE_BUSY_TIMEOUT_MS=10000
```

## Debug Output

Debug output goes to the Linux console/UART0 path. UART commands still use
UART1.

Default output keeps final result lines and explicit structured summaries, but
suppresses normal `[DBG]` progress logs. To see protocol and storage progress:

```sh
export SRC_REAL_DEBUG=proto,write,nvme
```

Example enabled output:

```text
[DBG][WRITE] dma_init_s2mm_ring failed ch=2 desc_bytes=16777216
[DBG][NVME] CQ timeout ch=2 issued=128 completed=59 inflight=69 submitted_sectors=65536/65536 status=0x00000005 waited_us=5000000
[DBG][TCP] completion timeout dma=0x00030000 dmasr=0x00000000 desc=0 completed=0/4 desc_status=0x00000000
```

Full UART hex dumps are disabled by default. Enable them only when checking
field alignment or frame length:

```sh
export SRC_REAL_DEBUG_HEX=1
```

Then the parser prints accepted frames and tail-error frames in hex:

```text
[RXHEX] frame_ok len=64: 55 CC 11 ...
[RXHEX] frame_tail_error expected_len=64 actual_tail=00
[RXHEX] frame_tail_error_data len=64: 55 CC 11 ...
```

For `0x11`, the parser expects exactly 64 bytes and byte 63 must be `0xAA`.
The hex dump is intended for checking field alignment, reserved byte count, and
whether the host is sending raw hex bytes instead of ASCII text.

Full verbose debug is disabled by default. Enable it only for detailed board
debugging:

```sh
export SRC_REAL_DEBUG_VERBOSE=1
```

Verbose mode adds high-frequency details such as NVMe command submit/complete,
DMA reset registers, TCP route/register details, storage worker fork/ready
messages, and per-network-chunk progress. It can produce a lot of UART0 output,
so keep it off during normal capture tests.

## Troubleshooting Order

1. Check UART0/Linux console for `[DBG][MAIN] protocol ready`.
2. Send `0x11` and check `[RX] frame_ok cmd=0x11`, then check
   `[DBG][PROTO] 0x11 ACK success`.
3. Send `0x21 SWITCH_OFF`, then wait for
   `[DBG][PROTO] 0x21 stop ACK result=0x11`.
4. Send `0x61`; ch0/ch1/ch2 disk detection success should return `0x11`, failure returns `0xFF`.
5. Send `0x41` and verify `file_size`, `start_sector`, `sector_count`, and `calibration_type`.
6. Use `network-send --dry-run` to verify file lookup before hardware send.
7. For non-dry-run, verify `/dev/mem` permission, NVMe link, DMA status, and TCP downstream `tready`.

## Current Limits

- Supported storage channels are ch0, ch1, and ch2.
- Network send and storage are not allowed to run concurrently.
- Network send transmits raw file data only; it does not implement protocol chapter 3 packet headers, tails, or MD5.
- Serial protocol file indexes are 0-based. Database and metadata file indexes are 1-based.
- Metadata keeps the legacy 32-bit file-size field for format compatibility. Files larger than
  4GiB use `UINT32_MAX` in that field, while SQLite and the UART protocol retain the exact
  64-bit byte count; the 32-bit sector count limits one file to roughly 2TiB.

## DMA-first receive pipeline

Storage uses six explicit slot ownership states: `DMA_WRITABLE`,
`DMA_COMPLETED_UNHARVESTED`, `READY_FOR_NVME`, `NVME_BUSY`,
`REQUEUE_PENDING`, and `FREE`. A hardware snapshot walks every BD and checks
the descriptor completion bit before counting a nominally DMA-writable slot.
The six state counts must always add up to the configured slot count.

`dma_writable == 0` is a terminal receive-integrity failure because the Aurora
RX-only path has no upstream backpressure. Releasing a slot later does not
clear this failure. The final result uses
`dma_bd_exhausted_no_upstream_backpressure` and the worker exits nonzero.

The default scheduling policy is `SCHED_RR`, with producer/writer priorities
70/60 on ch0 and ch1, and 30/20 on ch2. If RT scheduling is unavailable the
worker logs the effective fallback. Relevant variables are:

- `SRC_REAL_PRODUCER_RT_POLICY=rr|fifo|other`
- `SRC_REAL_WRITER_RT_POLICY=rr|fifo|other`
- `SRC_REAL_CH{0,1,2}_PRODUCER_RT_PRIO`
- `SRC_REAL_CH{0,1,2}_WRITER_RT_PRIO`
- `SRC_REAL_CH{0,1,2}_HARVEST_BATCH_MAX` (defaults: 32, 32, 4)
- `SRC_REAL_HARVEST_BATCH_MAX` (global compatibility fallback)
- `SRC_REAL_CH{0,1,2}_DMA_POLL_SLEEP_US` (defaults: 0, 0, 50)
- `SRC_REAL_DMA_BD_LOW_WATERMARK`
- `SRC_REAL_STRICT_END_TO_END=0|1`
- `SRC_REAL_EXPECTED_BYTES_CH{0,1,2}`

The serial frame format is unchanged. Command `0x11` now performs PRESTART;
workers initialize NVMe, queues, and a halted BD ring, then report
`storage_ready`. Command `0x21 START` releases all selected workers through
independent control pipes and waits for `storage_started`. This is a software
barrier (`start_gate_mode=software_barrier`), not a zero-skew FPGA gate.

Receive-only diagnosis, with no NVMe writer:

```sh
sudo ./src_real_app dma-rx-benchmark --channel 0 --duration-sec 30 --source transfer
```

No FPGA source-byte counter address exists in the current address map, so logs
report `source_counter_available=0`. Strict mode requires an expected byte
count until FPGA exposes a stable per-channel 64-bit accepted-byte counter,
snapshot/clear control, and overflow indication.
