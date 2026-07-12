# src_real 代码算法功能对应文档

本文档按当前代码说明 `src_real` 的功能、算法流程和主要代码位置。这里以源码为准，尤其是当前已经改为 DMA->DDR 与 DDR->NVMe 并行的存盘逻辑。

## 1. 总体结构

`src_real_app` 是板卡 Linux 上的集成控制程序，主进程负责串口协议、任务调度和 ACK，长时间数据任务通过子进程 worker 执行。

| 功能 | 主要代码 | 说明 |
|---|---|---|
| 串口协议解析和 ACK | `src/system.c`, `src/serial_proto.c` | 接收 `0x11/0x21/0x41/0x51/0x61/0x71` 命令并回复 |
| 存盘 worker 调度 | `src/system.c:start_storage_worker()` | `0x11` 后提前 fork 存盘 worker，worker 先准备 DMA/NVMe |
| 采集开始/停止 | `src/system.c:handle_frame()` | `0x21=0x11` 只 ACK 开始；`0x21=0xFF` 给 worker 发停止信号并等待退出 |
| DMA/NVMe 存盘算法 | `src/ccb_commands.c:execute_write_with_result()` | AXI DMA S2MM 写 DDR ring，NVMe writer 线程从 DDR 写 SSD |
| DDR pattern 写盘测试 | `src/ccb_commands.c:execute_ddr_pattern_store_with_result()` | CPU 写 ch2 DDR pattern，再用 NVMe 写 SSD 并登记数据库 |
| 硬件 MMIO/NVMe/DMA 底层 | `src/ccb_hw.c` | `/dev/mem` 映射、AXIS switch、DMA SG ring、NVMe command |
| 文件列表和任务数据库 | `src/file_list.c`, `src/system.c` | `filelist.db` 和任务状态记录 |
| metadata | `src/ccb_metadata.c` | 每通道文件条目、LBA 分配、重复检查 |
| 网络发送 | `src/system.c:network_send_existing_file()`, `src/ccb_tcp_transfer.c` | NVMe read 到 DDR，再走 TCP MM2S DMA 发送 |
| SSD PCIe reset 工具 | `tools/ssd_pcie_reset.c` | 通过 AXI GPIO `0x40010000` 控制低有效 `pcie_rstn` |

## 2. 串口命令到功能映射

| 命令 | 入口 | 功能 |
|---|---|---|
| `0x11` task info | `system.c:handle_frame()` | 解析任务号、过境时间、文件模式；创建任务记录；启动对应存盘 worker；等待 `storage_ready` 后 ACK |
| `0x21` acquisition control | `system.c:handle_frame()` | `0x11` 表示开始采集，直接 ACK；`0xFF` 表示停止采集，向 worker 发 SIGTERM 并等待写盘完成 |
| `0x41` file list | `system.c:handle_frame()` | 查询文件列表、同步 flash 数据库、清空文件列表和 metadata |
| `0x51` file operation | `system.c:handle_frame()` | 网络下载指定文件、删除单个文件、删除全部文件 |
| `0x61` status | `system.c:handle_frame()` | 检查 ch0/ch1/ch2 NVMe 状态和任务状态 |
| `0x71` stop transfer | `system.c:handle_frame()` | 停止网络发送 worker，并通知 NVMe read/TCP transfer 停止 |

## 3. 任务模式到通道映射

代码位置：`src/system.c:build_file_plan()` 和 `acq_type_from_task_mode()`。

| `task_file_mode` | 文件类型 | 启动通道 | 0x21 ACK 采集位 | 说明 |
|---|---|---|---|---|
| `0x11` `TASK_FILE_MODE_CALIB_ONLY` | `FILE_TYPE_CALIB` | ch2 | `ACQ_TYPE_ENVELOPE` | 校准数据，只开低速通道 |
| `0x22` `TASK_FILE_MODE_LOW_ONLY` | `FILE_TYPE_LOW` | ch2 | `ACQ_TYPE_ENVELOPE` | 低速/包络数据，只开低速通道 |
| `0x33` `TASK_FILE_MODE_HIGH_ONLY` | `FILE_TYPE_I` + `FILE_TYPE_Q` | ch0 + ch1 | `ACQ_TYPE_HIGH_I | ACQ_TYPE_HIGH_Q` | 仅高速 I/Q |
| `0xAA` `TASK_FILE_MODE_ALL` | `FILE_TYPE_LOW` + `FILE_TYPE_I` + `FILE_TYPE_Q` | ch2 + ch0 + ch1 | `ACQ_TYPE_ENVELOPE | ACQ_TYPE_HIGH_I | ACQ_TYPE_HIGH_Q` | 全采集 |

`0x11` 仍按 `FILE_TYPE_CALIB` 记录文件，便于下载和文件列表区分；但采集开关位与 `0x22` 一样只表示 ch2。

## 4. 硬件地址和通道配置

代码位置：`src/ccb_config.c`, `include/ccb_types.h`。

### ch0 HIGH_I

| 项 | 地址/大小 |
|---|---:|
| NVMe host MMIO | `0x44a00000` |
| AXI DMA MMIO | `0x41e00000` |
| AXIS switch MMIO | `0x44a10000` |
| descriptor CPU 地址 | `0x20000000` |
| descriptor DMA 地址 | `0x10000000` |
| descriptor 大小 | `0x4000` |
| DDR CPU 窗口 | `0x10000000`, 64MiB |
| DDR DMA/NVMe 视角 | `0x00000000`, 运行时1GiB |
| 默认 DMA descriptor payload | 16MiB |

### ch1 HIGH_Q

| 项 | 地址/大小 |
|---|---:|
| NVMe host MMIO | `0xa0080000` |
| AXI DMA MMIO | `0xa0060000` |
| AXIS switch MMIO | `0xa0070000` |
| descriptor CPU 地址 | `0x30000000` |
| descriptor DMA 地址 | `0x10000000` |
| descriptor 大小 | `0x4000` |
| DDR CPU 窗口 | `0xd0000000`, 64MiB |
| DDR DMA/NVMe 视角 | `0x00000000`, 运行时1GiB |
| 默认 DMA descriptor payload | 16MiB |

### ch2 LOW_SPEED/CALIB

| 项 | 地址/大小 |
|---|---:|
| NVMe host MMIO | `0x00010000` |
| AXI DMA MMIO | `0x00030000` |
| AXIS switch MMIO | `0x00040000` |
| descriptor CPU 地址 | `0x20004000` |
| descriptor DMA 地址 | `0x10000000` |
| descriptor 大小 | `0x4000` |
| DDR CPU 窗口 | `0xc0000000`, 64MiB |
| DDR DMA/NVMe 视角 | `0x00000000`, 512MiB |
| 默认 DMA descriptor payload | 16MiB |

注意：CPU 只 mmap 64MiB DDR 窗口；DMA/NVMe 使用硬件本地 DDR offset，可覆盖完整 ring。

### SSD PCIe reset GPIO

| 项 | 地址/含义 |
|---|---:|
| AXI GPIO base | `0x40010000` |
| DATA offset | `0x00` |
| TRI offset | `0x04` |
| bit | `0` |
| 有效电平 | 低有效，`0` 表示复位，`1` 表示释放复位 |

对应工具：`tools/ssd_pcie_reset.c`。PetaLinux `myapp` 中可安装为 `/usr/bin/ssd_rst.elf`。

## 5. 存盘算法：DMA->DDR 与 DDR->NVMe 并行

代码位置：`src/ccb_commands.c`。

### 5.0 DDR pattern 写盘测试

入口：`storage.elf ddr-pattern-store`，代码为
`execute_ddr_pattern_store_with_result()`。

用途是绕过外部 S2MM 输入，直接在 ch2 CPU 视角 DDR `0xc0000000`
写入默认 32MiB pattern，然后用 ch2 DMA/NVMe 视角地址 `0x00000000`
写入 SSD。每个 32-bit word 的格式：

```text
word[n] = ((n & 0xffff) << 16) | (n & 0xffff)
```

写盘完成后复用现有 metadata 和 `filelist.db` 登记逻辑，所以上位机可以通过
现有文件列表和 TCP 下载流程读取该文件。该模式本身不主动 TCP 发送。

测试开始时先对通道AXI DMA执行full reset，防止上一次异常任务留下的S2MM继续
覆盖DDR ring。默认32MiB测试随后执行两级完整校验：CPU填充后输出
`ddr_pattern_verify stage=cpu_fill`；NVMe写完成后再读回到ch2 CPU DDR窗口的
第二个32MiB区域，逐32-bit word检查并输出
`ddr_pattern_verify stage=ssd_readback`。只有两级都通过才登记metadata和数据库。
网络下载在第一个chunk的NVMe read完成、TCP MM2S启动前输出
`[DBG][NET] DDR prefix before TCP ...`，用于区分SSD/DDR问题和TCP数据通路问题。

### 5.1 初始化阶段

入口：`execute_write_with_result()`。

流程：

1. 选择通道配置：`find_channel(args->channel_id)`。
2. 打开硬件运行时：`channel_runtime_open()`。
3. 检查 NVMe：`nvme_probe()`。
4. 读取 metadata：`metadata_read()`。
5. 分配 metadata slot 和自动 LBA：`metadata_alloc_slot_and_lba()`。
6. 配置 AXIS switch 输入源：`axis_switch_select()`。
7. 初始化 S2MM SG ring：`dma_init_s2mm_ring()`。
8. 打印 `storage_ready`，主进程据此给 `0x11` ACK 成功。

### 5.2 DMA producer

主线程作为 producer，只负责从 AXI DMA 收已经完成的 descriptor。

核心逻辑：

```text
while continuous or not enough bytes:
    dma_harvest_one()
    if descriptor complete:
        actual = descriptor status length
        dma_received_bytes += actual
        push(slot, actual) to StorageWriteQueue
```

对应代码：

- `dma_harvest_one()`：读取 descriptor complete bit 和实际长度。
- `storage_queue_push()`：把 `(slot, bytes)` 放入待写盘队列。

这里不会等待 NVMe 写完，所以 DMA 可以继续接收后续 descriptor，直到 DDR ring 中未落盘 slot 用尽。

软件额外维护两层 descriptor ownership：

- `dma_hw_desc_count`：当前提交给 AXI DMA 的 BD 数量，对应 Xilinx 驱动的
  `HwCnt` 概念。
- `slot_busy[slot]`：该 DDR slot 已完成 DMA、但仍在队列或 NVMe writer 中。
- `slot_state[slot]`：诊断用状态机，取值为
  `FREE -> DMA_OWNED -> DMA_DONE_READY -> NVME_BUSY -> FREE/DMA_OWNED`。
  它不改变默认调度，只用于 `storage_pipeline` 和最终统计判断数据是在等
  writer、还是 writer 在等 DMA。

harvest 一个 BD 时 `dma_hw_desc_count--`。当该值为 0 时，producer 不再绕回
读取旧 completion，而是等待 writer 重新提交 BD。同一 busy slot 如果再次入队，
任务立即报错，避免静默写出重复数据。

### 5.3 NVMe consumer

单独的 writer 线程执行 DDR->NVMe。

核心逻辑：

```text
while queue not done:
    pop(slot, bytes)
    flush_slot_to_nvme(slot, bytes)
    if producer still running:
        dma_requeue_one(slot)
```

对应代码：

- `storage_nvme_writer_thread()`
- `flush_slot_to_nvme()`
- `dma_requeue_one()`

重要约束：descriptor 只有在对应 DDR slot 已写入 NVMe 后才归还给 DMA。这样可以避免 DMA 覆盖尚未落盘的数据。

默认仍走 single-slot legacy writer。`SRC_REAL_NVME_CROSS_SLOT_QD=1` 才启用
跨 slot 全局 QD writer。`SRC_REAL_PIPELINE_MODE` 和
`SRC_REAL_CHx_FAST_PIPELINE` 当前作为测试配置日志输出，保留 legacy 回退。

日志等级和周期统计：

```text
SRC_REAL_LOG_LEVEL=quiet|summary|debug|trace
SRC_REAL_PIPELINE_STATS_SEC=5
storage_pipeline channel=... sec=5 dma_mib_s=... nvme_mib_s=... input_mib_s=...
ready_q=... ready_q_avg=... ready_q_max=...
dma_writable_slots=... software_free_slots=...
ready_slots=... nvme_busy_slots=...
writer_idle_us=... writer_active_us=... harvest_batch_avg=...
nvme_cmd_submitted_1s=... nvme_cmd_completed_1s=...
submit_stall_count=... submit_stall_max_us=...
```

判断方法：

- `ready_q`、`nvme_busy_slots`、`ring_full_count` 高：NVMe drain 追不上输入。
- `writer_idle_us` 高且 `ready_q_cur` 低：DMA/Aurora 输入不足或采集空闲。
- `nvme_active_qd_avg` 明显低于配置 QD：NVMe command feed 或 CQ poll 有空泡。
- `dma_writable_slots`：当前挂给 AXI DMA、可继续接收数据的 slot 数。
- `software_free_slots`：软件侧空闲但尚未挂给 DMA 的 slot 数。
- `ready_slots`：DMA 已完成、等待 NVMe writer 的 slot 数。
- `nvme_busy_slots`：正在被 NVMe writer 读取写 SSD 的 slot 数。
- `submit_stall_count/submit_stall_max_us`：NVMe submit 单次耗时超过 1000us 的
  累计次数和最大耗时；它不会逐次打印，只出现在周期和最终统计中。

writer 归还 BD 的顺序遵循 PG021 Tail Pointer Mode：

```text
NVMe write complete
  -> clear BD status/Completed
  -> memory barrier
  -> dma_hw_desc_count++
  -> write S2MM_TAILDESC
```

当前没有设置 `S2MM_DMACR.Cyclic BD Enable`，因此不是 AXI DMA 硬件 cyclic
mode。只有 BD 的 `next_desc` 在内存中首尾相连，实际可处理到哪个 BD 仍由
`TAILDESC` pause pointer 控制。

### 5.4 DMA idle 提示

continuous 模式下，producer 在至少收到过一个 DMA completed descriptor 后，如果连续一段时间没有新的 descriptor completion，会打印提示：

```text
storage_idle_detected channel=... task=... file_index=...
  idle_ms=...
  dma_received_bytes=...
  queued_file_bytes=...
  manual_stop_required=1
```

默认阈值是 5000 ms，可通过环境变量 `SRC_REAL_STORAGE_IDLE_NOTICE_MS` 调整；设为 `0` 可关闭该提示。

这个提示只表示“软件观察到 DMA 已连续空闲一段时间”，不会自动停止任务、不会停止 DMA、不会写 metadata，也不会返回停止 ACK。实际结束仍由上位机发送 `0x21=0xFF` 触发。
该提示使用正常 `printf` 输出，不依赖 verbose debug。

### 5.5 停止阶段

`0x21=0xFF` 后，主进程向 storage worker 发 SIGTERM，worker 内部设置停止标志。

停止流程：

1. producer 观察到 stop signal。
2. producer 继续短时间 drain 已完成 descriptor。
3. producer 调用 `storage_queue_finish()`，通知 writer 不再有新 slot。
4. `dma_stop_s2mm()` 记录 S2MM CR/SR、CURDESC、TAILDESC、下一个 BD status、
   HW-owned 数量以及累计 RXSOF/RXEOF，然后按旧版稳定时序对 MM2S/S2MM
   执行 AXI DMA full reset。
5. `pthread_join()` 等待 writer 写完队列中所有 slot。
6. 写 metadata。
7. 输出 `storage_transfer_done` 和 `storage_worker_done`。

每次新任务在重建 SG ring 后、写 `CURDESC/TAILDESC` 前也执行一次 full reset，
保证上一次存储或 TCP MM2S 操作不会把 AXI DMA 内部状态带入新任务。reset 成功时
停止日志输出 `result=full_reset`；reset 超时则任务失败。尾包完整性仍由停止前的
RXSOF/RXEOF、BD status 和 `tail_incomplete` 独立标记，不能因为 reset 成功而忽略。

当前发送端约定每 16MiB 产生一次 TLAST，因此正常 80MiB 任务应观察到 5 次
RXSOF 和 5 次 RXEOF，且 `dma_rx_packet_open=0`。停止日志为：

```text
storage_dma_stop_begin channel=... s2mm_cr=... s2mm_sr=...
  curdesc=... taildesc=... next_bd=... next_bd_status=...
  hw_owned=... rxsof_count=... rxeof_count=... rx_packet_open=...
storage_dma_stop_done channel=... result=full_reset|failed ...
```

若 NVMe 队列已全部完成、`dma_received_bytes == file_bytes` 且 metadata 已写成功，
但 DMA halt/reset 最终失败，软件仍保留 metadata 和 `filelist.db` 文件记录，便于
下载已经落盘的数据；worker 仍非零退出、任务状态为 `TASK_FAILED`，停止命令仍
回复失败 ACK。最终 `storage_worker_result` 中会明确输出
`data_persisted=1 integrity_ok=0 integrity_risk=dma_stop_recovery_failed`。

如果发送端最后一段数据没有形成 DMA completed descriptor，这段数据仍不会进入队列。也就是说最终数据完整性仍要求 AXI-Stream 端正确处理 `tready/tvalid/tlast`，并且停止前最后一段能让 DMA BD 完成。

## 6. 存盘统计字段含义

默认 `SRC_REAL_LOG_LEVEL=quiet` 不输出周期统计。设置
`SRC_REAL_LOG_LEVEL=summary` 后，按 `SRC_REAL_PIPELINE_STATS_SEC` 输出一条单行
周期统计；默认 5 秒，设为 0 可关闭：

```text
storage_pipeline channel=... sec=5
  dma_mib_s=... nvme_mib_s=... input_mib_s=...
  ready_q=... ready_q_avg=... ready_q_max=...
  dma_writable_slots=... software_free_slots=...
  ready_slots=... nvme_busy_slots=...
  buffered=... ring_full=... no_free=...
  active_qd_avg=... active_qd_max=...
  writer_rt_enabled=... writer_rt_prio=...
  writer_submit_stall_count=... writer_submit_stall_max_us=...
  writer_empty_wait_us=... writer_drain_active_us=...
  ready_q_nonempty_us=...
  writer_drain_loop_count=... writer_slots_per_drain_loop=...
```

旧 `storage_stats` 默认不打印。需要兼容旧 grep 时设置
`SRC_REAL_ENABLE_STORAGE_STATS=1`。

`SRC_REAL_DMA_IDLE_DONE_MS` 默认 5000。连续采集中，只有在该 channel 至少收到
过一个 DMA completed descriptor 后，idle timer 才生效；超时后打印
`storage_dma_idle_done` 并走现有 stop/drain/finalize 流程。没有收到任何数据时
不会自动完成。

`SRC_REAL_CHx_WRITER_RT_PRIO` 可尝试把对应 channel writer 设为 `SCHED_FIFO`。
设置失败只打印 warning，不中断任务。`SRC_REAL_WRITER_BACKLOG_MODE=1` 保持
ready queue 有积压时 writer 处于连续 drain 状态；默认 0 保持 legacy 回退。

`ring_full_count > 0` 表示 DDR ring 曾被填满。由于 Aurora RX-only simplex 无
反压，最终 `integrity_ok=0`，`integrity_risk=ddr_ring_full_no_aurora_backpressure`。
即使已有 DDR 数据可以 drain 并落盘，也不能把该文件当作无风险完整数据。

成功时 worker 输出：

```text
storage_transfer_done channel=... task=... file_index=...
  dma_received_bytes=...
  file_bytes=...
  ssd_sector_bytes=...
  sectors=...
  chunks=...
  elapsed_ms=...
  nvme_write_ms=...
  file_mib_s=...
  nvme_mib_s=...
  dma_desc_completed_count=...
  dma_desc_interval_min_us=...
  dma_desc_interval_max_us=...
  dma_desc_interval_avg_us=...
  nvme_cmd_size_bytes=...
  nvme_cmd_count=...
  nvme_cmd_bytes_total=...
  nvme_write_bytes_done=...
  nvme_cmd_latency_min_us=...
  nvme_cmd_latency_max_us=...
  nvme_cmd_latency_avg_us=...
  nvme_qd_requested=...
  nvme_qd_effective=...
  nvme_active_qd_max=...
  nvme_active_qd_avg=...
  ring_full_count=...
  ring_full_total_ms=...
  max_ddr_busy_slots=...
  max_ddr_buffered_bytes=...
  tail_incomplete=...
  integrity_ok=...
  integrity_risk=...
  dma_stop_result=...
  dma_stop_recovered=...
  dma_rxsof_count=...
  dma_rxeof_count=...
  dma_rx_packet_open=...
  continuous=...
```

主进程输出：

```text
storage_worker_done task=... channel=... file_index=...
  dma_received_bytes=...
  file_bytes=...
  ssd_sector_bytes=...
  start_lba=...
  sectors=...
  elapsed_ms=...
  nvme_write_ms=...
```

字段含义：

| 字段 | 含义 |
|---|---|
| `dma_received_bytes` | DMA completed descriptor status 中累计的实际字节数 |
| `file_bytes` | 软件记录为文件有效大小的字节数 |
| `ssd_sector_bytes` | 实际按 512B sector 写入 SSD 覆盖的字节数 |
| `sectors` | 文件占用 NVMe sector 数 |
| `chunks` | 写入 NVMe 的 DDR slot 数量 |
| `elapsed_ms` | 从开始采集循环到队列写完的总耗时 |
| `nvme_write_ms` | writer 线程累计调用 NVMe write 的耗时 |
| `file_mib_s` | 按 `file_bytes / elapsed_ms` 计算的整体吞吐 |
| `nvme_mib_s` | 按 `file_bytes / nvme_write_ms` 计算的 NVMe 写盘吞吐 |
| `max_ddr_busy_slots` | 本次任务同时处于待写/正在写 NVMe 状态的最大 DDR slot 数 |
| `max_ddr_buffered_bytes` | 本次任务的最大实际排队/写盘字节数 |
| `integrity_ok` | 未发生 ring full、未检测到 tail incomplete、DMA stop 成功且 DMA/file 字节数相等时为1 |
| `integrity_risk` | `none`、`ddr_ring_full_no_aurora_backpressure`、`tail_descriptor_incomplete`、`dma_file_byte_mismatch`或`dma_stop_recovery_failed` |
| `data_persisted` | NVMe 数据、metadata 和文件数据库记录均已保留时为1；该值不代表任务控制成功 |
| `dma_stop_result` | `0`为任务级full reset成功，`-1`为reset失败；`1`保留给兼容旧日志的恢复状态 |
| `dma_rxsof_count`/`dma_rxeof_count` | 已完成 BD 中累计的 AXI DMA RXSOF/RXEOF 数量 |
| `dma_rx_packet_open` | 已看到 RXSOF 但尚未看到匹配 RXEOF，为1时存在尾 frame 不完整风险 |

MicroBlaze是32位ABI，不能用`long`保存文件字节数。当前`FileRecord`、文件列表
查询、SQLite绑定和网络下载长度统一使用`uint64_t/sqlite3_int64`，串口继续发送
big-endian uint64。读取旧数据库时如果发现负的32位`file_size`，按
`sector_count * 512`恢复，避免转换成接近`UINT64_MAX`的错误数值。

SSD metadata保持原有32字节`FileEntry`布局：超过4GiB时
`file_size_bytes=UINT32_MAX`作为兼容标记，精确字节数保存在SQLite；LBA分配和
读盘使用完整`sector_count`。因此不改变裸机metadata格式，单文件上限由
32位sector count决定，约2TiB。

判断方法：

- `dma_received_bytes < 发送端字节数`：DMA 前级或停止边界仍可能丢数据，重点查 AXI-Stream backpressure/TLAST。
- `dma_received_bytes == file_bytes`：软件没有截断已完成 DMA 数据。
- `ssd_sector_bytes >= file_bytes`：正常，NVMe 按 512B sector 写，最多多覆盖 511 字节。
- `nvme_write_ms` 接近 `elapsed_ms`：NVMe writer 是主要瓶颈。
- `elapsed_ms` 明显大于发送端耗时：说明 DDR ring 正在吸收突发，写盘仍在后台追赶。

## 7. NVMe 写盘算法

代码位置：`src/ccb_hw.c:nvme_rw()`。

`flush_slot_to_nvme()` 把一个 DDR slot 转为 NVMe write：

1. producer 在 DMA harvest 时就计算并记录 `start_lba`、`sectors`、`hw_addr`。
2. `sectors = bytes_to_sectors(bytes)`，尾部不足 512B 时仍占用一个完整 sector。
3. producer 用 `next_queue_lba += sectors` 推进 LBA cursor；writer 不再用
   `file_offset / 512` 反推 LBA，也不在写盘完成后串行推进 LBA。
4. 默认 single-slot writer 调用 `nvme_write_slot_qd(rt, slot, start_lba, sectors, hw_addr)`。

环境变量`SRC_REAL_NVME_CMD_KIB`允许配置256、512、1024、2048、4096KiB；
也支持按通道覆盖：`SRC_REAL_NVME_CMD_KIB_CH0`、`SRC_REAL_NVME_CMD_KIB_CH1`、
`SRC_REAL_NVME_CMD_KIB_CH2`。默认值按通道区分：ch0/ch1高速通道默认请求
1024KiB，ch2低速通道默认请求256KiB。实际值会按4MiB代码上限和NVMe
Host/SSD报告的`max_dts`回退。拆分始终限制在本次slot的有效sector范围内，
并保持LBA和DDR地址连续递增。

`SRC_REAL_NVME_QD`支持1、2、4、8、16、32，也支持按通道覆盖：`SRC_REAL_NVME_QD_CH0`、
`SRC_REAL_NVME_QD_CH1`、`SRC_REAL_NVME_QD_CH2`。默认值按通道区分：ch0/ch1
默认8，ch2默认4；软件安全上限为32。启动时打印：

```text
nvme_storage_config ... nvme_qd_requested=... nvme_qd_effective=...
  nvme_qd_limit_reason=...
```

当前IntelliProp NVMe Host Core配置为`USE_CMD_REGISTERS=TRUE`、
`ENABLE_TAGS=TRUE`、`MAX_NUM_TAGS=255`。软件不直接维护标准NVMe SQ/CQ内存：

1. 写command context并置`CMD_PENDING`，等待该位清零只表示Host Core已接收命令。
2. `SQ_FIFO_FULL`用于阻止继续提交。
3. `CQ_FIFO_EMPTY=0`后读取CQ CID寄存器；该读取在Host Core内部自动pop CQ FIFO。
4. 根据返回CID查找pending表，不能按提交顺序假设completion顺序。

每个pending项记录CID、slot、slot offset、LBA、DDR地址、sector数、字节数和
提交时间。一个slot仅在sector全部提交、completed等于submitted、inflight为0且
failed为0时返回成功；writer随后才清`slot_busy`并调用`dma_requeue_one()`。

QD调度在单个DDR slot内部先尽量提交到配置QD；随后每轮等待/处理一个CQ
completion，释放对应CID后立即回到submit refill，直到该slot的所有sector都
完成。这样避免“每提交一条就drain全部CQ”导致active QD偏低，同时仍通过
`SQ_FIFO_FULL`、pending表和CID匹配保证不会越过Host Core运行时能力。
最终统计中的`nvme_active_qd_max`和`nvme_active_qd_avg`用于判断实际QD是否
接近配置值。

默认`SRC_REAL_NVME_CROSS_SLOT_QD=0`时，QD只在单个slot内部并发，不会启动多个
writer线程。设置`SRC_REAL_NVME_CROSS_SLOT_QD=1`时，每个通道仍只有一个writer
线程，但writer会按batch跨多个completed slot做全局QD调度；batch大小由
`SRC_REAL_NVME_CROSS_SLOT_BATCH`控制，默认8。任一CQ错误、未知CID、重复/无pending
completion或slot invariant失败都会停止提交，标记writer error，且不会归还不安全slot。

NVMe poll/backoff默认兼容旧逻辑：

- `SRC_REAL_NVME_BUSY_POLL_US=0`
- `SRC_REAL_NVME_POLL_SLEEP_US=10`

producer在没有DMA completion时按ring水位自适应sleep：

- `SRC_REAL_STORAGE_POLL_SLEEP_US=100`
- `SRC_REAL_STORAGE_HIGH_WATERMARK_POLL_US=10`
- `SRC_REAL_STORAGE_CRITICAL_WATERMARK_POLL_US=0`

## 8. DMA SG ring 算法

代码位置：`src/ccb_hw.c:dma_init_s2mm_ring()`。

初始化：

1. `desc_count = dma_ring_bytes / dma_desc_bytes`。
2. 检查 descriptor BRAM 是否够放所有 BD。
3. 每个 BD 指向一个固定 DDR slot：

```text
desc[i].buffer_addr = ddr_hw_base + i * dma_desc_bytes
```

4. BD 的 `next_desc` 首尾相连，但不启用 DMACR cyclic bit。
5. 按 `src_real (1)` 的稳定时序，在每次任务初始化时向 MM2S/S2MM DMACR
   写 Reset，并等待两个方向的 Reset bit 都清零。
6. 清除旧 IRQ status。
7. 保留当前 descriptor ownership、completed/status 清理和防重复入队状态。
8. 清除旧 IRQ status，写 S2MM `CURDESC` 和初始 `TAILDESC`，按 PG021 Tail
   Pointer Mode 启动 DMA。

收包：

- `dma_harvest_one()` 按顺序检查 `next_harvest_bd`。
- 只有 `dma_hw_desc_count > 0` 才检查当前 BD，防止完整 ring 后绕回重复 harvest。
- complete 后检查 BD error bits，并读取 `status & DESC_STS_LEN_MASK` 作为实际接收字节。
- verbose 模式打印 BD 的 `RXSOF`、`RXEOF`、实际长度和 HW-owned 数量。
- writer 写完该 slot 后调用 `dma_requeue_one()` 清 status 并更新 taildesc。

PG021 明确指出：硬件如果获取到 `Completed=1` 的 stale BD，会置位
`SGIntErr` 并停止。因此软件必须先清 Completed，再推进 `TAILDESC`。

正常任务停止恢复为旧版 full reset，确保下一个任务从确定状态开始。reset 在
storage worker 已停止 producer 且 writer 不再归还 BD 后执行，不改变“slot 全部
写盘完成后才能归还 DMA”的约束。若 reset 失败但数据已经完整写入 NVMe，则保留
metadata 和数据库文件记录，同时让任务和 ACK 保持失败。

### 8.1 ch0/ch1 1GiB ring 生效条件

`storage-write` 对 ch0/ch1 默认使用 8MiB slot：

```text
1073741824 bytes / 8388608 bytes = 128 slots
```

descriptor BRAM 为 0x4000，`DmaSgDesc=64`，最多 256 个 BD，因此
128 BD 可以容纳。启动时 `storage_pipeline_config` 必须能看到：

```text
requested_ring_bytes=1073741824
effective_ring_bytes=1073741824
slot_bytes=8388608
total_slots=128
ring_clamp_reason=none
dma_bd_count=128
```

如果请求值被压小、slot 不能整除 ring、或 BD 数超过 BRAM 容量，worker 会打印
`storage_ring_config_error` 并失败，不再静默按较小 ring 继续采集。

### 8.2 ch2 ring 饱和

ch2 有 32 个 16MiB slot，总缓存 512MiB。当输入平均速率长期高于 NVMe
写盘速率时，所有 slot 都可能处于 busy 状态。程序会打印：

```text
storage_ring_warning channel=2 ring_full_count=1 no_free=1 buffered=536870912
  busy_slots=32 total_slots=32 captured_bytes=...
```

此时 AXI DMA S2MM 会降低 `TREADY`，但反压只能传到内部 AXIS
FIFO 和 channel switch。PG074 明确说明 Aurora 64B/66B RX 用户接口
没有内建用户数据缓冲，因此没有 `m_axi_rx_tready`。当前 Aurora IP
配置为 `RX-only Simplex`、`flow_mode=None`、`c_nfc=false`，生成参数
`HAS_TREADY=0`；生成 HDL 中 channel switch 对 Aurora 输入的 `tready`
也是未连接网络。

因此 DDR ring 满后，Aurora 仍可能继续输出，未被 DMA 接收的数据会在
DMA 前丢失。当前硬件无法依靠 AXI-Stream `TREADY` 对 Aurora 对端实现
无损反压。长时间无损采集必须使发送端平均速率不高于 NVMe 写盘
速率，或增加独立流控信号，或使用支持返回通路的 Aurora 配置并实现
Native Flow Control。

任务完成日志还包含：

```text
max_ddr_busy_slots=...
max_ddr_buffered_bytes=...
```

## 9. 网络发送算法

代码位置：

- `src/system.c:network_send_existing_file()`
- `src/ccb_tcp_transfer.c`

流程：

```text
当前目录 ./filelist.db 查文件
  -> 根据 file type 找通道
  -> NVMe read 到 DDR offset 0
  -> TCP MM2S DMA 从 DDR 发出
```

这里的 `filelist.db` 是当前工作目录中的运行时数据库。程序启动不再挂载 flash1，
也不从 flash 恢复数据库或 metadata；flash1 是否可用不会阻止 `storage.elf`
启动和正常存储。

TCP switch 输入按 FPGA `axis_switch_0` 连接映射：

| 文件类型/通道 | TCP switch input |
|---|---:|
| ch0 `FILE_TYPE_I` | `0` |
| ch1 `FILE_TYPE_Q` | `1` |
| ch2 `FILE_TYPE_LOW` / `FILE_TYPE_CALIB` | `2` |

网络发送按 `TCP_MAX_BYTES_PER_DESC` 分块，当前默认 16MiB 一块，以保证每块由 DMA EOF 产生一次 TLAST。
`SRC_REAL_NETWORK_LIMIT_MB_S`默认`0`，表示不限速；设置为非0后按每个chunk的
平均耗时补sleep，例如`SRC_REAL_NETWORK_LIMIT_MB_S=10`约束到约10MiB/s。
`SRC_REAL_NETWORK_TASK_TIMEOUT_SECONDS`默认`0`，表示父进程不再对整个网络
worker设置总任务超时；`SRC_REAL_NETWORK_TIMEOUT_US`仍是单次NVMe/TCP操作超时。

TCP MM2S 和存储 S2MM 位于同一个 AXI DMA 实例。每个 TCP chunk 的 DMA 生命周期为：

1. 检查 MM2S/S2MM Reset bit，发现卡住时立即失败。
2. MM2S 无 error 时清 `RS` 并确认 Halted；如果本来已 Halted 则直接使用。
3. 仅 MM2S DMASR 存在 error 时执行一次受检查的整核 soft reset。
4. 清除旧 IRQ status，重建 MM2S descriptor，写 `CURDESC`、置 `RS`、写 `TAILDESC`。
5. 所有 descriptor 完成后只 halt MM2S，不执行 reset。
6. 用户停止或普通超时同样走 halt；仅 cleanup 时确认存在 DMA error 才整核 reset。

旧版在每个16MiB chunk前后都整核reset，800MiB下载会触发约100次reset；异常路径
还存在只写Reset而不等待完成的情况。该逻辑已删除，避免TCP下载后把S2MM留在
Reset状态，导致下一次存储初始化失败。显式`network-reset`命令仍执行整核reset，
并同时检查MM2S和S2MM Reset bit是否清零。

完成输出：

```text
network_send_done task=... file_index=... proto_file_type=... file_bytes=... chunks=...
```

用户停止时，worker会输出`network_send_stopped`，包含已处理chunk序号、总chunk数、
当前文件偏移和LBA，便于定位卡在第几个16MiB chunk。

## 10. Metadata 和数据库关系

| 数据 | 代码 | 作用 |
|---|---|---|
| NVMe metadata | `src/ccb_metadata.c` | 每通道保存文件 task、file index、size、start_lba、sector_count |
| `filelist.db` | `src/file_list.c` | 上位机文件列表查询、网络发送查找 |
| `logs.db` | `src/logger.c` | 运行日志 |

`0x41 control=0x22` 会把当前 `filelist.db` 与每通道 NVMe metadata
（`meta_ch0.bin`、`meta_ch2.bin`）一起同步到 flash1，但要求用户事先手动挂载
flash1。未挂载时只有该同步命令返回失败，不影响 daemon、存储或网络下载。
当前版本没有开机自动挂载和启动恢复功能。

存盘成功后：

1. `execute_write_with_result()` 写 metadata。
2. storage worker 调用 `record_storage_result_to_db()` 写 `filelist.db`。
3. 主进程通过 worker 输出解析 `storage_worker_done`，更新任务状态。

同一任务编号再次存盘时，worker 先查询 `filelist.db` 的最大 `file_index`，
再用 SSD metadata 复核；最终序号取两侧已用最大值之后的下一个值。只有同任务
尚无文件时才保留请求中的初始序号。该逻辑不改变 TCP 下载使用的已有文件序号。

## 11. 关键风险和排查点

1. 当前 Aurora RX 无 `TREADY` 且未启用 Native Flow Control，AXI-Stream backpressure 不能传回对端。DDR ring 满后应将本次采集视为已发生数据完整性风险。
2. 停止采集时，最后不足一个 descriptor 的数据如果没有让 DMA BD complete，软件无法写入这段数据。
3. DDR ring 只能吸收突发。如果平均输入速率长期高于 NVMe 写盘速率，ring 最终会满；当前硬件不能把该状态反压到 Aurora 对端，因此会丢数。
4. NVMe写盘支持最大QD32，但实际吞吐仍受Host Core协商tag数、SQ FIFO、PCIe和SSD持续写性能限制。
5. CPU 只可访问 64MiB DDR 窗口，不要用 CPU 地址判断完整 ring 数据。
6. TCP 下载查当前目录 `filelist.db`。如果运行目录不对，会查到空库或旧库；不要再从 `/mnt/spi1/filelist.db` 判断下载目标。
7. TCP与存储共享一个AXI DMA实例。正常切换只应halt对应方向；整核reset仅用于
   DMASR error恢复，否则可能因S2MM/SG时钟域未运行而卡住Reset bit。
8. 父进程默认只缓存worker尾部输出；需要把worker内部日志写入`tee`文件时，设置
   `SRC_REAL_DEBUG=WRITE,DMA,STORAGE`或`SRC_REAL_DEBUG=NET,TCP`，程序会转发对应
   storage/network worker输出。
