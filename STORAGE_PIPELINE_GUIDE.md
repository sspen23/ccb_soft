# 三通道 Aurora → DMA → DDR → NVMe/SSD 存储流程

本文面向第一次接触项目的开发者，说明当前软件如何接收三通道采集数据、写入DDR ring、提交NVMe，以及停止和调度逻辑。

## 1. 总体链路

```text
Aurora/采集源
  → AXI-Stream拆分、AXIS switch/FIFO
  → AXI DMA S2MM
  → DDR ring buffer
  → producer harvest
  → ready queue
  → NVMe SQ/CQ
  → SSD
```

软件不直接读取Aurora。软件初始化DMA、回收完成BD、管理DDR slot、提交NVMe命令、处理completion，并在停止时完成数据排空。

## 2. Aurora到DMA

ch0/ch1高速路径：

```text
Aurora 256-bit AXI-Stream
  → axis_256_to_2x128_split
  → ch0/ch1各自128-bit AXI-Stream
  → AXIS switch/FIFO
  → ch0/ch1 DMA S2MM
```

ch2使用独立的低速AXI-Stream路径：

```text
低速采集源 → AXIS FIFO/switch → ch2 DMA S2MM
```

DMA接口使用`TVALID/TREADY`握手，`TLAST`表示packet结束，`TKEEP`表示最后一个beat的有效字节。若上游不能接受反压，必须自己缓存数据；否则在`TREADY=0`时继续推进可能丢beat，软件只能发现短帧，不能恢复丢失数据。

## 3. DMA和DDR ring

PERF_QD8配置：

| 通道 | DDR ring | descriptor | BD数量 | writer |
|---|---:|---:|---:|---|
| ch0 | 1 GiB | 8 MiB | 128 | cross-slot |
| ch1 | 1 GiB | 8 MiB | 128 | cross-slot |
| ch2 | 512 MiB | 16 MiB | 32 | legacy |

每个BD包含next descriptor、DDR buffer地址、control长度和status。DMA完成后写入完成标志、实际长度、SOF/EOF等信息。软件从`status & DESC_STS_LEN_MASK`得到`actual_bytes`。

处理规则：

- 完整descriptor：放入ready queue。
- 512字节对齐partial：按实际长度写入NVMe。
- 非512字节对齐partial：不能直接提交NVMe，计入`tail_unqueued_bytes`并丢弃当前partial。
- DMA error、descriptor error或ownership不确定：硬失败。

## 4. Producer

Producer循环为：

```text
读取DMA状态
  → harvest completed BD
  → 检查BD status和ownership
  → 按submission_sequence确定顺序
  → 计算file offset/LBA
  → 有效slot进入ready queue
  → requeue descriptor
  → 更新统计
```

收到STOP或AUTO_DRAIN后：

```text
禁止新的descriptor requeue
  → 等待packet boundary
  → quiesce DMA
  → harvest剩余completed BD
  → stable-empty重复扫描
  → producer_done
```

`completed_unharvested`表示DMA已完成但软件尚未处理，属于后续harvest工作，不是quiesce失败条件。

## 5. Writer和NVMe

Writer从ready queue取DDR slot，将其拆成256 KiB command：

```text
ready queue取slot
  → 拆分256 KiB command
  → 分配LBA/PRP/CID
  → 提交NVMe SQ
  → 轮询CQ
  → CID匹配completion
  → 更新slot和字节账本
```

当前NVMe目标为`QD=8`。ch0/ch1使用cross-slot writer，最多4个active slot、CQ batch=8；ch2使用legacy writer，最多1个active slot。NVMe SQ是否真正填充，以`nvme_active_qd_avg`和`nvme_active_qd_max`判断。

## 6. 进程、线程和控制命令

父进程负责UART、worker创建、启动屏障、STOP、结果聚合和filelist。每个通道通常对应一个worker进程，worker内有producer和writer线程。

```text
0x11 PRESTART
  → 创建worker、初始化DMA/NVMe、等待READY、返回ACK

0x21 START
  → 发送ARM、启动DMA、等待ARMED
  → 发送RUN、等待RUNNING、返回ACK
```

当前本地代码中，PRESTART到START可以等待很长时间，不再使用5秒ARM等待超时。监督模式也不再自动注入30秒首帧超时。STOP、控制管道关闭或协议错误仍可唤醒worker。

## 7. CPU调度

默认使用普通CFS调度，不使用SCHED_RR：

```text
writer   = SCHED_OTHER
producer = SCHED_OTHER
RT priority = 0
```

PERF_QD8的CFS配置：

| 通道 | nominal输入 | writer weight | producer weight | writer nice | producer nice |
|---|---:|---:|---:|---:|---:|
| ch0 | 1200 MiB/s | 15 | 15 | -6 | -6 |
| ch1 | 1200 MiB/s | 15 | 15 | -6 | -6 |
| ch2 | 80 MiB/s | 3 | 1 | 2 | 6 |

producer权重按输入速率保护DMA收割；writer独立分配权重，给ch2 legacy writer保留最低写盘份额，同时让ch0/ch1继续保持NVMe SQ接近QD=8。调度只能延迟DDR耗尽，不能解决输入总速率长期大于SSD写入速率的问题。

## 8. INPUT_COMPLETE和STORAGE_DRAINED

`INPUT_COMPLETE`只表示输入稳定结束。单通道进入`INPUT_IDLE_CANDIDATE`需要：至少收到过数据、连续500 ms没有DMA活动、完成BD计数不变、没有半包和DMA错误。父进程只有确认三个目标通道都候选后，才触发统一AUTO_DRAIN。

`STORAGE_DRAINED`还必须同时满足：DMA已quiesce、`completed_unharvested=0`、ready/active/inflight均为0、`submit_count=completion_count`、`queued_payload_bytes=nvme_completed_payload_bytes`、`ring_occupied_bytes=0`。这些条件需要稳定重复观察，不能只根据queue empty判断。

## 9. STOP和DRAIN

自动结束和人工STOP共用同一套流程：

```text
DRAIN_REQUESTED
  → 禁止descriptor requeue
  → packet boundary
  → DMA quiesce
  → harvest completed BD
  → stable-empty harvest
  → writer drain ready queue
  → drain NVMe inflight
  → 检查字节账本
  → STORAGE_DRAINED
```

自动结束路径是`RUNNING → INPUT_COMPLETE → DRAIN → STORAGE_DRAINED → ready_for_stop=1 → STOP → FINALIZE`。提前STOP则是`RUNNING → STOP → DRAIN → STORAGE_DRAINED → 直接FINALIZE`。DRAINED之后的STOP不应再次quiesce、harvest或启动writer drain。

## 10. DRAIN_READY、FINAL和不完整数据

`DRAIN_READY`只表示数据面已经排空，不是最终任务结果。`FINAL`表示STOP后metadata、数据库、filelist和worker退出均已完成。

如果尾包不能安全写入，任务可以是`status=failed`，但仍然`data_persisted=1`，并以实际落盘的`file_bytes`更新filelist。字节账本必须满足：`dma_harvested_payload_bytes = queued_payload_bytes + tail_unqueued_bytes`，且`queued_payload_bytes = nvme_completed_payload_bytes`。
