# 三通道 DMA→DDR→NVMe 上板测试方案

板卡程序固定为 `/tmp/storage.elf`，默认 UART 为 `/dev/ttyUL1`。程序不带参数启动，worker 由主进程自动创建，不要手动启动 `--storage-worker`。

所有测试使用相同的 FPGA bitstream、SSD、数据源、CPU 频率、输入时长、数据量和 UART 控制顺序。每组至少连续 5 次，每次使用不同 `TASK_ID`。建议每次写入 1 GiB。

## 0. 预检查

```bash
sudo -v
sudo id
sudo ls -l /tmp/storage.elf /dev/mem /dev/ttyUL1
sudo pgrep -a -f storage.elf || true
sudo dmesg -T | tail -n 100
```

若上一轮出现 `nvme_queue_reset_unavailable`、`ownership unresolved`、`submit_accept_timeout` 或 `writer_abort_timeout`，先保存日志并重新上电或 reset，不要直接重复。

```bash
export RUN_ID=C_run1
export RUN_DIR="$HOME/ccb_board_tests/$RUN_ID"
mkdir -p "$RUN_DIR"
sudo sh -c 'if [ -f /tmp/storage_perf.log ]; then mv /tmp/storage_perf.log /tmp/storage_perf.log.before_'"$RUN_ID"'; fi'
```

## 1. UART 顺序

启动程序后，通过原有 UART 控制端发送：

```text
0x11：任务配置/预启动
0x21 ON：开始采集
固定数据量输入
0x21 OFF：停止采集
```

不要修改串口帧格式和控制顺序。

## 2. 启动命令模板

`PROFILE`、`COMPAT`、`CMD_KIB` 按测试组替换。512 KiB profile 测试删除 `SRC_REAL_NVME_CMD_KIB`。

```bash
export RUN_ID=C_run1
export RUN_DIR="$HOME/ccb_board_tests/$RUN_ID"
mkdir -p "$RUN_DIR"
sudo sh -c 'cd /tmp && exec env UART_DEV_PATH=/dev/ttyUL1 CCB_STORAGE_PROFILE=PROFILE CCB_STORAGE_COMPAT_MODE=COMPAT CCB_PERF_ENABLE=1 CCB_PERF_INTERVAL_MS=1000 CCB_LOG_LEVEL=perf SRC_REAL_NVME_CMD_KIB=CMD_KIB /tmp/storage.elf' 2>&1 | tee "$RUN_DIR/main.log"
```

## 3. A～G 测试组

每组使用对应版本的 `/tmp/storage.elf`；同组重复测试时只递增 `RUN_ID`。

### A：原始 cross-slot + 256 KiB

```bash
export RUN_ID=A_run1
mkdir -p "$HOME/ccb_board_tests/$RUN_ID"
sudo sh -c 'cd /tmp && exec env UART_DEV_PATH=/dev/ttyUL1 CCB_STORAGE_PROFILE=PERF_QD8 CCB_STORAGE_COMPAT_MODE=1 CCB_PERF_ENABLE=1 CCB_PERF_INTERVAL_MS=1000 CCB_LOG_LEVEL=perf SRC_REAL_NVME_CMD_KIB=256 /tmp/storage.elf' 2>&1 | tee "$HOME/ccb_board_tests/$RUN_ID/main.log"
```

### B：legacy + 256 KiB + QD8

```bash
export RUN_ID=B_run1
mkdir -p "$HOME/ccb_board_tests/$RUN_ID"
sudo sh -c 'cd /tmp && exec env UART_DEV_PATH=/dev/ttyUL1 CCB_STORAGE_PROFILE=LEGACY_FAST_BASELINE CCB_STORAGE_COMPAT_MODE=1 CCB_PERF_ENABLE=1 CCB_PERF_INTERVAL_MS=1000 CCB_LOG_LEVEL=perf SRC_REAL_NVME_CMD_KIB=256 /tmp/storage.elf' 2>&1 | tee "$HOME/ccb_board_tests/$RUN_ID/main.log"
```

### C：legacy + 512 KiB + QD8

第一优先级基线，要求 `effective_command_bytes=524288`。

```bash
export RUN_ID=C_run1
mkdir -p "$HOME/ccb_board_tests/$RUN_ID"
sudo sh -c 'cd /tmp && exec env UART_DEV_PATH=/dev/ttyUL1 CCB_STORAGE_PROFILE=LEGACY_FAST_BASELINE CCB_STORAGE_COMPAT_MODE=0 CCB_PERF_ENABLE=1 CCB_PERF_INTERVAL_MS=1000 CCB_LOG_LEVEL=perf /tmp/storage.elf' 2>&1 | tee "$HOME/ccb_board_tests/$RUN_ID/main.log"
```

### D：queue lock 外 DMA requeue

部署包含 `debeb39` 的版本，命令与 C 相同。

```bash
export RUN_ID=D_run1
mkdir -p "$HOME/ccb_board_tests/$RUN_ID"
sudo sh -c 'cd /tmp && exec env UART_DEV_PATH=/dev/ttyUL1 CCB_STORAGE_PROFILE=LEGACY_FAST_BASELINE CCB_STORAGE_COMPAT_MODE=0 CCB_PERF_ENABLE=1 CCB_PERF_INTERVAL_MS=1000 CCB_LOG_LEVEL=perf /tmp/storage.elf' 2>&1 | tee "$HOME/ccb_board_tests/$RUN_ID/main.log"
```

### E：dynamic/emergency harvest

部署包含 `06b4e11` 的版本，命令与 C 相同。

```bash
export RUN_ID=E_run1
mkdir -p "$HOME/ccb_board_tests/$RUN_ID"
sudo sh -c 'cd /tmp && exec env UART_DEV_PATH=/dev/ttyUL1 CCB_STORAGE_PROFILE=LEGACY_FAST_BASELINE CCB_STORAGE_COMPAT_MODE=0 CCB_PERF_ENABLE=1 CCB_PERF_INTERVAL_MS=1000 CCB_LOG_LEVEL=perf /tmp/storage.elf' 2>&1 | tee "$HOME/ccb_board_tests/$RUN_ID/main.log"
```

### F：统计降频

部署包含 `69c1ee3` 的版本，命令与 C 相同。

```bash
export RUN_ID=F_run1
mkdir -p "$HOME/ccb_board_tests/$RUN_ID"
sudo sh -c 'cd /tmp && exec env UART_DEV_PATH=/dev/ttyUL1 CCB_STORAGE_PROFILE=LEGACY_FAST_BASELINE CCB_STORAGE_COMPAT_MODE=0 CCB_PERF_ENABLE=1 CCB_PERF_INTERVAL_MS=1000 CCB_LOG_LEVEL=perf /tmp/storage.elf' 2>&1 | tee "$HOME/ccb_board_tests/$RUN_ID/main.log"
```

### G：优化后的 cross-slot + 512 KiB

部署包含 `ec901b1` 的版本。

```bash
export RUN_ID=G_run1
mkdir -p "$HOME/ccb_board_tests/$RUN_ID"
sudo sh -c 'cd /tmp && exec env UART_DEV_PATH=/dev/ttyUL1 CCB_STORAGE_PROFILE=CROSS_SLOT_EXPERIMENTAL CCB_STORAGE_COMPAT_MODE=0 CCB_PERF_ENABLE=1 CCB_PERF_INTERVAL_MS=1000 CCB_LOG_LEVEL=perf /tmp/storage.elf' 2>&1 | tee "$HOME/ccb_board_tests/$RUN_ID/main.log"
```

B～G 使用历史阶段版本时，在隔离 worktree 中叠加 `fbadb40`，修正 `MaxTransferSize` 解析。

## 4. 日志保存与检查

```bash
export RUN_DIR="$HOME/ccb_board_tests/$RUN_ID"
mkdir -p "$RUN_DIR"
sudo cp /tmp/storage_perf.log "$RUN_DIR/" 2>/dev/null || true
sudo cp /tmp/logs.db "$RUN_DIR/" 2>/dev/null || true
sudo cp /tmp/filelist.db "$RUN_DIR/" 2>/dev/null || true
sudo dmesg -T | tail -n 200 > "$RUN_DIR/dmesg_tail.log"
```

检查有效配置和 NVMe 能力：

```bash
grep -E 'storage_effective_config|nvme_capability|Storage channel config' "$RUN_DIR/main.log"
```

检查 STOP 阶段和最终结果：

```bash
grep -E 'storage_stop_phase|stop_requested|packet_boundary|dma_quiesced|harvest_stable_empty|writer_drained|finalized|storage_final|storage_result' "$RUN_DIR/main.log"
```

检查严重错误：

```bash
grep -Ei 'unknown.*CID|ownership unresolved|submit_accept_timeout|completion.*lost|requeue_after_stop|descriptor ownership invariant failed|RT throttling|nvme_queue_reset_unavailable|writer_abort_timeout' "$RUN_DIR/main.log" "$RUN_DIR/storage_perf.log" || true
```

性能解析：

```bash
awk -v wanted_task=TASK_ID \
  -f /path/to/storage_perf_summary.awk \
  "$RUN_DIR/storage_perf.log" | tee "$RUN_DIR/summary.csv"
```

解析器退出码为 2 时，该轮测试作废。另开终端记录 CPU 和上下文切换：

```bash
sudo pidstat -u -w -p "$(pgrep -n -x storage.elf)" 1
```

## 5. 通过条件

每个 channel 的 `storage_final` 必须满足：

```text
ready_count=0
active_count=0
global_inflight=0
completed_unharvested=0
submit_count=completion_count
integrity_ok=1
persisted=1
```

每个 worker 必须只有一条 `storage_final` 和一条 `storage_result`。出现 reset、ownership、CID、submit timeout、completion 丢失或 STOP 后 requeue 时，该轮标记 `INVALID`，保存日志并重新上电后再测。

## 6. 结果填写模板

每次复制以下内容保存为 `$RUN_DIR/result.md`：

```text
测试组：
运行编号：
程序 commit：
FPGA bitstream：
SSD：
TASK_ID：
数据量/时长：
输入源：
CPU 频率：

profile：
pipeline_mode：
writer_mode：
requested_command_bytes：
effective_command_bytes：
nvme_max_transfer_raw：
nvme_max_transfer_bytes：
nvme_qd：
descriptor_bytes：

ch0 capture_window_mib_s：
ch1 capture_window_mib_s：
ch2 capture_window_mib_s：
ch0 nvme_wall_mib_s：
ch1 nvme_wall_mib_s：
ch2 nvme_wall_mib_s：
aggregate persisted MiB/s：
active_qd_avg/max：
ready_q_max：
completed_unharvested_max：
dma_writable_min：
submit_count/completion_count：
tail_unqueued_bytes：
persisted_bytes：
STOP 耗时：

final ready/active/global_inflight/completed_unharvested：
integrity_ok：
storage_result：
错误日志：
是否重新上电：
结论：PASS / FAIL / INVALID
备注：
```
