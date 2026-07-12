# TCP 下载 512MiB 间隔重复问题代码说明

## 当前现象

TCP 下载后的文件仍存在 512MiB 间隔的数据重复。将下载流水改回接近旧行为的 `SRC_REAL_NETWORK_PIPELINE_SLOTS=1` 后仍复现；将 `CCB_NVME_MAX_SECTORS=256` 改小后重复间隔仍为 512MiB。

这两个对照结果说明：

- 问题不只是 TCP 下载 pipeline 的 DDR slot 复用导致。
- 问题不随单条 NVMe read command size 改变，基本排除“2048 条 256KiB command wrap”这个单一原因。
- 512MiB = `0x100000` sectors，仍然需要重点检查 SSD/NVMe read/write 的 LBA 解释是否存在低 20 bit 周期。

## 相关源码文件

### 1. 存储写盘主流程

文件：

- `src_real/src/ccb_commands.c`
- `vitis/storage/src/ccb_commands.c`

关键函数：

- `execute_write_with_result()`
- `storage_nvme_writer_thread()`
- `flush_slot_to_nvme()`
- `nvme_write_slot_qd()`

写盘逻辑：

1. `execute_write_with_result()` 打开 channel runtime，读取 metadata。
2. `metadata_alloc_slot_and_lba()` 根据已有 metadata 找到下一个 `start_lba`。
3. DMA S2MM 把 Aurora 数据搬到 DDR ring。
4. producer 把完成的 DMA slot 放入软件 queue。
5. producer 入队时记录该 slot 的 `start_lba`、`sectors` 和 DDR hardware address。
6. writer 线程从 queue 取 slot，调用 `flush_slot_to_nvme()`。
7. `flush_slot_to_nvme()` 将一个 DMA slot 拆成 NVMe write command 写入 SSD。
8. 文件结束后记录：
   - `start_lba`
   - `sector_count`
   - `file_size`
   - `file_index`

当前检查结论：

- 写盘路径的软件 LBA 是 `uint64_t`。
- 单文件内部 LBA 由 producer 按 `bytes_to_sectors(bytes)` 推进，没有 512MiB 取模。
- `metadata` 中 `start_lba` 是 `uint64_t`。
- `sector_count` 是 `uint32_t`，对当前小于 2TiB 的单文件不是 512MiB 问题来源。

### 2. Metadata 和 LBA 分配

文件：

- `src_real/src/ccb_metadata.c`
- `vitis/storage/src/ccb_metadata.c`
- `src_real/include/ccb_types.h`

关键函数：

- `metadata_alloc_slot_and_lba()`
- `metadata_check_lba_overlap()`
- `metadata_find_by_task()`

LBA 分配方式：

```c
next_lba = DATA_START_LBA;
for each valid metadata entry:
    end_lba = entry.start_lba + entry.sector_count;
    next_lba = max(next_lba, end_lba);
```

当前检查结论：

- 新文件使用已有文件最大 `end_lba` 后追加。
- 没有按 512MiB 或 `0x100000` sectors 回绕。
- 如果 metadata 本身记录正确，则下载起始 LBA 应该是连续递增的 SSD sector 地址。

### 3. DB 文件列表记录

文件：

- `src_real/src/system.c`
- `vitis/storage/src/system.c`
- `src_real/src/file_list.c`

关键函数：

- `record_storage_result_to_db()`
- `file_add()`
- `file_query_by_index()`

记录内容：

- `rec.start_sector = result->start_lba`
- `rec.sector_count = result->sector_count`
- `rec.file_size = result->size_bytes`
- `rec.channel_id = result->channel_id`
- `rec.proto_file_type_code = args->proto_file_type`

当前检查结论：

- DB 中 `start_sector` 和 `sector_count` 是 `uint64_t` 类型。
- 下载时按 task 编号和 file_index 查询 DB 记录。
- 软件层面没有发现 DB 查询后对 `start_sector` 做 512MiB 取模。

### 4. TCP 下载主流程

文件：

- `src_real/src/system.c`
- `vitis/storage/src/system.c`

关键函数：

- `network_send_existing_file()`
- `network_read_thread()`
- `network_send_thread()`

当前下载逻辑：

1. `file_query_by_index()` 查询文件 DB 记录。
2. 根据文件类型选择 channel：
   - LOW/CALIB -> ch2
   - HIGH_I -> ch0
3. `cur_lba = rec.start_sector`
4. 每个 TCP chunk 最大 16MiB：
   - `chunk_bytes = min(remaining, 16MiB)`
   - `read_sectors = bytes_to_sectors(chunk_bytes)`
5. producer 线程执行：
   - `nvme_rw(rt, false, cur_lba, read_sectors, ddr_hw)`
   - `cur_lba += read_sectors`
6. consumer 线程按 chunk index 顺序执行：
   - `tcp_transfer_send(ddr_hw, send_bytes)`

当前安全约束：

- `slot` 状态为 `FREE -> READING -> READY -> SENDING -> FREE`。
- TCP 发送完成后 slot 才释放给 NVMe 读线程复用。
- `SRC_REAL_NETWORK_PIPELINE_SLOTS=1` 时行为接近旧版单 buffer 串行下载。

当前检查结论：

- 下载路径中 `cur_lba` 是 `uint64_t`，每 chunk 按 `read_sectors` 递增。
- 没有 512MiB 取模。
- `SRC_REAL_NETWORK_PIPELINE_SLOTS=1` 仍复现，说明 pipeline slot 复用不是根因。

### 5. NVMe command 提交

文件：

- `src_real/src/ccb_hw.c`
- `vitis/storage/src/ccb_hw.c`

关键函数：

- `nvme_rw()`
- `nvme_issue_one_sync()`
- `nvme_write_slot_qd()`
- `nvme_submit_command_async()`

软件写入 Host Core 寄存器：

```c
reg_write32(QUEUE_CTX0, ((sectors - 1) << 16) | opcode);
reg_write32(QUEUE_CTX1, cid);
reg_write32(QUEUE_PRP1_L, hw_addr[31:0]);
reg_write32(QUEUE_PRP1_H, hw_addr[63:32]);
reg_write32(QUEUE_LBA_L, lba[31:0]);
reg_write32(QUEUE_LBA_H, lba[63:32]);
reg_write32(QUEUE_TX_CTRL, CMD_PENDING);
```

当前检查结论：

- 软件向 Host Core 写了 64-bit LBA 的低 32 位和高 32 位。
- 如果硬件正确使用 `QUEUE_LBA_L/H`，软件层面不应出现 512MiB 周期。
- 由于 FPGA 工程中 NVMe Host Core 是第三方 IP：`intelliprop.com:ip:iprop_nvme_host_core:1.80a`，普通 RTL 源码不可见，无法直接检查 IP 内部是否完整使用 SLBA 位宽。

### 6. TCP MM2S 发送

文件：

- `src_real/src/ccb_tcp_transfer.c`
- `vitis/storage/src/ccb_tcp_transfer.c`

关键函数：

- `tcp_transfer_send()`
- `tcp_prepare_desc()`
- `tcp_prepare_mm2s_mapped()`

发送逻辑：

1. 每个 TCP transfer 当前最多 16MiB。
2. 准备 AXI DMA MM2S descriptor。
3. descriptor buffer address = 当前 DDR slot 的硬件地址。
4. 设置 SOF/EOF。
5. 等待 descriptor complete。

当前检查结论：

- TCP 只按给定 DDR 地址和长度发送。
- 旧版固定 DDR 前 16MiB 也复现 512MiB 间隔重复，因此 TCP 侧不是首要疑点。

## 当前强疑点

### 疑点 1：NVMe Host Core 对 SLBA 只使用了低 20 bit 或存在 512MiB 地址周期

512MiB = `0x20000000` bytes = `0x100000` sectors。

如果 Host Core 在生成 NVMe command 时只使用了 SLBA 低 20 bit，或者某处对 SLBA 做了 `0xFFFFF` mask，则现象就是每 512MiB 重复。

这个疑点与以下现象一致：

- 改 `CCB_NVME_MAX_SECTORS` 后重复间隔仍是 512MiB。
- 固定 DDR 单 buffer 和下载 pipeline 都能复现。
- 软件层面的 LBA 变量没有 512MiB 取模。

### 疑点 2：写入阶段和读取阶段都经过同一个 Host Core LBA 问题

如果 write command 和 read command 都存在 512MiB LBA 周期，则 SSD 上的真实写入区域可能已经被周期覆盖，下载时只能读到重复数据。

需要区分：

- 写盘时已经重复/覆盖。
- 写盘正确，但读盘时 LBA 读取重复。

当前单靠 TCP 下载结果无法区分这两种情况。

### 疑点 3：第三方 Host Core 的 SQ/CQ 或命令表存在固定深度环回

由于 512MiB 也等于 `2048 * 256KiB`，曾怀疑 2048 条 command wrap。但将 `CCB_NVME_MAX_SECTORS` 改小后间隔仍为 512MiB，单纯 command count wrap 的可能性降低。

## 已加入的定位日志

文件：

- `src_real/src/system.c`
- `vitis/storage/src/system.c`

下载时每个 chunk 打印：

```text
[DBG][NET] read chunk=...
    lba=...
    bytes=...
    sectors=...
    nvme_cmd_first=...
    nvme_cmd_last=...
    nvme_cmd_sectors=...
```

512MiB 文件边界打印：

```text
[DBG][NET] 512MiB boundary ...
```

跨 2048 条 NVMe command 边界打印：

```text
[DBG][NET] 2048cmd boundary ...
```

## 推荐下一步一起检查

### 1. 对比写盘完成日志和下载日志

重点看：

- `storage_transfer_done ... start_lba=... sectors=...`
- `db_write_done`
- `[DBG][NET] read chunk=... lba=...`
- `[DBG][NET] 512MiB boundary ... lba=...`

预期：

- 下载 `chunk=0` 的 `lba` 应等于 DB 记录的 `start_sector`。
- 每个 16MiB chunk 的 LBA 应递增 `32768` sectors。
- 每 512MiB 文件偏移处 LBA 应递增 `0x100000` sectors。

### 2. 增加 SSD readback 自校验模式

建议增加一个单独 CLI 测试：

1. 在一个 LBA 写入 pattern A。
2. 在 `LBA + 0x100000` 写入 pattern B。
3. 分别读回两个位置。
4. 如果读回相同或互相覆盖，则证明 NVMe write/read 的 LBA 周期存在于 Host Core 或 SSD 访问路径。

这个测试不经过 TCP，可以区分 TCP 问题和 NVMe/SSD 问题。

### 3. FPGA / IP 侧需要确认的点

由于 `iprop_nvme_host_core` 源码不可见，需要从 IP 文档、BD 参数或 ILA 确认：

- Host Core 接收的 SLBA 是否为完整 64-bit。
- `QUEUE_LBA_L/H` 是否确实对应 Host Core 的 SLBA low/high。
- NVM SQ command 中 CDW10/CDW11 是否分别使用 SLBA low/high。
- ILA 抓取跨 512MiB 边界前后的 command：
  - opcode
  - cid
  - PRP1
  - SLBA low
  - SLBA high
  - NLB

### 4. 暂时保持的运行建议

定位期间建议使用：

```bash
sudo env SRC_REAL_NETWORK_PIPELINE_SLOTS=1 ./storage.elf
```

这样下载路径最接近旧版单 buffer，减少变量。

## 当前结论

目前没有在软件的 LBA 分配、DB 记录、下载 LBA 递增、TCP 发送逻辑中找到 512MiB 取模或周期复用。

如果下一次日志显示：

- 文件偏移相差 512MiB；
- 软件打印的 LBA 也相差 `0x100000` sectors；
- 但下载数据仍重复；

则最强疑点是 NVMe Host Core 或其寄存器/command 映射对 SLBA 的高位处理存在问题，而不是 Linux C 程序中的 chunk/DDR/TCP 逻辑。
