这是一个基于 Xilinx KU115 的 FPGA 工程，Vivado block design 名称是 design_1。板上运行 MicroBlaze + PetaLinux，软件通过 AXI-Lite/MMIO 控制 FPGA 中的 DMA、AXIS switch、NVMe Host Core、TCP 发送模块等。

  系统目标：
  把外部采集数据分成三个通道写入三块 SSD，并支持从 SSD 读回后通过 TCP AXI-Stream 发送出去。

  三条数据通道：
  1. ch0：高速 I 路，HIGH_I，文件类型 FILE_TYPE_I = 1
  2. ch1：高速 Q 路，HIGH_Q，文件类型 FILE_TYPE_Q = 2
  3. ch2：低速/包络/校准路，LOW_SPEED/CALIB，文件类型 FILE_TYPE_LOW = 0 或 FILE_TYPE_CALIB = 3

  每个通道都有：
  - 独立 AXI DMA
  - 独立 DDR 数据缓冲
  - 独立 NVMe Host Core
  - 独立 PCIe SSD 链路
  - 独立通道内部 AXIS switch
  - 独立 descriptor BRAM

  MicroBlaze 软件不搬运大数据，只做控制：
  - 配置 AXIS switch
  - 配置 AXI DMA SG descriptor ring
  - 配置 NVMe Host Core command register
  - 管理文件 metadata 和任务状态
  - 控制 TCP 下载路径

  串口协议控制：
  - 0x11：下发任务，启动对应通道的存盘 worker
  - 0x21：开始/停止采集
  - 0x41：查询文件列表
  - 0x51：文件下载/删除
  - 0x61：检查 ch0/ch1/ch2 SSD 状态
  - 0x71：停止 TCP 网络传输

  任务模式：
  - 0x11 calib only：只启动 ch2，保存校准文件
  - 0x22 low only：只启动 ch2，保存低速文件
  - 0x33 high only：启动 ch0 + ch1，保存高速 I/Q
  - 0xAA all：启动 ch2 + ch0 + ch1，低速和高速 I/Q 全部保存

  整体存盘数据流：
  外部 AXI-Stream 采集数据
  -> 通道内部 AXIS switch
  -> AXIS FIFO
  -> AXI DMA S2MM
  -> 通道 DDR ring
  -> NVMe Host Core 从 DDR 读数据
  -> SSD

  整体下载数据流：
  SSD
  -> NVMe Host Core read
  -> 通道 DDR
  -> AXI DMA MM2S
  -> clock converter / data width converter / FIFO
  -> 顶层 TCP AXIS switch
  -> tcp_top_axis_stream_0/s_axis_tcp_tx_0
  -> TCP/RGMII 发送

  ------------------------------------------------------------
  ch0/ch1 高速采集输入路径
  ------------------------------------------------------------

  ch0/ch1 不是各自直接接一个 Aurora。当前 block design 中，高速 Aurora 输出是 256-bit AXI-Stream，然后通过一个 split 模块拆成两个 128-bit AXI-Stream：

  aurora_64b66b_0/USER_DATA_M_AXIS_RX
    -> axis_256_to_2x128_sp_0/s_axis

  axis_256_to_2x128_sp_0/m_axis_hi
    -> adh_data_channel_0/Aurora_S00_AXIS_0
    -> adh_data_channel_0/axis_switch_0/S00_AXIS

  axis_256_to_2x128_sp_0/m_axis_lo
    -> adh_data_channel_1/Aurora_S00_AXIS_0
    -> adh_data_channel_1/axis_switch_0/S00_AXIS

  也就是说：
  - 高速 Aurora 的高半部分进入 ch0
  - 高速 Aurora 的低半部分进入 ch1

  ch0 内部存盘路径：
  axis_256_to_2x128_sp_0/m_axis_hi
    -> adh_data_channel_0/Aurora_S00_AXIS_0
    -> adh_data_channel_0/axis_switch_0/S00_AXIS
    -> adh_data_channel_0/axis_switch_0/M00_AXIS
    -> axis_data_fifo_0/S_AXIS
    -> axis_data_fifo_0/M_AXIS
    -> axi_dma_0/S_AXIS_S2MM
    -> axi_dma_0/M_AXI_S2MM
    -> axi_interconnect_1/S01_AXI
    -> ddr4_64b_0/C0_DDR4_S_AXI

  ch1 内部存盘路径：
  axis_256_to_2x128_sp_0/m_axis_lo
    -> adh_data_channel_1/Aurora_S00_AXIS_0
    -> adh_data_channel_1/axis_switch_0/S00_AXIS
    -> adh_data_channel_1/axis_switch_0/M00_AXIS
    -> axis_data_fifo_0/S_AXIS
    -> axis_data_fifo_0/M_AXIS
    -> axi_dma_0/S_AXIS_S2MM
    -> axi_dma_0/M_AXI_S2MM
    -> axi_interconnect_1/S01_AXI
    -> ddr4_64b_0/C0_DDR4_S_AXI

  ch0/ch1 内部 AXIS switch 还有测试输入：
  axis_counter_0/m_axis
    -> channel axis_switch_0/S01_AXIS

  软件选择：
  - SOURCE_TRANSFER -> switch input 0
  - SOURCE_TEST -> switch input 1

  ------------------------------------------------------------
  ch2 低速/校准采集输入路径
  ------------------------------------------------------------

  ch2 是独立的低速 Aurora 输入，不经过 ch0/ch1 的 256-to-2x128 split。

  外部 adl Aurora RX
    -> adl_data_channel_2/AURORA_RX
    -> adl_data_channel_2 内部 aurora_64b66b_0
    -> adl_data_channel_2/axis_switch_0/S00_AXIS
    -> adl_data_channel_2/axis_switch_0/M00_AXIS
    -> axis_data_fifo_0/S_AXIS
    -> axis_data_fifo_0/M_AXIS
    -> axi_dma_0/S_AXIS_S2MM
    -> axi_dma_0/M_AXI_S2MM
    -> axi_interconnect_1/S01_AXI
    -> ddr4_16b_0/C0_DDR4_S_AXI

  ch2 内部 AXIS switch 也有测试输入：
  axis_counter_0/m_axis
    -> adl_data_channel_2/axis_switch_0/S01_AXIS

  ------------------------------------------------------------
  每通道硬件地址
  ------------------------------------------------------------

  ch0 HIGH_I：
  - NVMe Host Core AXI-Lite base: 0x44A00000
  - AXI DMA AXI-Lite base: 0x41E00000
  - 通道内部 AXIS switch base: 0x44A10000
  - descriptor CPU base: 0x20000000
  - descriptor DMA-view base: 0x10000000
  - descriptor BRAM size: 0x4000
  - DDR CPU-visible mapping: none
  - DDR DMA/NVMe hardware-view base: 0x00000000
  - DDR DMA/NVMe hardware-view range: 2 GiB
  - manual PRP BRAM CPU/NVMe base: 0xC0000000
  - manual PRP BRAM size: 32 KiB
  - 软件默认 ring: 1 GiB
  - 软件允许最大 ring: 2 GiB
  - 默认 DMA descriptor payload: 16 MiB

  ch1 HIGH_Q：
  - NVMe Host Core AXI-Lite base: 0xA0080000
  - AXI DMA AXI-Lite base: 0xA0060000
  - 通道内部 AXIS switch base: 0xA0070000
  - descriptor CPU base: 0x30000000
  - descriptor DMA-view base: 0x10000000
  - descriptor BRAM size: 0x4000
  - DDR CPU-visible mapping: none
  - DDR DMA/NVMe hardware-view base: 0x00000000
  - DDR DMA/NVMe hardware-view range: 2 GiB
  - manual PRP BRAM CPU/NVMe base: 0xC2000000
  - manual PRP BRAM size: 32 KiB
  - 软件默认 ring: 1 GiB
  - 软件允许最大 ring: 2 GiB
  - 默认 DMA descriptor payload: 16 MiB

  ch2 LOW_SPEED/CALIB：
  - NVMe Host Core AXI-Lite base: 0x00010000
  - AXI DMA AXI-Lite base: 0x00030000
  - 通道内部 AXIS switch base: 0x00040000
  - descriptor CPU base: 0x20004000
  - descriptor DMA-view base: 0x10000000
  - descriptor BRAM size: 0x4000
  - DDR CPU-visible mapping: none
  - DDR DMA/NVMe hardware-view base: 0x00000000
  - DDR DMA/NVMe hardware-view range: 512 MiB
  - Host Core PRP mode: auto
  - 软件 ring: 512 MiB
  - 默认 DMA descriptor payload: 16 MiB

  注意：
  通道数据DDR不在CPU地址空间中。
  DMA descriptor、NVMe PRP和TCP MM2S必须使用硬件视角地址，也就是
  0x00000000 + offset。

  ------------------------------------------------------------
  NVMe Host Core
  ------------------------------------------------------------

  三个通道都使用 IntelliProp NVMe Host Core：
  intelliprop.com:ip:iprop_nvme_host_core:1.80a

  共同点：
  - USE_CMD_REGISTERS = TRUE
  - 软件通过 command register 提交 NVMe read/write
  - 软件不直接维护标准内存 SQ/CQ
  - Completion 通过 Host Core 的 CQ FIFO / CID register 读取

  ch2 NVMe Host Core：
  - DATA_BUFFER_ADDRESS = 0x0000000000000000
  - DATA_BUFFER_SPAN    = 0x0000000020000000，即 512 MiB
  - NVM_SQ_ADDRESS      = 0x00000000A0000000
  - NVM_CQ_ADDRESS      = 0x00000000A0010000
  - ADMIN_SQ_ADDRESS    = 0x00000000A2020000
  - ADMIN_CQ_ADDRESS    = 0x00000000A2021000
  - ADMIN_MEM_ADDRESS   = 0x00000000A0200000
  - PCIE_S_AXI_ADDR     = 0x0000000000000000
  - PCIE_S_AXI_CTL_ADDR = 0x0000000080000000

  ch0 NVMe Host Core：
  - DATA_BUFFER_ADDRESS = 0x0000000000000000
  - DATA_BUFFER_SPAN    = 0x0000000080000000，即 2 GiB
  - NVM_SQ_ADDRESS      = 0x00000000A0000000
  - NVM_CQ_ADDRESS      = 0x00000000A0010000
  - ADMIN_SQ_ADDRESS    = 0x00000000A0020000
  - ADMIN_CQ_ADDRESS    = 0x00000000A0021000
  - ADMIN_MEM_ADDRESS   = 0x00000000A0200000
  - PCIE_S_AXI_ADDR     = 0x0000000000000000
  - PCIE_S_AXI_CTL_ADDR = 0x0000000080000000

  ch1 NVMe Host Core：
  - DATA_BUFFER_ADDRESS = 0x0000000000000000
  - DATA_BUFFER_SPAN    = 0x0000000080000000，即 2 GiB
  - NVM_SQ_ADDRESS      = 0x00000000A0000000
  - NVM_CQ_ADDRESS      = 0x00000000A0010000
  - ADMIN_SQ_ADDRESS    = 0x00000000A1020000
  - ADMIN_CQ_ADDRESS    = 0x00000000A1021000
  - ADMIN_MEM_ADDRESS   = 0x00000000A0200000
  - PCIE_S_AXI_ADDR     = 0x0000000000000000
  - PCIE_S_AXI_CTL_ADDR = 0x0000000080000000

  软件 NVMe 写盘逻辑：
  1. AXI DMA S2MM 先把采集数据写到 DDR ring。
  2. 每个 DMA descriptor 默认 16 MiB，对应一个 DDR slot。
  3. descriptor 完成后，软件让 NVMe Host Core 从对应 DDR offset 读数据并写 SSD。
  4. NVMe PRP 地址使用 DDR hardware-view offset，不使用 CPU mmap 地址。
  5. 软件当前支持 NVMe QD 1/2/4/8/16/32。
  6. ch0/ch1 默认 QD=8，ch2 默认 QD=4。
  7. 在一个 DDR slot 内，软件先尽量提交到配置 QD，然后每完成一个 CQ completion 就 refill 一个新 command。
  8. `storage_worker_done` 会输出 nvme_cmd_count、nvme_write_ms、nvme_active_qd_max、nvme_active_qd_avg、ring_full_count 等统计。

  ------------------------------------------------------------
  TCP 下载路径
  ------------------------------------------------------------

  TCP 下载不是走每通道内部 storage switch，而是走顶层 TCP AXIS switch。

  顶层 TCP AXIS switch：
  - instance: axis_switch_0
  - AXI-Lite control base: 0xA0040000
  - NUM_SI = 3
  - M00_AXIS -> tcp_top_axis_stream_0/s_axis_tcp_tx_0

  ch0 下载路径：
  adh_data_channel_0/M_AXIS_MM2S
    -> axis_clock_converter_0/S_AXIS
    -> axis_clock_converter_0/M_AXIS
    -> axis_dwidth_converter_0/S_AXIS
    -> axis_dwidth_converter_0/M_AXIS
    -> axis_data_fifo_0/S_AXIS
    -> axis_data_fifo_0/M_AXIS
    -> top axis_switch_0/S00_AXIS

  ch1 下载路径：
  adh_data_channel_1/M_AXIS_MM2S
    -> axis_clock_converter_1/S_AXIS
    -> axis_clock_converter_1/M_AXIS
    -> axis_dwidth_converter_1/S_AXIS
    -> axis_dwidth_converter_1/M_AXIS
    -> axis_data_fifo_1/S_AXIS
    -> axis_data_fifo_1/M_AXIS
    -> top axis_switch_0/S01_AXIS

  ch2 下载路径：
  adl_data_channel_2/M_AXIS_MM2S
    -> axis_clock_converter_2/S_AXIS
    -> axis_clock_converter_2/M_AXIS
    -> axis_dwidth_converter_2/S_AXIS
    -> axis_dwidth_converter_2/M_AXIS
    -> axis_data_fifo_2/S_AXIS
    -> axis_data_fifo_2/M_AXIS
    -> top axis_switch_0/S02_AXIS

  top axis_switch_0/M00_AXIS
    -> tcp_top_axis_stream_0/s_axis_tcp_tx_0

  软件 TCP switch 选择：
  - 下载 ch0 文件时选 input 0
  - 下载 ch1 文件时选 input 1
  - 下载 ch2 文件时选 input 2

  网络发送时：
  1. 软件根据 filelist.db 找到文件 metadata。
  2. 根据文件类型决定通道：
     - FILE_TYPE_I -> ch0
     - FILE_TYPE_Q -> ch1
     - FILE_TYPE_LOW / FILE_TYPE_CALIB -> ch2
  3. NVMe Host Core 从 SSD 读数据到该通道 DDR。
  4. AXI DMA MM2S 从 DDR 发 AXI-Stream。
  5. 顶层 TCP switch 选对应输入送到 tcp_top_axis_stream。
  6. 默认每个 TCP DMA chunk 是 16 MiB，由 MM2S descriptor EOF 产生 TLAST。
  7. TCP 传输默认不限速；SRC_REAL_NETWORK_LIMIT_MB_S 非 0 时才限速。
  8. 网络 worker 默认没有总任务超时；SRC_REAL_NETWORK_TASK_TIMEOUT_SECONDS 非 0 时才启用总超时。

  ------------------------------------------------------------
  当前性能和限制
  ------------------------------------------------------------

  1. 上游是 ADC/Aurora 数据流，无法由软件反压或降速。
  2. 如果平均输入速率大于 SSD 写盘速率，DDR ring 迟早会满。
  3. ch0/ch1 当前输入速率约每通道 1.5 GB/s。
  4. ch0/ch1 软件默认只使用低 1 GiB DDR ring，但可通过环境变量改成 2 GiB。
  5. 当前测试中 raw DDR-to-NVMe 写 64 MiB 大约 65~76 ms，说明纯 DDR->NVMe->SSD 路径可达到约 0.8~1.0 GB/s 级别。
  6. 连续采集路径中，ch0/ch1 仍可能出现 ring_full_count，说明采集写入 DDR 的速度长期高于后台 NVMe drain 速度。
  7. 当前瓶颈更可能在连续采集路径的 DMA ring、DDR 竞争、worker 调度、NVMe Host Core/SSD 持续写组合，而不是单次 raw NVMe 写能力。
  8. ch2 是低速通道，DDR ring 512 MiB，默认 QD=4，通常不会像 ch0/ch1 那样成为高速存储瓶颈。










# FPGA Block Design and Address Map Notes

This note is extracted from the current Vivado block design:

`/home/sspen/sspen/Projects/nvme/DBQ_CCB/DBQ_CCB_ku115/DBQ_CCB_ku115.srcs/sources_1/bd/design_1/design_1.bd`

and cross-checked against:

- `src_real/src/ccb_config.c`
- `src_real/include/ccb_config.h`
- `src_real/src/ccb_hw.c`
- NVMe Host Core wrapper files under `DBQ_CCB_ku115.ip_user_files/bd/design_1/ipshared/ab03/src/`

## 1. CPU-visible MMIO and shared-BRAM address map

MicroBlaze `/microblaze_0` Data address space:

| Function | BD segment | CPU base | Range | Software config |
|---|---|---:|---:|---|
| ch2 DMA SG descriptor BRAM CPU view | `/adl_data_channel_2/axi_bram_ctrl_0/S_AXI/Mem0` | `0x20004000` | `16K` | `desc_cpu_base=0x20004000`, `desc_cpu_size=0x4000` |
| ch0 DMA SG descriptor BRAM CPU view | `/adh_data_channel_0/axi_bram_ctrl_0/S_AXI/Mem0` | `0x20000000` | `16K` | `desc_cpu_base=0x20000000`, `desc_cpu_size=0x4000` |
| ch1 DMA SG descriptor BRAM CPU view | `/adh_data_channel_1/axi_bram_ctrl_0/S_AXI/Mem0` | `0x30000000` | `16K` | `desc_cpu_base=0x30000000`, `desc_cpu_size=0x4000` |
| ch0 manual PRP BRAM | `/shared_mem_ch0/axi_bram_ctrl_0/S_AXI/Mem0` | `0xC0000000` | `32K` | `prp_list_cpu_base=prp_list_hw_base=0xC0000000` |
| ch1 manual PRP BRAM | `/shared_mem_ch1/axi_bram_ctrl_0/S_AXI/Mem0` | `0xC2000000` | `32K` | `prp_list_cpu_base=prp_list_hw_base=0xC2000000` |
| ch2 shared BRAM, auto PRP mode | `/shared_mem_ch2/axi_bram_ctrl_0/S_AXI/Mem0` | `0xC4000000` | `32K` | software does not use it for PRP lists |
| ch2 AXI DMA control | `/adl_data_channel_2/axi_dma_0/S_AXI_LITE/Reg` | `0x00030000` | `64K` | `dma_base=0x00030000` |
| ch0 AXI DMA control | `/adh_data_channel_0/axi_dma_0/S_AXI_LITE/Reg` | `0x41E00000` | `64K` | `dma_base=0x41E00000` |
| ch1 AXI DMA control | `/adh_data_channel_1/axi_dma_0/S_AXI_LITE/Reg` | `0xA0060000` | `64K` | `dma_base=0xA0060000` |
| ch2 internal AXIS switch control | `/adl_data_channel_2/axis_switch_0/S_AXI_CTRL/Reg` | `0x00040000` | `64K` | `axis_switch_base=0x00040000` |
| ch0 internal AXIS switch control | `/adh_data_channel_0/axis_switch_0/S_AXI_CTRL/Reg` | `0x44A10000` | `64K` | `axis_switch_base=0x44A10000` |
| ch1 internal AXIS switch control | `/adh_data_channel_1/axis_switch_0/S_AXI_CTRL/Reg` | `0xA0070000` | `64K` | `axis_switch_base=0xA0070000` |
| top TCP AXIS switch control | `/axis_switch_0/S_AXI_CTRL/Reg` | `0xA0040000` | `64K` | `TCP_SWITCH_BASE_DEFAULT=0xA0040000` |
| ch2 NVMe Host Core command/control regs | `/adl_data_channel_2/NVMe_hier_0/nvme_host_core_0/S_AXI/reg0` | `0x00010000` | `64K` | `nvme_base=0x00010000` |
| ch0 NVMe Host Core command/control regs | `/adh_data_channel_0/NVMe_hier_0/nvme_host_core_0/S_AXI/reg0` | `0x44A00000` | `64K` | `nvme_base=0x44A00000` |
| ch1 NVMe Host Core command/control regs | `/adh_data_channel_1/NVMe_hier_0/nvme_host_core_0/S_AXI/reg0` | `0xA0080000` | `64K` | `nvme_base=0xA0080000` |
| SSD reset GPIO, active-low output | `/axi_gpio_ssd_rstn/S_AXI/Reg` | `0x40010000` | `64K` | used by `ssd_rst` |
| MicroBlaze main DDR | `/ddr4_0/C0_DDR4...` | `0x80000000` | `512M` | Linux memory area |

Notes:

- ch0/ch1/ch2 descriptor BRAM is `16K` in BD and current `src_real` software uses the full `0x4000` bytes.
- ch0/ch1/ch2 channel data DDR does not appear in the MicroBlaze Data address
  space. `0xC0000000`, `0xC2000000`, and `0xC4000000` are shared BRAM
  apertures, not data-DDR windows.

## 2. DMA/NVMe hardware-view data address map

Channel data DDR has no CPU physical address in the current design.

| Path | Master address space | Target DDR | Master-view base | Master-view range | CPU view |
|---|---|---|---:|---:|---:|
| ch2 AXI DMA SG | `Data_SG` | ch2 descriptor BRAM | `0x10000000` | `16K` | CPU writes desc at `0x20004000` |
| ch2 AXI DMA MM2S | `Data_MM2S` | ch2 DDR | `0x00000000` | `512M` | not CPU-mapped |
| ch2 AXI DMA S2MM | `Data_S2MM` | ch2 DDR | `0x00000000` | `512M` | not CPU-mapped |
| ch2 NVMe/PCIe M_AXI | `M_AXI` | ch2 DDR | `0x00000000` | `512M` | not CPU-mapped |
| ch0 AXI DMA SG | `Data_SG` | ch0 descriptor BRAM | `0x10000000` | `16K` | CPU writes desc at `0x20000000` |
| ch0 AXI DMA MM2S | `Data_MM2S` | ch0 DDR | `0x00000000` | `2G` | not CPU-mapped |
| ch0 AXI DMA S2MM | `Data_S2MM` | ch0 DDR | `0x00000000` | `2G` | not CPU-mapped |
| ch0 NVMe/PCIe M_AXI | `M_AXI` | ch0 DDR | `0x00000000` | `2G` | not CPU-mapped |
| ch1 AXI DMA SG | `Data_SG` | ch1 descriptor BRAM | `0x10000000` | `16K` | CPU writes desc at `0x30000000` |
| ch1 AXI DMA MM2S | `Data_MM2S` | ch1 DDR | `0x00000000` | `2G` | not CPU-mapped |
| ch1 AXI DMA S2MM | `Data_S2MM` | ch1 DDR | `0x00000000` | `2G` | not CPU-mapped |
| ch1 NVMe/PCIe M_AXI | `M_AXI` | ch1 DDR | `0x00000000` | `2G` | not CPU-mapped |

Software consequence:

- DMA BD buffer addresses and NVMe PRP addresses must use `ddr_hw_base + offset`, currently `0x00000000 + offset`.
- Software does not map channel data DDR through `/dev/mem`; APIs pass
  `ddr_offset` or `ddr_hw_addr`.

## 3. Storage RX datapath

### ch2 storage path

BD interface connections:

```text
Aurora USER_DATA_M_AXIS_RX
  -> adl_data_channel_2/axis_switch_0/S00_AXIS
axis_counter_0/m_axis
  -> adl_data_channel_2/axis_switch_0/S01_AXIS
adl_data_channel_2/axis_switch_0/M00_AXIS
  -> axis_data_fifo_0/S_AXIS
axis_data_fifo_0/M_AXIS
  -> axi_dma_0/S_AXIS_S2MM
axi_dma_0/M_AXI_S2MM
  -> axi_interconnect_1/S01_AXI
axi_interconnect_1/M00_AXI
  -> ddr4_16b_0/C0_DDR4_S_AXI
```

Control:

- ch2 switch control base: `0x00040000`
- AXIS switch register offsets used by software:
  - `0x0000`: control
  - `0x0040`: `MI0_MUX`
  - update bit: bit 1
- Current software selection:
  - `SOURCE_TRANSFER` -> input `0`
  - `SOURCE_TEST` -> input `1`

### ch0 storage path

The high-rate Aurora stream is split before it enters the ch0/ch1 channel
hierarchies:

```text
aurora_64b66b_0/USER_DATA_M_AXIS_RX
  -> axis_256_to_2x128_sp_0/s_axis
axis_256_to_2x128_sp_0/m_axis_hi
  -> adh_data_channel_0/Aurora_S00_AXIS_0
adh_data_channel_0/Aurora_S00_AXIS_0
  -> adh_data_channel_0/axis_switch_0/S00_AXIS
```

The rest of the ch0 channel-local storage path is:

```text
axis_counter_0/m_axis
  -> adh_data_channel_0/axis_switch_0/S01_AXIS
adh_data_channel_0/axis_switch_0/M00_AXIS
  -> axis_data_fifo_0/S_AXIS
axis_data_fifo_0/M_AXIS
  -> axi_dma_0/S_AXIS_S2MM
axi_dma_0/M_AXI_S2MM
  -> axi_interconnect_1/S01_AXI
axi_interconnect_1/M00_AXI
  -> ddr4_64b_0/C0_DDR4_S_AXI
```

Control:

- ch0 switch control base: `0x44A10000`
- ch0 DMA base: `0x41E00000`

### ch1 storage path

ch1 receives the low half of the same high-rate Aurora split:

```text
aurora_64b66b_0/USER_DATA_M_AXIS_RX
  -> axis_256_to_2x128_sp_0/s_axis
axis_256_to_2x128_sp_0/m_axis_lo
  -> adh_data_channel_1/Aurora_S00_AXIS_0
adh_data_channel_1/Aurora_S00_AXIS_0
  -> adh_data_channel_1/axis_switch_0/S00_AXIS
```

The rest of the ch1 channel-local storage path is:

```text
axis_counter_0/m_axis
  -> adh_data_channel_1/axis_switch_0/S01_AXIS
adh_data_channel_1/axis_switch_0/M00_AXIS
  -> axis_data_fifo_0/S_AXIS
axis_data_fifo_0/M_AXIS
  -> axi_dma_0/S_AXIS_S2MM
axi_dma_0/M_AXI_S2MM
  -> axi_interconnect_1/S01_AXI
axi_interconnect_1/M00_AXI
  -> ddr4_64b_0/C0_DDR4_S_AXI
```

Control:

- ch1 switch control base: `0xA0070000`
- ch1 DMA base: `0xA0060000`

## 4. TCP download datapath

Top-level TCP path is separate from the per-channel storage RX switches.

BD interface connections:

```text
adh_data_channel_0/M_AXIS_MM2S
  -> axis_clock_converter_0/S_AXIS
axis_clock_converter_0/M_AXIS
  -> axis_dwidth_converter_0/S_AXIS
axis_dwidth_converter_0/M_AXIS
  -> axis_data_fifo_0/S_AXIS
axis_data_fifo_0/M_AXIS
  -> top axis_switch_0/S00_AXIS

adl_data_channel_2/M_AXIS_MM2S
  -> axis_clock_converter_2/S_AXIS
axis_clock_converter_2/M_AXIS
  -> axis_dwidth_converter_2/S_AXIS
axis_dwidth_converter_2/M_AXIS
  -> axis_data_fifo_2/S_AXIS
axis_data_fifo_2/M_AXIS
  -> top axis_switch_0/S02_AXIS

adh_data_channel_1/M_AXIS_MM2S
  -> axis_clock_converter_1/S_AXIS
axis_clock_converter_1/M_AXIS
  -> axis_dwidth_converter_1/S_AXIS
axis_dwidth_converter_1/M_AXIS
  -> axis_data_fifo_1/S_AXIS
axis_data_fifo_1/M_AXIS
  -> top axis_switch_0/S01_AXIS

top axis_switch_0/M00_AXIS
  -> tcp_top_axis_stream_0/s_axis_tcp_tx_0
```

Top TCP switch:

- AXIS switch base: `0xA0040000`
- `S00_AXIS` = ch0 MM2S stream
- `S01_AXIS` = ch1 MM2S stream
- `S02_AXIS` = ch2 MM2S stream
- Software should select:
  - ch0 download: input `0`
  - ch1 download: input `1`
  - ch2 download: input `2`

Current `src_real` note:

- `TCP_SWITCH_BASE_DEFAULT=0xA0040000`
- `TCP_DMA_BASE_DEFAULT=0x41E00000`
- `TCP_DESC_CPU_BASE_DEFAULT=0x20000000`
- Those defaults are ch0-compatible.
- The network-send path overrides switch input per channel in `system.c`; keep checking that ch0/ch1/ch2 downloads use input `0/1/2`.

## 5. NVMe Host Core address regions and queue memory

The IP is `intelliprop.com:ip:iprop_nvme_host_core:1.80a`.

### ch2 NVMe Host Core instance

Instance:

`adl_data_channel_2/NVMe_hier_0/nvme_host_core_0`

Parameters:

| Parameter | Value |
|---|---:|
| `USE_CMD_REGISTERS` | `TRUE` |
| `DATA_BUFFER_ADDRESS` | `0x0000000000000000` |
| `DATA_BUFFER_SPAN` | `0x0000000020000000` (`512M`) |
| `NVM_SQ_ADDRESS` | `0x00000000A0000000` |
| `NVM_CQ_ADDRESS` | `0x00000000A0010000` |
| `ADMIN_SQ_ADDRESS` | `0x00000000A2020000` |
| `ADMIN_CQ_ADDRESS` | `0x00000000A2021000` |
| `ADMIN_MEM_ADDRESS` | `0x00000000A0200000` |
| `PCIE_S_AXI_ADDR` | `0x0000000000000000` |
| `PCIE_S_AXI_CTL_ADDR` | `0x0000000080000000` |

### ch0 NVMe Host Core instance

Instance:

`adh_data_channel_0/NVMe_hier_0/nvme_host_core_0`

Parameters:

| Parameter | Value |
|---|---:|
| `USE_CMD_REGISTERS` | `TRUE` |
| `DATA_BUFFER_ADDRESS` | `0x0000000000000000` |
| `DATA_BUFFER_SPAN` | `0x0000000080000000` (`2G`) |
| `NVM_SQ_ADDRESS` | `0x00000000A0000000` |
| `NVM_CQ_ADDRESS` | `0x00000000A0010000` |
| `ADMIN_SQ_ADDRESS` | `0x00000000A0020000` |
| `ADMIN_CQ_ADDRESS` | `0x00000000A0021000` |
| `ADMIN_MEM_ADDRESS` | `0x00000000A0200000` |
| `PCIE_S_AXI_ADDR` | `0x0000000000000000` |
| `PCIE_S_AXI_CTL_ADDR` | `0x0000000080000000` |

### ch1 NVMe Host Core instance

Instance:

`adh_data_channel_1/NVMe_hier_0/nvme_host_core_0`

Parameters:

| Parameter | Value |
|---|---:|
| `USE_CMD_REGISTERS` | `TRUE` |
| `DATA_BUFFER_ADDRESS` | `0x0000000000000000` |
| `DATA_BUFFER_SPAN` | `0x0000000080000000` (`2G`) |
| `NVM_SQ_ADDRESS` | `0x00000000A0000000` |
| `NVM_CQ_ADDRESS` | `0x00000000A0010000` |
| `ADMIN_SQ_ADDRESS` | `0x00000000A1020000` |
| `ADMIN_CQ_ADDRESS` | `0x00000000A1021000` |
| `ADMIN_MEM_ADDRESS` | `0x00000000A0200000` |
| `PCIE_S_AXI_ADDR` | `0x0000000000000000` |
| `PCIE_S_AXI_CTL_ADDR` | `0x0000000080000000` |

## 6. NVMe command register model used by software

Software register offsets from `src_real/src/ccb_hw.c`:

| Register | Offset from NVMe base | Meaning in current software |
|---|---:|---|
| `GENERIC_NVM_STATUS` | `0x0014` | link/status |
| `GENERIC_MAXLBA_L` | `0x0018` | max LBA low |
| `GENERIC_MAXLBA_H` | `0x001C` | max LBA high |
| `QUEUE_INT_STATUS` | `0x0080` | queue interrupt/status |
| `QUEUE_CUR_CQ_CID` | `0x008C` | read pops one completion FIFO entry |
| `QUEUE_PRP1_L` | `0x0090` | PRP1/data buffer address low |
| `QUEUE_PRP1_H` | `0x0094` | PRP1/data buffer address high |
| `QUEUE_CTX0` | `0x00A0` | `(sectors - 1) << 16 | opcode` |
| `QUEUE_CTX1` | `0x00A4` | command CID |
| `QUEUE_LBA_L` | `0x00A8` | command LBA low 32 bits |
| `QUEUE_LBA_H` | `0x00AC` | command LBA high 32 bits |
| `QUEUE_TX_CTRL` | `0x00B0` | bit0 `CMD_PENDING` |
| `QUEUE_TX_STATUS` | `0x00B4` | SQ full / CQ empty status |
| `QUEUE_SQ_PTRS` | `0x00B8` | SQ head/tail debug |
| `QUEUE_CQ_PTRS` | `0x00BC` | CQ head/tail debug |

Submit sequence in software:

```text
write QUEUE_CTX0   = ((sectors - 1) << 16) | opcode
write QUEUE_CTX1   = cid
write QUEUE_PRP1_L = ddr_hw_addr[31:0]
write QUEUE_PRP1_H = ddr_hw_addr[63:32]
write QUEUE_LBA_L  = lba[31:0]
write QUEUE_LBA_H  = lba[63:32]
barrier
write QUEUE_TX_CTRL bit0 = 1
poll QUEUE_TX_CTRL bit0 == 0
```

Completion sequence:

```text
poll QUEUE_TX_STATUS bit2 == 0  // CQ FIFO not empty
read QUEUE_CUR_CQ_CID          // pops one completion entry
cid    = bits [15:0]
status = bits [31:16]
```

## 7. QUEUE_LBA_L/H to NVMe SQ CDW10/CDW11

What is confirmed from the current FPGA source:

- `USE_CMD_REGISTERS=TRUE`, so software commands go through the Host Core command register block.
- `ipr_nvme_core_wrap_axi_if.v` exposes command-context wires:
  - `CmdLBA_from_cmd_regs[63:0]`
  - `CmdSectCnt_from_cmd_regs[15:0]`
  - `CmdCID_from_cmd_regs[15:0]`
  - `CmdPRPAddr1_from_cmd_regs[63:0]`
- With `USE_CMD_REGISTERS == "TRUE"`, the wrapper assigns:
  - `CmdLBA_internal = CmdLBA_from_cmd_regs`
  - `CmdSectCnt_internal = CmdSectCnt_from_cmd_regs`
  - `CmdCID_internal = CmdCID_from_cmd_regs`
  - `CmdPRPAddr1_internal = CmdPRPAddr1_from_cmd_regs`
- `ipr_nvme_host_core_wrap.v` connects `CmdLBA[ii*64+:64]` into each command queue register/SQ path.

What is not visible in plain source:

- The final construction of the NVMe SQ entry, specifically exact assignment of `CmdLBA[31:0]` to CDW10 and `CmdLBA[63:32]` to CDW11, is inside encrypted/encoded IP files such as:
  - `ipr_nvme_queue_regs_enc.v`
  - `ipr_nvme_sq_sm_enc.v`
  - `ipr_nvme_sq_axi_enc.v`

Inferred mapping, based on standard NVMe NVM Read/Write command format and the exposed Host Core signal names:

| Software register | Host Core signal | Expected NVMe SQ field |
|---|---|---|
| `QUEUE_LBA_L` | `CmdLBA[31:0]` | CDW10 SLBA low |
| `QUEUE_LBA_H` | `CmdLBA[63:32]` | CDW11 SLBA high |
| `QUEUE_CTX0[31:16]` = `sectors - 1` | `CmdSectCnt` | CDW12 NLB |
| `QUEUE_CTX0[7:0]` = opcode | `CmdOpcode` | CDW0 opcode |
| `QUEUE_CTX1[15:0]` | `CmdCID` | CDW0 CID |
| `QUEUE_PRP1_L/H` | `CmdPRPAddr1` | PRP1 |

Risk note:

- The block design confirms `CmdLBA` is 64-bit and fed into the command queue path.
- The exact CDW10/CDW11 bit assignment cannot be proven from plain RTL because the final SQ encoder is encrypted/encoded.
- Historical `ssd-lba-wrap-test` results made a simple low-20-bit LBA wrap
  unlikely. The CPU pattern diagnostics are disabled in the current software
  because channel data DDR is no longer CPU-accessible.

## 8. Current software/FPGA correspondence summary

The current `src_real` address configuration matches the Vivado BD for the important paths:

- ch2 storage:
  - DMA `0x00030000`: match
  - ch2 internal switch `0x00040000`: match
  - NVMe regs `0x00010000`: match
  - desc CPU `0x20004000`: match base, software uses full `16K`
  - desc DMA `0x10000000`: match DMA SG view
  - DDR DMA/NVMe address `0x00000000`: match ch2 DMA/NVMe master view

- ch0 storage/TCP default:
  - DMA `0x41E00000`: match
  - ch0 internal switch `0x44A10000`: match
  - NVMe regs `0x44A00000`: match
  - desc CPU `0x20000000`: match
  - desc DMA `0x10000000`: match DMA SG view
  - manual PRP BRAM CPU/NVMe `0xC0000000`: match, `32K`
  - DDR DMA/NVMe address `0x00000000`: match ch0 DMA/NVMe master view

- ch1 storage/TCP:
  - DMA `0xA0060000`: match
  - ch1 internal switch `0xA0070000`: match
  - NVMe regs `0xA0080000`: match
  - desc CPU `0x30000000`: match
  - desc DMA `0x10000000`: match DMA SG view
  - manual PRP BRAM CPU/NVMe `0xC2000000`: match, `32K`
  - DDR DMA/NVMe address `0x00000000`: match ch1 DMA/NVMe master view

- TCP download top switch:
  - top switch `0xA0040000`: match
  - input `0` = ch0
  - input `1` = ch1
  - input `2` = ch2
