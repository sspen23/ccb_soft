CC ?= gcc
CFLAGS ?= -O2 -Wall -Wextra -std=c11 -D_DEFAULT_SOURCE
LDFLAGS ?=
LDLIBS ?= -lsqlite3 -lpthread -latomic

SQLITE_SYSROOT ?= /home/sspen/sspen/Projects/nvme/CCB_DBQ_linux/ccb_linux_23_1/build/tmp/sysroots-components/microblazeel-v11.0-bs-cmp-re-mh-div/sqlite3/usr
ifneq (,$(findstring microblaze,$(CC)))
ifneq (,$(wildcard $(SQLITE_SYSROOT)/include/sqlite3.h))
CFLAGS += -I$(SQLITE_SYSROOT)/include
LDFLAGS += -L$(SQLITE_SYSROOT)/lib
endif
endif

TARGET := src_real_app

SRCS := \
	src/system.c \
	src/serial_proto.c \
	src/file_list.c \
	src/logger.c \
	src/debug_uart.c \
	src/ccb_cli.c \
	src/ccb_config.c \
	src/ccb_hw.c \
	src/ccb_metadata.c \
	src/ccb_storage_ipc.c \
	src/ccb_storage_diag.c \
	src/ccb_storage_pipeline.c \
	src/ccb_storage_supervisor.c \
	src/ccb_storage_perf.c \
	src/ccb_commands.c \
	src/ccb_tcp_transfer.c

.PHONY: all clean mock-bd-test storage-host-tests

all: $(TARGET)

$(TARGET): $(SRCS)
	$(CC) $(CFLAGS) -Iinclude -o $@ $(SRCS) $(LDFLAGS) $(LDLIBS)

clean:
	rm -f $(TARGET) $(TARGET).exe

mock-bd-test:
	$(CC) $(CFLAGS) -Wformat=2 -ffunction-sections -fdata-sections -Iinclude \
		tests/mock_bd_ring_test.c src/ccb_hw.c -Wl,--gc-sections -o /tmp/mock_bd_ring_test
	/tmp/mock_bd_ring_test

storage-host-tests:
	$(CC) $(CFLAGS) -Wformat=2 -Iinclude tests/mock_storage_ipc_test.c \
		src/ccb_storage_ipc.c -o /tmp/mock_storage_ipc_test
	/tmp/mock_storage_ipc_test
	$(CC) $(CFLAGS) -Wformat=2 -Iinclude tests/mock_storage_pipeline_test.c \
		src/ccb_storage_pipeline.c -lpthread -o /tmp/mock_storage_pipeline_test
	/tmp/mock_storage_pipeline_test
	$(CC) $(CFLAGS) -Wformat=2 -Iinclude tests/mock_storage_diag_test.c \
		src/ccb_storage_diag.c -latomic -o /tmp/mock_storage_diag_test
	/tmp/mock_storage_diag_test
	$(CC) $(CFLAGS) -Wformat=2 -Iinclude tests/mock_storage_supervisor_test.c \
		src/ccb_storage_supervisor.c src/ccb_storage_ipc.c -o /tmp/mock_storage_supervisor_test
	/tmp/mock_storage_supervisor_test
	$(CC) $(CFLAGS) -Wformat=2 -Iinclude tests/mock_storage_perf_test.c \
		src/ccb_storage_perf.c src/ccb_storage_ipc.c -o /tmp/mock_storage_perf_test
	/tmp/mock_storage_perf_test
	$(CC) $(CFLAGS) -Wformat=2 -ffunction-sections -fdata-sections -Iinclude \
		tests/mock_nvme_cross_slot_test.c src/ccb_hw.c src/debug_uart.c -Wl,--gc-sections -o /tmp/mock_nvme_cross_slot_test
	/tmp/mock_nvme_cross_slot_test
	$(CC) $(CFLAGS) -Wformat=2 -ffunction-sections -fdata-sections -Iinclude \
		tests/mock_dma_harvest_batch_test.c src/ccb_hw.c -Wl,--gc-sections -o /tmp/mock_dma_harvest_batch_test
	/tmp/mock_dma_harvest_batch_test
