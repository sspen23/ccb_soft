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
DIAG_TARGET := ccb_diag
STORAGE_CONFIG_SRC := src/storage_config.c
STORAGE_ERROR_SRC := src/storage_error.c
STORAGE_WORKER_SRC := src/storage_worker.c
STORAGE_QUEUE_SRC := src/storage_queue.c
STORAGE_STOP_SRC := src/storage_stop.c
STORAGE_WRITER_SRC := src/storage_writer.c
STORAGE_HEALTH_SRC := src/storage_health.c
STORAGE_CONTROLLER_SRC := src/storage_controller.c
STORAGE_PROCESS_SRC := src/storage_process.c

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
	src/ccb_storage_task.c \
	src/ccb_storage_perf.c \
	src/ccb_storage_commit.c \
	src/ccb_storage_sync_outbox.c \
	src/storage_config.c \
	src/storage_controller.c \
	src/storage_error.c \
	src/storage_health.c \
	src/storage_queue.c \
	src/storage_process.c \
	src/storage_stop.c \
	src/storage_worker.c \
	src/storage_writer.c \
	src/ccb_commands.c \
	src/ccb_tcp_transfer.c

.PHONY: all diag clean mock-bd-test storage-host-tests

all: $(TARGET)

$(TARGET): $(SRCS)
	$(CC) $(CFLAGS) -Iinclude -o $@ $(SRCS) $(LDFLAGS) $(LDLIBS)

diag: $(DIAG_TARGET)

$(DIAG_TARGET): $(SRCS)
	$(CC) $(CFLAGS) -DCCB_BUILD_DIAG -ffunction-sections -fdata-sections -Iinclude \
		-o $@ $(SRCS) $(LDFLAGS) -Wl,--gc-sections $(LDLIBS)

clean:
	rm -f $(TARGET) $(TARGET).exe $(DIAG_TARGET) $(DIAG_TARGET).exe

mock-bd-test:
	$(CC) $(CFLAGS) -Wformat=2 -ffunction-sections -fdata-sections -Iinclude \
	tests/mock_bd_ring_test.c src/ccb_hw.c src/debug_uart.c $(STORAGE_CONFIG_SRC) -Wl,--gc-sections -o /tmp/mock_bd_ring_test
	/tmp/mock_bd_ring_test

storage-host-tests:
	$(CC) $(CFLAGS) -Wformat=2 -Iinclude tests/mock_storage_controller_test.c \
		$(STORAGE_CONTROLLER_SRC) $(STORAGE_ERROR_SRC) -o /tmp/mock_storage_controller_test
	/tmp/mock_storage_controller_test
	$(CC) $(CFLAGS) -Wformat=2 -Iinclude tests/mock_storage_health_test.c \
		$(STORAGE_HEALTH_SRC) -lpthread -o /tmp/mock_storage_health_test
	/tmp/mock_storage_health_test
	$(CC) $(CFLAGS) -Wformat=2 -Iinclude tests/mock_storage_queue_test.c \
		$(STORAGE_QUEUE_SRC) -lpthread -o /tmp/mock_storage_queue_test
	/tmp/mock_storage_queue_test
	$(CC) $(CFLAGS) -Wformat=2 -Iinclude tests/mock_storage_requeue_test.c \
		$(STORAGE_QUEUE_SRC) -lpthread -o /tmp/mock_storage_requeue_test
	/tmp/mock_storage_requeue_test
	$(CC) $(CFLAGS) -Wformat=2 -Iinclude tests/mock_storage_worker_state_test.c \
		$(STORAGE_WORKER_SRC) -o /tmp/mock_storage_worker_state_test
	/tmp/mock_storage_worker_state_test
	$(CC) $(CFLAGS) -Wformat=2 -Iinclude tests/mock_storage_error_test.c \
		$(STORAGE_ERROR_SRC) -o /tmp/mock_storage_error_test
	/tmp/mock_storage_error_test
	$(CC) $(CFLAGS) -Wformat=2 -Iinclude tests/mock_storage_profile_test.c \
		$(STORAGE_CONFIG_SRC) -o /tmp/mock_storage_profile_test
	/tmp/mock_storage_profile_test
	$(CC) $(CFLAGS) -Wformat=2 -Iinclude tests/mock_protocol_golden_test.c \
		src/serial_proto.c src/debug_uart.c $(STORAGE_CONFIG_SRC) -o /tmp/mock_protocol_golden_test
	/tmp/mock_protocol_golden_test
	$(CC) $(CFLAGS) -Wformat=2 -Iinclude tests/mock_storage_commit_test.c \
		src/ccb_storage_commit.c -o /tmp/mock_storage_commit_test
	/tmp/mock_storage_commit_test
	$(CC) $(CFLAGS) -Wformat=2 -Iinclude tests/mock_storage_ipc_test.c \
		src/ccb_storage_ipc.c $(STORAGE_CONFIG_SRC) $(STORAGE_ERROR_SRC) -o /tmp/mock_storage_ipc_test
	/tmp/mock_storage_ipc_test
	$(CC) $(CFLAGS) -Wformat=2 -Iinclude tests/mock_storage_pipeline_test.c \
		src/ccb_storage_pipeline.c $(STORAGE_QUEUE_SRC) $(STORAGE_STOP_SRC) $(STORAGE_WRITER_SRC) -lpthread -o /tmp/mock_storage_pipeline_test
	/tmp/mock_storage_pipeline_test
	$(CC) $(CFLAGS) -Wformat=2 -Iinclude tests/mock_storage_stop_lifecycle_test.c \
		src/ccb_storage_pipeline.c $(STORAGE_QUEUE_SRC) $(STORAGE_STOP_SRC) $(STORAGE_WRITER_SRC) -lpthread -o /tmp/mock_storage_stop_lifecycle_test
	/tmp/mock_storage_stop_lifecycle_test
	$(CC) $(CFLAGS) -Wformat=2 -Iinclude tests/mock_storage_diag_test.c \
		src/ccb_storage_diag.c -latomic -o /tmp/mock_storage_diag_test
	/tmp/mock_storage_diag_test
	$(CC) $(CFLAGS) -Wformat=2 -Iinclude tests/mock_storage_supervisor_test.c \
		src/ccb_storage_supervisor.c src/ccb_storage_ipc.c $(STORAGE_CONFIG_SRC) $(STORAGE_ERROR_SRC) -o /tmp/mock_storage_supervisor_test
	/tmp/mock_storage_supervisor_test
	$(CC) $(CFLAGS) -Wformat=2 -Iinclude tests/mock_storage_task_test.c \
		src/ccb_storage_task.c -o /tmp/mock_storage_task_test
	/tmp/mock_storage_task_test
	$(CC) $(CFLAGS) -Wformat=2 -Iinclude tests/mock_storage_perf_test.c \
		src/ccb_storage_perf.c src/ccb_storage_ipc.c $(STORAGE_CONFIG_SRC) $(STORAGE_ERROR_SRC) -o /tmp/mock_storage_perf_test
	/tmp/mock_storage_perf_test
	$(CC) $(CFLAGS) -Wformat=2 -Iinclude tests/mock_storage_log_test.c $(STORAGE_CONFIG_SRC) \
		-o /tmp/mock_storage_log_test
	/tmp/mock_storage_log_test
	$(CC) $(CFLAGS) -Wformat=2 -Iinclude tests/mock_storage_sync_outbox_test.c \
		src/ccb_storage_sync_outbox.c -o /tmp/mock_storage_sync_outbox_test
	/tmp/mock_storage_sync_outbox_test
	$(CC) $(CFLAGS) -Wformat=2 -Wno-unused-function -Wno-stringop-truncation -ffunction-sections -fdata-sections -Iinclude \
		tests/mock_storage_config_test.c src/ccb_commands.c $(STORAGE_CONFIG_SRC) $(STORAGE_ERROR_SRC) $(STORAGE_WORKER_SRC) $(STORAGE_QUEUE_SRC) $(STORAGE_STOP_SRC) $(STORAGE_WRITER_SRC) -Wl,--gc-sections -o /tmp/mock_storage_config_test
	/tmp/mock_storage_config_test
	$(CC) $(CFLAGS) -Wformat=2 -ffunction-sections -fdata-sections -Iinclude \
		tests/mock_nvme_cross_slot_test.c src/ccb_hw.c src/debug_uart.c $(STORAGE_CONFIG_SRC) $(STORAGE_ERROR_SRC) -Wl,--gc-sections -o /tmp/mock_nvme_cross_slot_test
	/tmp/mock_nvme_cross_slot_test
	$(CC) $(CFLAGS) -Wformat=2 -Wno-unused-function -Wno-stringop-truncation \
		-ffunction-sections -fdata-sections -Iinclude tests/mock_cross_slot_writer_lifecycle_test.c \
		src/ccb_commands.c src/ccb_hw.c src/debug_uart.c $(STORAGE_CONFIG_SRC) $(STORAGE_ERROR_SRC) $(STORAGE_WORKER_SRC) $(STORAGE_QUEUE_SRC) $(STORAGE_STOP_SRC) $(STORAGE_WRITER_SRC) -Wl,--gc-sections -o /tmp/mock_cross_slot_writer_lifecycle_test
	/tmp/mock_cross_slot_writer_lifecycle_test
	$(CC) $(CFLAGS) -Wformat=2 -ffunction-sections -fdata-sections -Iinclude \
		tests/mock_nvme_legacy_submit_test.c src/ccb_hw.c src/debug_uart.c $(STORAGE_CONFIG_SRC) $(STORAGE_ERROR_SRC) -lpthread -Wl,--gc-sections -o /tmp/mock_nvme_legacy_submit_test
	/tmp/mock_nvme_legacy_submit_test
	$(CC) $(CFLAGS) -Wformat=2 -ffunction-sections -fdata-sections -Iinclude \
		tests/mock_nvme_capability_test.c src/ccb_hw.c src/debug_uart.c $(STORAGE_CONFIG_SRC) $(STORAGE_ERROR_SRC) -Wl,--gc-sections -o /tmp/mock_nvme_capability_test
	/tmp/mock_nvme_capability_test
	$(CC) $(CFLAGS) -Wformat=2 -ffunction-sections -fdata-sections -Iinclude \
		tests/mock_dma_harvest_batch_test.c src/ccb_hw.c $(STORAGE_CONFIG_SRC) -Wl,--gc-sections -o /tmp/mock_dma_harvest_batch_test
	/tmp/mock_dma_harvest_batch_test
	$(CC) $(CFLAGS) -Wformat=2 -ffunction-sections -fdata-sections -Iinclude \
		tests/mock_first_dma_timeout_test.c src/ccb_hw.c src/ccb_storage_pipeline.c $(STORAGE_CONFIG_SRC) $(STORAGE_QUEUE_SRC) -lpthread -Wl,--gc-sections -o /tmp/mock_first_dma_timeout_test
	/tmp/mock_first_dma_timeout_test
