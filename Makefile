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
	src/ccb_commands.c \
	src/ccb_tcp_transfer.c

.PHONY: all clean

all: $(TARGET)

$(TARGET): $(SRCS)
	$(CC) $(CFLAGS) -Iinclude -o $@ $(SRCS) $(LDFLAGS) $(LDLIBS)

clean:
	rm -f $(TARGET) $(TARGET).exe
