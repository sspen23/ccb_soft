CC ?= gcc
CFLAGS ?= -O2 -Wall -Wextra -std=c11 -D_DEFAULT_SOURCE
LDFLAGS ?=

TARGET := ssd_pcie_reset
SRCS := ssd_pcie_reset.c

.PHONY: all clean

all: $(TARGET)

$(TARGET): $(SRCS)
	$(CC) $(CFLAGS) -o $@ $(SRCS) $(LDFLAGS)

clean:
	rm -f $(TARGET) $(TARGET).exe
