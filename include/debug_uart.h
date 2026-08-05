#ifndef DEBUG_UART_H
#define DEBUG_UART_H

#include <stddef.h>

int debug_uart_init(void);
void debug_uart_close(void);
void debug_uart_write(const char *data, size_t len);
void dbg_printf(const char *fmt, ...);
void dbg_status_printf(const char *fmt, ...);
int dbg_category_enabled(const char *category);
int dbg_verbose_enabled(void);
void dbg_verbose_printf(const char *fmt, ...);

#endif
