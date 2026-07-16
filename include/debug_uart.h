#ifndef DEBUG_UART_H
#define DEBUG_UART_H

int debug_uart_init(void);
void debug_uart_close(void);
void dbg_printf(const char *fmt, ...);
void dbg_status_printf(const char *fmt, ...);
int dbg_category_enabled(const char *category);
int dbg_verbose_enabled(void);
void dbg_verbose_printf(const char *fmt, ...);

#endif
