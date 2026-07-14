#include "debug_uart.h"
#include "storage_config.h"

#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>

#ifndef DEBUG_CONSOLE_PATH
#define DEBUG_CONSOLE_PATH "/dev/console"
#endif

#define DEBUG_BUF_BYTES 512

static int g_debug_fd = -1;
static int g_debug_init_tried = 0;
static int g_verbose_cached = -1;
static int g_debug_mask_cached = 0;
static uint32_t g_debug_mask = 0u;

#define DBG_CAT_GENERAL (1u << 0)
#define DBG_CAT_MAIN    (1u << 1)
#define DBG_CAT_PROTO   (1u << 2)
#define DBG_CAT_CAPTURE (1u << 3)
#define DBG_CAT_WORKER  (1u << 4)
#define DBG_CAT_WRITE   (1u << 5)
#define DBG_CAT_NVME    (1u << 6)
#define DBG_CAT_DMA     (1u << 7)
#define DBG_CAT_STORAGE (1u << 8)
#define DBG_CAT_NET     (1u << 9)
#define DBG_CAT_TCP     (1u << 10)
#define DBG_CAT_PATTERN (1u << 11)
#define DBG_CAT_DISK    (1u << 12)
#define DBG_CAT_FLASH   (1u << 13)
#define DBG_CAT_UART    (1u << 14)
#define DBG_CAT_TASK    (1u << 15)
#define DBG_CAT_FILE_OP (1u << 16)
#define DBG_CAT_HW      (1u << 17)
#define DBG_CAT_WRAP    (1u << 18)
#define DBG_CAT_CONT    (1u << 19)
#define DBG_CAT_ALL     0xffffffffu

typedef struct {
    const char *name;
    uint32_t mask;
} DebugCategory;

static const DebugCategory kDebugCategories[] = {
    {"GENERAL", DBG_CAT_GENERAL},
    {"MAIN", DBG_CAT_MAIN},
    {"PROTO", DBG_CAT_PROTO},
    {"CAPTURE", DBG_CAT_CAPTURE},
    {"WORKER", DBG_CAT_WORKER},
    {"WRITE", DBG_CAT_WRITE},
    {"NVME", DBG_CAT_NVME},
    {"DMA", DBG_CAT_DMA},
    {"STORAGE", DBG_CAT_STORAGE},
    {"NET", DBG_CAT_NET},
    {"TCP", DBG_CAT_TCP},
    {"PATTERN", DBG_CAT_PATTERN},
    {"DISK", DBG_CAT_DISK},
    {"FLASH", DBG_CAT_FLASH},
    {"UART", DBG_CAT_UART},
    {"TASK", DBG_CAT_TASK},
    {"FILE_OP", DBG_CAT_FILE_OP},
    {"HW", DBG_CAT_HW},
    {"WRAP", DBG_CAT_WRAP},
    {"CONT", DBG_CAT_CONT},
};

static int env_flag_enabled(const char *name)
{
    const char *value = storage_config_compat_getenv(name);

    if (!value || value[0] == '\0') {
        return 0;
    }
    if (strcmp(value, "0") == 0 ||
        strcmp(value, "false") == 0 ||
        strcmp(value, "FALSE") == 0 ||
        strcmp(value, "off") == 0 ||
        strcmp(value, "OFF") == 0 ||
        strcmp(value, "no") == 0 ||
        strcmp(value, "NO") == 0) {
        return 0;
    }
    return 1;
}

static int token_is_all(const char *token, size_t len)
{
    return (len == 1u && token[0] == '1') ||
           (len == 3u && strncasecmp(token, "all", len) == 0) ||
           (len == 4u && strncasecmp(token, "true", len) == 0) ||
           (len == 2u && strncasecmp(token, "on", len) == 0) ||
           (len == 3u && strncasecmp(token, "yes", len) == 0);
}

static int token_is_separator(char c)
{
    return c == ',' || c == ':' || c == ';' || c == '|' || c == '+' ||
           c == ' ' || c == '\t' || c == '\n' || c == '\r';
}

static uint32_t debug_category_mask(const char *name, size_t len)
{
    size_t i;

    for (i = 0; i < sizeof(kDebugCategories) / sizeof(kDebugCategories[0]); ++i) {
        if (strlen(kDebugCategories[i].name) == len &&
            strncasecmp(kDebugCategories[i].name, name, len) == 0) {
            return kDebugCategories[i].mask;
        }
    }
    return 0u;
}

static uint32_t parse_debug_mask(const char *value)
{
    uint32_t mask = 0u;
    const char *p = value;

    if (!value || value[0] == '\0') {
        return 0u;
    }
    while (*p != '\0') {
        const char *start;
        size_t len;

        while (*p != '\0' && token_is_separator(*p)) {
            ++p;
        }
        start = p;
        while (*p != '\0' && !token_is_separator(*p)) {
            ++p;
        }
        len = (size_t)(p - start);
        if (len == 0u) {
            continue;
        }
        if (token_is_all(start, len)) {
            return DBG_CAT_ALL;
        }
        mask |= debug_category_mask(start, len);
    }
    return mask;
}

static uint32_t debug_mask(void)
{
    const char *value;

    if (g_debug_mask_cached) {
        return g_debug_mask;
    }
    g_debug_mask_cached = 1;
    if (env_flag_enabled("SRC_REAL_DEBUG_VERBOSE") ||
        env_flag_enabled("CCB_DEBUG_VERBOSE")) {
        g_debug_mask = DBG_CAT_ALL;
        return g_debug_mask;
    }
    value = storage_config_compat_getenv("SRC_REAL_DEBUG");
    if (!value || value[0] == '\0') {
        value = storage_config_compat_getenv("CCB_DEBUG");
    }
    g_debug_mask = parse_debug_mask(value);
    return g_debug_mask;
}

int dbg_category_enabled(const char *category)
{
    uint32_t mask = debug_mask();

    if (mask == DBG_CAT_ALL) {
        return 1;
    }
    if (mask == 0u) {
        return 0;
    }
    if (!category || category[0] == '\0') {
        return (mask & DBG_CAT_GENERAL) != 0u;
    }
    return (mask & debug_category_mask(category, strlen(category))) != 0u;
}

static int dbg_message_enabled(const char *fmt)
{
    const char *start;
    const char *end;
    uint32_t mask;
    uint32_t category_mask;

    if (!fmt) {
        return 0;
    }
    if (strncmp(fmt, "[DBG][", 6) != 0) {
        return dbg_category_enabled("GENERAL");
    }
    start = fmt + 6;
    end = strchr(start, ']');
    if (!end || end == start) {
        return dbg_category_enabled("GENERAL");
    }
    mask = debug_mask();
    if (mask == DBG_CAT_ALL) {
        return 1;
    }
    category_mask = debug_category_mask(start, (size_t)(end - start));
    return category_mask != 0u && (mask & category_mask) != 0u;
}

static const char *debug_console_path(void)
{
    const char *env_path = storage_config_compat_getenv("DEBUG_CONSOLE_PATH");
    if (env_path && env_path[0] != '\0') {
        return env_path;
    }
    return DEBUG_CONSOLE_PATH;
}

int debug_uart_init(void)
{
    const char *path;

    if (g_debug_fd >= 0) {
        return 0;
    }
    if (g_debug_init_tried) {
        return -1;
    }
    g_debug_init_tried = 1;

    path = debug_console_path();
    g_debug_fd = open(path, O_WRONLY | O_NONBLOCK);
    if (g_debug_fd < 0) {
        g_debug_fd = STDERR_FILENO;
        dbg_verbose_printf("[DBG] debug console fallback to stderr path=%s errno=%d\n", path, errno);
        return 0;
    }

    dbg_verbose_printf("[DBG] debug console ready path=%s\n", path);
    return 0;
}

void debug_uart_close(void)
{
    if (g_debug_fd >= 0 && g_debug_fd != STDERR_FILENO) {
        close(g_debug_fd);
    }
    g_debug_fd = -1;
}

void dbg_printf(const char *fmt, ...)
{
    char buf[DEBUG_BUF_BYTES];
    va_list ap;
    int n;
    ssize_t written;

    if (!dbg_message_enabled(fmt)) {
        return;
    }
    if (g_debug_fd < 0 && debug_uart_init() != 0) {
        return;
    }

    va_start(ap, fmt);
    n = vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);

    if (n <= 0) {
        return;
    }
    if (n > (int)sizeof(buf)) {
        n = (int)sizeof(buf);
    }
    written = write(g_debug_fd, buf, (size_t)n);
    (void)written;
}

int dbg_verbose_enabled(void)
{
    if (g_verbose_cached < 0) {
        g_verbose_cached = env_flag_enabled("SRC_REAL_DEBUG_VERBOSE") ||
                           env_flag_enabled("CCB_DEBUG_VERBOSE");
    }
    return g_verbose_cached != 0;
}

void dbg_verbose_printf(const char *fmt, ...)
{
    char buf[DEBUG_BUF_BYTES];
    va_list ap;
    int n;
    ssize_t written;

    if (!dbg_verbose_enabled()) {
        return;
    }
    if (g_debug_fd < 0 && debug_uart_init() != 0) {
        return;
    }

    va_start(ap, fmt);
    n = vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);

    if (n <= 0) {
        return;
    }
    if (n > (int)sizeof(buf)) {
        n = (int)sizeof(buf);
    }
    written = write(g_debug_fd, buf, (size_t)n);
    (void)written;
}
