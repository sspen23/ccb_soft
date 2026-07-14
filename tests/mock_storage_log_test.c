#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include "ccb_storage_log.h"

static void set_level(const char *level)
{
    unsetenv("SRC_REAL_LEGACY_STORAGE_TEXT");
    if (level) setenv("SRC_REAL_LOG_LEVEL", level, 1);
    else unsetenv("SRC_REAL_LOG_LEVEL");
}

static void test_quiet(void)
{
    set_level("quiet");
    assert(storage_log_severity_enabled(STORAGE_LOG_ALWAYS_CRITICAL));
    assert(!storage_log_severity_enabled(STORAGE_LOG_SUMMARY));
    assert(!storage_log_severity_enabled(STORAGE_LOG_DEBUG));
    assert(!storage_log_severity_enabled(STORAGE_LOG_TRACE));
}

static void test_summary(void)
{
    set_level("summary");
    assert(storage_log_severity_enabled(STORAGE_LOG_ALWAYS_CRITICAL));
    assert(storage_log_severity_enabled(STORAGE_LOG_SUMMARY));
    assert(!storage_log_severity_enabled(STORAGE_LOG_DEBUG));
    assert(!storage_log_severity_enabled(STORAGE_LOG_TRACE));
}

static void test_debug(void)
{
    set_level("debug");
    assert(storage_log_severity_enabled(STORAGE_LOG_ALWAYS_CRITICAL));
    assert(storage_log_severity_enabled(STORAGE_LOG_SUMMARY));
    assert(storage_log_severity_enabled(STORAGE_LOG_DEBUG));
    assert(!storage_log_severity_enabled(STORAGE_LOG_TRACE));
}

static void test_trace(void)
{
    set_level("trace");
    assert(storage_log_severity_enabled(STORAGE_LOG_ALWAYS_CRITICAL));
    assert(storage_log_severity_enabled(STORAGE_LOG_SUMMARY));
    assert(storage_log_severity_enabled(STORAGE_LOG_DEBUG));
    assert(storage_log_severity_enabled(STORAGE_LOG_TRACE));
}

static void test_legacy_does_not_override_explicit_level(void)
{
    set_level("quiet");
    setenv("SRC_REAL_LEGACY_STORAGE_TEXT", "1", 1);
    assert(!storage_log_severity_enabled(STORAGE_LOG_SUMMARY));
    unsetenv("SRC_REAL_LOG_LEVEL");
    assert(storage_log_severity_enabled(STORAGE_LOG_DEBUG));
    unsetenv("SRC_REAL_LEGACY_STORAGE_TEXT");
}

int main(void)
{
    test_quiet();
    test_summary();
    test_debug();
    test_trace();
    test_legacy_does_not_override_explicit_level();
    puts("mock_storage_log_test: ok");
    return 0;
}
