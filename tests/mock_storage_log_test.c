#include <assert.h>
#include <stdio.h>
#include "ccb_storage_log.h"

static void test_error(void)
{
    assert(storage_log_level_from_config(CCB_LOG_ERROR) ==
           STORAGE_LOG_ALWAYS_CRITICAL);
}

static void test_info_and_perf(void)
{
    assert(storage_log_level_from_config(CCB_LOG_INFO) == STORAGE_LOG_SUMMARY);
    assert(storage_log_level_from_config(CCB_LOG_PERF) == STORAGE_LOG_SUMMARY);
}

static void test_debug(void)
{
    assert(storage_log_level_from_config(CCB_LOG_DEBUG) == STORAGE_LOG_DEBUG);
}

int main(void)
{
    test_error();
    test_info_and_perf();
    test_debug();
    puts("mock_storage_log_test: ok");
    return 0;
}
