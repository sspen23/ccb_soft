#!/bin/sh
set -eu

case_name=${1:-}
case "$case_name" in
    A)
        profile=PERF_QD8
        compat=1
        command_kib=256
        revision=57fc095
        ;;
    B)
        profile=LEGACY_FAST_BASELINE
        compat=1
        command_kib=256
        revision=07872e3
        ;;
    C)
        profile=LEGACY_FAST_BASELINE
        compat=0
        command_kib=
        revision=07872e3
        ;;
    D)
        profile=LEGACY_FAST_BASELINE
        compat=0
        command_kib=
        revision=debeb39
        ;;
    E)
        profile=LEGACY_FAST_BASELINE
        compat=0
        command_kib=
        revision=06b4e11
        ;;
    F)
        profile=LEGACY_FAST_BASELINE
        compat=0
        command_kib=
        revision=69c1ee3
        ;;
    G)
        profile=CROSS_SLOT_EXPERIMENTAL
        compat=0
        command_kib=
        revision=ec901b1
        ;;
    *)
        echo "usage: sh tools/storage_ab_env.sh A|B|C|D|E|F|G" >&2
        exit 2
        ;;
esac

echo "# revision=$revision"
echo "export CCB_STORAGE_PROFILE=$profile"
echo "export CCB_STORAGE_COMPAT_MODE=$compat"
echo "export CCB_PERF_ENABLE=1"
echo "export CCB_PERF_INTERVAL_MS=1000"
echo "export CCB_LOG_LEVEL=perf"
if [ -n "$command_kib" ]; then
    echo "export SRC_REAL_NVME_CMD_KIB=$command_kib"
else
    echo "unset SRC_REAL_NVME_CMD_KIB"
fi
