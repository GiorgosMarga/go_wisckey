#!/bin/bash
NAME=${1:-profile}
BENCH=${2:-"\."}
PKG=${3:-"./internal/lsm/"}

go test -bench="${BENCH}" -benchmem \
    -cpuprofile="${NAME}_cpu.prof" \
    -memprofile="${NAME}_mem.prof" \
    -count=5 \
    "${PKG}"