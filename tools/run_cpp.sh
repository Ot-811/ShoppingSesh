#!/bin/sh
# usage: run_cpp.sh DATA OUT : run binary on DATA, convert parquet to CSV in OUT
BIN=${BIN:-/tmp/ss}
rm -rf "$2.pq"; mkdir -p "$2.pq"
DATA_DIR="$1" OUTPUT_DIR="$2.pq" NCPU=${NCPU:-3} $BIN 2>"$2.log" || exit 1
python3 "$(dirname "$0")/pqread.py" "$2.pq" "$2"
