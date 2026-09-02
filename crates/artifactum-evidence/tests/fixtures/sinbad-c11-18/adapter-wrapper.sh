#!/usr/bin/env bash
set -euo pipefail
request_host="$1"
result_host="$2"
io_dir="$(cd "$(dirname "$request_host")" && pwd)"
exec docker run --rm -v "$io_dir":/io sinbad-oracle-fenicsx-sv0-c5-test:local \
	sinbad-oracle-fenicsx "/io/$(basename "$request_host")" "/io/$(basename "$result_host")"
