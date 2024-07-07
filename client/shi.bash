#!/bin/bash

set -Eeuo pipefail

print_error_line() {
    echo "An error occurred on line $1"
}

trap 'print_error_line $LINENO' ERR
SHI_SOURCE_TYPE="$1"
SHI_SOURCE="$2"
SHI_CHANNEL="$3"

cat | curl -X 'POST' \
  "$SHI_HOST/ingest?api_key=$SHI_API_KEY&source_type=$SHI_SOURCE_TYPE&source=$SHI_SOURCE&channel=$SHI_CHANNEL" \
  -H 'accept: application/json' \
  -H 'Content-Type: text/plain' \
  --silent \
  --data-binary @-
