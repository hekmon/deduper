#!/usr/bin/env bash

set -e

crosscompile () {
    GOOS="$1" GOARCH="$2" go build -trimpath -ldflags="-s -w" -o 'deduper'
    zip -9 "deduper_v1.0.0_${1}_${2}.zip" 'deduper'
}

echo '* Compiling for Linux'
crosscompile 'linux' 'amd64'