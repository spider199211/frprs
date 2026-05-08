#!/usr/bin/env sh
set -eu
cd "$(dirname "$0")"
exec ./frps -c ./conf/frps.toml
