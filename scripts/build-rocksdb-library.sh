#!/usr/bin/env sh

# Builds the RocksDB shim into a shared library for the JavaScript backend.
#
# usage: build-rocksdb-library.sh output shim-id shim.c
#
# The shim opens RocksDB itself, from the library the rocksdbjni jar carries, so nothing of RocksDB is linked
# in, and the library exports only the shim's xkafka_rocksdb_* functions.

set -eu

if [ "$#" -ne 3 ]; then
  echo "usage: $0 output shim-id shim.c" >&2
  exit 2
fi

output="$1"
shim_id="$2"
shim="$3"

mkdir -p "$(dirname "$output")"

if [ "$(uname -s)" = "Darwin" ]; then
  cc -O2 -fPIC -Wall -Wextra -DSCALANATIVE_LINK_DL -DXKAFKA_ROCKSDB_SHIM_ID="\"$shim_id\"" -dynamiclib -o "$output" "$shim" \
    -install_name "@rpath/$(basename "$output")" -Wl,-exported_symbol,_xkafka_rocksdb_\* -lpthread
else
  exports="$(dirname "$output")/xkafka_rocksdb.map"
  printf '{ global: xkafka_rocksdb_*; local: *; };\n' > "$exports"
  cc -O2 -fPIC -Wall -Wextra -DSCALANATIVE_LINK_DL -DXKAFKA_ROCKSDB_SHIM_ID="\"$shim_id\"" -shared -o "$output" "$shim" \
    -Wl,--version-script="$exports" -Wl,-Bsymbolic -ldl -lpthread
fi
