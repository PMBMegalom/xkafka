#!/usr/bin/env sh

# Builds the C shim and librdkafka into one shared library for the JavaScript backend.
#
# usage: build-librdkafka-library.sh archive build-directory output shim-id shim.c...
#
# librdkafka's own build compiles OpenSSL, zlib and zstd from source and links them in statically, and the
# library exports only the shim's xkafka_* functions, so nothing in it can clash with the OpenSSL that Node
# carries. The librdkafka build is kept in build-directory and reused while its marker is there.

set -eu

if [ "$#" -lt 5 ]; then
  echo "usage: $0 archive build-directory output shim-id shim.c..." >&2
  exit 2
fi

archive="$1"
build="$2"
output="$3"
shim_id="$4"
shift 4

source="$build/source"
marker="$build/.librdkafka-static-built"
jobs="$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 2)"

if [ ! -f "$marker" ] || [ ! -f "$source/src/librdkafka-static.a" ]; then
  rm -rf "$source"
  mkdir -p "$source"
  tar -xzf "$archive" --strip-components=1 -C "$source"
  # zlib.net has served a page in place of the release archive, which the checksum then refuses, so zlib comes from its own GitHub
  # release, checked against the same checksum.
  zlib_module="$source/mklove/modules/configure.zlib"
  sed -i.orig 's#https://zlib.net/fossils/zlib-${ver}.tar.gz#https://github.com/madler/zlib/releases/download/v${ver}/zlib-${ver}.tar.gz#' "$zlib_module"
  if ! grep -q 'github.com/madler/zlib' "$zlib_module"; then
    echo "could not point the zlib download at its GitHub release" >&2
    exit 1
  fi
  (
    cd "$source"
    ./configure --install-deps --source-deps-only --enable-static --enable-ssl --enable-zlib --enable-zstd \
      --disable-gssapi --disable-curl --disable-lz4-ext
    make -C src -j"$jobs" librdkafka-static.a
  )
  touch "$marker"
fi

mkdir -p "$build/include/librdkafka" "$(dirname "$output")"
cp "$source/src/rdkafka.h" "$build/include/librdkafka/rdkafka.h"
cp "$source/src/rdkafka_mock.h" "$build/include/librdkafka/rdkafka_mock.h"

objects=""
for shim in "$@"; do
  object="$build/$(basename "$shim" .c).o"
  # The OpenSSL headers are those of the OpenSSL librdkafka's build compiled, which the library links statically.
  cc -O2 -fPIC -DSCALANATIVE_LINK_RDKAFKA -DXKAFKA_STATIC_OPENSSL -DXKAFKA_SHIM_ID="\"$shim_id\"" -I"$build/include" -I"$source/mklove/deps/dest/usr/include" \
    -c "$shim" -o "$object"
  objects="$objects $object"
done

if [ "$(uname -s)" = "Darwin" ]; then
  # shellcheck disable=SC2086
  cc -dynamiclib -o "$output" $objects "$source/src/librdkafka-static.a" \
    -Wl,-exported_symbol,_xkafka_\* -Wl,-dead_strip -lpthread -lm
else
  printf '{ global: xkafka_*; local: *; };\n' > "$build/exports.map"
  # shellcheck disable=SC2086
  cc -shared -o "$output" $objects "$source/src/librdkafka-static.a" \
    -Wl,--version-script="$build/exports.map" -Wl,--exclude-libs,ALL -Wl,-Bsymbolic -Wl,--gc-sections -lpthread -lm
fi
