'use strict';

// Finds xkafka's RocksDB shim and the RocksDB library it opens, prebuilt for the running platform. npm installs
// only the platform package whose os, cpu and libc match, so both are resolved from whichever one is present.

function libc() {
  if (process.platform !== 'linux') return '';
  const report = process.report.getReport();
  return report.header.glibcVersionRuntime ? '-gnu' : '-musl';
}

// The RocksDB library each platform package carries, by the name the rocksdbjni jar gives it.
const libraries = {
  'darwin-arm64': 'librocksdbjni-osx-arm64.jnilib',
  'darwin-x64': 'librocksdbjni-osx-x86_64.jnilib',
  'linux-x64-gnu': 'librocksdbjni-linux64.so',
  'linux-arm64-gnu': 'librocksdbjni-linux-aarch64.so',
  'linux-x64-musl': 'librocksdbjni-linux64-musl.so',
  'linux-arm64-musl': 'librocksdbjni-linux-aarch64-musl.so',
};

const platform = `${process.platform}-${process.arch}${libc()}`;
const shimFile = process.platform === 'darwin' ? 'libxkafka_rocksdb.dylib' : 'libxkafka_rocksdb.so';

let shim = null;
let library = null;
let error = null;
try {
  if (!(platform in libraries)) throw new Error('unsupported');
  shim = require.resolve(`@xkafka/rocksdb-${platform}/${shimFile}`);
  library = require.resolve(`@xkafka/rocksdb-${platform}/${libraries[platform]}`);
} catch (cause) {
  shim = null;
  library = null;
  error = `@xkafka/rocksdb has no prebuilt library for ${platform}`;
}

module.exports = { platform, shim, library, error };
