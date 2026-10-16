'use strict';

// Finds the prebuilt xkafka librdkafka library for the running platform. npm installs only the platform
// package whose os, cpu and libc match, so the library is resolved from whichever one is present.

function libc() {
  if (process.platform !== 'linux') return '';
  const report = process.report.getReport();
  return report.header.glibcVersionRuntime ? '-gnu' : '-musl';
}

const platform = `${process.platform}-${process.arch}${libc()}`;
const file = process.platform === 'darwin' ? 'libxkafka_librdkafka.dylib' : 'libxkafka_librdkafka.so';

let library = null;
let error = null;
try {
  library = require.resolve(`@xkafka/librdkafka-${platform}/${file}`);
} catch (cause) {
  error = `@xkafka/librdkafka has no prebuilt library for ${platform}`;
}

module.exports = { platform, library, error };
