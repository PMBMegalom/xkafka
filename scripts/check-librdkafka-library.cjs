'use strict';

// Loads a built xkafka librdkafka library through koffi and checks that it reports the expected identity.
// usage: node check-librdkafka-library.cjs koffi-directory library shim-id

const [koffiDirectory, library, expected] = process.argv.slice(2);
const koffi = require(require.resolve('koffi', { paths: [koffiDirectory] }));
const loaded = koffi.load(library);
const version = loaded.func('const char *xkafka_version_str(void)')();
const identity = loaded.func('const char *xkafka_shim_id(void)')();

if (identity !== expected) {
  console.error(`${library} reports ${identity}, expected ${expected}`);
  process.exit(1);
}
console.log(`${library}: librdkafka ${version}, shim ${identity}`);
