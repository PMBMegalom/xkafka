'use strict';

// Loads a built xkafka RocksDB shim through koffi, checks that it reports the expected identity, has it open
// the RocksDB library and checks its release, then writes a key to a new store and reads it back.
// usage: node check-rocksdb-library.cjs koffi-directory shim rocksdb-library shim-id rocksdb-version

const fs = require('fs');
const os = require('os');
const path = require('path');

const [koffiDirectory, shimPath, library, expected, version] = process.argv.slice(2);
const koffi = require(require.resolve('koffi', { paths: [koffiDirectory] }));
const shim = koffi.load(shimPath);
const identity = shim.func('const char *xkafka_rocksdb_identity(void)');
const load = shim.func('int xkafka_rocksdb_load(const char *path, const char *expected, _Out_ void **error)');
const open = shim.func('void *xkafka_rocksdb_open(const char *path, const char **families, size_t count, _Out_ void **error)');
const write = shim.func('int xkafka_rocksdb_write(void *store, const void *batch, size_t length, _Out_ void **error)');
const get = shim.func(
  'int xkafka_rocksdb_get(void *store, size_t family, const void *key, size_t key_length, _Out_ void **value, _Out_ size_t *value_length, _Out_ int *found, _Out_ void **error)'
);
const close = shim.func('void xkafka_rocksdb_close(void *store)');
const free = shim.func('void xkafka_rocksdb_free(void *pointer)');

function fail(message) {
  console.error(`${shimPath}: ${message}`);
  process.exit(1);
}

if (identity() !== expected) fail(`reports ${identity()}, expected ${expected}`);

const error = [null];
if (load(library, version, error) !== 0) fail(koffi.decode(error[0], 'char', -1));

const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'xkafka-rocksdb-'));
const store = open(path.join(directory, 'store'), ['default'], 1, error);
if (store === null) fail(koffi.decode(error[0], 'char', -1));

// One put of key 0x80 with the value "checked", encoded as the shim takes a batch.
const batch = Buffer.from([1, 0, 0, 0, 0, 1, 0x80, 0, 0, 0, 7, ...Buffer.from('checked')]);
if (write(store, batch, batch.length, error) !== 0) fail(koffi.decode(error[0], 'char', -1));
const value = [null];
const length = [0];
const found = [0];
if (get(store, 0, Buffer.from([0x80]), 1, value, length, found, error) !== 0) fail(koffi.decode(error[0], 'char', -1));
const read = found[0] === 1 ? Buffer.from(koffi.view(value[0], Number(length[0]))).toString() : null;
free(value[0]);
close(store);
fs.rmSync(directory, { recursive: true, force: true });
if (read !== 'checked') fail(`read back ${read}, expected checked`);

console.log(`${shimPath}: shim ${identity()}, RocksDB ${version} from ${library}, a store wrote and read back a key`);
