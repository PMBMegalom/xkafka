#ifdef SCALANATIVE_LINK_DL

/* xkafka's RocksDB shim, which the JavaScript and Native backends call.
 *
 * RocksDB is the library the rocksdbjni jar carries for each platform, which
 * exports RocksDB's C API. The shim opens it with dlopen and finds each
 * function it calls by name, so it needs no RocksDB headers and records no
 * path in what links it.
 *
 * Every call takes and answers whole values, so a caller crosses into C once
 * for each, which matters to the JavaScript backend, which pays for every
 * crossing.
 *
 * Loading
 *
 *   int xkafka_rocksdb_load(const char *path, const char *expected, char **error)
 *
 *   Opens the RocksDB library at `path` once for the process, finds its
 *   functions, and checks that it is the RocksDB release `expected` names,
 *   such as "10.1.3". With no path, it tries in turn the file the
 *   XKAFKA_ROCKSDB_LIBRARY environment variable names, where one is set and
 *   nothing else; the platform's file from the rocksdbjni jar, such as
 *   librocksdbjni-linux64.so, in the directory of the running executable; and
 *   that file name through the system's own library search. A later call
 *   loads nothing more and only checks the release. Answers 0, or -1 with the
 *   reason in `error`.
 *
 *   const char *xkafka_rocksdb_version(void)
 *
 *   The loaded library's RocksDB release, or NULL before one is loaded.
 *
 *   const char *xkafka_rocksdb_identity(void)
 *
 *   The RocksDB release and the shim's identity the build gave it, such as
 *   "10.1.3-0123456789ab", which the JavaScript backend compares with its own
 *   so it refuses a library built from other sources. Empty where the build
 *   gave none, as Scala Native's does.
 *
 * Stores
 *
 *   void *xkafka_rocksdb_open(const char *path, const char *const *families, size_t count, char **error)
 *
 *   Opens the store at `path`, creating the directory, the store and any of
 *   the `count` column families that are missing. A family is named by its
 *   index in `families` in every other call. The default family is always
 *   opened, as RocksDB requires, and is reached by index only where
 *   `families` names it. Answers the store, or NULL with the reason in
 *   `error`. A store that holds a family not listed is refused, as RocksDB
 *   refuses it.
 *
 *   int xkafka_rocksdb_get(void *store, size_t family, const void *key, size_t key_length,
 *                          void **value, size_t *value_length, int *found, char **error)
 *
 *   Writes 1 to `found` and the value to `value` and `value_length`, or 0 to
 *   `found` where the key has none. A value is the caller's to free, and an
 *   empty one is NULL.
 *
 *   int xkafka_rocksdb_write(void *store, const void *batch, size_t length, char **error)
 *
 *   Applies an encoded batch as one atomic write. The batch is a sequence of
 *   operations, each a one-byte operation, a one-byte family index, then its
 *   byte strings, each preceded by its length as an unsigned 32-bit big-endian
 *   integer:
 *
 *     1  put           key, value
 *     2  delete        key
 *     3  delete range  from (included), to (excluded)
 *
 *   A batch that does not decode is refused before anything is written.
 *
 *   int xkafka_rocksdb_range(void *store, size_t family, const void *from, size_t from_length,
 *                            const void *to, size_t to_length, size_t limit,
 *                            void **entries, size_t *length, char **error)
 *
 *   The entries from `from`, included, up to `to`, excluded, in RocksDB's
 *   order, which compares keys as unsigned bytes. A NULL bound is no bound; an
 *   empty key needs a pointer that is not NULL. At most `limit` entries come
 *   back, or every one for a limit of 0, encoded as a key and a value each
 *   preceded by its length, as in a batch. The entries are the caller's to
 *   free, and none at all are NULL with a length of 0. A caller pages through
 *   a range by asking again from the last key followed by a zero byte, the
 *   key just after it, until fewer than `limit` entries come back.
 *
 *   int xkafka_rocksdb_flush(void *store, char **error)
 *
 *   Writes every family's memtables to disk and waits for it, all families at
 *   once. Writes skip the write-ahead log, so a store is durable only once it
 *   is flushed; a checkpoint flushes first.
 *
 *   void xkafka_rocksdb_close(void *store)
 *
 *   Closes the store, which flushes what was written since the last flush,
 *   and frees it. Nothing else may use the store meanwhile or after.
 *
 *   void xkafka_rocksdb_free(void *pointer)
 *
 *   Frees what the shim answered: a value, entries or an error.
 *
 * Calls answer 0 for success and -1 for a failure, whose message, RocksDB's
 * own where it has one, is written to `error` as a string for the caller to
 * free. Where nothing failed, `error` is left alone.
 *
 * Calls on their own thread
 *
 * Each xkafka_rocksdb_async_* function takes the arguments of the call it
 * names, with an out-parameter for what that call answers, starts the call on
 * a new detached thread and returns at once. When the call has finished, its
 * answer is written through `result` and `done` is called with the caller's
 * token, from that thread. The JavaScript backend uses these, so a call that
 * waits on the disk holds none of Node's shared worker threads. Strings are
 * copied; every other pointer must stay valid until `done` is called.
 * Starting answers 0, or -1 when no thread could be started, in which case
 * `done` is never called. */

#include <dlfcn.h>
#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#ifdef __APPLE__
#include <mach-o/dyld.h>
#endif

/* The rocksdbjni jar's file for this platform. musl defines no macro of its
 * own, so a Linux C library that is not glibc is taken to be musl. */
#if defined(__APPLE__) && defined(__aarch64__)
#define XKAFKA_ROCKSDB_FILE "librocksdbjni-osx-arm64.jnilib"
#elif defined(__APPLE__) && defined(__x86_64__)
#define XKAFKA_ROCKSDB_FILE "librocksdbjni-osx-x86_64.jnilib"
#elif defined(__linux__) && defined(__x86_64__) && defined(__GLIBC__)
#define XKAFKA_ROCKSDB_FILE "librocksdbjni-linux64.so"
#elif defined(__linux__) && defined(__x86_64__)
#define XKAFKA_ROCKSDB_FILE "librocksdbjni-linux64-musl.so"
#elif defined(__linux__) && defined(__aarch64__) && defined(__GLIBC__)
#define XKAFKA_ROCKSDB_FILE "librocksdbjni-linux-aarch64.so"
#elif defined(__linux__) && defined(__aarch64__)
#define XKAFKA_ROCKSDB_FILE "librocksdbjni-linux-aarch64-musl.so"
#endif

#ifndef XKAFKA_ROCKSDB_SHIM_ID
#define XKAFKA_ROCKSDB_SHIM_ID ""
#endif

/* Kafka Streams' RocksDBStore settings, which the shim's stores share. */
#define XKAFKA_ROCKSDB_BLOCK_CACHE_SIZE (50 * 1024 * 1024)
#define XKAFKA_ROCKSDB_BLOCK_SIZE 4096
#define XKAFKA_ROCKSDB_BLOOM_BITS_PER_KEY 10.0
#define XKAFKA_ROCKSDB_WRITE_BUFFER_SIZE (16 * 1024 * 1024)
#define XKAFKA_ROCKSDB_MAX_WRITE_BUFFERS 3
#define XKAFKA_ROCKSDB_NO_COMPRESSION 0
#define XKAFKA_ROCKSDB_UNIVERSAL_COMPACTION 1
#define XKAFKA_ROCKSDB_ERROR_LOG_LEVEL 3

#define XKAFKA_ROCKSDB_PUT 1
#define XKAFKA_ROCKSDB_DELETE 2
#define XKAFKA_ROCKSDB_DELETE_RANGE 3

typedef void (*xkafka_rocksdb_done_t)(int64_t token);

/* RocksDB's C API, as the shim calls it. Its handles are opaque. */
typedef struct {
        void *(*options_create)(void);
        void (*options_destroy)(void *);
        void (*options_set_create_if_missing)(void *, unsigned char);
        void (*options_set_create_missing_column_families)(void *, unsigned char);
        void (*options_set_atomic_flush)(void *, unsigned char);
        void (*options_set_optimize_filters_for_hits)(void *, int);
        void (*options_set_write_buffer_size)(void *, size_t);
        void (*options_set_max_write_buffer_number)(void *, int);
        void (*options_set_compression)(void *, int);
        void (*options_set_compaction_style)(void *, int);
        void (*options_set_info_log_level)(void *, int);
        void (*options_increase_parallelism)(void *, int);
        void (*options_set_block_based_table_factory)(void *, void *);
        void *(*block_based_options_create)(void);
        void (*block_based_options_destroy)(void *);
        void (*block_based_options_set_block_size)(void *, size_t);
        void (*block_based_options_set_block_cache)(void *, void *);
        void (*block_based_options_set_filter_policy)(void *, void *);
        void *(*cache_create_lru)(size_t);
        void (*cache_destroy)(void *);
        void *(*filterpolicy_create_bloom)(double);
        void *(*open_column_families)(const void *, const char *, int, const char *const *, const void *const *, void **, char **);
        void (*column_family_handle_destroy)(void *);
        void (*close)(void *);
        void *(*writeoptions_create)(void);
        void (*writeoptions_destroy)(void *);
        void (*writeoptions_disable_WAL)(void *, int);
        void *(*readoptions_create)(void);
        void (*readoptions_destroy)(void *);
        void (*readoptions_set_iterate_upper_bound)(void *, const char *, size_t);
        void *(*flushoptions_create)(void);
        void (*flushoptions_destroy)(void *);
        void (*flushoptions_set_wait)(void *, unsigned char);
        void *(*get_pinned_cf)(void *, const void *, void *, const char *, size_t, char **);
        const char *(*pinnableslice_value)(const void *, size_t *);
        void (*pinnableslice_destroy)(void *);
        void *(*writebatch_create)(void);
        void (*writebatch_destroy)(void *);
        void (*writebatch_put_cf)(void *, void *, const char *, size_t, const char *, size_t);
        void (*writebatch_delete_cf)(void *, void *, const char *, size_t);
        void (*writebatch_delete_range_cf)(void *, void *, const char *, size_t, const char *, size_t);
        void (*write)(void *, const void *, void *, char **);
        void *(*create_iterator_cf)(void *, const void *, void *);
        void (*iter_seek)(void *, const char *, size_t);
        void (*iter_seek_to_first)(void *);
        unsigned char (*iter_valid)(const void *);
        void (*iter_next)(void *);
        const char *(*iter_key)(const void *, size_t *);
        const char *(*iter_value)(const void *, size_t *);
        void (*iter_get_error)(const void *, char **);
        void (*iter_destroy)(void *);
        void (*flush_cfs)(void *, const void *, void **, int, char **);
        void (*free)(void *);
        /* rocksdbjni's RocksDB.version(), which answers a constant and reads
         * neither of its arguments. */
        int32_t (*jni_version)(void *, void *);
} xkafka_rocksdb_api_t;

/* Each function's name and where its address goes. */
typedef struct {
        const char *name;
        size_t offset;
} xkafka_rocksdb_symbol_t;

#define XKAFKA_ROCKSDB_SYMBOL(field, name) {name, offsetof(xkafka_rocksdb_api_t, field)}

static const xkafka_rocksdb_symbol_t xkafka_rocksdb_symbols[] = {
    XKAFKA_ROCKSDB_SYMBOL(options_create, "rocksdb_options_create"),
    XKAFKA_ROCKSDB_SYMBOL(options_destroy, "rocksdb_options_destroy"),
    XKAFKA_ROCKSDB_SYMBOL(options_set_create_if_missing, "rocksdb_options_set_create_if_missing"),
    XKAFKA_ROCKSDB_SYMBOL(options_set_create_missing_column_families, "rocksdb_options_set_create_missing_column_families"),
    XKAFKA_ROCKSDB_SYMBOL(options_set_atomic_flush, "rocksdb_options_set_atomic_flush"),
    XKAFKA_ROCKSDB_SYMBOL(options_set_optimize_filters_for_hits, "rocksdb_options_set_optimize_filters_for_hits"),
    XKAFKA_ROCKSDB_SYMBOL(options_set_write_buffer_size, "rocksdb_options_set_write_buffer_size"),
    XKAFKA_ROCKSDB_SYMBOL(options_set_max_write_buffer_number, "rocksdb_options_set_max_write_buffer_number"),
    XKAFKA_ROCKSDB_SYMBOL(options_set_compression, "rocksdb_options_set_compression"),
    XKAFKA_ROCKSDB_SYMBOL(options_set_compaction_style, "rocksdb_options_set_compaction_style"),
    XKAFKA_ROCKSDB_SYMBOL(options_set_info_log_level, "rocksdb_options_set_info_log_level"),
    XKAFKA_ROCKSDB_SYMBOL(options_increase_parallelism, "rocksdb_options_increase_parallelism"),
    XKAFKA_ROCKSDB_SYMBOL(options_set_block_based_table_factory, "rocksdb_options_set_block_based_table_factory"),
    XKAFKA_ROCKSDB_SYMBOL(block_based_options_create, "rocksdb_block_based_options_create"),
    XKAFKA_ROCKSDB_SYMBOL(block_based_options_destroy, "rocksdb_block_based_options_destroy"),
    XKAFKA_ROCKSDB_SYMBOL(block_based_options_set_block_size, "rocksdb_block_based_options_set_block_size"),
    XKAFKA_ROCKSDB_SYMBOL(block_based_options_set_block_cache, "rocksdb_block_based_options_set_block_cache"),
    XKAFKA_ROCKSDB_SYMBOL(block_based_options_set_filter_policy, "rocksdb_block_based_options_set_filter_policy"),
    XKAFKA_ROCKSDB_SYMBOL(cache_create_lru, "rocksdb_cache_create_lru"),
    XKAFKA_ROCKSDB_SYMBOL(cache_destroy, "rocksdb_cache_destroy"),
    XKAFKA_ROCKSDB_SYMBOL(filterpolicy_create_bloom, "rocksdb_filterpolicy_create_bloom"),
    XKAFKA_ROCKSDB_SYMBOL(open_column_families, "rocksdb_open_column_families"),
    XKAFKA_ROCKSDB_SYMBOL(column_family_handle_destroy, "rocksdb_column_family_handle_destroy"),
    XKAFKA_ROCKSDB_SYMBOL(close, "rocksdb_close"),
    XKAFKA_ROCKSDB_SYMBOL(writeoptions_create, "rocksdb_writeoptions_create"),
    XKAFKA_ROCKSDB_SYMBOL(writeoptions_destroy, "rocksdb_writeoptions_destroy"),
    XKAFKA_ROCKSDB_SYMBOL(writeoptions_disable_WAL, "rocksdb_writeoptions_disable_WAL"),
    XKAFKA_ROCKSDB_SYMBOL(readoptions_create, "rocksdb_readoptions_create"),
    XKAFKA_ROCKSDB_SYMBOL(readoptions_destroy, "rocksdb_readoptions_destroy"),
    XKAFKA_ROCKSDB_SYMBOL(readoptions_set_iterate_upper_bound, "rocksdb_readoptions_set_iterate_upper_bound"),
    XKAFKA_ROCKSDB_SYMBOL(flushoptions_create, "rocksdb_flushoptions_create"),
    XKAFKA_ROCKSDB_SYMBOL(flushoptions_destroy, "rocksdb_flushoptions_destroy"),
    XKAFKA_ROCKSDB_SYMBOL(flushoptions_set_wait, "rocksdb_flushoptions_set_wait"),
    XKAFKA_ROCKSDB_SYMBOL(get_pinned_cf, "rocksdb_get_pinned_cf"),
    XKAFKA_ROCKSDB_SYMBOL(pinnableslice_value, "rocksdb_pinnableslice_value"),
    XKAFKA_ROCKSDB_SYMBOL(pinnableslice_destroy, "rocksdb_pinnableslice_destroy"),
    XKAFKA_ROCKSDB_SYMBOL(writebatch_create, "rocksdb_writebatch_create"),
    XKAFKA_ROCKSDB_SYMBOL(writebatch_destroy, "rocksdb_writebatch_destroy"),
    XKAFKA_ROCKSDB_SYMBOL(writebatch_put_cf, "rocksdb_writebatch_put_cf"),
    XKAFKA_ROCKSDB_SYMBOL(writebatch_delete_cf, "rocksdb_writebatch_delete_cf"),
    XKAFKA_ROCKSDB_SYMBOL(writebatch_delete_range_cf, "rocksdb_writebatch_delete_range_cf"),
    XKAFKA_ROCKSDB_SYMBOL(write, "rocksdb_write"),
    XKAFKA_ROCKSDB_SYMBOL(create_iterator_cf, "rocksdb_create_iterator_cf"),
    XKAFKA_ROCKSDB_SYMBOL(iter_seek, "rocksdb_iter_seek"),
    XKAFKA_ROCKSDB_SYMBOL(iter_seek_to_first, "rocksdb_iter_seek_to_first"),
    XKAFKA_ROCKSDB_SYMBOL(iter_valid, "rocksdb_iter_valid"),
    XKAFKA_ROCKSDB_SYMBOL(iter_next, "rocksdb_iter_next"),
    XKAFKA_ROCKSDB_SYMBOL(iter_key, "rocksdb_iter_key"),
    XKAFKA_ROCKSDB_SYMBOL(iter_value, "rocksdb_iter_value"),
    XKAFKA_ROCKSDB_SYMBOL(iter_get_error, "rocksdb_iter_get_error"),
    XKAFKA_ROCKSDB_SYMBOL(iter_destroy, "rocksdb_iter_destroy"),
    XKAFKA_ROCKSDB_SYMBOL(flush_cfs, "rocksdb_flush_cfs"),
    XKAFKA_ROCKSDB_SYMBOL(free, "rocksdb_free"),
    XKAFKA_ROCKSDB_SYMBOL(jni_version, "Java_org_rocksdb_RocksDB_version"),
};

/* The library, once loaded, and the functions found in it. Both are written
 * once, under the lock, before `loaded` is set, and only read after. */
static pthread_mutex_t xkafka_rocksdb_lock = PTHREAD_MUTEX_INITIALIZER;
static xkafka_rocksdb_api_t xkafka_rocksdb_api;
static char xkafka_rocksdb_loaded_version[32];
static int xkafka_rocksdb_loaded = 0;

typedef struct {
        void *db;
        size_t count;
        /* The families by index, then the default family where `families`
         * did not name it, which is kept only to be flushed and closed. */
        void **families;
        size_t opened;
        void *write_options;
        void *read_options;
        void *flush_options;
} xkafka_rocksdb_store_t;

static char *xkafka_rocksdb_copy(const char *value) {
        size_t size = strlen(value) + 1;
        char *copy = (char *)malloc(size);

        if (copy != NULL)
                memcpy(copy, value, size);
        return copy;
}

static int xkafka_rocksdb_fail(char **error, const char *message) {
        if (error != NULL)
                *error = xkafka_rocksdb_copy(message);
        return -1;
}

static int xkafka_rocksdb_failf(char **error, const char *format, const char *a, const char *b) {
        char message[1024];

        snprintf(message, sizeof message, format, a, b);
        return xkafka_rocksdb_fail(error, message);
}

/* Moves an error RocksDB allocated into one the shim allocated, so the caller
 * frees whatever it gets with the shim's own free. */
static int xkafka_rocksdb_take_error(char **error, char *rocksdb_error) {
        int failed = xkafka_rocksdb_fail(error, rocksdb_error);

        xkafka_rocksdb_api.free(rocksdb_error);
        return failed;
}

/* The directory the running executable is in, with a trailing slash, or an
 * empty string where it cannot be told. */
static void xkafka_rocksdb_executable_directory(char *out, size_t size) {
        char path[PATH_MAX];
        char *slash;

        out[0] = '\0';
#ifdef __APPLE__
        {
                uint32_t length = (uint32_t)sizeof path;
                if (_NSGetExecutablePath(path, &length) != 0)
                        return;
        }
#else
        {
                ssize_t length = readlink("/proc/self/exe", path, sizeof path - 1);
                if (length <= 0)
                        return;
                path[length] = '\0';
        }
#endif
        slash = strrchr(path, '/');
        if (slash == NULL || (size_t)(slash - path) + 2 > size)
                return;
        memcpy(out, path, (size_t)(slash - path) + 1);
        out[(slash - path) + 1] = '\0';
}

/* Opens one candidate, appending why it could not be opened to `tried`. */
static void *xkafka_rocksdb_open_library(const char *path, char *tried, size_t size) {
        void *library = dlopen(path, RTLD_NOW | RTLD_LOCAL);
        size_t used = strlen(tried);

        if (library == NULL && used < size) {
                const char *reason = dlerror();
                snprintf(tried + used, size - used, "%s%s (%s)", used == 0 ? "" : "; ", path, reason == NULL ? "not found" : reason);
        }
        return library;
}

static int xkafka_rocksdb_check_version(const char *expected, char **error) {
        if (expected != NULL && strcmp(expected, xkafka_rocksdb_loaded_version) != 0)
                return xkafka_rocksdb_failf(error, "the loaded RocksDB library is release %s, but xkafka needs %s", xkafka_rocksdb_loaded_version,
                                            expected);
        return 0;
}

int xkafka_rocksdb_load(const char *path, const char *expected, char **error) {
        char tried[4096] = "";
        void *library = NULL;
        xkafka_rocksdb_api_t api;
        size_t index;
        int32_t encoded;
        int result;

        pthread_mutex_lock(&xkafka_rocksdb_lock);
        if (xkafka_rocksdb_loaded) {
                result = xkafka_rocksdb_check_version(expected, error);
                pthread_mutex_unlock(&xkafka_rocksdb_lock);
                return result;
        }

        if (path != NULL) {
                library = xkafka_rocksdb_open_library(path, tried, sizeof tried);
        } else if (getenv("XKAFKA_ROCKSDB_LIBRARY") != NULL && getenv("XKAFKA_ROCKSDB_LIBRARY")[0] != '\0') {
                library = xkafka_rocksdb_open_library(getenv("XKAFKA_ROCKSDB_LIBRARY"), tried, sizeof tried);
        } else {
#ifdef XKAFKA_ROCKSDB_FILE
                char beside[PATH_MAX + sizeof XKAFKA_ROCKSDB_FILE];
                xkafka_rocksdb_executable_directory(beside, PATH_MAX);
                if (beside[0] != '\0') {
                        strcat(beside, XKAFKA_ROCKSDB_FILE);
                        library = xkafka_rocksdb_open_library(beside, tried, sizeof tried);
                }
                if (library == NULL)
                        library = xkafka_rocksdb_open_library(XKAFKA_ROCKSDB_FILE, tried, sizeof tried);
#else
                pthread_mutex_unlock(&xkafka_rocksdb_lock);
                return xkafka_rocksdb_fail(error, "rocksdbjni carries no RocksDB library for this platform");
#endif
        }
        if (library == NULL) {
                pthread_mutex_unlock(&xkafka_rocksdb_lock);
                return xkafka_rocksdb_failf(error, "could not load the RocksDB library: %s%s", tried, "");
        }

        for (index = 0; index < sizeof xkafka_rocksdb_symbols / sizeof xkafka_rocksdb_symbols[0]; index++) {
                void *address = dlsym(library, xkafka_rocksdb_symbols[index].name);
                if (address == NULL) {
                        dlclose(library);
                        pthread_mutex_unlock(&xkafka_rocksdb_lock);
                        return xkafka_rocksdb_failf(error, "the library is not the RocksDB of rocksdbjni: it has no %s%s",
                                                    xkafka_rocksdb_symbols[index].name, "");
                }
                memcpy((char *)&api + xkafka_rocksdb_symbols[index].offset, &address, sizeof address);
        }

        encoded = api.jni_version(NULL, NULL);
        snprintf(xkafka_rocksdb_loaded_version, sizeof xkafka_rocksdb_loaded_version, "%d.%d.%d", (encoded >> 16) & 0xff,
                 (encoded >> 8) & 0xff, encoded & 0xff);
        if (expected != NULL && strcmp(expected, xkafka_rocksdb_loaded_version) != 0) {
                result = xkafka_rocksdb_failf(error, "the RocksDB library is release %s, but xkafka needs %s", xkafka_rocksdb_loaded_version,
                                              expected);
                xkafka_rocksdb_loaded_version[0] = '\0';
                dlclose(library);
                pthread_mutex_unlock(&xkafka_rocksdb_lock);
                return result;
        }

        /* The library stays loaded for the rest of the process: RocksDB keeps
         * threads of its own that outlive any store. */
        xkafka_rocksdb_api = api;
        xkafka_rocksdb_loaded = 1;
        pthread_mutex_unlock(&xkafka_rocksdb_lock);
        return 0;
}

const char *xkafka_rocksdb_version(void) {
        const char *version;

        pthread_mutex_lock(&xkafka_rocksdb_lock);
        version = xkafka_rocksdb_loaded ? xkafka_rocksdb_loaded_version : NULL;
        pthread_mutex_unlock(&xkafka_rocksdb_lock);
        return version;
}

const char *xkafka_rocksdb_identity(void) { return XKAFKA_ROCKSDB_SHIM_ID; }

void xkafka_rocksdb_free(void *pointer) { free(pointer); }

static int xkafka_rocksdb_ready(char **error) {
        int loaded;

        pthread_mutex_lock(&xkafka_rocksdb_lock);
        loaded = xkafka_rocksdb_loaded;
        pthread_mutex_unlock(&xkafka_rocksdb_lock);
        return loaded ? 0 : xkafka_rocksdb_fail(error, "the RocksDB library is not loaded");
}

/* Creates `path` and every directory above it that is missing. */
static int xkafka_rocksdb_create_directories(const char *path, char **error) {
        char *copy = xkafka_rocksdb_copy(path);
        char *cursor;
        int result = 0;

        if (copy == NULL)
                return xkafka_rocksdb_fail(error, "out of memory");
        for (cursor = copy + 1; result == 0; cursor++) {
                int last = *cursor == '\0';
                if (*cursor != '/' && !last)
                        continue;
                *cursor = '\0';
                if (mkdir(copy, 0755) != 0 && errno != EEXIST)
                        result = xkafka_rocksdb_failf(error, "could not create %s: %s", copy, strerror(errno));
                if (last)
                        break;
                *cursor = '/';
        }
        free(copy);
        return result;
}

static void xkafka_rocksdb_store_free(xkafka_rocksdb_store_t *store) {
        if (store->write_options != NULL)
                xkafka_rocksdb_api.writeoptions_destroy(store->write_options);
        if (store->read_options != NULL)
                xkafka_rocksdb_api.readoptions_destroy(store->read_options);
        if (store->flush_options != NULL)
                xkafka_rocksdb_api.flushoptions_destroy(store->flush_options);
        free(store->families);
        free(store);
}

/* The options Kafka Streams' RocksDBStore opens a store with. */
static void *xkafka_rocksdb_options(void) {
        void *options = xkafka_rocksdb_api.options_create();
        void *table = xkafka_rocksdb_api.block_based_options_create();
        void *cache = xkafka_rocksdb_api.cache_create_lru(XKAFKA_ROCKSDB_BLOCK_CACHE_SIZE);
        long processors = sysconf(_SC_NPROCESSORS_ONLN);

        /* The table options keep the cache and own the filter policy, and the
         * options copy the table options, so only the options remain to be
         * freed. */
        xkafka_rocksdb_api.block_based_options_set_block_cache(table, cache);
        xkafka_rocksdb_api.cache_destroy(cache);
        xkafka_rocksdb_api.block_based_options_set_block_size(table, XKAFKA_ROCKSDB_BLOCK_SIZE);
        xkafka_rocksdb_api.block_based_options_set_filter_policy(table, xkafka_rocksdb_api.filterpolicy_create_bloom(XKAFKA_ROCKSDB_BLOOM_BITS_PER_KEY));
        xkafka_rocksdb_api.options_set_block_based_table_factory(options, table);
        xkafka_rocksdb_api.block_based_options_destroy(table);

        xkafka_rocksdb_api.options_set_create_if_missing(options, 1);
        xkafka_rocksdb_api.options_set_create_missing_column_families(options, 1);
        xkafka_rocksdb_api.options_set_atomic_flush(options, 1);
        xkafka_rocksdb_api.options_set_optimize_filters_for_hits(options, 1);
        xkafka_rocksdb_api.options_set_write_buffer_size(options, XKAFKA_ROCKSDB_WRITE_BUFFER_SIZE);
        xkafka_rocksdb_api.options_set_max_write_buffer_number(options, XKAFKA_ROCKSDB_MAX_WRITE_BUFFERS);
        xkafka_rocksdb_api.options_set_compression(options, XKAFKA_ROCKSDB_NO_COMPRESSION);
        xkafka_rocksdb_api.options_set_compaction_style(options, XKAFKA_ROCKSDB_UNIVERSAL_COMPACTION);
        xkafka_rocksdb_api.options_set_info_log_level(options, XKAFKA_ROCKSDB_ERROR_LOG_LEVEL);
        xkafka_rocksdb_api.options_increase_parallelism(options, processors < 2 ? 2 : (int)processors);
        return options;
}

void *xkafka_rocksdb_open(const char *path, const char *const *families, size_t count, char **error) {
        xkafka_rocksdb_store_t *store;
        const char **names;
        const void **options;
        void *shared;
        char *rocksdb_error = NULL;
        size_t index;
        int has_default = 0;

        if (xkafka_rocksdb_ready(error) != 0)
                return NULL;
        if (path == NULL || path[0] == '\0') {
                xkafka_rocksdb_fail(error, "a store needs a path");
                return NULL;
        }
        if (count > 255) {
                xkafka_rocksdb_fail(error, "a store has at most 255 column families");
                return NULL;
        }
        for (index = 0; index < count; index++)
                if (strcmp(families[index], "default") == 0)
                        has_default = 1;
        if (xkafka_rocksdb_create_directories(path, error) != 0)
                return NULL;

        store = (xkafka_rocksdb_store_t *)calloc(1, sizeof(xkafka_rocksdb_store_t));
        names = (const char **)calloc(count + 1, sizeof(const char *));
        options = (const void **)calloc(count + 1, sizeof(void *));
        if (store != NULL)
                store->families = (void **)calloc(count + 1, sizeof(void *));
        if (store == NULL || names == NULL || options == NULL || store->families == NULL) {
                if (store != NULL)
                        free(store->families);
                free(store);
                free(names);
                free(options);
                xkafka_rocksdb_fail(error, "out of memory");
                return NULL;
        }
        store->count = count;
        store->opened = count + (has_default ? 0 : 1);
        shared = xkafka_rocksdb_options();
        for (index = 0; index < count; index++)
                names[index] = families[index];
        if (!has_default)
                names[count] = "default";
        for (index = 0; index < store->opened; index++)
                options[index] = shared;

        store->db = xkafka_rocksdb_api.open_column_families(shared, path, (int)store->opened, names, options, store->families, &rocksdb_error);
        xkafka_rocksdb_api.options_destroy(shared);
        free(names);
        free(options);
        if (rocksdb_error != NULL) {
                xkafka_rocksdb_take_error(error, rocksdb_error);
                xkafka_rocksdb_store_free(store);
                return NULL;
        }

        /* A store is refilled from its changelog after a failure, so its writes
         * skip the write-ahead log, and a flush waits until it has finished. */
        store->write_options = xkafka_rocksdb_api.writeoptions_create();
        xkafka_rocksdb_api.writeoptions_disable_WAL(store->write_options, 1);
        store->read_options = xkafka_rocksdb_api.readoptions_create();
        store->flush_options = xkafka_rocksdb_api.flushoptions_create();
        xkafka_rocksdb_api.flushoptions_set_wait(store->flush_options, 1);
        return store;
}

static int xkafka_rocksdb_family(xkafka_rocksdb_store_t *store, size_t family, void **handle, char **error) {
        char index[32];

        if (store == NULL)
                return xkafka_rocksdb_fail(error, "no store");
        if (family >= store->count) {
                snprintf(index, sizeof index, "%zu", family);
                return xkafka_rocksdb_failf(error, "the store has no column family %s%s", index, "");
        }
        *handle = store->families[family];
        return 0;
}

int xkafka_rocksdb_get(void *handle, size_t family, const void *key, size_t key_length, void **value, size_t *value_length, int *found,
                       char **error) {
        xkafka_rocksdb_store_t *store = (xkafka_rocksdb_store_t *)handle;
        void *column = NULL;
        void *slice;
        char *rocksdb_error = NULL;
        const char *bytes;
        size_t length = 0;

        *value = NULL;
        *value_length = 0;
        *found = 0;
        if (xkafka_rocksdb_family(store, family, &column, error) != 0)
                return -1;
        slice = xkafka_rocksdb_api.get_pinned_cf(store->db, store->read_options, column, key == NULL ? "" : (const char *)key, key_length,
                                                 &rocksdb_error);
        if (rocksdb_error != NULL)
                return xkafka_rocksdb_take_error(error, rocksdb_error);
        if (slice == NULL)
                return 0;
        bytes = xkafka_rocksdb_api.pinnableslice_value(slice, &length);
        if (length > 0) {
                *value = malloc(length);
                if (*value == NULL) {
                        xkafka_rocksdb_api.pinnableslice_destroy(slice);
                        return xkafka_rocksdb_fail(error, "out of memory");
                }
                memcpy(*value, bytes, length);
        }
        *value_length = length;
        *found = 1;
        xkafka_rocksdb_api.pinnableslice_destroy(slice);
        return 0;
}

/* Reads one length-prefixed byte string of a batch, or answers -1 where the
 * batch ends before it does. */
static int xkafka_rocksdb_read_bytes(const unsigned char *batch, size_t length, size_t *at, const char **bytes, size_t *size) {
        uint32_t prefix;

        if (length - *at < 4)
                return -1;
        prefix = ((uint32_t)batch[*at] << 24) | ((uint32_t)batch[*at + 1] << 16) | ((uint32_t)batch[*at + 2] << 8) | (uint32_t)batch[*at + 3];
        *at += 4;
        if (length - *at < prefix)
                return -1;
        *bytes = (const char *)batch + *at;
        *size = prefix;
        *at += prefix;
        return 0;
}

int xkafka_rocksdb_write(void *handle, const void *encoded, size_t length, char **error) {
        xkafka_rocksdb_store_t *store = (xkafka_rocksdb_store_t *)handle;
        const unsigned char *batch = (const unsigned char *)encoded;
        void *writes;
        char *rocksdb_error = NULL;
        char where[64];
        size_t at = 0;

        if (store == NULL)
                return xkafka_rocksdb_fail(error, "no store");
        writes = xkafka_rocksdb_api.writebatch_create();
        while (at < length) {
                size_t start = at;
                const char *first = NULL;
                const char *second = NULL;
                size_t first_size = 0;
                size_t second_size = 0;
                void *column = NULL;
                int operation;
                int decoded;

                if (length - at < 2) {
                        decoded = -1;
                } else {
                        operation = batch[at];
                        if (xkafka_rocksdb_family(store, batch[at + 1], &column, error) != 0) {
                                xkafka_rocksdb_api.writebatch_destroy(writes);
                                return -1;
                        }
                        at += 2;
                        decoded = xkafka_rocksdb_read_bytes(batch, length, &at, &first, &first_size);
                        if (decoded == 0 && operation != XKAFKA_ROCKSDB_DELETE)
                                decoded = xkafka_rocksdb_read_bytes(batch, length, &at, &second, &second_size);
                        if (decoded == 0 && operation == XKAFKA_ROCKSDB_PUT)
                                xkafka_rocksdb_api.writebatch_put_cf(writes, column, first, first_size, second, second_size);
                        else if (decoded == 0 && operation == XKAFKA_ROCKSDB_DELETE)
                                xkafka_rocksdb_api.writebatch_delete_cf(writes, column, first, first_size);
                        else if (decoded == 0 && operation == XKAFKA_ROCKSDB_DELETE_RANGE)
                                xkafka_rocksdb_api.writebatch_delete_range_cf(writes, column, first, first_size, second, second_size);
                        else if (decoded == 0)
                                decoded = -2;
                }
                if (decoded != 0) {
                        xkafka_rocksdb_api.writebatch_destroy(writes);
                        snprintf(where, sizeof where, "%zu", start);
                        return xkafka_rocksdb_failf(error, decoded == -2 ? "the batch has an unknown operation at byte %s%s"
                                                                        : "the batch ends inside its operation at byte %s%s",
                                                    where, "");
                }
        }
        xkafka_rocksdb_api.write(store->db, store->write_options, writes, &rocksdb_error);
        xkafka_rocksdb_api.writebatch_destroy(writes);
        if (rocksdb_error != NULL)
                return xkafka_rocksdb_take_error(error, rocksdb_error);
        return 0;
}

/* A buffer the range's entries are encoded into, which grows as they come. */
typedef struct {
        unsigned char *bytes;
        size_t length;
        size_t capacity;
} xkafka_rocksdb_buffer_t;

static int xkafka_rocksdb_append(xkafka_rocksdb_buffer_t *buffer, const char *bytes, size_t size) {
        size_t needed = buffer->length + 4 + size;

        if (size > UINT32_MAX)
                return -1;
        if (needed > buffer->capacity) {
                size_t capacity = buffer->capacity == 0 ? 4096 : buffer->capacity;
                unsigned char *grown;
                while (capacity < needed)
                        capacity *= 2;
                grown = (unsigned char *)realloc(buffer->bytes, capacity);
                if (grown == NULL)
                        return -1;
                buffer->bytes = grown;
                buffer->capacity = capacity;
        }
        buffer->bytes[buffer->length] = (unsigned char)(size >> 24);
        buffer->bytes[buffer->length + 1] = (unsigned char)(size >> 16);
        buffer->bytes[buffer->length + 2] = (unsigned char)(size >> 8);
        buffer->bytes[buffer->length + 3] = (unsigned char)size;
        if (size > 0)
                memcpy(buffer->bytes + buffer->length + 4, bytes, size);
        buffer->length = needed;
        return 0;
}

int xkafka_rocksdb_range(void *handle, size_t family, const void *from, size_t from_length, const void *to, size_t to_length, size_t limit,
                         void **entries, size_t *length, char **error) {
        xkafka_rocksdb_store_t *store = (xkafka_rocksdb_store_t *)handle;
        xkafka_rocksdb_buffer_t buffer = {NULL, 0, 0};
        void *column = NULL;
        void *read_options;
        void *iterator;
        char *rocksdb_error = NULL;
        size_t count = 0;
        int failed = 0;

        *entries = NULL;
        *length = 0;
        if (xkafka_rocksdb_family(store, family, &column, error) != 0)
                return -1;
        /* The bound is RocksDB's to read while the iterator lives, and stays
         * the caller's until the call returns. */
        read_options = xkafka_rocksdb_api.readoptions_create();
        if (to != NULL)
                xkafka_rocksdb_api.readoptions_set_iterate_upper_bound(read_options, (const char *)to, to_length);
        iterator = xkafka_rocksdb_api.create_iterator_cf(store->db, read_options, column);
        if (from != NULL)
                xkafka_rocksdb_api.iter_seek(iterator, (const char *)from, from_length);
        else
                xkafka_rocksdb_api.iter_seek_to_first(iterator);
        while (!failed && (limit == 0 || count < limit) && xkafka_rocksdb_api.iter_valid(iterator)) {
                size_t key_size = 0;
                size_t value_size = 0;
                const char *key = xkafka_rocksdb_api.iter_key(iterator, &key_size);
                const char *value = xkafka_rocksdb_api.iter_value(iterator, &value_size);
                failed = xkafka_rocksdb_append(&buffer, key, key_size) != 0 || xkafka_rocksdb_append(&buffer, value, value_size) != 0;
                count++;
                xkafka_rocksdb_api.iter_next(iterator);
        }
        xkafka_rocksdb_api.iter_get_error(iterator, &rocksdb_error);
        xkafka_rocksdb_api.iter_destroy(iterator);
        xkafka_rocksdb_api.readoptions_destroy(read_options);
        if (rocksdb_error != NULL || failed) {
                free(buffer.bytes);
                return rocksdb_error != NULL ? xkafka_rocksdb_take_error(error, rocksdb_error) : xkafka_rocksdb_fail(error, "out of memory");
        }
        *entries = buffer.bytes;
        *length = buffer.length;
        return 0;
}

int xkafka_rocksdb_flush(void *handle, char **error) {
        xkafka_rocksdb_store_t *store = (xkafka_rocksdb_store_t *)handle;
        char *rocksdb_error = NULL;

        if (store == NULL)
                return xkafka_rocksdb_fail(error, "no store");
        xkafka_rocksdb_api.flush_cfs(store->db, store->flush_options, store->families, (int)store->opened, &rocksdb_error);
        if (rocksdb_error != NULL)
                return xkafka_rocksdb_take_error(error, rocksdb_error);
        return 0;
}

void xkafka_rocksdb_close(void *handle) {
        xkafka_rocksdb_store_t *store = (xkafka_rocksdb_store_t *)handle;
        size_t index;

        if (store == NULL)
                return;
        for (index = 0; index < store->opened; index++)
                xkafka_rocksdb_api.column_family_handle_destroy(store->families[index]);
        xkafka_rocksdb_api.close(store->db);
        xkafka_rocksdb_store_free(store);
}

/* ---- Calls on their own thread ---- */

typedef struct xkafka_rocksdb_job_s {
        void (*run)(struct xkafka_rocksdb_job_s *job);
        void (*release)(struct xkafka_rocksdb_job_s *job);
        xkafka_rocksdb_done_t done;
        int64_t token;
} xkafka_rocksdb_job_t;

static void *xkafka_rocksdb_job_thread(void *argument) {
        xkafka_rocksdb_job_t *job = (xkafka_rocksdb_job_t *)argument;
        xkafka_rocksdb_done_t done = job->done;
        int64_t token = job->token;

        job->run(job);
        if (job->release != NULL)
                job->release(job);
        free(job);
        /* Last, since the caller may release what the call wrote into as soon
         * as it hears back. */
        done(token);
        return NULL;
}

static int xkafka_rocksdb_job_start(xkafka_rocksdb_job_t *job, xkafka_rocksdb_done_t done, int64_t token) {
        pthread_attr_t attributes;
        pthread_t thread;
        int started = 0;

        job->done = done;
        job->token = token;
        if (pthread_attr_init(&attributes) == 0) {
                pthread_attr_setdetachstate(&attributes, PTHREAD_CREATE_DETACHED);
                started = pthread_create(&thread, &attributes, xkafka_rocksdb_job_thread, job) == 0;
                pthread_attr_destroy(&attributes);
        }
        if (!started) {
                if (job->release != NULL)
                        job->release(job);
                free(job);
                return -1;
        }
        return 0;
}

typedef struct {
        xkafka_rocksdb_job_t job;
        char *path;
        char **families;
        size_t count;
        char **error;
        void **result;
} xkafka_rocksdb_open_job_t;

static void xkafka_rocksdb_open_run(xkafka_rocksdb_job_t *job) {
        xkafka_rocksdb_open_job_t *call = (xkafka_rocksdb_open_job_t *)job;
        *call->result = xkafka_rocksdb_open(call->path, (const char *const *)call->families, call->count, call->error);
}

static void xkafka_rocksdb_open_release(xkafka_rocksdb_job_t *job) {
        xkafka_rocksdb_open_job_t *call = (xkafka_rocksdb_open_job_t *)job;
        size_t index;

        free(call->path);
        if (call->families != NULL)
                for (index = 0; index < call->count; index++)
                        free(call->families[index]);
        free(call->families);
}

int xkafka_rocksdb_async_open(const char *path, const char *const *families, size_t count, char **error, void **result,
                              xkafka_rocksdb_done_t done, int64_t token) {
        xkafka_rocksdb_open_job_t *call = (xkafka_rocksdb_open_job_t *)calloc(1, sizeof(xkafka_rocksdb_open_job_t));
        size_t index;
        int copied = 1;

        if (call == NULL)
                return -1;
        call->job.run = xkafka_rocksdb_open_run;
        call->job.release = xkafka_rocksdb_open_release;
        call->count = count;
        call->error = error;
        call->result = result;
        call->path = path == NULL ? NULL : xkafka_rocksdb_copy(path);
        call->families = (char **)calloc(count + 1, sizeof(char *));
        copied = (path == NULL || call->path != NULL) && call->families != NULL;
        for (index = 0; copied && index < count; index++) {
                call->families[index] = xkafka_rocksdb_copy(families[index]);
                copied = call->families[index] != NULL;
        }
        if (!copied) {
                xkafka_rocksdb_open_release(&call->job);
                free(call);
                return -1;
        }
        return xkafka_rocksdb_job_start(&call->job, done, token);
}

typedef struct {
        xkafka_rocksdb_job_t job;
        void *store;
        size_t family;
        const void *key;
        size_t key_length;
        void **value;
        size_t *value_length;
        int *found;
        char **error;
        int32_t *result;
} xkafka_rocksdb_get_job_t;

static void xkafka_rocksdb_get_run(xkafka_rocksdb_job_t *job) {
        xkafka_rocksdb_get_job_t *call = (xkafka_rocksdb_get_job_t *)job;
        *call->result =
            xkafka_rocksdb_get(call->store, call->family, call->key, call->key_length, call->value, call->value_length, call->found, call->error);
}

int xkafka_rocksdb_async_get(void *store, size_t family, const void *key, size_t key_length, void **value, size_t *value_length, int *found,
                             char **error, int32_t *result, xkafka_rocksdb_done_t done, int64_t token) {
        xkafka_rocksdb_get_job_t *call = (xkafka_rocksdb_get_job_t *)calloc(1, sizeof(xkafka_rocksdb_get_job_t));

        if (call == NULL)
                return -1;
        call->job.run = xkafka_rocksdb_get_run;
        call->store = store;
        call->family = family;
        call->key = key;
        call->key_length = key_length;
        call->value = value;
        call->value_length = value_length;
        call->found = found;
        call->error = error;
        call->result = result;
        return xkafka_rocksdb_job_start(&call->job, done, token);
}

typedef struct {
        xkafka_rocksdb_job_t job;
        void *store;
        const void *batch;
        size_t length;
        char **error;
        int32_t *result;
} xkafka_rocksdb_write_job_t;

static void xkafka_rocksdb_write_run(xkafka_rocksdb_job_t *job) {
        xkafka_rocksdb_write_job_t *call = (xkafka_rocksdb_write_job_t *)job;
        *call->result = xkafka_rocksdb_write(call->store, call->batch, call->length, call->error);
}

int xkafka_rocksdb_async_write(void *store, const void *batch, size_t length, char **error, int32_t *result, xkafka_rocksdb_done_t done,
                               int64_t token) {
        xkafka_rocksdb_write_job_t *call = (xkafka_rocksdb_write_job_t *)calloc(1, sizeof(xkafka_rocksdb_write_job_t));

        if (call == NULL)
                return -1;
        call->job.run = xkafka_rocksdb_write_run;
        call->store = store;
        call->batch = batch;
        call->length = length;
        call->error = error;
        call->result = result;
        return xkafka_rocksdb_job_start(&call->job, done, token);
}

typedef struct {
        xkafka_rocksdb_job_t job;
        void *store;
        size_t family;
        const void *from;
        size_t from_length;
        const void *to;
        size_t to_length;
        size_t limit;
        void **entries;
        size_t *length;
        char **error;
        int32_t *result;
} xkafka_rocksdb_range_job_t;

static void xkafka_rocksdb_range_run(xkafka_rocksdb_job_t *job) {
        xkafka_rocksdb_range_job_t *call = (xkafka_rocksdb_range_job_t *)job;
        *call->result = xkafka_rocksdb_range(call->store, call->family, call->from, call->from_length, call->to, call->to_length, call->limit,
                                             call->entries, call->length, call->error);
}

int xkafka_rocksdb_async_range(void *store, size_t family, const void *from, size_t from_length, const void *to, size_t to_length,
                               size_t limit, void **entries, size_t *length, char **error, int32_t *result, xkafka_rocksdb_done_t done,
                               int64_t token) {
        xkafka_rocksdb_range_job_t *call = (xkafka_rocksdb_range_job_t *)calloc(1, sizeof(xkafka_rocksdb_range_job_t));

        if (call == NULL)
                return -1;
        call->job.run = xkafka_rocksdb_range_run;
        call->store = store;
        call->family = family;
        call->from = from;
        call->from_length = from_length;
        call->to = to;
        call->to_length = to_length;
        call->limit = limit;
        call->entries = entries;
        call->length = length;
        call->error = error;
        call->result = result;
        return xkafka_rocksdb_job_start(&call->job, done, token);
}

typedef struct {
        xkafka_rocksdb_job_t job;
        void *store;
        int closing;
        char **error;
        int32_t *result;
} xkafka_rocksdb_store_job_t;

static void xkafka_rocksdb_store_run(xkafka_rocksdb_job_t *job) {
        xkafka_rocksdb_store_job_t *call = (xkafka_rocksdb_store_job_t *)job;

        if (call->closing) {
                xkafka_rocksdb_close(call->store);
        } else {
                *call->result = xkafka_rocksdb_flush(call->store, call->error);
        }
}

static int xkafka_rocksdb_store_call(void *store, int closing, char **error, int32_t *result, xkafka_rocksdb_done_t done, int64_t token) {
        xkafka_rocksdb_store_job_t *call = (xkafka_rocksdb_store_job_t *)calloc(1, sizeof(xkafka_rocksdb_store_job_t));

        if (call == NULL)
                return -1;
        call->job.run = xkafka_rocksdb_store_run;
        call->store = store;
        call->closing = closing;
        call->error = error;
        call->result = result;
        return xkafka_rocksdb_job_start(&call->job, done, token);
}

int xkafka_rocksdb_async_flush(void *store, char **error, int32_t *result, xkafka_rocksdb_done_t done, int64_t token) {
        return xkafka_rocksdb_store_call(store, 0, error, result, done, token);
}

/* Closing flushes what is left and waits for RocksDB's background work, so
 * it may take a while too. */
int xkafka_rocksdb_async_close(void *store, xkafka_rocksdb_done_t done, int64_t token) {
        return xkafka_rocksdb_store_call(store, 1, NULL, NULL, done, token);
}

#endif
