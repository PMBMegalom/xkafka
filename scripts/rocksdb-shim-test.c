/* Tests the RocksDB shim through the library the npm package ships, for
 * scripts/rocksdb-shim-test.sh to run under a leak checker.
 *
 * Everything the shim hands back is freed and every store is closed, so any
 * allocation left unreachable at exit is the shim's.
 *
 * usage: rocksdb-shim-test library version identity directory
 *        rocksdb-shim-test --search version directory
 *
 * The first form tests every call, the shim's own threads included, with the
 * library at the path given. The second loads it as a Native program does,
 * with no path, from XKAFKA_ROCKSDB_LIBRARY or beside the executable. */

/* glibc declares nftw only for X/Open. */
#define _XOPEN_SOURCE 700

#include <dirent.h>
#include <ftw.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

typedef void (*xkafka_rocksdb_done_t)(int64_t token);

int xkafka_rocksdb_load(const char *path, const char *expected, char **error);
const char *xkafka_rocksdb_version(void);
const char *xkafka_rocksdb_identity(void);
void *xkafka_rocksdb_open(const char *path, const char *const *families, size_t count, char **error);
int xkafka_rocksdb_get(void *store, size_t family, const void *key, size_t key_length, void **value, size_t *value_length, int *found,
                       char **error);
int xkafka_rocksdb_write(void *store, const void *batch, size_t length, char **error);
int xkafka_rocksdb_range(void *store, size_t family, const void *from, size_t from_length, const void *to, size_t to_length, size_t limit,
                         void **entries, size_t *length, char **error);
int xkafka_rocksdb_flush(void *store, char **error);
void xkafka_rocksdb_close(void *store);
void xkafka_rocksdb_free(void *pointer);
int xkafka_rocksdb_async_open(const char *path, const char *const *families, size_t count, char **error, void **result,
                              xkafka_rocksdb_done_t done, int64_t token);
int xkafka_rocksdb_async_get(void *store, size_t family, const void *key, size_t key_length, void **value, size_t *value_length, int *found,
                             char **error, int32_t *result, xkafka_rocksdb_done_t done, int64_t token);
int xkafka_rocksdb_async_write(void *store, const void *batch, size_t length, char **error, int32_t *result, xkafka_rocksdb_done_t done,
                               int64_t token);
int xkafka_rocksdb_async_range(void *store, size_t family, const void *from, size_t from_length, const void *to, size_t to_length,
                               size_t limit, void **entries, size_t *length, char **error, int32_t *result, xkafka_rocksdb_done_t done,
                               int64_t token);
int xkafka_rocksdb_async_flush(void *store, char **error, int32_t *result, xkafka_rocksdb_done_t done, int64_t token);
int xkafka_rocksdb_async_close(void *store, xkafka_rocksdb_done_t done, int64_t token);

static int failures = 0;

#define CHECK(condition, ...)                                                                                                              \
        do {                                                                                                                               \
                if (!(condition)) {                                                                                                        \
                        fprintf(stderr, "%s:%d: ", __FILE__, __LINE__);                                                                    \
                        fprintf(stderr, __VA_ARGS__);                                                                                      \
                        fprintf(stderr, "\n");                                                                                             \
                        failures++;                                                                                                        \
                }                                                                                                                          \
        } while (0)

/* Checks that a call failed with a message containing `expected`, and frees it. The message is
 * read through its slot, since a call passed as `result` may run after the other arguments are read. */
static void expect_error(int result, char **error, const char *expected, const char *what) {
        CHECK(result != 0, "%s should fail", what);
        CHECK(*error != NULL && strstr(*error, expected) != NULL, "%s should fail with \"%s\", got \"%s\"", what, expected,
              *error == NULL ? "(none)" : *error);
        xkafka_rocksdb_free(*error);
        *error = NULL;
}

/* ---- Batches, as the Scala bindings encode them ---- */

typedef struct {
        unsigned char bytes[4096];
        size_t length;
} batch_t;

static void add_bytes(batch_t *batch, const void *bytes, size_t size) {
        batch->bytes[batch->length++] = (unsigned char)(size >> 24);
        batch->bytes[batch->length++] = (unsigned char)(size >> 16);
        batch->bytes[batch->length++] = (unsigned char)(size >> 8);
        batch->bytes[batch->length++] = (unsigned char)size;
        memcpy(batch->bytes + batch->length, bytes, size);
        batch->length += size;
}

static void add(batch_t *batch, int operation, int family, const void *first, size_t first_size, const void *second, size_t second_size) {
        batch->bytes[batch->length++] = (unsigned char)operation;
        batch->bytes[batch->length++] = (unsigned char)family;
        add_bytes(batch, first, first_size);
        if (second != NULL)
                add_bytes(batch, second, second_size);
}

static void put(batch_t *batch, int family, const char *key, size_t key_size, const char *value) {
        add(batch, 1, family, key, key_size, value, strlen(value));
}

/* ---- Reading answers back ---- */

/* The value under a key, as a string, or NULL where there is none. */
static char *get(void *store, size_t family, const char *key, size_t key_size) {
        void *value = NULL;
        size_t length = 99;
        int found = -1;
        char *error = NULL;
        char *copy;

        CHECK(xkafka_rocksdb_get(store, family, key, key_size, &value, &length, &found, &error) == 0, "get failed: %s", error);
        if (error != NULL || !found) {
                xkafka_rocksdb_free(error);
                CHECK(value == NULL && length == 0, "a missing key should have no value");
                return NULL;
        }
        copy = (char *)calloc(1, length + 1);
        memcpy(copy, value, length);
        xkafka_rocksdb_free(value);
        return copy;
}

static void expect_value(void *store, size_t family, const char *key, size_t key_size, const char *expected) {
        char *value = get(store, family, key, key_size);

        if (expected == NULL)
                CHECK(value == NULL, "key %02x should have no value, has \"%s\"", (unsigned char)key[0], value);
        else
                CHECK(value != NULL && strcmp(value, expected) == 0, "key %02x should be \"%s\", is \"%s\"", (unsigned char)key[0], expected,
                      value == NULL ? "(none)" : value);
        free(value);
}

static uint32_t read_length(const unsigned char *bytes) {
        return ((uint32_t)bytes[0] << 24) | ((uint32_t)bytes[1] << 16) | ((uint32_t)bytes[2] << 8) | (uint32_t)bytes[3];
}

/* The first byte of each key in a range, as hex, and the values' first bytes. */
static void describe_range(const unsigned char *entries, size_t length, char *keys, char *values) {
        size_t at = 0;

        keys[0] = '\0';
        values[0] = '\0';
        while (at < length) {
                uint32_t key_size = read_length(entries + at);
                const unsigned char *key = entries + at + 4;
                uint32_t value_size;
                at += 4 + key_size;
                value_size = read_length(entries + at);
                sprintf(keys + strlen(keys), "%s%02x%s", keys[0] == '\0' ? "" : " ", key[0], key_size > 1 ? "+" : "");
                strncat(values, (const char *)entries + at + 4, value_size);
                at += 4 + value_size;
        }
}

static void expect_range(void *store, size_t family, const char *from, size_t from_size, const char *to, size_t to_size, size_t limit,
                         const char *expected_keys, const char *expected_values) {
        void *entries = NULL;
        size_t length = 0;
        char *error = NULL;
        char keys[256];
        char values[256];

        CHECK(xkafka_rocksdb_range(store, family, from, from_size, to, to_size, limit, &entries, &length, &error) == 0, "range failed: %s",
              error);
        xkafka_rocksdb_free(error);
        describe_range((const unsigned char *)entries, length, keys, values);
        CHECK(strcmp(keys, expected_keys) == 0, "range should hold keys [%s], holds [%s]", expected_keys, keys);
        CHECK(strcmp(values, expected_values) == 0, "range should hold values [%s], holds [%s]", expected_values, values);
        CHECK((entries == NULL) == (length == 0), "an empty range should be NULL, and only it");
        xkafka_rocksdb_free(entries);
}

/* ---- Waiting for the shim's threads ---- */

static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t reported = PTHREAD_COND_INITIALIZER;
static int64_t last_token = 0;

static void done(int64_t token) {
        pthread_mutex_lock(&lock);
        last_token = token;
        pthread_cond_signal(&reported);
        pthread_mutex_unlock(&lock);
}

static void await_token(int64_t token) {
        pthread_mutex_lock(&lock);
        while (last_token != token)
                pthread_cond_wait(&reported, &lock);
        pthread_mutex_unlock(&lock);
}

/* Checks every write-ahead log in the store is empty, as writes skip it. */
static void expect_empty_logs(const char *store) {
        DIR *directory = opendir(store);
        struct dirent *entry;
        char path[2048];
        struct stat status;
        int logs = 0;

        CHECK(directory != NULL, "could not list %s", store);
        if (directory == NULL)
                return;
        while ((entry = readdir(directory)) != NULL) {
                size_t length = strlen(entry->d_name);
                if (length < 4 || strcmp(entry->d_name + length - 4, ".log") != 0)
                        continue;
                logs++;
                snprintf(path, sizeof path, "%s/%s", store, entry->d_name);
                CHECK(stat(path, &status) == 0 && status.st_size == 0, "the write-ahead log %s should be empty", entry->d_name);
        }
        closedir(directory);
        CHECK(logs > 0, "the store should have a write-ahead log, if an empty one");
}

/* Checks the options RocksDB recorded for the store hold each of `expected`. */
static void expect_options(const char *store, const char *const *expected, size_t count) {
        DIR *directory = opendir(store);
        struct dirent *entry;
        char path[2048] = "";
        char *text = NULL;
        size_t index;

        CHECK(directory != NULL, "could not list %s", store);
        if (directory == NULL)
                return;
        while ((entry = readdir(directory)) != NULL)
                if (strncmp(entry->d_name, "OPTIONS-", 8) == 0)
                        snprintf(path, sizeof path, "%s/%s", store, entry->d_name);
        closedir(directory);
        if (path[0] != '\0') {
                FILE *file = fopen(path, "r");
                long size;
                if (file != NULL && fseek(file, 0, SEEK_END) == 0 && (size = ftell(file)) > 0 && fseek(file, 0, SEEK_SET) == 0) {
                        text = (char *)calloc(1, (size_t)size + 1);
                        if (fread(text, 1, (size_t)size, file) != (size_t)size) {
                                free(text);
                                text = NULL;
                        }
                }
                if (file != NULL)
                        fclose(file);
        }
        CHECK(text != NULL, "the store should record its options");
        for (index = 0; text != NULL && index < count; index++)
                CHECK(strstr(text, expected[index]) != NULL, "the store's options should hold %s", expected[index]);
        free(text);
}

static int remove_entry(const char *path, const struct stat *status, int flag, struct FTW *walk) {
        (void)status;
        (void)flag;
        (void)walk;
        return remove(path);
}

static void test_loading(const char *library, const char *version, const char *identity) {
        char *error = NULL;

        CHECK(strcmp(xkafka_rocksdb_identity(), identity) == 0, "the shim should be %s, is %s", identity, xkafka_rocksdb_identity());
        CHECK(xkafka_rocksdb_version() == NULL, "no release should be loaded yet");

        expect_error(xkafka_rocksdb_load("/nonexistent/librocksdb.so", version, &error), &error, "/nonexistent/librocksdb.so",
                     "loading a missing library");
        error = NULL;
        expect_error(xkafka_rocksdb_load(library, "0.0.0", &error), &error, "but xkafka needs 0.0.0", "loading another release");
        error = NULL;
        CHECK(xkafka_rocksdb_version() == NULL, "a refused release should leave nothing loaded");
        CHECK(xkafka_rocksdb_open("unused", NULL, 0, &error) == NULL, "a store should not open before RocksDB is loaded");
        expect_error(-1, &error, "not loaded", "opening before loading");
        error = NULL;

        CHECK(xkafka_rocksdb_load(library, version, &error) == 0, "loading %s failed: %s", library, error);
        CHECK(xkafka_rocksdb_version() != NULL && strcmp(xkafka_rocksdb_version(), version) == 0, "the loaded release should be %s", version);
        CHECK(xkafka_rocksdb_load(NULL, version, &error) == 0, "loading again should keep the library: %s", error);
        expect_error(xkafka_rocksdb_load(NULL, "0.0.0", &error), &error, "but xkafka needs 0.0.0", "asking a loaded library for another release");
}

/* What Kafka Streams' RocksDBStore sets, as RocksDB records it, for both families. */
static const char *const options[] = {
    "create_missing_column_families=true",
    "atomic_flush=true",
    "info_log_level=ERROR_LEVEL",
    "[CFOptions \"offsets\"]",
    "write_buffer_size=16777216",
    "max_write_buffer_number=3",
    "  compression=kNoCompression",
    "compaction_style=kCompactionStyleUniversal",
    "optimize_filters_for_hits=true",
    "  block_size=4096",
    "filter_policy=bloomfilter",
};

static void test_store(const char *directory) {
        const char *families[] = {"default", "offsets"};
        const char *fewer[] = {"default"};
        char path[1024];
        char *error = NULL;
        void *store;
        batch_t batch = {{0}, 0};
        const char k01[] = {0x01}, k7f[] = {0x7f}, k80[] = {(char)0x80}, kff[] = {(char)0xff}, kff00[] = {(char)0xff, 0x00};
        const char k10[] = {0x10}, k20[] = {0x20}, k30[] = {0x30}, k40[] = {0x40};
        void *none = NULL;
        size_t none_length = 0;

        snprintf(path, sizeof path, "%s/nested/store", directory);
        store = xkafka_rocksdb_open(path, families, 2, &error);
        CHECK(store != NULL, "opening %s failed: %s", path, error);
        if (store == NULL)
                return;
        expect_options(path, options, sizeof options / sizeof options[0]);

        /* One put, then the keys a signed comparison would misplace. */
        put(&batch, 0, k01, 1, "a");
        CHECK(xkafka_rocksdb_write(store, batch.bytes, batch.length, &error) == 0, "writing one put failed: %s", error);
        expect_value(store, 0, k01, 1, "a");
        expect_value(store, 1, k01, 1, NULL);
        expect_value(store, 0, k7f, 1, NULL);

        batch.length = 0;
        put(&batch, 0, kff, 1, "e");
        put(&batch, 0, k80, 1, "c");
        put(&batch, 0, k7f, 1, "b");
        put(&batch, 0, kff00, 2, "f");
        put(&batch, 1, k01, 1, "offset");
        put(&batch, 0, k10, 1, "x");
        put(&batch, 0, k20, 1, "y");
        put(&batch, 0, k30, 1, "z");
        put(&batch, 0, k40, 1, "");
        CHECK(xkafka_rocksdb_write(store, batch.bytes, batch.length, &error) == 0, "writing a batch failed: %s", error);
        expect_value(store, 1, k01, 1, "offset");
        expect_value(store, 0, k40, 1, "");
        expect_empty_logs(path);

        /* Deletes, of one key and of a range, in one batch with a put. */
        batch.length = 0;
        add(&batch, 2, 0, k7f, 1, NULL, 0);
        add(&batch, 3, 0, k10, 1, k30, 1);
        put(&batch, 0, k20, 1, "back");
        CHECK(xkafka_rocksdb_write(store, batch.bytes, batch.length, &error) == 0, "writing deletes failed: %s", error);
        expect_value(store, 0, k7f, 1, NULL);
        expect_value(store, 0, k10, 1, NULL);
        expect_value(store, 0, k20, 1, "back");
        expect_value(store, 0, k30, 1, "z");

        expect_range(store, 0, NULL, 0, NULL, 0, 0, "01 20 30 40 80 ff ff+", "abackzcef");
        expect_range(store, 0, k30, 1, kff, 1, 0, "30 40 80", "zc");
        expect_range(store, 0, k80, 1, NULL, 0, 0, "80 ff ff+", "cef");
        expect_range(store, 0, NULL, 0, k30, 1, 0, "01 20", "aback");
        expect_range(store, 0, NULL, 0, NULL, 0, 2, "01 20", "aback");
        expect_range(store, 0, kff, 1, NULL, 0, 1, "ff", "e");
        /* The key just after 0xff, to page on from it. */
        expect_range(store, 0, kff00, 2, NULL, 0, 1, "ff+", "f");
        expect_range(store, 0, k30, 1, k30, 1, 0, "", "");
        expect_range(store, 1, NULL, 0, NULL, 0, 0, "01", "offset");

        /* Batches that do not decode, or name no family, write nothing. */
        batch.length = 0;
        put(&batch, 0, k01, 1, "never");
        expect_error(xkafka_rocksdb_write(store, batch.bytes, batch.length - 1, &error), &error, "ends inside its operation at byte 0",
                     "a cut batch");
        error = NULL;
        batch.bytes[0] = 9;
        expect_error(xkafka_rocksdb_write(store, batch.bytes, batch.length, &error), &error, "unknown operation at byte 0", "an unknown operation");
        error = NULL;
        batch.length = 0;
        put(&batch, 0, k01, 1, "never");
        put(&batch, 2, k01, 1, "never");
        expect_error(xkafka_rocksdb_write(store, batch.bytes, batch.length, &error), &error, "no column family 2", "an unknown family");
        error = NULL;
        expect_value(store, 0, k01, 1, "a");
        expect_error(xkafka_rocksdb_range(store, 5, NULL, 0, NULL, 0, 0, &none, &none_length, &error), &error, "no column family 5",
                     "a range of an unknown family");
        error = NULL;

        CHECK(xkafka_rocksdb_flush(store, &error) == 0, "flushing failed: %s", error);
        xkafka_rocksdb_close(store);

        /* RocksDB's own message, for a store that holds a family not named. */
        CHECK(xkafka_rocksdb_open(path, fewer, 1, &error) == NULL, "a store with an unnamed family should not open");
        expect_error(-1, &error, "offsets", "opening without a family the store holds");
        error = NULL;

        store = xkafka_rocksdb_open(path, families, 2, &error);
        CHECK(store != NULL, "reopening failed: %s", error);
        if (store == NULL)
                return;
        expect_range(store, 0, NULL, 0, NULL, 0, 0, "01 20 30 40 80 ff ff+", "abackzcef");
        expect_value(store, 1, k01, 1, "offset");
        xkafka_rocksdb_close(store);

        /* A store whose families leave out the default one opens it all the same. */
        snprintf(path, sizeof path, "%s/other", directory);
        store = xkafka_rocksdb_open(path, families + 1, 1, &error);
        CHECK(store != NULL, "opening without the default family failed: %s", error);
        if (store != NULL) {
                batch.length = 0;
                put(&batch, 0, k01, 1, "only");
                CHECK(xkafka_rocksdb_write(store, batch.bytes, batch.length, &error) == 0, "writing failed: %s", error);
                CHECK(xkafka_rocksdb_flush(store, &error) == 0, "flushing every family failed: %s", error);
                xkafka_rocksdb_close(store);
        }

        snprintf(path, sizeof path, "%s/nested/store/CURRENT", directory);
        CHECK(xkafka_rocksdb_open(path, families, 2, &error) == NULL, "a store should not open inside a file");
        expect_error(-1, &error, "CURRENT", "opening a store inside a file");
}

static void test_threads(const char *directory) {
        const char *families[] = {"default"};
        char path[1024];
        char *error = NULL;
        void *store = NULL;
        void *value = NULL;
        void *entries = NULL;
        size_t length = 0;
        int found = 0;
        int32_t result = -1;
        batch_t batch = {{0}, 0};
        const char key[] = {0x42};

        snprintf(path, sizeof path, "%s/threads", directory);
        CHECK(xkafka_rocksdb_async_open(path, families, 1, &error, &store, done, 1) == 0, "could not start opening");
        await_token(1);
        CHECK(store != NULL, "opening on a thread failed: %s", error);
        if (store == NULL)
                return;

        put(&batch, 0, key, 1, "threaded");
        CHECK(xkafka_rocksdb_async_write(store, batch.bytes, batch.length, &error, &result, done, 2) == 0, "could not start writing");
        await_token(2);
        CHECK(result == 0, "writing on a thread failed: %s", error);

        result = -1;
        CHECK(xkafka_rocksdb_async_get(store, 0, key, 1, &value, &length, &found, &error, &result, done, 3) == 0, "could not start getting");
        await_token(3);
        CHECK(result == 0 && found && length == 8 && memcmp(value, "threaded", 8) == 0, "getting on a thread failed: %s", error);
        xkafka_rocksdb_free(value);

        result = -1;
        CHECK(xkafka_rocksdb_async_range(store, 0, NULL, 0, NULL, 0, 0, &entries, &length, &error, &result, done, 4) == 0,
              "could not start a range");
        await_token(4);
        CHECK(result == 0 && length == 4 + 1 + 4 + 8, "a range on a thread failed: %s", error);
        xkafka_rocksdb_free(entries);

        result = -1;
        CHECK(xkafka_rocksdb_async_get(store, 3, key, 1, &value, &length, &found, &error, &result, done, 5) == 0, "could not start getting");
        await_token(5);
        expect_error(result, &error, "no column family 3", "getting from an unknown family on a thread");
        error = NULL;

        result = -1;
        CHECK(xkafka_rocksdb_async_flush(store, &error, &result, done, 6) == 0, "could not start flushing");
        await_token(6);
        CHECK(result == 0, "flushing on a thread failed: %s", error);

        CHECK(xkafka_rocksdb_async_close(store, done, 7) == 0, "could not start closing");
        await_token(7);

        store = NULL;
        CHECK(xkafka_rocksdb_async_open(path, families, 1, &error, &store, done, 8) == 0, "could not start reopening");
        await_token(8);
        CHECK(store != NULL, "reopening on a thread failed: %s", error);
        if (store != NULL) {
                expect_value(store, 0, key, 1, "threaded");
                xkafka_rocksdb_close(store);
        }
}

int main(int argc, char **argv) {
        char *error = NULL;
        const char *directory;

        if (argc == 4 && strcmp(argv[1], "--search") == 0) {
                directory = argv[3];
                CHECK(xkafka_rocksdb_load(NULL, argv[2], &error) == 0, "loading with no path failed: %s", error);
                xkafka_rocksdb_free(error);
                if (failures == 0) {
                        const char *families[] = {"default"};
                        char path[1024];
                        void *store;
                        snprintf(path, sizeof path, "%s/searched", directory);
                        store = xkafka_rocksdb_open(path, families, 1, &error);
                        CHECK(store != NULL, "opening failed: %s", error);
                        xkafka_rocksdb_free(error);
                        xkafka_rocksdb_close(store);
                }
        } else if (argc == 5) {
                directory = argv[4];
                test_loading(argv[1], argv[2], argv[3]);
                if (failures == 0) {
                        test_store(directory);
                        test_threads(directory);
                }
        } else {
                fprintf(stderr, "usage: %s library version identity directory\n       %s --search version directory\n", argv[0], argv[0]);
                return 2;
        }

        nftw(directory, remove_entry, 16, FTW_DEPTH | FTW_PHYS);
        if (failures != 0) {
                fprintf(stderr, "%d checks failed\n", failures);
                return 1;
        }
        printf("RocksDB %s through the shim: every check passed\n", xkafka_rocksdb_version());
        return 0;
}
