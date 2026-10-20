#ifdef SCALANATIVE_LINK_RDKAFKA

/* Calls that can wait on the network, run on a thread of their own.
 *
 * Each xkafka_async_* function takes the arguments of the shim function it
 * names, starts that function on a new detached thread, and returns at once.
 * When the call finishes, its result is written through `result` and `done`
 * is called with the caller's token from that thread. The JavaScript backend
 * uses these, so a call that waits holds none of Node's shared worker threads,
 * which file, DNS and crypto work also need.
 *
 * Strings and string arrays are copied, since the caller's copies only last
 * for the starting call. Every other pointer, the error buffer and the
 * numeric arrays included, must stay valid until `done` is called.
 *
 * Starting returns 0, or -1 when no thread could be started, in which case
 * `done` is never called. */

#include <librdkafka/rdkafka.h>

#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

typedef void (*xkafka_done_t)(int64_t token);

/* The shim functions run here, as the shim defines them. */
void xkafka_producer_destroy(rd_kafka_t *producer, int timeout_ms);
void xkafka_consumer_destroy(rd_kafka_t *consumer);
void xkafka_admin_destroy(rd_kafka_t *client);
void xkafka_producer_poll(rd_kafka_t *producer, int timeout_ms);
int xkafka_producer_init_transactions(rd_kafka_t *producer, int timeout_ms, char *error, size_t error_size, int32_t *error_code,
                                      int32_t *is_fatal, int32_t *is_retriable, int32_t *txn_requires_abort);
int xkafka_producer_commit_transaction(rd_kafka_t *producer, int timeout_ms, char *error, size_t error_size, int32_t *error_code,
                                       int32_t *is_fatal, int32_t *is_retriable, int32_t *txn_requires_abort);
int xkafka_producer_abort_transaction(rd_kafka_t *producer, int timeout_ms, char *error, size_t error_size, int32_t *error_code,
                                      int32_t *is_fatal, int32_t *is_retriable, int32_t *txn_requires_abort);
int xkafka_producer_send_offsets(rd_kafka_t *producer, void *metadata, const char *const *topics, const int32_t *partitions,
                                 const int64_t *offset_values, const int32_t *leader_epochs, const char *const *offset_metadata,
                                 size_t count, int timeout_ms, char *error, size_t error_size, int32_t *error_code, int32_t *is_fatal,
                                 int32_t *is_retriable, int32_t *txn_requires_abort);
rd_kafka_message_t *xkafka_consumer_poll(rd_kafka_t *consumer, int timeout_ms, int *status, char *error, size_t error_size,
                                         int32_t *error_code);
int xkafka_consumer_commit(rd_kafka_t *consumer, const char *const *topics, const int32_t *partitions, const int64_t *offset_values,
                           const int32_t *leader_epochs, const char *const *metadata, size_t count, int timeout_ms, char *error,
                           size_t error_size, int32_t *error_code);
void *xkafka_consumer_committed(rd_kafka_t *consumer, const char *const *topics, const int32_t *partitions, size_t count,
                                int timeout_ms, char *error, size_t error_size, int32_t *error_code);
int xkafka_consumer_watermark_offsets(rd_kafka_t *consumer, const char *topic, int32_t partition, int64_t *low, int64_t *high,
                                      int timeout_ms, char *error, size_t error_size, int32_t *error_code);
int xkafka_consumer_offsets_for_times(rd_kafka_t *consumer, const char *const *topics, const int32_t *partitions,
                                      const int64_t *timestamps, size_t count, int64_t *offset_values, int timeout_ms, char *error,
                                      size_t error_size, int32_t *error_code);
int xkafka_consumer_seek(rd_kafka_t *consumer, const char *topic, int32_t partition, int64_t offset, int timeout_ms, char *error,
                         size_t error_size, int32_t *error_code);
void *xkafka_consumer_metadata(rd_kafka_t *consumer, const char *topic, int timeout_ms, char *error, size_t error_size,
                               int32_t *error_code);
int xkafka_admin_create_topics(rd_kafka_t *client, const char *const *names, const int32_t *partitions, const int32_t *replication,
                               const char *const *config_names, const char *const *config_values, const size_t *config_counts,
                               size_t count, int timeout_ms, char *error, size_t error_size, int32_t *error_code);
int xkafka_admin_delete_topics(rd_kafka_t *client, const char *const *names, size_t count, int timeout_ms, char *error,
                               size_t error_size, int32_t *error_code);
int xkafka_admin_create_partitions(rd_kafka_t *client, const char *topic, int32_t count, int timeout_ms, char *error,
                                   size_t error_size, int32_t *error_code);
int xkafka_admin_delete_records(rd_kafka_t *client, const char *const *topics, const int32_t *partitions, const int64_t *offsets,
                                size_t count, int timeout_ms, int32_t *codes, int64_t *low_watermarks, char *error,
                                size_t error_size, int32_t *error_code);
void *xkafka_admin_describe_topic_configs(rd_kafka_t *client, const char *const *names, size_t count, int timeout_ms, char *error,
                                          size_t error_size, int32_t *error_code);

/* What every job starts with. `run` makes the call and writes its result;
 * `release` frees the job's own copies. */
typedef struct xkafka_job_s {
        void (*run)(struct xkafka_job_s *job);
        void (*release)(struct xkafka_job_s *job);
        xkafka_done_t done;
        int64_t token;
} xkafka_job_t;

/* The error out-parameters every call but the destroys and the poll of a
 * producer report through. They belong to the caller. */
typedef struct xkafka_error_slots_s {
        char *error;
        size_t error_size;
        int32_t *error_code;
} xkafka_error_slots_t;

/* The transactional flags, also the caller's. */
typedef struct xkafka_flags_s {
        int32_t *is_fatal;
        int32_t *is_retriable;
        int32_t *txn_requires_abort;
} xkafka_flags_t;

static void *xkafka_job_thread(void *argument) {
        xkafka_job_t *job = (xkafka_job_t *)argument;
        xkafka_done_t done = job->done;
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

/* Starts the job, or frees it and reports -1 when no thread can be started. */
static int xkafka_job_start(xkafka_job_t *job, xkafka_done_t done, int64_t token) {
        pthread_attr_t attributes;
        pthread_t thread;
        int started;

        job->done = done;
        job->token = token;
        if (pthread_attr_init(&attributes) != 0) {
                if (job->release != NULL)
                        job->release(job);
                free(job);
                return -1;
        }
        pthread_attr_setdetachstate(&attributes, PTHREAD_CREATE_DETACHED);
        started = pthread_create(&thread, &attributes, xkafka_job_thread, job) == 0;
        pthread_attr_destroy(&attributes);
        if (!started) {
                if (job->release != NULL)
                        job->release(job);
                free(job);
                return -1;
        }
        return 0;
}

static char *xkafka_copy_string(const char *value) {
        size_t size;
        char *copy;

        if (value == NULL)
                return NULL;
        size = strlen(value) + 1;
        copy = (char *)malloc(size);
        if (copy != NULL)
                memcpy(copy, value, size);
        return copy;
}

static void xkafka_free_strings(char **values, size_t count) {
        size_t index;

        if (values == NULL)
                return;
        for (index = 0; index < count; index++)
                free(values[index]);
        free(values);
}

/* Copies `count` strings. Reports failure through `ok`, so a missing array and
 * an allocation failure are told apart. */
static char **xkafka_copy_strings(const char *const *values, size_t count, int *ok) {
        char **copy;
        size_t index;

        if (values == NULL || count == 0)
                return NULL;
        copy = (char **)calloc(count, sizeof(char *));
        if (copy == NULL) {
                *ok = 0;
                return NULL;
        }
        for (index = 0; index < count; index++) {
                copy[index] = xkafka_copy_string(values[index]);
                if (copy[index] == NULL && values[index] != NULL) {
                        xkafka_free_strings(copy, count);
                        *ok = 0;
                        return NULL;
                }
        }
        return copy;
}

/* ---- Destroys and producer polls ---- */

typedef struct {
        xkafka_job_t job;
        rd_kafka_t *client;
        int timeout_ms;
        int kind;
} xkafka_client_job_t;

enum { XKAFKA_DESTROY_PRODUCER, XKAFKA_DESTROY_CONSUMER, XKAFKA_DESTROY_ADMIN, XKAFKA_POLL_PRODUCER };

static void xkafka_client_job_run(xkafka_job_t *job) {
        xkafka_client_job_t *call = (xkafka_client_job_t *)job;

        switch (call->kind) {
        case XKAFKA_DESTROY_PRODUCER:
                xkafka_producer_destroy(call->client, call->timeout_ms);
                break;
        case XKAFKA_DESTROY_CONSUMER:
                xkafka_consumer_destroy(call->client);
                break;
        case XKAFKA_DESTROY_ADMIN:
                xkafka_admin_destroy(call->client);
                break;
        default:
                xkafka_producer_poll(call->client, call->timeout_ms);
                break;
        }
}

static int xkafka_client_job(rd_kafka_t *client, int timeout_ms, int kind, xkafka_done_t done, int64_t token) {
        xkafka_client_job_t *call = (xkafka_client_job_t *)calloc(1, sizeof(xkafka_client_job_t));

        if (call == NULL)
                return -1;
        call->job.run = xkafka_client_job_run;
        call->client = client;
        call->timeout_ms = timeout_ms;
        call->kind = kind;
        return xkafka_job_start(&call->job, done, token);
}

int xkafka_async_producer_destroy(rd_kafka_t *producer, int timeout_ms, xkafka_done_t done, int64_t token) {
        return xkafka_client_job(producer, timeout_ms, XKAFKA_DESTROY_PRODUCER, done, token);
}

int xkafka_async_consumer_destroy(rd_kafka_t *consumer, xkafka_done_t done, int64_t token) {
        return xkafka_client_job(consumer, 0, XKAFKA_DESTROY_CONSUMER, done, token);
}

int xkafka_async_admin_destroy(rd_kafka_t *client, xkafka_done_t done, int64_t token) {
        return xkafka_client_job(client, 0, XKAFKA_DESTROY_ADMIN, done, token);
}

int xkafka_async_producer_poll(rd_kafka_t *producer, int timeout_ms, xkafka_done_t done, int64_t token) {
        return xkafka_client_job(producer, timeout_ms, XKAFKA_POLL_PRODUCER, done, token);
}

/* ---- Transactions ---- */

typedef struct {
        xkafka_job_t job;
        rd_kafka_t *producer;
        int timeout_ms;
        int kind;
        xkafka_error_slots_t slots;
        xkafka_flags_t flags;
        int32_t *result;
} xkafka_transaction_job_t;

enum { XKAFKA_INIT_TRANSACTIONS, XKAFKA_COMMIT_TRANSACTION, XKAFKA_ABORT_TRANSACTION };

static void xkafka_transaction_job_run(xkafka_job_t *job) {
        xkafka_transaction_job_t *call = (xkafka_transaction_job_t *)job;
        xkafka_error_slots_t *s = &call->slots;
        xkafka_flags_t *f = &call->flags;

        switch (call->kind) {
        case XKAFKA_INIT_TRANSACTIONS:
                *call->result = xkafka_producer_init_transactions(call->producer, call->timeout_ms, s->error, s->error_size, s->error_code,
                                                                  f->is_fatal, f->is_retriable, f->txn_requires_abort);
                break;
        case XKAFKA_COMMIT_TRANSACTION:
                *call->result = xkafka_producer_commit_transaction(call->producer, call->timeout_ms, s->error, s->error_size, s->error_code,
                                                                   f->is_fatal, f->is_retriable, f->txn_requires_abort);
                break;
        default:
                *call->result = xkafka_producer_abort_transaction(call->producer, call->timeout_ms, s->error, s->error_size, s->error_code,
                                                                  f->is_fatal, f->is_retriable, f->txn_requires_abort);
                break;
        }
}

static int xkafka_transaction_job(rd_kafka_t *producer, int timeout_ms, int kind, char *error, size_t error_size, int32_t *error_code,
                                  int32_t *is_fatal, int32_t *is_retriable, int32_t *txn_requires_abort, int32_t *result,
                                  xkafka_done_t done, int64_t token) {
        xkafka_transaction_job_t *call = (xkafka_transaction_job_t *)calloc(1, sizeof(xkafka_transaction_job_t));

        if (call == NULL)
                return -1;
        call->job.run = xkafka_transaction_job_run;
        call->producer = producer;
        call->timeout_ms = timeout_ms;
        call->kind = kind;
        call->slots = (xkafka_error_slots_t){error, error_size, error_code};
        call->flags = (xkafka_flags_t){is_fatal, is_retriable, txn_requires_abort};
        call->result = result;
        return xkafka_job_start(&call->job, done, token);
}

int xkafka_async_producer_init_transactions(rd_kafka_t *producer, int timeout_ms, char *error, size_t error_size, int32_t *error_code,
                                            int32_t *is_fatal, int32_t *is_retriable, int32_t *txn_requires_abort, int32_t *result,
                                            xkafka_done_t done, int64_t token) {
        return xkafka_transaction_job(producer, timeout_ms, XKAFKA_INIT_TRANSACTIONS, error, error_size, error_code, is_fatal,
                                      is_retriable, txn_requires_abort, result, done, token);
}

int xkafka_async_producer_commit_transaction(rd_kafka_t *producer, int timeout_ms, char *error, size_t error_size, int32_t *error_code,
                                             int32_t *is_fatal, int32_t *is_retriable, int32_t *txn_requires_abort, int32_t *result,
                                             xkafka_done_t done, int64_t token) {
        return xkafka_transaction_job(producer, timeout_ms, XKAFKA_COMMIT_TRANSACTION, error, error_size, error_code, is_fatal,
                                      is_retriable, txn_requires_abort, result, done, token);
}

int xkafka_async_producer_abort_transaction(rd_kafka_t *producer, int timeout_ms, char *error, size_t error_size, int32_t *error_code,
                                            int32_t *is_fatal, int32_t *is_retriable, int32_t *txn_requires_abort, int32_t *result,
                                            xkafka_done_t done, int64_t token) {
        return xkafka_transaction_job(producer, timeout_ms, XKAFKA_ABORT_TRANSACTION, error, error_size, error_code, is_fatal,
                                      is_retriable, txn_requires_abort, result, done, token);
}

/* ---- Calls that name partitions: offsets sent, committed, read or searched ---- */

typedef struct {
        xkafka_job_t job;
        rd_kafka_t *client;
        void *metadata;
        char **topics;
        const int32_t *partitions;
        const int64_t *values_in;
        const int32_t *leader_epochs;
        char **metadata_values;
        int64_t *values_out;
        int32_t *codes;
        size_t count;
        int timeout_ms;
        int kind;
        xkafka_error_slots_t slots;
        xkafka_flags_t flags;
        int32_t *result;
        void **pointer_result;
} xkafka_partitions_job_t;

enum { XKAFKA_SEND_OFFSETS, XKAFKA_COMMIT, XKAFKA_COMMITTED, XKAFKA_OFFSETS_FOR_TIMES, XKAFKA_DELETE_RECORDS };

static void xkafka_partitions_job_run(xkafka_job_t *job) {
        xkafka_partitions_job_t *call = (xkafka_partitions_job_t *)job;
        const char *const *topics = (const char *const *)call->topics;
        xkafka_error_slots_t *s = &call->slots;
        xkafka_flags_t *f = &call->flags;

        switch (call->kind) {
        case XKAFKA_SEND_OFFSETS:
                *call->result = xkafka_producer_send_offsets(call->client, call->metadata, topics, call->partitions, call->values_in,
                                                             call->leader_epochs, (const char *const *)call->metadata_values, call->count,
                                                             call->timeout_ms, s->error, s->error_size, s->error_code, f->is_fatal,
                                                             f->is_retriable, f->txn_requires_abort);
                break;
        case XKAFKA_COMMIT:
                *call->result = xkafka_consumer_commit(call->client, topics, call->partitions, call->values_in, call->leader_epochs,
                                                       (const char *const *)call->metadata_values, call->count, call->timeout_ms, s->error,
                                                       s->error_size, s->error_code);
                break;
        case XKAFKA_COMMITTED:
                *call->pointer_result = xkafka_consumer_committed(call->client, topics, call->partitions, call->count, call->timeout_ms,
                                                                  s->error, s->error_size, s->error_code);
                break;
        case XKAFKA_OFFSETS_FOR_TIMES:
                *call->result = xkafka_consumer_offsets_for_times(call->client, topics, call->partitions, call->values_in, call->count,
                                                                  call->values_out, call->timeout_ms, s->error, s->error_size,
                                                                  s->error_code);
                break;
        default:
                *call->result = xkafka_admin_delete_records(call->client, topics, call->partitions, call->values_in, call->count,
                                                            call->timeout_ms, call->codes, call->values_out, s->error, s->error_size,
                                                            s->error_code);
                break;
        }
}

static void xkafka_partitions_job_release(xkafka_job_t *job) {
        xkafka_partitions_job_t *call = (xkafka_partitions_job_t *)job;
        xkafka_free_strings(call->topics, call->count);
        xkafka_free_strings(call->metadata_values, call->count);
}

static xkafka_partitions_job_t *xkafka_partitions_job(rd_kafka_t *client, const char *const *topics, const int32_t *partitions,
                                                      size_t count, int timeout_ms, int kind, char *error, size_t error_size,
                                                      int32_t *error_code, int32_t *result) {
        xkafka_partitions_job_t *call = (xkafka_partitions_job_t *)calloc(1, sizeof(xkafka_partitions_job_t));
        int ok = 1;

        if (call == NULL)
                return NULL;
        call->topics = xkafka_copy_strings(topics, count, &ok);
        if (!ok) {
                free(call);
                return NULL;
        }
        call->job.run = xkafka_partitions_job_run;
        call->job.release = xkafka_partitions_job_release;
        call->client = client;
        call->partitions = partitions;
        call->count = count;
        call->timeout_ms = timeout_ms;
        call->kind = kind;
        call->slots = (xkafka_error_slots_t){error, error_size, error_code};
        call->result = result;
        return call;
}

/* Copies the offsets' metadata, which the call reads after the starting call returns. Frees the job where it cannot. */
static int xkafka_partitions_job_metadata(xkafka_partitions_job_t *call, const char *const *metadata) {
        int ok = 1;

        call->metadata_values = xkafka_copy_strings(metadata, call->count, &ok);
        if (!ok) {
                xkafka_free_strings(call->topics, call->count);
                free(call);
        }
        return ok;
}

int xkafka_async_producer_send_offsets(rd_kafka_t *producer, void *metadata, const char *const *topics, const int32_t *partitions,
                                       const int64_t *offset_values, const int32_t *leader_epochs, const char *const *offset_metadata,
                                       size_t count, int timeout_ms, char *error, size_t error_size, int32_t *error_code,
                                       int32_t *is_fatal, int32_t *is_retriable, int32_t *txn_requires_abort, int32_t *result,
                                       xkafka_done_t done, int64_t token) {
        xkafka_partitions_job_t *call = xkafka_partitions_job(producer, topics, partitions, count, timeout_ms, XKAFKA_SEND_OFFSETS,
                                                              error, error_size, error_code, result);

        if (call == NULL || !xkafka_partitions_job_metadata(call, offset_metadata))
                return -1;
        call->metadata = metadata;
        call->values_in = offset_values;
        call->leader_epochs = leader_epochs;
        call->flags = (xkafka_flags_t){is_fatal, is_retriable, txn_requires_abort};
        return xkafka_job_start(&call->job, done, token);
}

int xkafka_async_consumer_commit(rd_kafka_t *consumer, const char *const *topics, const int32_t *partitions,
                                 const int64_t *offset_values, const int32_t *leader_epochs, const char *const *metadata, size_t count,
                                 int timeout_ms, char *error, size_t error_size, int32_t *error_code, int32_t *result, xkafka_done_t done,
                                 int64_t token) {
        xkafka_partitions_job_t *call =
            xkafka_partitions_job(consumer, topics, partitions, count, timeout_ms, XKAFKA_COMMIT, error, error_size, error_code, result);

        if (call == NULL || !xkafka_partitions_job_metadata(call, metadata))
                return -1;
        call->values_in = offset_values;
        call->leader_epochs = leader_epochs;
        return xkafka_job_start(&call->job, done, token);
}

int xkafka_async_consumer_committed(rd_kafka_t *consumer, const char *const *topics, const int32_t *partitions, size_t count,
                                    int timeout_ms, char *error, size_t error_size, int32_t *error_code, void **result, xkafka_done_t done,
                                    int64_t token) {
        xkafka_partitions_job_t *call = xkafka_partitions_job(consumer, topics, partitions, count, timeout_ms, XKAFKA_COMMITTED, error,
                                                              error_size, error_code, NULL);

        if (call == NULL)
                return -1;
        call->pointer_result = result;
        return xkafka_job_start(&call->job, done, token);
}

int xkafka_async_consumer_offsets_for_times(rd_kafka_t *consumer, const char *const *topics, const int32_t *partitions,
                                            const int64_t *timestamps, size_t count, int64_t *offset_values, int timeout_ms,
                                            char *error, size_t error_size, int32_t *error_code, int32_t *result, xkafka_done_t done,
                                            int64_t token) {
        xkafka_partitions_job_t *call = xkafka_partitions_job(consumer, topics, partitions, count, timeout_ms, XKAFKA_OFFSETS_FOR_TIMES,
                                                              error, error_size, error_code, result);

        if (call == NULL)
                return -1;
        call->values_in = timestamps;
        call->values_out = offset_values;
        return xkafka_job_start(&call->job, done, token);
}

int xkafka_async_admin_delete_records(rd_kafka_t *client, const char *const *topics, const int32_t *partitions, const int64_t *offsets,
                                      size_t count, int timeout_ms, int32_t *codes, int64_t *low_watermarks, char *error,
                                      size_t error_size, int32_t *error_code, int32_t *result, xkafka_done_t done, int64_t token) {
        xkafka_partitions_job_t *call = xkafka_partitions_job(client, topics, partitions, count, timeout_ms, XKAFKA_DELETE_RECORDS,
                                                              error, error_size, error_code, result);

        if (call == NULL)
                return -1;
        call->values_in = offsets;
        call->codes = codes;
        call->values_out = low_watermarks;
        return xkafka_job_start(&call->job, done, token);
}

/* ---- Calls about one topic, or about one partition of it ---- */

typedef struct {
        xkafka_job_t job;
        rd_kafka_t *client;
        char *topic;
        int32_t partition;
        int64_t offset;
        int64_t *low;
        int64_t *high;
        int timeout_ms;
        int kind;
        xkafka_error_slots_t slots;
        int32_t *result;
        void **pointer_result;
} xkafka_topic_job_t;

enum { XKAFKA_WATERMARKS, XKAFKA_SEEK, XKAFKA_METADATA, XKAFKA_CREATE_PARTITIONS };

static void xkafka_topic_job_run(xkafka_job_t *job) {
        xkafka_topic_job_t *call = (xkafka_topic_job_t *)job;
        xkafka_error_slots_t *s = &call->slots;

        switch (call->kind) {
        case XKAFKA_WATERMARKS:
                *call->result = xkafka_consumer_watermark_offsets(call->client, call->topic, call->partition, call->low, call->high,
                                                                  call->timeout_ms, s->error, s->error_size, s->error_code);
                break;
        case XKAFKA_SEEK:
                *call->result = xkafka_consumer_seek(call->client, call->topic, call->partition, call->offset, call->timeout_ms, s->error,
                                                     s->error_size, s->error_code);
                break;
        case XKAFKA_METADATA:
                *call->pointer_result =
                    xkafka_consumer_metadata(call->client, call->topic, call->timeout_ms, s->error, s->error_size, s->error_code);
                break;
        default:
                *call->result = xkafka_admin_create_partitions(call->client, call->topic, call->partition, call->timeout_ms, s->error,
                                                               s->error_size, s->error_code);
                break;
        }
}

static void xkafka_topic_job_release(xkafka_job_t *job) { free(((xkafka_topic_job_t *)job)->topic); }

static xkafka_topic_job_t *xkafka_topic_job(rd_kafka_t *client, const char *topic, int timeout_ms, int kind, char *error,
                                            size_t error_size, int32_t *error_code) {
        xkafka_topic_job_t *call = (xkafka_topic_job_t *)calloc(1, sizeof(xkafka_topic_job_t));

        if (call == NULL)
                return NULL;
        call->topic = xkafka_copy_string(topic);
        if (call->topic == NULL && topic != NULL) {
                free(call);
                return NULL;
        }
        call->job.run = xkafka_topic_job_run;
        call->job.release = xkafka_topic_job_release;
        call->client = client;
        call->timeout_ms = timeout_ms;
        call->kind = kind;
        call->slots = (xkafka_error_slots_t){error, error_size, error_code};
        return call;
}

int xkafka_async_consumer_watermark_offsets(rd_kafka_t *consumer, const char *topic, int32_t partition, int64_t *low, int64_t *high,
                                            int timeout_ms, char *error, size_t error_size, int32_t *error_code, int32_t *result,
                                            xkafka_done_t done, int64_t token) {
        xkafka_topic_job_t *call = xkafka_topic_job(consumer, topic, timeout_ms, XKAFKA_WATERMARKS, error, error_size, error_code);

        if (call == NULL)
                return -1;
        call->partition = partition;
        call->low = low;
        call->high = high;
        call->result = result;
        return xkafka_job_start(&call->job, done, token);
}

int xkafka_async_consumer_seek(rd_kafka_t *consumer, const char *topic, int32_t partition, int64_t offset, int timeout_ms, char *error,
                               size_t error_size, int32_t *error_code, int32_t *result, xkafka_done_t done, int64_t token) {
        xkafka_topic_job_t *call = xkafka_topic_job(consumer, topic, timeout_ms, XKAFKA_SEEK, error, error_size, error_code);

        if (call == NULL)
                return -1;
        call->partition = partition;
        call->offset = offset;
        call->result = result;
        return xkafka_job_start(&call->job, done, token);
}

int xkafka_async_consumer_metadata(rd_kafka_t *client, const char *topic, int timeout_ms, char *error, size_t error_size,
                                   int32_t *error_code, void **result, xkafka_done_t done, int64_t token) {
        xkafka_topic_job_t *call = xkafka_topic_job(client, topic, timeout_ms, XKAFKA_METADATA, error, error_size, error_code);

        if (call == NULL)
                return -1;
        call->pointer_result = result;
        return xkafka_job_start(&call->job, done, token);
}

int xkafka_async_admin_create_partitions(rd_kafka_t *client, const char *topic, int32_t count, int timeout_ms, char *error,
                                         size_t error_size, int32_t *error_code, int32_t *result, xkafka_done_t done, int64_t token) {
        xkafka_topic_job_t *call = xkafka_topic_job(client, topic, timeout_ms, XKAFKA_CREATE_PARTITIONS, error, error_size, error_code);

        if (call == NULL)
                return -1;
        call->partition = count;
        call->result = result;
        return xkafka_job_start(&call->job, done, token);
}

/* ---- The consumer's poll ---- */

typedef struct {
        xkafka_job_t job;
        rd_kafka_t *consumer;
        int timeout_ms;
        int *status;
        xkafka_error_slots_t slots;
        void **result;
} xkafka_poll_job_t;

static void xkafka_poll_job_run(xkafka_job_t *job) {
        xkafka_poll_job_t *call = (xkafka_poll_job_t *)job;
        *call->result = xkafka_consumer_poll(call->consumer, call->timeout_ms, call->status, call->slots.error, call->slots.error_size,
                                             call->slots.error_code);
}

int xkafka_async_consumer_poll(rd_kafka_t *consumer, int timeout_ms, int *status, char *error, size_t error_size, int32_t *error_code,
                               void **result, xkafka_done_t done, int64_t token) {
        xkafka_poll_job_t *call = (xkafka_poll_job_t *)calloc(1, sizeof(xkafka_poll_job_t));

        if (call == NULL)
                return -1;
        call->job.run = xkafka_poll_job_run;
        call->consumer = consumer;
        call->timeout_ms = timeout_ms;
        call->status = status;
        call->slots = (xkafka_error_slots_t){error, error_size, error_code};
        call->result = result;
        return xkafka_job_start(&call->job, done, token);
}

/* ---- Administration calls that name topics ---- */

typedef struct {
        xkafka_job_t job;
        rd_kafka_t *client;
        char **names;
        size_t count;
        const int32_t *partitions;
        const int32_t *replication;
        char **config_names;
        char **config_values;
        size_t config_total;
        const size_t *config_counts;
        int timeout_ms;
        int kind;
        xkafka_error_slots_t slots;
        int32_t *result;
        void **pointer_result;
} xkafka_topics_job_t;

enum { XKAFKA_CREATE_TOPICS, XKAFKA_DELETE_TOPICS, XKAFKA_DESCRIBE_CONFIGS };

static void xkafka_topics_job_run(xkafka_job_t *job) {
        xkafka_topics_job_t *call = (xkafka_topics_job_t *)job;
        const char *const *names = (const char *const *)call->names;
        xkafka_error_slots_t *s = &call->slots;

        switch (call->kind) {
        case XKAFKA_CREATE_TOPICS:
                *call->result = xkafka_admin_create_topics(call->client, names, call->partitions, call->replication,
                                                           (const char *const *)call->config_names,
                                                           (const char *const *)call->config_values, call->config_counts, call->count,
                                                           call->timeout_ms, s->error, s->error_size, s->error_code);
                break;
        case XKAFKA_DELETE_TOPICS:
                *call->result =
                    xkafka_admin_delete_topics(call->client, names, call->count, call->timeout_ms, s->error, s->error_size, s->error_code);
                break;
        default:
                *call->pointer_result = xkafka_admin_describe_topic_configs(call->client, names, call->count, call->timeout_ms, s->error,
                                                                            s->error_size, s->error_code);
                break;
        }
}

static void xkafka_topics_job_release(xkafka_job_t *job) {
        xkafka_topics_job_t *call = (xkafka_topics_job_t *)job;

        xkafka_free_strings(call->names, call->count);
        xkafka_free_strings(call->config_names, call->config_total);
        xkafka_free_strings(call->config_values, call->config_total);
}

static xkafka_topics_job_t *xkafka_topics_job(rd_kafka_t *client, const char *const *names, size_t count, int timeout_ms, int kind,
                                              char *error, size_t error_size, int32_t *error_code) {
        xkafka_topics_job_t *call = (xkafka_topics_job_t *)calloc(1, sizeof(xkafka_topics_job_t));
        int ok = 1;

        if (call == NULL)
                return NULL;
        call->names = xkafka_copy_strings(names, count, &ok);
        if (!ok) {
                free(call);
                return NULL;
        }
        call->job.run = xkafka_topics_job_run;
        call->job.release = xkafka_topics_job_release;
        call->client = client;
        call->count = count;
        call->timeout_ms = timeout_ms;
        call->kind = kind;
        call->slots = (xkafka_error_slots_t){error, error_size, error_code};
        return call;
}

int xkafka_async_admin_create_topics(rd_kafka_t *client, const char *const *names, const int32_t *partitions, const int32_t *replication,
                                     const char *const *config_names, const char *const *config_values, const size_t *config_counts,
                                     size_t count, int timeout_ms, char *error, size_t error_size, int32_t *error_code,
                                     int32_t *result, xkafka_done_t done, int64_t token) {
        xkafka_topics_job_t *call = xkafka_topics_job(client, names, count, timeout_ms, XKAFKA_CREATE_TOPICS, error, error_size, error_code);
        size_t index;
        int ok = 1;

        if (call == NULL)
                return -1;
        for (index = 0; index < count; index++)
                call->config_total += config_counts[index];
        call->config_names = xkafka_copy_strings(config_names, call->config_total, &ok);
        call->config_values = xkafka_copy_strings(config_values, call->config_total, &ok);
        if (!ok) {
                xkafka_topics_job_release(&call->job);
                free(call);
                return -1;
        }
        call->partitions = partitions;
        call->replication = replication;
        call->config_counts = config_counts;
        call->result = result;
        return xkafka_job_start(&call->job, done, token);
}

int xkafka_async_admin_delete_topics(rd_kafka_t *client, const char *const *names, size_t count, int timeout_ms, char *error,
                                     size_t error_size, int32_t *error_code, int32_t *result, xkafka_done_t done, int64_t token) {
        xkafka_topics_job_t *call = xkafka_topics_job(client, names, count, timeout_ms, XKAFKA_DELETE_TOPICS, error, error_size, error_code);

        if (call == NULL)
                return -1;
        call->result = result;
        return xkafka_job_start(&call->job, done, token);
}

int xkafka_async_admin_describe_topic_configs(rd_kafka_t *client, const char *const *names, size_t count, int timeout_ms, char *error,
                                              size_t error_size, int32_t *error_code, void **result, xkafka_done_t done, int64_t token) {
        xkafka_topics_job_t *call =
            xkafka_topics_job(client, names, count, timeout_ms, XKAFKA_DESCRIBE_CONFIGS, error, error_size, error_code);

        if (call == NULL)
                return -1;
        call->pointer_result = result;
        return xkafka_job_start(&call->job, done, token);
}

#endif
