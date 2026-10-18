/* Drives every xkafka_async_* function of the JavaScript backend's library
 * against one broker, for scripts/js-leak-check.sh to run under a leak checker.
 *
 * The calls go through the library the npm package ships, as koffi makes them
 * from Node, and everything they hand back is released, so any allocation left
 * unreachable at exit is the library's.
 *
 * usage: js-leak-check bootstrap-servers */

#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

typedef struct rd_kafka_s rd_kafka_t;
typedef void (*xkafka_done_t)(int64_t token);

#define ERROR_ARGUMENTS char *error, size_t error_size, int32_t *error_code
#define FLAG_ARGUMENTS int32_t *is_fatal, int32_t *is_retriable, int32_t *txn_requires_abort
#define DONE_ARGUMENTS xkafka_done_t done, int64_t token

/* The library's own functions, as the shim defines them. */
rd_kafka_t *xkafka_admin_new(const char *brokers, const char *client_id, const char *const *property_names,
                             const char *const *property_values, size_t property_count, ERROR_ARGUMENTS);
rd_kafka_t *xkafka_producer_new(const char *brokers, const char *client_id, const char *const *property_names,
                                const char *const *property_values, size_t property_count, ERROR_ARGUMENTS);
rd_kafka_t *xkafka_consumer_new(const char *brokers, const char *client_id, const char *group_id, const char *auto_offset_reset,
                                const char *const *property_names, const char *const *property_values, size_t property_count,
                                ERROR_ARGUMENTS);
void *xkafka_headers_new(size_t count);
int xkafka_headers_add(void *headers, const char *name, const void *value, size_t value_size, ERROR_ARGUMENTS);
void *xkafka_batch_new(size_t capacity);
int xkafka_batch_add(rd_kafka_t *producer, void *batch, const char *topic, int32_t partition, int64_t timestamp, const void *key,
                     size_t key_size, const void *value, size_t value_size, void *headers, ERROR_ARGUMENTS);
size_t xkafka_batch_pending(const void *batch);
int xkafka_batch_await(rd_kafka_t *producer, void *batch, ERROR_ARGUMENTS);
void xkafka_batch_free(void *batch);
int xkafka_producer_begin_transaction(rd_kafka_t *producer, ERROR_ARGUMENTS, FLAG_ARGUMENTS);
void *xkafka_subscription_new(size_t count);
void xkafka_subscription_add(void *subscription, const char *topic);
void xkafka_subscription_destroy(void *subscription);
int xkafka_consumer_subscribe(rd_kafka_t *consumer, const void *subscription, ERROR_ARGUMENTS);
void *xkafka_consumer_take_rebalance(rd_kafka_t *consumer);
void xkafka_consumer_apply_rebalance(rd_kafka_t *consumer, void *event);
int64_t xkafka_message_offset(const void *message);
int32_t xkafka_message_partition(const void *message);
void xkafka_message_destroy(void *message);
void *xkafka_consumer_group_metadata(rd_kafka_t *consumer);
void xkafka_consumer_group_metadata_destroy(void *metadata);
size_t xkafka_metadata_partition_count_at(const void *metadata, size_t index);
void xkafka_metadata_destroy(void *metadata);
size_t xkafka_configs_entry_count(void *configs, size_t index);
void xkafka_configs_destroy(void *configs);

/* The calls that run on threads of their own. */
int xkafka_async_producer_destroy(rd_kafka_t *producer, int timeout_ms, DONE_ARGUMENTS);
int xkafka_async_consumer_destroy(rd_kafka_t *consumer, DONE_ARGUMENTS);
int xkafka_async_admin_destroy(rd_kafka_t *client, DONE_ARGUMENTS);
int xkafka_async_producer_poll(rd_kafka_t *producer, int timeout_ms, DONE_ARGUMENTS);
int xkafka_async_producer_init_transactions(rd_kafka_t *producer, int timeout_ms, ERROR_ARGUMENTS, FLAG_ARGUMENTS, int32_t *result,
                                            DONE_ARGUMENTS);
int xkafka_async_producer_commit_transaction(rd_kafka_t *producer, int timeout_ms, ERROR_ARGUMENTS, FLAG_ARGUMENTS, int32_t *result,
                                             DONE_ARGUMENTS);
int xkafka_async_producer_abort_transaction(rd_kafka_t *producer, int timeout_ms, ERROR_ARGUMENTS, FLAG_ARGUMENTS, int32_t *result,
                                            DONE_ARGUMENTS);
int xkafka_async_producer_send_offsets(rd_kafka_t *producer, void *metadata, const char *const *topics, const int32_t *partitions,
                                       const int64_t *offset_values, size_t count, int timeout_ms, ERROR_ARGUMENTS, FLAG_ARGUMENTS,
                                       int32_t *result, DONE_ARGUMENTS);
int xkafka_async_consumer_poll(rd_kafka_t *consumer, int timeout_ms, int *status, ERROR_ARGUMENTS, void **result, DONE_ARGUMENTS);
int xkafka_async_consumer_commit(rd_kafka_t *consumer, const char *const *topics, const int32_t *partitions,
                                 const int64_t *offset_values, size_t count, int timeout_ms, ERROR_ARGUMENTS, int32_t *result,
                                 DONE_ARGUMENTS);
int xkafka_async_consumer_committed(rd_kafka_t *consumer, const char *const *topics, const int32_t *partitions, size_t count,
                                    int64_t *offset_values, int timeout_ms, ERROR_ARGUMENTS, int32_t *result, DONE_ARGUMENTS);
int xkafka_async_consumer_offsets_for_times(rd_kafka_t *consumer, const char *const *topics, const int32_t *partitions,
                                            const int64_t *timestamps, size_t count, int64_t *offset_values, int timeout_ms,
                                            ERROR_ARGUMENTS, int32_t *result, DONE_ARGUMENTS);
int xkafka_async_consumer_watermark_offsets(rd_kafka_t *consumer, const char *topic, int32_t partition, int64_t *low, int64_t *high,
                                            int timeout_ms, ERROR_ARGUMENTS, int32_t *result, DONE_ARGUMENTS);
int xkafka_async_consumer_seek(rd_kafka_t *consumer, const char *topic, int32_t partition, int64_t offset, int timeout_ms,
                               ERROR_ARGUMENTS, int32_t *result, DONE_ARGUMENTS);
int xkafka_async_consumer_metadata(rd_kafka_t *client, const char *topic, int timeout_ms, ERROR_ARGUMENTS, void **result,
                                   DONE_ARGUMENTS);
int xkafka_async_admin_create_topics(rd_kafka_t *client, const char *const *names, const int32_t *partitions, const int32_t *replication,
                                     const char *const *config_names, const char *const *config_values, const size_t *config_counts,
                                     size_t count, int timeout_ms, ERROR_ARGUMENTS, int32_t *result, DONE_ARGUMENTS);
int xkafka_async_admin_delete_topics(rd_kafka_t *client, const char *const *names, size_t count, int timeout_ms, ERROR_ARGUMENTS,
                                     int32_t *result, DONE_ARGUMENTS);
int xkafka_async_admin_create_partitions(rd_kafka_t *client, const char *topic, int32_t count, int timeout_ms, ERROR_ARGUMENTS,
                                         int32_t *result, DONE_ARGUMENTS);
int xkafka_async_admin_delete_records(rd_kafka_t *client, const char *const *topics, const int32_t *partitions, const int64_t *offsets,
                                      size_t count, int timeout_ms, int32_t *codes, int64_t *low_watermarks, ERROR_ARGUMENTS,
                                      int32_t *result, DONE_ARGUMENTS);
int xkafka_async_admin_describe_topic_configs(rd_kafka_t *client, const char *const *names, size_t count, int timeout_ms,
                                              ERROR_ARGUMENTS, void **result, DONE_ARGUMENTS);

enum { TIMEOUT_MS = 30000, RECORDS = 20 };

static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t answered = PTHREAD_COND_INITIALIZER;
static int64_t last_answer = 0;
static int64_t last_token = 0;

/* The library calls this from the call's own thread. */
static void done(int64_t token) {
        pthread_mutex_lock(&lock);
        last_answer = token;
        pthread_cond_broadcast(&answered);
        pthread_mutex_unlock(&lock);
}

/* What every call reports a failure through. */
static char error[512];
static int32_t error_code;
static int32_t is_fatal, is_retriable, txn_requires_abort;
static int32_t result;

static void fail(const char *call) {
        fprintf(stderr, "%s failed: %s (code %d)\n", call, error, error_code);
        exit(1);
}

static int64_t next_token(void) {
        error[0] = '\0';
        error_code = 0;
        result = -1;
        return ++last_token;
}

/* Waits for the call that `started` began, one call being out at a time. */
static void await_answer(const char *call, int started) {
        if (started != 0) {
                fprintf(stderr, "%s could not start its thread\n", call);
                exit(1);
        }
        pthread_mutex_lock(&lock);
        while (last_answer != last_token)
                pthread_cond_wait(&answered, &lock);
        pthread_mutex_unlock(&lock);
}

/* Waits for a call that reports through `result`, and fails unless it succeeded. */
static void await_success(const char *call, int started) {
        await_answer(call, started);
        if (result != 0)
                fail(call);
}

#define ERRORS error, sizeof error, &error_code
#define FLAGS &is_fatal, &is_retriable, &txn_requires_abort
#define CALL(name, ...) await_success(#name, name(__VA_ARGS__, done, next_token()))

/* Applies every rebalance the consumer's polls left, as the backend does. */
static void apply_rebalances(rd_kafka_t *consumer) {
        void *event;

        while ((event = xkafka_consumer_take_rebalance(consumer)) != NULL)
                xkafka_consumer_apply_rebalance(consumer, event);
}

/* Polls until `wanted` records have arrived, and reports the next offset of the last one. */
static int64_t consume(rd_kafka_t *consumer, int wanted) {
        int received = 0;
        int polls = 0;
        int64_t next = -1;

        while (received < wanted) {
                void *message = NULL;
                int status = 0;

                if (++polls > 600) {
                        fprintf(stderr, "only %d of %d records arrived\n", received, wanted);
                        exit(1);
                }
                await_answer("xkafka_async_consumer_poll", xkafka_async_consumer_poll(consumer, 100, &status, ERRORS, &message, done,
                                                                                      next_token()));
                apply_rebalances(consumer);
                if (status < 0)
                        fail("xkafka_async_consumer_poll");
                if (status > 0) {
                        if (xkafka_message_partition(message) != 0) {
                                fprintf(stderr, "a record arrived from partition %d\n", xkafka_message_partition(message));
                                exit(1);
                        }
                        next = xkafka_message_offset(message) + 1;
                        xkafka_message_destroy(message);
                        received++;
                }
        }
        return next;
}

/* Sends `count` records with a header each in one batch, polling the producer until all are delivered. */
static void produce(rd_kafka_t *producer, const char *topic, int count) {
        void *batch = xkafka_batch_new((size_t)count);
        int index;

        for (index = 0; index < count; index++) {
                char value[32];
                void *headers = xkafka_headers_new(1);

                snprintf(value, sizeof value, "record %d", index);
                if (xkafka_headers_add(headers, "index", value, strlen(value), ERRORS) != 0)
                        fail("xkafka_headers_add");
                /* The message owns its headers once it is accepted. */
                if (xkafka_batch_add(producer, batch, topic, 0, -1, "key", 3, value, strlen(value), headers, ERRORS) != 0)
                        fail("xkafka_batch_add");
        }
        while (xkafka_batch_pending(batch) > 0)
                await_answer("xkafka_async_producer_poll", xkafka_async_producer_poll(producer, 100, done, next_token()));
        if (xkafka_batch_await(producer, batch, ERRORS) != 0)
                fail("xkafka_batch_await");
        xkafka_batch_free(batch);
}

int main(int argc, char **argv) {
        const char *brokers;
        char topic[48], group[64], transactional_id[64];
        const char *topics[1];
        const int32_t first_partition[] = {0};
        rd_kafka_t *admin, *producer, *consumer;
        void *subscription, *metadata, *configs, *group_metadata;
        int64_t next, committed, low, high, found, low_watermark;
        int64_t timestamps[1];
        int32_t codes[1];

        if (argc != 2) {
                fprintf(stderr, "usage: %s bootstrap-servers\n", argv[0]);
                return 2;
        }
        brokers = argv[1];
        snprintf(topic, sizeof topic, "xkafka-js-leaks-%ld", (long)getpid());
        snprintf(group, sizeof group, "%s-group", topic);
        snprintf(transactional_id, sizeof transactional_id, "%s-transactions", topic);
        topics[0] = topic;

        admin = xkafka_admin_new(brokers, "xkafka-js-leaks", NULL, NULL, 0, ERRORS);
        if (admin == NULL)
                fail("xkafka_admin_new");
        {
                const int32_t partitions[] = {1}, replication[] = {1};
                const char *config_names[] = {"retention.ms"}, *config_values[] = {"600000"};
                const size_t config_counts[] = {1};

                CALL(xkafka_async_admin_create_topics, admin, topics, partitions, replication, config_names, config_values,
                     config_counts, 1, TIMEOUT_MS, ERRORS, &result);
        }
        CALL(xkafka_async_admin_create_partitions, admin, topic, 2, TIMEOUT_MS, ERRORS, &result);
        configs = NULL;
        await_answer("xkafka_async_admin_describe_topic_configs",
                     xkafka_async_admin_describe_topic_configs(admin, topics, 1, TIMEOUT_MS, ERRORS, &configs, done, next_token()));
        if (configs == NULL || xkafka_configs_entry_count(configs, 0) == 0)
                fail("xkafka_async_admin_describe_topic_configs");
        xkafka_configs_destroy(configs);

        {
                const char *names[] = {"transactional.id"}, *values[] = {transactional_id};

                producer = xkafka_producer_new(brokers, "xkafka-js-leaks", names, values, 1, ERRORS);
        }
        if (producer == NULL)
                fail("xkafka_producer_new");
        CALL(xkafka_async_producer_init_transactions, producer, TIMEOUT_MS, ERRORS, FLAGS, &result);
        if (xkafka_producer_begin_transaction(producer, ERRORS, FLAGS) != 0)
                fail("xkafka_producer_begin_transaction");
        produce(producer, topic, RECORDS);
        CALL(xkafka_async_producer_commit_transaction, producer, TIMEOUT_MS, ERRORS, FLAGS, &result);

        consumer = xkafka_consumer_new(brokers, "xkafka-js-leaks", group, "earliest", NULL, NULL, 0, ERRORS);
        if (consumer == NULL)
                fail("xkafka_consumer_new");
        subscription = xkafka_subscription_new(1);
        xkafka_subscription_add(subscription, topic);
        if (xkafka_consumer_subscribe(consumer, subscription, ERRORS) != 0)
                fail("xkafka_consumer_subscribe");
        xkafka_subscription_destroy(subscription);
        next = consume(consumer, RECORDS);

        CALL(xkafka_async_consumer_commit, consumer, topics, first_partition, &next, 1, TIMEOUT_MS, ERRORS, &result);
        committed = -1;
        CALL(xkafka_async_consumer_committed, consumer, topics, first_partition, 1, &committed, TIMEOUT_MS, ERRORS, &result);
        if (committed != next) {
                fprintf(stderr, "committed %lld, expected %lld\n", (long long)committed, (long long)next);
                return 1;
        }
        CALL(xkafka_async_consumer_watermark_offsets, consumer, topic, 0, &low, &high, TIMEOUT_MS, ERRORS, &result);
        timestamps[0] = 0;
        CALL(xkafka_async_consumer_offsets_for_times, consumer, topics, first_partition, timestamps, 1, &found, TIMEOUT_MS, ERRORS,
             &result);
        CALL(xkafka_async_consumer_seek, consumer, topic, 0, low, TIMEOUT_MS, ERRORS, &result);
        metadata = NULL;
        await_answer("xkafka_async_consumer_metadata",
                     xkafka_async_consumer_metadata(consumer, topic, TIMEOUT_MS, ERRORS, &metadata, done, next_token()));
        if (metadata == NULL || xkafka_metadata_partition_count_at(metadata, 0) != 2)
                fail("xkafka_async_consumer_metadata");
        xkafka_metadata_destroy(metadata);

        /* A transaction that records the consumer's offsets, then is abandoned. */
        group_metadata = xkafka_consumer_group_metadata(consumer);
        if (xkafka_producer_begin_transaction(producer, ERRORS, FLAGS) != 0)
                fail("xkafka_producer_begin_transaction");
        produce(producer, topic, 1);
        CALL(xkafka_async_producer_send_offsets, producer, group_metadata, topics, first_partition, &next, 1, TIMEOUT_MS, ERRORS, FLAGS,
             &result);
        CALL(xkafka_async_producer_abort_transaction, producer, TIMEOUT_MS, ERRORS, FLAGS, &result);
        xkafka_consumer_group_metadata_destroy(group_metadata);
        await_answer("xkafka_async_consumer_destroy", xkafka_async_consumer_destroy(consumer, done, next_token()));

        {
                const int64_t cut[] = {-1};

                CALL(xkafka_async_admin_delete_records, admin, topics, first_partition, cut, 1, TIMEOUT_MS, codes, &low_watermark, ERRORS,
                     &result);
        }
        if (codes[0] != 0) {
                fprintf(stderr, "deleting records reported code %d\n", codes[0]);
                return 1;
        }
        CALL(xkafka_async_admin_delete_topics, admin, topics, 1, TIMEOUT_MS, ERRORS, &result);

        await_answer("xkafka_async_producer_destroy", xkafka_async_producer_destroy(producer, TIMEOUT_MS, done, next_token()));
        await_answer("xkafka_async_admin_destroy", xkafka_async_admin_destroy(admin, done, next_token()));
        printf("every asynchronous call of the library ran: %lld calls\n", (long long)last_token);
        return 0;
}
