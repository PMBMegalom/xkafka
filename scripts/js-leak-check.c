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
size_t xkafka_batch_count(const void *batch);
int xkafka_batch_error_at(rd_kafka_t *producer, void *batch, size_t index, ERROR_ARGUMENTS);
void xkafka_batch_free(void *batch);
int xkafka_producer_begin_transaction(rd_kafka_t *producer, ERROR_ARGUMENTS, FLAG_ARGUMENTS);
void *xkafka_subscription_new(size_t count);
void xkafka_subscription_add(void *subscription, const char *topic);
void xkafka_subscription_destroy(void *subscription);
int xkafka_consumer_subscribe(rd_kafka_t *consumer, const void *subscription, ERROR_ARGUMENTS);
void *xkafka_consumer_take_rebalance(rd_kafka_t *consumer);
void xkafka_consumer_apply_rebalance(rd_kafka_t *consumer, void *event, const char *const *start_topics,
                                     const int32_t *start_partitions, const int64_t *start_offsets,
                                     const int32_t *start_leader_epochs, size_t start_count);
int64_t xkafka_message_offset(const void *message);
int32_t xkafka_message_partition(const void *message);
void xkafka_message_destroy(void *message);
void *xkafka_consumer_group_metadata(rd_kafka_t *consumer);
void *xkafka_group_metadata_new(const char *group_id, int32_t generation_id, const char *member_id);
void xkafka_consumer_group_metadata_destroy(void *metadata);
size_t xkafka_metadata_partition_count_at(const void *metadata, size_t index);
void xkafka_metadata_destroy(void *metadata);
size_t xkafka_configs_entry_count(void *configs, size_t index);
size_t xkafka_configs_resource_count(void *configs);
int32_t xkafka_configs_resource_error(void *configs, size_t index);
void xkafka_configs_destroy(void *configs);
const char *xkafka_cluster_id(void *cluster);
const void *xkafka_cluster_controller(void *cluster);
size_t xkafka_cluster_broker_count(void *cluster);
const void *xkafka_cluster_broker_at(void *cluster, size_t index);
void xkafka_cluster_destroy(void *cluster);
const char *xkafka_node_host(const void *node);
size_t xkafka_topics_count(void *topics);
int32_t xkafka_topic_error(void *topics, size_t topic);
int64_t xkafka_topic_id_most(void *topics, size_t topic);
int64_t xkafka_topic_id_least(void *topics, size_t topic);
size_t xkafka_topic_partition_count(void *topics, size_t topic);
size_t xkafka_topic_replica_count(void *topics, size_t topic, size_t partition);
size_t xkafka_topic_in_sync_count(void *topics, size_t topic, size_t partition);
void xkafka_topics_destroy(void *topics);
size_t xkafka_groups_count(void *groups);
const char *xkafka_groups_id_at(void *groups, size_t index);
void xkafka_groups_destroy(void *groups);
size_t xkafka_descriptions_count(void *descriptions);
int32_t xkafka_description_error(void *descriptions, size_t group);
size_t xkafka_description_member_count(void *descriptions, size_t group);
size_t xkafka_member_partition_count(void *descriptions, size_t group, size_t member);
void xkafka_descriptions_destroy(void *descriptions);
size_t xkafka_assignment_count(const void *assignment);

/* The calls that run on threads of their own. */
int xkafka_async_producer_destroy(rd_kafka_t *producer, int timeout_ms, DONE_ARGUMENTS);
int xkafka_async_consumer_destroy(rd_kafka_t *consumer, DONE_ARGUMENTS);
int xkafka_async_admin_destroy(rd_kafka_t *client, DONE_ARGUMENTS);
int xkafka_async_producer_poll(rd_kafka_t *producer, int timeout_ms, DONE_ARGUMENTS);
int xkafka_async_producer_flush(rd_kafka_t *producer, int timeout_ms, DONE_ARGUMENTS);
int xkafka_async_producer_init_transactions(rd_kafka_t *producer, int timeout_ms, ERROR_ARGUMENTS, FLAG_ARGUMENTS, int32_t *result,
                                            DONE_ARGUMENTS);
int xkafka_async_producer_commit_transaction(rd_kafka_t *producer, int timeout_ms, ERROR_ARGUMENTS, FLAG_ARGUMENTS, int32_t *result,
                                             DONE_ARGUMENTS);
int xkafka_async_producer_abort_transaction(rd_kafka_t *producer, int timeout_ms, ERROR_ARGUMENTS, FLAG_ARGUMENTS, int32_t *result,
                                            DONE_ARGUMENTS);
int xkafka_async_producer_send_offsets(rd_kafka_t *producer, void *metadata, const char *const *topics, const int32_t *partitions,
                                       const int64_t *offset_values, const int32_t *leader_epochs, const char *const *offset_metadata,
                                       size_t count, int timeout_ms, ERROR_ARGUMENTS, FLAG_ARGUMENTS, int32_t *result, DONE_ARGUMENTS);
int xkafka_async_consumer_poll(rd_kafka_t *consumer, int timeout_ms, int *status, ERROR_ARGUMENTS, void **result, DONE_ARGUMENTS);
int xkafka_async_consumer_commit(rd_kafka_t *consumer, const char *const *topics, const int32_t *partitions,
                                 const int64_t *offset_values, const int32_t *leader_epochs, const char *const *metadata, size_t count,
                                 int timeout_ms, ERROR_ARGUMENTS, int32_t *result, DONE_ARGUMENTS);
int xkafka_async_consumer_committed(rd_kafka_t *consumer, const char *const *topics, const int32_t *partitions, size_t count,
                                    int timeout_ms, ERROR_ARGUMENTS, void **result, DONE_ARGUMENTS);
int64_t xkafka_committed_offset_at(const void *committed, size_t index);
const char *xkafka_committed_metadata_at(const void *committed, size_t index, size_t *size);
void xkafka_committed_destroy(void *committed);
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
int xkafka_async_admin_describe_cluster(rd_kafka_t *client, int timeout_ms, ERROR_ARGUMENTS, void **result, DONE_ARGUMENTS);
int xkafka_async_admin_describe_topics(rd_kafka_t *client, const char *const *names, size_t count, int timeout_ms, ERROR_ARGUMENTS,
                                       void **result, DONE_ARGUMENTS);
int xkafka_async_admin_list_groups(rd_kafka_t *client, int timeout_ms, ERROR_ARGUMENTS, void **result, DONE_ARGUMENTS);
int xkafka_async_admin_describe_groups(rd_kafka_t *client, const char *const *groups, size_t count, int timeout_ms, ERROR_ARGUMENTS,
                                       void **result, DONE_ARGUMENTS);
int xkafka_async_admin_delete_groups(rd_kafka_t *client, const char *const *groups, size_t count, int timeout_ms, int32_t *codes,
                                     ERROR_ARGUMENTS, int32_t *result, DONE_ARGUMENTS);
int xkafka_async_admin_alter_topic_configs(rd_kafka_t *client, const char *const *topics, const char *const *names,
                                           const char *const *values, const int32_t *operations, size_t count, int timeout_ms,
                                           ERROR_ARGUMENTS, void **result, DONE_ARGUMENTS);
int xkafka_async_admin_elect_preferred_leaders(rd_kafka_t *client, const char *const *topics, const int32_t *partitions, size_t count,
                                               int timeout_ms, int32_t *codes, ERROR_ARGUMENTS, int32_t *result, DONE_ARGUMENTS);
int xkafka_async_admin_list_group_offsets(rd_kafka_t *client, const char *group, const char *const *topics, const int32_t *partitions,
                                          size_t count, int timeout_ms, ERROR_ARGUMENTS, void **result, DONE_ARGUMENTS);
int xkafka_async_admin_alter_group_offsets(rd_kafka_t *client, const char *group, const char *const *topics, const int32_t *partitions,
                                           const int64_t *offset_values, const int32_t *leader_epochs, const char *const *metadata,
                                           size_t count, int timeout_ms, int32_t *codes, ERROR_ARGUMENTS, int32_t *result,
                                           DONE_ARGUMENTS);
int xkafka_async_admin_delete_group_offsets(rd_kafka_t *client, const char *group, const char *const *topics, const int32_t *partitions,
                                            size_t count, int timeout_ms, int32_t *codes, ERROR_ARGUMENTS, int32_t *result,
                                            DONE_ARGUMENTS);

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
                xkafka_consumer_apply_rebalance(consumer, event, NULL, NULL, NULL, NULL, 0);
}

/* Polls until `wanted` records and then the end of their partition have arrived, and reports the next offset of the last record. */
static int64_t consume(rd_kafka_t *consumer, int wanted) {
        int received = 0;
        int polls = 0;
        int ended = 0;
        int64_t next = -1;

        while (received < wanted || !ended) {
                void *message = NULL;
                int status = 0;

                if (++polls > 600) {
                        fprintf(stderr, "only %d of %d records arrived, ended: %d\n", received, wanted, ended);
                        exit(1);
                }
                await_answer("xkafka_async_consumer_poll", xkafka_async_consumer_poll(consumer, 100, &status, ERRORS, &message, done,
                                                                                      next_token()));
                apply_rebalances(consumer);
                if (status < 0)
                        fail("xkafka_async_consumer_poll");
                if (status == 2) {
                        ended = received == wanted;
                        xkafka_message_destroy(message);
                } else if (status > 0) {
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
        await_answer("xkafka_async_producer_flush", xkafka_async_producer_flush(producer, 100, done, next_token()));
        while (xkafka_batch_pending(batch) > 0)
                await_answer("xkafka_async_producer_poll", xkafka_async_producer_poll(producer, 100, done, next_token()));
        for (index = 0; index < (int)xkafka_batch_count(batch); index++)
                if (xkafka_batch_error_at(producer, batch, (size_t)index, ERRORS) != 0)
                        fail("xkafka_batch_error_at");
        xkafka_batch_free(batch);
}

/* Sends one record with a header that is too large for the producer, which takes it and answers it as refused. */
static void produce_too_large(rd_kafka_t *producer, const char *topic) {
        void *batch = xkafka_batch_new(1);
        void *headers = xkafka_headers_new(1);
        size_t size = 2000000;
        char *value = calloc(size, 1);

        if (value == NULL)
                fail("calloc");
        if (xkafka_headers_add(headers, "index", "large", 5, ERRORS) != 0)
                fail("xkafka_headers_add");
        if (xkafka_batch_add(producer, batch, topic, 0, -1, "key", 3, value, size, headers, ERRORS) != 0)
                fail("xkafka_batch_add");
        if (xkafka_batch_pending(batch) != 0 || xkafka_batch_count(batch) != 1)
                fail("xkafka_batch_add of a record too large");
        if (xkafka_batch_error_at(producer, batch, 0, ERRORS) == 0 || error_code != 10)
                fail("xkafka_batch_error_at of a record too large");
        free(value);
        xkafka_batch_free(batch);
}

/* Describes the group, and fails unless it has the members expected, each holding at least one partition. */
static void describe_group(rd_kafka_t *admin, const char *group, size_t members) {
        const char *groups[1] = {group};
        void *descriptions = NULL;
        size_t member;

        await_answer("xkafka_async_admin_describe_groups",
                     xkafka_async_admin_describe_groups(admin, groups, 1, TIMEOUT_MS, ERRORS, &descriptions, done, next_token()));
        if (descriptions == NULL || xkafka_descriptions_count(descriptions) != 1 || xkafka_description_error(descriptions, 0) != 0 ||
            xkafka_description_member_count(descriptions, 0) != members)
                fail("xkafka_async_admin_describe_groups");
        for (member = 0; member < members; member++)
                if (xkafka_member_partition_count(descriptions, 0, member) == 0)
                        fail("xkafka_async_admin_describe_groups");
        xkafka_descriptions_destroy(descriptions);
}

/* Lists the group's committed offsets, all of them or the first partition's, and fails unless there are as many as expected. */
static void group_offsets(rd_kafka_t *admin, const char *group, const char *const *topics, const int32_t *partitions, size_t count,
                          size_t expected) {
        void *offsets = NULL;

        await_answer("xkafka_async_admin_list_group_offsets",
                     xkafka_async_admin_list_group_offsets(admin, group, topics, partitions, count, TIMEOUT_MS, ERRORS, &offsets, done,
                                                           next_token()));
        if (offsets == NULL || xkafka_assignment_count(offsets) != expected)
                fail("xkafka_async_admin_list_group_offsets");
        xkafka_committed_destroy(offsets);
}

int main(int argc, char **argv) {
        const char *brokers;
        char topic[48], group[64], transactional_id[64];
        const char *topics[1];
        const int32_t first_partition[] = {0};
        rd_kafka_t *admin, *producer, *consumer;
        void *subscription, *metadata, *configs, *group_metadata;
        int64_t next, committed, low, high, found, low_watermark;
        const int32_t no_epoch[1] = {-1};
        const char *const notes[1] = {"leak check"};
        const char *const eof_names[1] = {"enable.partition.eof"};
        const char *const eof_values[1] = {"true"};
        void *stored;
        const char *note;
        size_t note_size;
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
                /* One property set and one unset, whose value is NULL. */
                const char *changed_topics[] = {topic, topic};
                const char *changed_names[] = {"retention.ms", "retention.bytes"};
                const char *changed_values[] = {"700000", NULL};
                const int32_t operations[] = {0, 1};

                configs = NULL;
                await_answer("xkafka_async_admin_alter_topic_configs",
                             xkafka_async_admin_alter_topic_configs(admin, changed_topics, changed_names, changed_values, operations, 2,
                                                                    TIMEOUT_MS, ERRORS, &configs, done, next_token()));
                if (configs == NULL || xkafka_configs_resource_count(configs) != 1 || xkafka_configs_resource_error(configs, 0) != 0)
                        fail("xkafka_async_admin_alter_topic_configs");
                xkafka_configs_destroy(configs);
        }
        /* A single broker already leads every partition it has, which the broker answers as ELECTION_NOT_NEEDED. */
        CALL(xkafka_async_admin_elect_preferred_leaders, admin, topics, first_partition, 1, TIMEOUT_MS, codes, ERRORS, &result);
        if (codes[0] != 0 && codes[0] != 84)
                fail("xkafka_async_admin_elect_preferred_leaders");
        {
                void *cluster = NULL, *described = NULL;
                size_t partition;

                await_answer("xkafka_async_admin_describe_cluster",
                             xkafka_async_admin_describe_cluster(admin, TIMEOUT_MS, ERRORS, &cluster, done, next_token()));
                if (cluster == NULL || xkafka_cluster_id(cluster) == NULL || xkafka_cluster_controller(cluster) == NULL ||
                    xkafka_cluster_broker_count(cluster) != 1 || xkafka_node_host(xkafka_cluster_broker_at(cluster, 0)) == NULL)
                        fail("xkafka_async_admin_describe_cluster");
                xkafka_cluster_destroy(cluster);
                await_answer("xkafka_async_admin_describe_topics",
                             xkafka_async_admin_describe_topics(admin, topics, 1, TIMEOUT_MS, ERRORS, &described, done, next_token()));
                if (described == NULL || xkafka_topics_count(described) != 1 || xkafka_topic_error(described, 0) != 0 ||
                    (xkafka_topic_id_most(described, 0) == 0 && xkafka_topic_id_least(described, 0) == 0) ||
                    xkafka_topic_partition_count(described, 0) != 2)
                        fail("xkafka_async_admin_describe_topics");
                for (partition = 0; partition < 2; partition++)
                        if (xkafka_topic_replica_count(described, 0, partition) != 1 || xkafka_topic_in_sync_count(described, 0, partition) != 1)
                                fail("xkafka_async_admin_describe_topics");
                xkafka_topics_destroy(described);
        }

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
        produce_too_large(producer, topic);
        CALL(xkafka_async_producer_commit_transaction, producer, TIMEOUT_MS, ERRORS, FLAGS, &result);

        consumer = xkafka_consumer_new(brokers, "xkafka-js-leaks", group, "earliest", eof_names, eof_values, 1, ERRORS);
        if (consumer == NULL)
                fail("xkafka_consumer_new");
        subscription = xkafka_subscription_new(1);
        xkafka_subscription_add(subscription, topic);
        if (xkafka_consumer_subscribe(consumer, subscription, ERRORS) != 0)
                fail("xkafka_consumer_subscribe");
        xkafka_subscription_destroy(subscription);
        next = consume(consumer, RECORDS);

        CALL(xkafka_async_consumer_commit, consumer, topics, first_partition, &next, no_epoch, notes, 1, TIMEOUT_MS, ERRORS, &result);
        stored = NULL;
        await_answer("xkafka_async_consumer_committed", xkafka_async_consumer_committed(consumer, topics, first_partition, 1,
                                                                                        TIMEOUT_MS, ERRORS, &stored, done, next_token()));
        if (stored == NULL)
                fail("xkafka_async_consumer_committed");
        committed = xkafka_committed_offset_at(stored, 0);
        note = xkafka_committed_metadata_at(stored, 0, &note_size);
        if (committed != next || note_size != strlen(notes[0]) || memcmp(note, notes[0], note_size) != 0) {
                fprintf(stderr, "committed %lld, expected %lld\n", (long long)committed, (long long)next);
                return 1;
        }
        xkafka_committed_destroy(stored);
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

        describe_group(admin, group, 1);

        /* A transaction that records the consumer's offsets, then is abandoned. */
        group_metadata = xkafka_consumer_group_metadata(consumer);
        if (xkafka_producer_begin_transaction(producer, ERRORS, FLAGS) != 0)
                fail("xkafka_producer_begin_transaction");
        produce(producer, topic, 1);
        CALL(xkafka_async_producer_send_offsets, producer, group_metadata, topics, first_partition, &next, no_epoch, notes, 1, TIMEOUT_MS,
             ERRORS, FLAGS, &result);
        CALL(xkafka_async_producer_abort_transaction, producer, TIMEOUT_MS, ERRORS, FLAGS, &result);
        xkafka_consumer_group_metadata_destroy(group_metadata);

        await_answer("xkafka_async_consumer_destroy", xkafka_async_consumer_destroy(consumer, done, next_token()));

        /* The same, now that the group has no members, under a group identity built from its parts. */
        group_metadata = xkafka_group_metadata_new(group, -1, "");
        if (group_metadata == NULL)
                fail("xkafka_group_metadata_new");
        if (xkafka_producer_begin_transaction(producer, ERRORS, FLAGS) != 0)
                fail("xkafka_producer_begin_transaction");
        produce(producer, topic, 1);
        CALL(xkafka_async_producer_send_offsets, producer, group_metadata, topics, first_partition, &next, no_epoch, notes, 1, TIMEOUT_MS,
             ERRORS, FLAGS, &result);
        CALL(xkafka_async_producer_abort_transaction, producer, TIMEOUT_MS, ERRORS, FLAGS, &result);
        xkafka_consumer_group_metadata_destroy(group_metadata);

        /* The group, now without members. */
        {
                const char *groups[1] = {group};
                void *listed = NULL;
                size_t index, found_group = 0;

                await_answer("xkafka_async_admin_list_groups",
                             xkafka_async_admin_list_groups(admin, TIMEOUT_MS, ERRORS, &listed, done, next_token()));
                if (listed == NULL)
                        fail("xkafka_async_admin_list_groups");
                for (index = 0; index < xkafka_groups_count(listed); index++)
                        if (strcmp(xkafka_groups_id_at(listed, index), group) == 0)
                                found_group = 1;
                xkafka_groups_destroy(listed);
                if (!found_group)
                        fail("xkafka_async_admin_list_groups");
                describe_group(admin, group, 0);
                group_offsets(admin, group, NULL, NULL, 0, 1);
                group_offsets(admin, group, topics, first_partition, 1, 1);
                CALL(xkafka_async_admin_alter_group_offsets, admin, group, topics, first_partition, &next, no_epoch, notes, 1, TIMEOUT_MS,
                     codes, ERRORS, &result);
                if (codes[0] != 0)
                        fail("xkafka_async_admin_alter_group_offsets");
                CALL(xkafka_async_admin_delete_group_offsets, admin, group, topics, first_partition, 1, TIMEOUT_MS, codes, ERRORS, &result);
                if (codes[0] != 0)
                        fail("xkafka_async_admin_delete_group_offsets");
                group_offsets(admin, group, NULL, NULL, 0, 0);
                CALL(xkafka_async_admin_delete_groups, admin, groups, 1, TIMEOUT_MS, codes, ERRORS, &result);
                if (codes[0] != 0)
                        fail("xkafka_async_admin_delete_groups");
        }

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
