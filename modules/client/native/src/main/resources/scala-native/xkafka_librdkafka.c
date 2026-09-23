#ifdef SCALANATIVE_LINK_RDKAFKA

#include <librdkafka/rdkafka.h>

#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct xkafka_batch_s;

typedef struct xkafka_delivery_s {
        int complete;
        rd_kafka_resp_err_t error;
        int32_t partition;
        int64_t offset;
        int64_t timestamp;
        struct xkafka_batch_s *owner;
} xkafka_delivery_t;

/* Delivery reports arrive after the enqueue returns, so the slots they write
 * into are heap allocated and outlive the call that produced them. Every
 * access happens on the thread holding the producer's permit, so the counters
 * need no synchronisation of their own. */
typedef struct xkafka_batch_s {
        size_t capacity;
        size_t count;
        size_t pending;
        xkafka_delivery_t *slots;
} xkafka_batch_t;

/* Per-client state reached through the librdkafka opaque.
 *
 * A refused certificate or a rejected credential is reported to the error
 * callback and never reaches a delivery report, which only ever sees the
 * retries ending. The last such failure is recorded here so the call that was
 * waiting can report the cause. librdkafka runs the callback on its own
 * thread, so the slot is guarded. */
typedef struct xkafka_client_s {
        int generation;
        pthread_mutex_t lock;
        int32_t security_error;
        char security_message[512];
} xkafka_client_t;

static xkafka_client_t *xkafka_client_new(void) {
        xkafka_client_t *state = (xkafka_client_t *)calloc(1, sizeof(xkafka_client_t));
        if (state == NULL)
                return NULL;
        if (pthread_mutex_init(&state->lock, NULL) != 0) {
                free(state);
                return NULL;
        }
        return state;
}

static void xkafka_client_free(xkafka_client_t *state) {
        if (state == NULL)
                return;
        pthread_mutex_destroy(&state->lock);
        free(state);
}

const char *xkafka_version_str(void) { return rd_kafka_version_str(); }

/* Failures report their librdkafka code alongside their text, both written
 * through the caller's own out-parameters. Nothing is shared between clients,
 * so concurrent failures on separate handles cannot overwrite each other. */
static void xkafka_set_error_at(char *error,
                                size_t error_size,
                                int32_t *error_code,
                                const char *value,
                                int32_t code) {
        if (error_code != NULL)
                *error_code = code;
        if (error != NULL && error_size > 0)
                snprintf(error, error_size, "%s", value);
}

/* RD_KAFKA_RESP_ERR_UNKNOWN, for failures the shim raises itself. */
static void xkafka_set_error(char *error, size_t error_size, int32_t *error_code, const char *value) {
        xkafka_set_error_at(error, error_size, error_code, value, -1);
}

/* Only the two failures that a waiting call cannot otherwise explain are kept. */
static void xkafka_error_callback(rd_kafka_t *client,
                                  int err,
                                  const char *reason,
                                  void *opaque) {
        xkafka_client_t *state = (xkafka_client_t *)opaque;

        (void)client;
        if (state == NULL)
                return;
        if (err != RD_KAFKA_RESP_ERR__AUTHENTICATION && err != RD_KAFKA_RESP_ERR__SSL)
                return;

        pthread_mutex_lock(&state->lock);
        state->security_error = (int32_t)err;
        snprintf(state->security_message, sizeof(state->security_message), "%s",
                 reason == NULL ? rd_kafka_err2str((rd_kafka_resp_err_t)err) : reason);
        pthread_mutex_unlock(&state->lock);
}

/* Reports the recorded failure and clears it, so a later unrelated error is
 * never attributed to a connection that has since been repaired. */
static int xkafka_take_security_error(rd_kafka_t *client,
                                      char *error,
                                      size_t error_size,
                                      int32_t *error_code) {
        xkafka_client_t *state = (xkafka_client_t *)rd_kafka_opaque(client);
        int found = 0;

        if (state == NULL)
                return 0;

        pthread_mutex_lock(&state->lock);
        if (state->security_error != 0) {
                xkafka_set_error_at(error, error_size, error_code,
                                    state->security_message, state->security_error);
                state->security_error = 0;
                found = 1;
        }
        pthread_mutex_unlock(&state->lock);
        return found;
}

static int xkafka_conf_set(rd_kafka_conf_t *conf,
                           const char *name,
                           const char *value,
                           char *error,
                           size_t error_size,
                           int32_t *error_code) {
        /* rd_kafka_conf_set writes its own message, so only the code is set here. */
        if (rd_kafka_conf_set(conf, name, value, error, error_size) !=
            RD_KAFKA_CONF_OK) {
                if (error_code != NULL)
                        *error_code = RD_KAFKA_RESP_ERR__INVALID_ARG;
                return -1;
        }
        return 0;
}

static int xkafka_conf_set_all(rd_kafka_conf_t *conf,
                               const char *const *names,
                               const char *const *values,
                               size_t count,
                               char *error,
                               size_t error_size,
                               int32_t *error_code) {
        size_t index;

        for (index = 0; index < count; index++) {
                if (xkafka_conf_set(conf, names[index], values[index], error,
                                    error_size, error_code) != 0)
                        return -1;
        }
        return 0;
}

static void xkafka_delivery_callback(rd_kafka_t *client,
                                     const rd_kafka_message_t *message,
                                     void *opaque) {
        xkafka_delivery_t *delivery =
            (xkafka_delivery_t *)message->_private;
        (void)client;
        (void)opaque;

        if (delivery == NULL)
                return;

        delivery->error = message->err;
        delivery->partition = message->partition;
        delivery->offset = message->offset;
        delivery->timestamp = rd_kafka_message_timestamp(message, NULL);
        delivery->complete = 1;

        if (delivery->owner != NULL && delivery->owner->pending > 0)
                delivery->owner->pending--;
}

rd_kafka_t *xkafka_producer_new(const char *brokers,
                                const char *client_id,
                                const char *const *property_names,
                                const char *const *property_values,
                                size_t property_count,
                                char *error,
                                size_t error_size,
                                int32_t *error_code) {
        rd_kafka_conf_t *conf = rd_kafka_conf_new();
        rd_kafka_t *producer;
        xkafka_client_t *state = xkafka_client_new();

        if (state == NULL) {
                rd_kafka_conf_destroy(conf);
                xkafka_set_error(error, error_size, error_code,
                                 "could not allocate the client state");
                return NULL;
        }

        /* The opaque travels with the client, so the error callback and the calls
         * that read it reach the same state. */
        rd_kafka_conf_set_opaque(conf, state);
        rd_kafka_conf_set_error_cb(conf, xkafka_error_callback);
        rd_kafka_conf_set_dr_msg_cb(conf, xkafka_delivery_callback);

        if (xkafka_conf_set_all(conf, property_names, property_values,
                                property_count, error, error_size, error_code) != 0 ||
            xkafka_conf_set(conf, "bootstrap.servers", brokers, error,
                            error_size, error_code) != 0) {
                rd_kafka_conf_destroy(conf);
                xkafka_client_free(state);
                return NULL;
        }
        if (client_id != NULL &&
            xkafka_conf_set(conf, "client.id", client_id, error, error_size, error_code) !=
                0) {
                rd_kafka_conf_destroy(conf);
                xkafka_client_free(state);
                return NULL;
        }

        producer =
            rd_kafka_new(RD_KAFKA_PRODUCER, conf, error, error_size);
        if (producer == NULL) {
                /* rd_kafka_new owns conf only on success, and the opaque with it. */
                rd_kafka_conf_destroy(conf);
                xkafka_client_free(state);
                return NULL;
        }
        return producer;
}

void xkafka_producer_destroy(rd_kafka_t *producer) {
        xkafka_client_t *state;

        if (producer == NULL)
                return;

        /* Read before destroying, since the opaque is unreachable afterwards. */
        state = (xkafka_client_t *)rd_kafka_opaque(producer);
        rd_kafka_flush(producer, 10000);
        rd_kafka_destroy(producer);
        xkafka_client_free(state);
}

rd_kafka_headers_t *xkafka_headers_new(size_t count) {
        return rd_kafka_headers_new(count);
}

void xkafka_headers_destroy(rd_kafka_headers_t *headers) {
        if (headers != NULL)
                rd_kafka_headers_destroy(headers);
}

int xkafka_headers_add(rd_kafka_headers_t *headers,
                       const char *name,
                       const void *value,
                       size_t value_size,
                       char *error,
                       size_t error_size,
                       int32_t *error_code) {
        rd_kafka_resp_err_t result =
            rd_kafka_header_add(headers, name, -1, value, value_size);
        if (result != RD_KAFKA_RESP_ERR_NO_ERROR) {
                xkafka_set_error_at(error, error_size, error_code, rd_kafka_err2str(result), result);
                return -1;
        }
        return 0;
}

xkafka_batch_t *xkafka_batch_new(size_t capacity) {
        xkafka_batch_t *batch;

        if (capacity == 0)
                return NULL;

        batch = (xkafka_batch_t *)calloc(1, sizeof(xkafka_batch_t));
        if (batch == NULL)
                return NULL;

        batch->slots =
            (xkafka_delivery_t *)calloc(capacity, sizeof(xkafka_delivery_t));
        if (batch->slots == NULL) {
                free(batch);
                return NULL;
        }

        batch->capacity = capacity;
        return batch;
}

/* Enqueues one message. The message owns a copy of its payload, so the caller's
 * buffers may be released as soon as this returns. */
int xkafka_batch_add(rd_kafka_t *producer,
                     xkafka_batch_t *batch,
                     const char *topic,
                     int32_t partition,
                     int64_t timestamp,
                     const void *key,
                     size_t key_size,
                     const void *value,
                     size_t value_size,
                     rd_kafka_headers_t *headers,
                     char *error,
                     size_t error_size,
                     int32_t *error_code) {
        xkafka_delivery_t *slot;
        rd_kafka_resp_err_t result;

        if (batch == NULL || batch->count >= batch->capacity) {
                if (headers != NULL)
                        rd_kafka_headers_destroy(headers);
                xkafka_set_error(error, error_size, error_code, "batch is full");
                return -1;
        }

        slot = &batch->slots[batch->count];
        slot->owner = batch;
        slot->partition = RD_KAFKA_PARTITION_UA;
        slot->offset = -1;
        slot->timestamp = -1;

        if (timestamp >= 0) {
                result = rd_kafka_producev(
                    producer, RD_KAFKA_V_TOPIC(topic),
                    RD_KAFKA_V_PARTITION(partition),
                    RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                    RD_KAFKA_V_TIMESTAMP(timestamp),
                    RD_KAFKA_V_KEY((void *)key, key_size),
                    RD_KAFKA_V_VALUE((void *)value, value_size),
                    RD_KAFKA_V_HEADERS(headers), RD_KAFKA_V_OPAQUE(slot),
                    RD_KAFKA_V_END);
        } else {
                result = rd_kafka_producev(
                    producer, RD_KAFKA_V_TOPIC(topic),
                    RD_KAFKA_V_PARTITION(partition),
                    RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                    RD_KAFKA_V_KEY((void *)key, key_size),
                    RD_KAFKA_V_VALUE((void *)value, value_size),
                    RD_KAFKA_V_HEADERS(headers), RD_KAFKA_V_OPAQUE(slot),
                    RD_KAFKA_V_END);
        }

        if (result != RD_KAFKA_RESP_ERR_NO_ERROR) {
                /* librdkafka only takes ownership of the headers on success. */
                if (headers != NULL)
                        rd_kafka_headers_destroy(headers);
                xkafka_set_error_at(error, error_size, error_code, rd_kafka_err2str(result), result);
                return -1;
        }

        batch->count++;
        batch->pending++;
        return 0;
}

/* Serves delivery reports until every enqueued message has one. */
/* Serves whatever delivery reports are ready. The caller decides how long to keep asking, so one
 * batch waiting for its reports does not stop another from being enqueued. */
void xkafka_producer_poll(rd_kafka_t *producer, int timeout_ms) {
        rd_kafka_poll(producer, timeout_ms);
}

size_t xkafka_batch_pending(const xkafka_batch_t *batch) {
        return batch == NULL ? 0 : batch->pending;
}

/* Reads the outcome of a batch whose reports have all arrived. */
int xkafka_batch_await(rd_kafka_t *producer,
                       xkafka_batch_t *batch,
                       char *error,
                       size_t error_size,
                       int32_t *error_code) {
        size_t index;

        if (batch == NULL) {
                xkafka_set_error(error, error_size, error_code, "batch is not allocated");
                return -1;
        }

        for (index = 0; index < batch->count; index++) {
                if (batch->slots[index].error !=
                    RD_KAFKA_RESP_ERR_NO_ERROR) {
                        /* A delivery report that never reached a broker reports the
                         * retries ending, so a recorded security failure is the cause. */
                        if (!xkafka_take_security_error(producer, error, error_size, error_code))
                                xkafka_set_error_at(error, error_size, error_code, rd_kafka_err2str(batch->slots[index].error), batch->slots[index].error);
                        return -1;
                }
        }
        return 0;
}

size_t xkafka_batch_count(const xkafka_batch_t *batch) {
        return batch == NULL ? 0 : batch->count;
}

int32_t xkafka_batch_partition_at(const xkafka_batch_t *batch, size_t index) {
        return batch->slots[index].partition;
}

int64_t xkafka_batch_offset_at(const xkafka_batch_t *batch, size_t index) {
        return batch->slots[index].offset;
}

int64_t xkafka_batch_timestamp_at(const xkafka_batch_t *batch, size_t index) {
        return batch->slots[index].timestamp;
}

/* Drains any outstanding reports before freeing, so librdkafka never writes
 * into slots that have already been released. */
void xkafka_batch_destroy(rd_kafka_t *producer, xkafka_batch_t *batch) {
        if (batch == NULL)
                return;

        while (batch->pending > 0)
                rd_kafka_poll(producer, 100);

        free(batch->slots);
        free(batch);
}

/* Registering a rebalance callback turns off librdkafka's own assignment, so this
 * has to reproduce it, including the split between the eager and cooperative
 * protocols. The generation counter lives in the client's opaque, so it belongs
 * to one consumer and is only ever touched while its permit is held. */
static void xkafka_rebalance_callback(rd_kafka_t *client,
                                      rd_kafka_resp_err_t err,
                                      rd_kafka_topic_partition_list_t *partitions,
                                      void *opaque) {
        xkafka_client_t *state = (xkafka_client_t *)opaque;
        int cooperative =
            strcmp(rd_kafka_rebalance_protocol(client), "COOPERATIVE") == 0;

        switch (err) {
        case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                if (cooperative)
                        rd_kafka_incremental_assign(client, partitions);
                else
                        rd_kafka_assign(client, partitions);
                break;
        case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                if (cooperative)
                        rd_kafka_incremental_unassign(client, partitions);
                else
                        rd_kafka_assign(client, NULL);
                break;
        default:
                /* Any other outcome needs the assignment cleared to resynchronise. */
                rd_kafka_assign(client, NULL);
                break;
        }

        if (state != NULL)
                state->generation++;
}

/* Bumped once per rebalance, so the consumer can notice one without comparing
 * assignments. Reading it costs nothing, so the poll loop can check every time. */
int xkafka_consumer_generation(rd_kafka_t *consumer) {
        xkafka_client_t *state = (xkafka_client_t *)rd_kafka_opaque(consumer);
        return state == NULL ? 0 : state->generation;
}

rd_kafka_t *xkafka_consumer_new(const char *brokers,
                                const char *client_id,
                                const char *group_id,
                                const char *auto_offset_reset,
                                const char *const *property_names,
                                const char *const *property_values,
                                size_t property_count,
                                char *error,
                                size_t error_size,
                                int32_t *error_code) {
        rd_kafka_conf_t *conf = rd_kafka_conf_new();
        rd_kafka_t *consumer;
        rd_kafka_resp_err_t result;
        xkafka_client_t *state = xkafka_client_new();

        if (state == NULL) {
                rd_kafka_conf_destroy(conf);
                xkafka_set_error(error, error_size, error_code,
                                 "could not allocate the client state");
                return NULL;
        }

        /* The opaque travels with the client, so the callbacks,
         * xkafka_consumer_generation and the calls that report a security
         * failure all reach the same state. */
        rd_kafka_conf_set_opaque(conf, state);
        rd_kafka_conf_set_error_cb(conf, xkafka_error_callback);
        rd_kafka_conf_set_rebalance_cb(conf, xkafka_rebalance_callback);

        if (xkafka_conf_set_all(conf, property_names, property_values,
                                property_count, error, error_size, error_code) != 0 ||
            xkafka_conf_set(conf, "bootstrap.servers", brokers, error,
                            error_size, error_code) != 0 ||
            xkafka_conf_set(conf, "group.id", group_id, error, error_size, error_code) !=
                0 ||
            xkafka_conf_set(conf, "auto.offset.reset", auto_offset_reset, error,
                            error_size, error_code) != 0 ||
            xkafka_conf_set(conf, "enable.auto.commit", "false", error,
                            error_size, error_code) != 0 ||
            xkafka_conf_set(conf, "enable.auto.offset.store", "false", error,
                            error_size, error_code) != 0) {
                rd_kafka_conf_destroy(conf);
                xkafka_client_free(state);
                return NULL;
        }
        if (client_id != NULL &&
            xkafka_conf_set(conf, "client.id", client_id, error, error_size, error_code) !=
                0) {
                rd_kafka_conf_destroy(conf);
                xkafka_client_free(state);
                return NULL;
        }

        consumer =
            rd_kafka_new(RD_KAFKA_CONSUMER, conf, error, error_size);
        if (consumer == NULL) {
                /* rd_kafka_new owns conf only on success, and the opaque with it. */
                rd_kafka_conf_destroy(conf);
                xkafka_client_free(state);
                return NULL;
        }

        result = rd_kafka_poll_set_consumer(consumer);
        if (result != RD_KAFKA_RESP_ERR_NO_ERROR) {
                xkafka_set_error_at(error, error_size, error_code, rd_kafka_err2str(result), result);
                rd_kafka_destroy(consumer);
                xkafka_client_free(state);
                return NULL;
        }
        return consumer;
}

void xkafka_consumer_destroy(rd_kafka_t *consumer) {
        xkafka_client_t *state;

        if (consumer == NULL)
                return;

        /* Read before destroying, since the opaque is unreachable afterwards. */
        state = (xkafka_client_t *)rd_kafka_opaque(consumer);
        rd_kafka_consumer_close(consumer);
        rd_kafka_destroy(consumer);
        xkafka_client_free(state);
}

rd_kafka_topic_partition_list_t *xkafka_subscription_new(size_t count) {
        return rd_kafka_topic_partition_list_new((int)count);
}

void xkafka_subscription_destroy(
    rd_kafka_topic_partition_list_t *subscription) {
        if (subscription != NULL)
                rd_kafka_topic_partition_list_destroy(subscription);
}

void xkafka_subscription_add(rd_kafka_topic_partition_list_t *subscription,
                             const char *topic) {
        rd_kafka_topic_partition_list_add(subscription, topic,
                                          RD_KAFKA_PARTITION_UA);
}

int xkafka_consumer_subscribe(
    rd_kafka_t *consumer,
    const rd_kafka_topic_partition_list_t *subscription,
    char *error,
    size_t error_size,
    int32_t *error_code) {
        rd_kafka_resp_err_t result = rd_kafka_subscribe(consumer, subscription);
        if (result != RD_KAFKA_RESP_ERR_NO_ERROR) {
                xkafka_set_error_at(error, error_size, error_code, rd_kafka_err2str(result), result);
                return -1;
        }
        return 0;
}

rd_kafka_message_t *xkafka_consumer_poll(rd_kafka_t *consumer,
                                         int timeout_ms,
                                         int *status,
                                         char *error,
                                         size_t error_size,
                                         int32_t *error_code) {
        rd_kafka_message_t *message =
            rd_kafka_consumer_poll(consumer, timeout_ms);
        if (message == NULL) {
                *status = 0;
                return NULL;
        }
        if (message->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                rd_kafka_message_destroy(message);
                *status = 0;
                return NULL;
        }
        if (message->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                if (!xkafka_take_security_error(consumer, error, error_size, error_code))
                        xkafka_set_error_at(error, error_size, error_code,
                                            rd_kafka_message_errstr(message), message->err);
                rd_kafka_message_destroy(message);
                *status = -1;
                return NULL;
        }
        *status = 1;
        return message;
}

void xkafka_message_destroy(rd_kafka_message_t *message) {
        if (message != NULL)
                rd_kafka_message_destroy(message);
}

const char *xkafka_message_topic(const rd_kafka_message_t *message) {
        return rd_kafka_topic_name(message->rkt);
}

int32_t xkafka_message_partition(const rd_kafka_message_t *message) {
        return message->partition;
}

int64_t xkafka_message_offset(const rd_kafka_message_t *message) {
        return message->offset;
}

int64_t xkafka_message_timestamp(const rd_kafka_message_t *message) {
        return rd_kafka_message_timestamp(message, NULL);
}

const void *xkafka_message_key(const rd_kafka_message_t *message) {
        return message->key;
}

size_t xkafka_message_key_size(const rd_kafka_message_t *message) {
        return message->key_len;
}

const void *xkafka_message_value(const rd_kafka_message_t *message) {
        return message->payload;
}

size_t xkafka_message_value_size(const rd_kafka_message_t *message) {
        return message->len;
}

size_t xkafka_message_header_count(const rd_kafka_message_t *message) {
        rd_kafka_headers_t *headers = NULL;
        if (rd_kafka_message_headers(message, &headers) !=
                RD_KAFKA_RESP_ERR_NO_ERROR ||
            headers == NULL)
                return 0;
        return rd_kafka_header_cnt(headers);
}

int xkafka_message_header_at(const rd_kafka_message_t *message,
                             size_t index,
                             const char **name,
                             const void **value,
                             size_t *value_size,
                             int *has_value) {
        rd_kafka_headers_t *headers = NULL;
        rd_kafka_resp_err_t result =
            rd_kafka_message_headers(message, &headers);
        if (result != RD_KAFKA_RESP_ERR_NO_ERROR)
                return -1;

        result = rd_kafka_header_get_all(headers, index, name, value,
                                         value_size);
        if (result != RD_KAFKA_RESP_ERR_NO_ERROR)
                return -1;
        *has_value = *value != NULL;
        return 0;
}

static rd_kafka_topic_partition_list_t *
xkafka_topic_partition_list(const char *const *topics,
                            const int32_t *partitions,
                            const int64_t *offset_values,
                            size_t count) {
        rd_kafka_topic_partition_list_t *native_offsets =
            rd_kafka_topic_partition_list_new((int)count);
        size_t index;

        for (index = 0; index < count; index++) {
                rd_kafka_topic_partition_t *entry =
                    rd_kafka_topic_partition_list_add(native_offsets,
                                                      topics[index],
                                                      partitions[index]);
                if (offset_values != NULL)
                        entry->offset = offset_values[index];
        }

        return native_offsets;
}

void *xkafka_consumer_assignment(rd_kafka_t *consumer,
                                 char *error,
                                 size_t error_size,
                                 int32_t *error_code) {
        rd_kafka_topic_partition_list_t *assignment = NULL;
        rd_kafka_resp_err_t result = rd_kafka_assignment(consumer, &assignment);

        if (result != RD_KAFKA_RESP_ERR_NO_ERROR) {
                xkafka_set_error_at(error, error_size, error_code, rd_kafka_err2str(result), result);
                return NULL;
        }
        return assignment;
}

void xkafka_assignment_destroy(void *assignment) {
        rd_kafka_topic_partition_list_destroy(assignment);
}

size_t xkafka_assignment_count(const void *assignment) {
        return (size_t)((const rd_kafka_topic_partition_list_t *)assignment)->cnt;
}

const char *xkafka_assignment_topic_at(const void *assignment, size_t index) {
        return ((const rd_kafka_topic_partition_list_t *)assignment)->elems[index].topic;
}

int32_t xkafka_assignment_partition_at(const void *assignment, size_t index) {
        return ((const rd_kafka_topic_partition_list_t *)assignment)->elems[index].partition;
}

int xkafka_consumer_committed(rd_kafka_t *consumer,
                              const char *const *topics,
                              const int32_t *partitions,
                              size_t count,
                              int64_t *offset_values,
                          int timeout_ms,
                              char *error,
                              size_t error_size,
                              int32_t *error_code) {
        rd_kafka_topic_partition_list_t *native_offsets =
            xkafka_topic_partition_list(topics, partitions, NULL, count);
        rd_kafka_resp_err_t result =
            rd_kafka_committed(consumer, native_offsets, timeout_ms);
        size_t index;

        if (result != RD_KAFKA_RESP_ERR_NO_ERROR) {
                xkafka_set_error_at(error, error_size, error_code, rd_kafka_err2str(result), result);
                rd_kafka_topic_partition_list_destroy(native_offsets);
                return -1;
        }
        for (index = 0; index < count; index++) {
                rd_kafka_topic_partition_t *entry = &native_offsets->elems[index];
                if (entry->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                        xkafka_set_error(error, error_size, error_code,
                                         rd_kafka_err2str(entry->err));
                        rd_kafka_topic_partition_list_destroy(native_offsets);
                        return -1;
                }
                offset_values[index] = entry->offset >= 0 ? entry->offset : -1;
        }
        rd_kafka_topic_partition_list_destroy(native_offsets);
        return 0;
}

int xkafka_consumer_watermark_offsets(rd_kafka_t *consumer,
                                      const char *topic,
                                      int32_t partition,
                                      int64_t *low,
                                      int64_t *high,
                                  int timeout_ms,
                                      char *error,
                                      size_t error_size,
                                      int32_t *error_code) {
        rd_kafka_resp_err_t result = rd_kafka_query_watermark_offsets(
            consumer, topic, partition, low, high, timeout_ms);

        if (result != RD_KAFKA_RESP_ERR_NO_ERROR) {
                xkafka_set_error_at(error, error_size, error_code, rd_kafka_err2str(result), result);
                return -1;
        }
        return 0;
}

int xkafka_consumer_offsets_for_times(rd_kafka_t *consumer,
                                      const char *const *topics,
                                      const int32_t *partitions,
                                      const int64_t *timestamps,
                                      size_t count,
                                      int64_t *offset_values,
                                  int timeout_ms,
                                      char *error,
                                      size_t error_size,
                                      int32_t *error_code) {
        rd_kafka_topic_partition_list_t *native_offsets =
            xkafka_topic_partition_list(topics, partitions, timestamps, count);
        rd_kafka_resp_err_t result =
            rd_kafka_offsets_for_times(consumer, native_offsets, timeout_ms);
        size_t index;

        if (result != RD_KAFKA_RESP_ERR_NO_ERROR) {
                xkafka_set_error_at(error, error_size, error_code, rd_kafka_err2str(result), result);
                rd_kafka_topic_partition_list_destroy(native_offsets);
                return -1;
        }
        for (index = 0; index < count; index++) {
                rd_kafka_topic_partition_t *entry = &native_offsets->elems[index];
                if (entry->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                        xkafka_set_error(error, error_size, error_code,
                                         rd_kafka_err2str(entry->err));
                        rd_kafka_topic_partition_list_destroy(native_offsets);
                        return -1;
                }
                offset_values[index] = entry->offset >= 0 ? entry->offset : -1;
        }
        rd_kafka_topic_partition_list_destroy(native_offsets);
        return 0;
}

void *xkafka_consumer_metadata(rd_kafka_t *consumer,
                               const char *topic,
                               int timeout_ms,
                               char *error,
                               size_t error_size,
                               int32_t *error_code) {
        rd_kafka_topic_t *native_topic = NULL;
        const struct rd_kafka_metadata *metadata = NULL;
        rd_kafka_resp_err_t result;
        int topic_index;

        if (topic != NULL) {
                native_topic = rd_kafka_topic_new(consumer, topic, NULL);
                if (native_topic == NULL) {
                        xkafka_set_error(error, error_size, error_code,
                                         rd_kafka_err2str(rd_kafka_last_error()));
                        return NULL;
                }
        }

        result = rd_kafka_metadata(consumer, topic == NULL, native_topic,
                                   &metadata, timeout_ms);
        if (native_topic != NULL)
                rd_kafka_topic_destroy(native_topic);
        if (result != RD_KAFKA_RESP_ERR_NO_ERROR) {
                xkafka_set_error_at(error, error_size, error_code, rd_kafka_err2str(result), result);
                return NULL;
        }

        for (topic_index = 0; topic_index < metadata->topic_cnt; topic_index++) {
                const rd_kafka_metadata_topic_t *topic_metadata =
                    &metadata->topics[topic_index];
                int partition_index;

                if (topic_metadata->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                        xkafka_set_error(error, error_size, error_code,
                                         rd_kafka_err2str(topic_metadata->err));
                        rd_kafka_metadata_destroy(metadata);
                        return NULL;
                }
                for (partition_index = 0;
                     partition_index < topic_metadata->partition_cnt;
                     partition_index++) {
                        rd_kafka_resp_err_t partition_error =
                            topic_metadata->partitions[partition_index].err;
                        if (partition_error != RD_KAFKA_RESP_ERR_NO_ERROR) {
                                xkafka_set_error(error, error_size, error_code,
                                                 rd_kafka_err2str(partition_error));
                                rd_kafka_metadata_destroy(metadata);
                                return NULL;
                        }
                }
        }
        return (void *)metadata;
}

void xkafka_metadata_destroy(void *metadata) {
        if (metadata != NULL)
                rd_kafka_metadata_destroy(
                    (const struct rd_kafka_metadata *)metadata);
}

size_t xkafka_metadata_topic_count(const void *metadata) {
        return (size_t)((const struct rd_kafka_metadata *)metadata)->topic_cnt;
}

const char *xkafka_metadata_topic_at(const void *metadata, size_t topic_index) {
        return ((const struct rd_kafka_metadata *)metadata)
            ->topics[topic_index]
            .topic;
}

size_t xkafka_metadata_partition_count_at(const void *metadata,
                                          size_t topic_index) {
        return (size_t)((const struct rd_kafka_metadata *)metadata)
            ->topics[topic_index]
            .partition_cnt;
}

int32_t xkafka_metadata_partition_at(const void *metadata,
                                     size_t topic_index,
                                     size_t partition_index) {
        return ((const struct rd_kafka_metadata *)metadata)
            ->topics[topic_index]
            .partitions[partition_index]
            .id;
}

/* Stops and restarts fetching for the listed partitions. Kafka keeps each paused partition's
 * position, so resuming continues from the record after the last one handed to the application. */
static int xkafka_consumer_pause_resume(rd_kafka_t *consumer,
                                        const char *const *topics,
                                        const int32_t *partitions,
                                        size_t count,
                                        int pause,
                                        char *error,
                                        size_t error_size,
                                        int32_t *error_code) {
        rd_kafka_topic_partition_list_t *list;
        rd_kafka_resp_err_t result;
        size_t index;

        list = rd_kafka_topic_partition_list_new((int)count);
        for (index = 0; index < count; index++)
                rd_kafka_topic_partition_list_add(list, topics[index], partitions[index]);

        result = pause ? rd_kafka_pause_partitions(consumer, list)
                       : rd_kafka_resume_partitions(consumer, list);
        rd_kafka_topic_partition_list_destroy(list);

        if (result != RD_KAFKA_RESP_ERR_NO_ERROR) {
                xkafka_set_error_at(error, error_size, error_code, rd_kafka_err2str(result), result);
                return -1;
        }
        return 0;
}

int xkafka_consumer_pause(rd_kafka_t *consumer,
                          const char *const *topics,
                          const int32_t *partitions,
                          size_t count,
                          char *error,
                          size_t error_size,
                          int32_t *error_code) {
        return xkafka_consumer_pause_resume(consumer, topics, partitions, count, 1, error, error_size, error_code);
}

int xkafka_consumer_resume(rd_kafka_t *consumer,
                           const char *const *topics,
                           const int32_t *partitions,
                           size_t count,
                           char *error,
                           size_t error_size,
                           int32_t *error_code) {
        return xkafka_consumer_pause_resume(consumer, topics, partitions, count, 0, error, error_size, error_code);
}

int xkafka_consumer_seek(rd_kafka_t *consumer,
                         const char *topic,
                         int32_t partition,
                         int64_t offset,
                     int timeout_ms,
                         char *error,
                         size_t error_size,
                         int32_t *error_code) {
        const char *topics[] = {topic};
        const int32_t partitions[] = {partition};
        const int64_t offsets[] = {offset};
        rd_kafka_topic_partition_list_t *native_offsets =
            xkafka_topic_partition_list(topics, partitions, offsets, 1);
        rd_kafka_error_t *result =
            rd_kafka_seek_partitions(consumer, native_offsets, timeout_ms);

        if (result != NULL) {
                xkafka_set_error(error, error_size, error_code,
                                 rd_kafka_error_string(result));
                rd_kafka_error_destroy(result);
                rd_kafka_topic_partition_list_destroy(native_offsets);
                return -1;
        }
        if (native_offsets->elems[0].err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                xkafka_set_error(error, error_size, error_code,
                                 rd_kafka_err2str(native_offsets->elems[0].err));
                rd_kafka_topic_partition_list_destroy(native_offsets);
                return -1;
        }
        rd_kafka_topic_partition_list_destroy(native_offsets);
        return 0;
}

int xkafka_consumer_commit(rd_kafka_t *consumer,
                           const char *const *topics,
                           const int32_t *partitions,
                           const int64_t *offset_values,
                           size_t count,
                           char *error,
                           size_t error_size,
                           int32_t *error_code) {
        rd_kafka_topic_partition_list_t *native_offsets =
            xkafka_topic_partition_list(topics, partitions, offset_values,
                                        count);
        rd_kafka_resp_err_t result;

        result = rd_kafka_commit(consumer, native_offsets, 0);
        rd_kafka_topic_partition_list_destroy(native_offsets);

        if (result != RD_KAFKA_RESP_ERR_NO_ERROR) {
                xkafka_set_error_at(error, error_size, error_code, rd_kafka_err2str(result), result);
                return -1;
        }
        return 0;
}

#endif
