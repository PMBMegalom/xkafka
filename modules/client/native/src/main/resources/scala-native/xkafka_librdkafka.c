#ifdef SCALANATIVE_LINK_RDKAFKA

#include <librdkafka/rdkafka.h>

#include <stdint.h>
#include <stdio.h>
#include <string.h>

typedef struct xkafka_delivery_s {
        int complete;
        rd_kafka_resp_err_t error;
        int32_t partition;
        int64_t offset;
        int64_t timestamp;
} xkafka_delivery_t;

const char *xkafka_version_str(void) { return rd_kafka_version_str(); }

static void xkafka_set_error(char *error, size_t error_size, const char *value) {
        if (error != NULL && error_size > 0)
                snprintf(error, error_size, "%s", value);
}

static int xkafka_conf_set(rd_kafka_conf_t *conf,
                           const char *name,
                           const char *value,
                           char *error,
                           size_t error_size) {
        if (rd_kafka_conf_set(conf, name, value, error, error_size) !=
            RD_KAFKA_CONF_OK)
                return -1;
        return 0;
}

static int xkafka_conf_set_all(rd_kafka_conf_t *conf,
                               const char *const *names,
                               const char *const *values,
                               size_t count,
                               char *error,
                               size_t error_size) {
        size_t index;

        for (index = 0; index < count; index++) {
                if (xkafka_conf_set(conf, names[index], values[index], error,
                                    error_size) != 0)
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
}

rd_kafka_t *xkafka_producer_new(const char *brokers,
                                const char *client_id,
                                const char *const *property_names,
                                const char *const *property_values,
                                size_t property_count,
                                char *error,
                                size_t error_size) {
        rd_kafka_conf_t *conf = rd_kafka_conf_new();
        rd_kafka_t *producer;

        if (xkafka_conf_set_all(conf, property_names, property_values,
                                property_count, error, error_size) != 0 ||
            xkafka_conf_set(conf, "bootstrap.servers", brokers, error,
                            error_size) != 0) {
                rd_kafka_conf_destroy(conf);
                return NULL;
        }
        if (client_id != NULL &&
            xkafka_conf_set(conf, "client.id", client_id, error, error_size) !=
                0) {
                rd_kafka_conf_destroy(conf);
                return NULL;
        }

        rd_kafka_conf_set_dr_msg_cb(conf, xkafka_delivery_callback);
        producer =
            rd_kafka_new(RD_KAFKA_PRODUCER, conf, error, error_size);
        return producer;
}

void xkafka_producer_destroy(rd_kafka_t *producer) {
        if (producer == NULL)
                return;
        rd_kafka_flush(producer, 10000);
        rd_kafka_destroy(producer);
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
                       size_t error_size) {
        rd_kafka_resp_err_t result =
            rd_kafka_header_add(headers, name, -1, value, value_size);
        if (result != RD_KAFKA_RESP_ERR_NO_ERROR) {
                xkafka_set_error(error, error_size, rd_kafka_err2str(result));
                return -1;
        }
        return 0;
}

int xkafka_producer_send(rd_kafka_t *producer,
                         const char *topic,
                         int32_t partition,
                         int64_t timestamp,
                         const void *key,
                         size_t key_size,
                         const void *value,
                         size_t value_size,
                         rd_kafka_headers_t *headers,
                         int32_t *result_partition,
                         int64_t *result_offset,
                         int64_t *result_timestamp,
                         char *error,
                         size_t error_size) {
        xkafka_delivery_t delivery = {0, RD_KAFKA_RESP_ERR_NO_ERROR, -1, -1,
                                      -1};
        rd_kafka_resp_err_t result;

        if (timestamp >= 0) {
                result = rd_kafka_producev(
                    producer, RD_KAFKA_V_TOPIC(topic),
                    RD_KAFKA_V_PARTITION(partition),
                    RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                    RD_KAFKA_V_TIMESTAMP(timestamp),
                    RD_KAFKA_V_KEY((void *)key, key_size),
                    RD_KAFKA_V_VALUE((void *)value, value_size),
                    RD_KAFKA_V_HEADERS(headers), RD_KAFKA_V_OPAQUE(&delivery),
                    RD_KAFKA_V_END);
        } else {
                result = rd_kafka_producev(
                    producer, RD_KAFKA_V_TOPIC(topic),
                    RD_KAFKA_V_PARTITION(partition),
                    RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                    RD_KAFKA_V_KEY((void *)key, key_size),
                    RD_KAFKA_V_VALUE((void *)value, value_size),
                    RD_KAFKA_V_HEADERS(headers), RD_KAFKA_V_OPAQUE(&delivery),
                    RD_KAFKA_V_END);
        }

        if (result != RD_KAFKA_RESP_ERR_NO_ERROR) {
                rd_kafka_headers_destroy(headers);
                xkafka_set_error(error, error_size, rd_kafka_err2str(result));
                return -1;
        }

        while (!delivery.complete)
                rd_kafka_poll(producer, 100);

        if (delivery.error != RD_KAFKA_RESP_ERR_NO_ERROR) {
                xkafka_set_error(error, error_size,
                                 rd_kafka_err2str(delivery.error));
                return -1;
        }

        *result_partition = delivery.partition;
        *result_offset = delivery.offset;
        *result_timestamp = delivery.timestamp;
        return 0;
}

rd_kafka_t *xkafka_consumer_new(const char *brokers,
                                const char *client_id,
                                const char *group_id,
                                const char *auto_offset_reset,
                                const char *const *property_names,
                                const char *const *property_values,
                                size_t property_count,
                                char *error,
                                size_t error_size) {
        rd_kafka_conf_t *conf = rd_kafka_conf_new();
        rd_kafka_t *consumer;
        rd_kafka_resp_err_t result;

        if (xkafka_conf_set_all(conf, property_names, property_values,
                                property_count, error, error_size) != 0 ||
            xkafka_conf_set(conf, "bootstrap.servers", brokers, error,
                            error_size) != 0 ||
            xkafka_conf_set(conf, "group.id", group_id, error, error_size) !=
                0 ||
            xkafka_conf_set(conf, "auto.offset.reset", auto_offset_reset, error,
                            error_size) != 0 ||
            xkafka_conf_set(conf, "enable.auto.commit", "false", error,
                            error_size) != 0 ||
            xkafka_conf_set(conf, "enable.auto.offset.store", "false", error,
                            error_size) != 0) {
                rd_kafka_conf_destroy(conf);
                return NULL;
        }
        if (client_id != NULL &&
            xkafka_conf_set(conf, "client.id", client_id, error, error_size) !=
                0) {
                rd_kafka_conf_destroy(conf);
                return NULL;
        }

        consumer =
            rd_kafka_new(RD_KAFKA_CONSUMER, conf, error, error_size);
        if (consumer == NULL)
                return NULL;

        result = rd_kafka_poll_set_consumer(consumer);
        if (result != RD_KAFKA_RESP_ERR_NO_ERROR) {
                xkafka_set_error(error, error_size, rd_kafka_err2str(result));
                rd_kafka_destroy(consumer);
                return NULL;
        }
        return consumer;
}

void xkafka_consumer_destroy(rd_kafka_t *consumer) {
        if (consumer == NULL)
                return;
        rd_kafka_consumer_close(consumer);
        rd_kafka_destroy(consumer);
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
    size_t error_size) {
        rd_kafka_resp_err_t result = rd_kafka_subscribe(consumer, subscription);
        if (result != RD_KAFKA_RESP_ERR_NO_ERROR) {
                xkafka_set_error(error, error_size, rd_kafka_err2str(result));
                return -1;
        }
        return 0;
}

rd_kafka_message_t *xkafka_consumer_poll(rd_kafka_t *consumer,
                                         int timeout_ms,
                                         int *status,
                                         char *error,
                                         size_t error_size) {
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
                xkafka_set_error(error, error_size,
                                 rd_kafka_message_errstr(message));
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

int xkafka_consumer_commit(rd_kafka_t *consumer,
                           const char *const *topics,
                           const int32_t *partitions,
                           const int64_t *offset_values,
                           size_t count,
                           char *error,
                           size_t error_size) {
        rd_kafka_topic_partition_list_t *native_offsets =
            rd_kafka_topic_partition_list_new((int)count);
        rd_kafka_resp_err_t result;
        size_t index;

        for (index = 0; index < count; index++) {
                rd_kafka_topic_partition_t *entry =
                    rd_kafka_topic_partition_list_add(native_offsets,
                                                      topics[index],
                                                      partitions[index]);
                entry->offset = offset_values[index];
        }
        result = rd_kafka_commit(consumer, native_offsets, 0);
        rd_kafka_topic_partition_list_destroy(native_offsets);

        if (result != RD_KAFKA_RESP_ERR_NO_ERROR) {
                xkafka_set_error(error, error_size, rd_kafka_err2str(result));
                return -1;
        }
        return 0;
}

#endif
