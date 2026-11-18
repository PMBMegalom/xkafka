/*
 * Copyright (c) 2026 xkafka contributors
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

/* The produce and consume measurements of the benchmark with librdkafka used directly, which xkafka drives on Scala.js and Scala Native, so
 * their figures have something to be compared with. It reads the benchmark's environment and prints its lines. */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <librdkafka/rdkafka.h>

static int records, value_bytes, keys, partitions;
static const char *bootstrap;
static long delivered, failed;

static double now(void) {
        struct timespec at;
        clock_gettime(CLOCK_MONOTONIC, &at);
        return at.tv_sec + at.tv_nsec / 1e9;
}

static int setting(const char *name, int otherwise) {
        const char *value = getenv(name);
        return value != NULL ? atoi(value) : otherwise;
}

static void report(const char *name, double start, double first, double end) {
        double total = end - start, active = end - first;
        double rate = records / (active > 0.001 ? active : 0.001);
        printf("%-13s %-28s %8d records  total %7.2f s  from first %7.2f s  %10.0f records/s  %7.2f MiB/s\n", "librdkafka", name, records,
               total, active, rate, rate * value_bytes / (1024 * 1024));
        fflush(stdout);
}

static rd_kafka_conf_t *configuration(void) {
        char error[512];
        rd_kafka_conf_t *conf = rd_kafka_conf_new();
        rd_kafka_conf_set(conf, "bootstrap.servers", bootstrap, error, sizeof error);
        return conf;
}

static void on_delivery(rd_kafka_t *client, const rd_kafka_message_t *message, void *opaque) {
        (void)client;
        (void)opaque;
        if (message->err)
                failed++;
        else
                delivered++;
}

static void create(rd_kafka_t *client, const char *topic) {
        char error[512];
        rd_kafka_NewTopic_t *made = rd_kafka_NewTopic_new(topic, partitions, 1, error, sizeof error);
        rd_kafka_queue_t *queue = rd_kafka_queue_new(client);
        rd_kafka_CreateTopics(client, &made, 1, NULL, queue);
        rd_kafka_event_t *answer = rd_kafka_queue_poll(queue, 30000);
        if (answer == NULL || rd_kafka_event_error(answer)) {
                fprintf(stderr, "could not create %s\n", topic);
                exit(1);
        }
        rd_kafka_event_destroy(answer);
        rd_kafka_queue_destroy(queue);
        rd_kafka_NewTopic_destroy(made);
}

static void produce(const char *topic) {
        char error[512], key[32];
        char *value = malloc(value_bytes);
        rd_kafka_conf_t *conf = configuration();
        memset(value, 'x', value_bytes);
        rd_kafka_conf_set(conf, "enable.idempotence", "true", error, sizeof error);
        rd_kafka_conf_set_dr_msg_cb(conf, on_delivery);
        rd_kafka_t *producer = rd_kafka_new(RD_KAFKA_PRODUCER, conf, error, sizeof error);
        create(producer, topic);
        double start = now();
        for (int n = 0; n < records; n++) {
                snprintf(key, sizeof key, "key-%d", n % keys);
                while (rd_kafka_producev(producer, RD_KAFKA_V_TOPIC(topic), RD_KAFKA_V_KEY(key, strlen(key)), RD_KAFKA_V_VALUE(value, value_bytes),
                                         RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY), RD_KAFKA_V_END) == RD_KAFKA_RESP_ERR__QUEUE_FULL)
                        rd_kafka_poll(producer, 10);
                rd_kafka_poll(producer, 0);
        }
        rd_kafka_flush(producer, 60000);
        if (failed > 0 || delivered != records) {
                fprintf(stderr, "%ld records were not delivered\n", records - delivered);
                exit(1);
        }
        report("produce", start, start, now());
        rd_kafka_destroy(producer);
        free(value);
}

static void consume(const char *topic, const char *group) {
        char error[512];
        rd_kafka_conf_t *conf = configuration();
        rd_kafka_conf_set(conf, "group.id", group, error, sizeof error);
        rd_kafka_conf_set(conf, "auto.offset.reset", "earliest", error, sizeof error);
        rd_kafka_t *consumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf, error, sizeof error);
        rd_kafka_poll_set_consumer(consumer);
        rd_kafka_topic_partition_list_t *topics = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(topics, topic, RD_KAFKA_PARTITION_UA);
        rd_kafka_subscribe(consumer, topics);
        double start = now(), first = 0;
        long read = 0;
        while (read < records) {
                rd_kafka_message_t *message = rd_kafka_consumer_poll(consumer, 100);
                if (message == NULL)
                        continue;
                if (!message->err) {
                        if (read == 0)
                                first = now();
                        read++;
                }
                rd_kafka_message_destroy(message);
        }
        report("consume", start, first, now());
        rd_kafka_topic_partition_list_destroy(topics);
        rd_kafka_consumer_close(consumer);
        rd_kafka_destroy(consumer);
}

int main(void) {
        char topic[128], group[128];
        long suffix = (long)time(NULL);
        bootstrap = getenv("XKAFKA_INTEGRATION_BOOTSTRAP_SERVERS") != NULL ? getenv("XKAFKA_INTEGRATION_BOOTSTRAP_SERVERS") : "127.0.0.1:19092";
        records = setting("XKAFKA_BENCH_RECORDS", 200000);
        value_bytes = setting("XKAFKA_BENCH_VALUE_BYTES", 100);
        keys = setting("XKAFKA_BENCH_KEYS", 1000);
        partitions = setting("XKAFKA_BENCH_PARTITIONS", 6);
        snprintf(topic, sizeof topic, "librdkafka-bench-input-%ld", suffix);
        snprintf(group, sizeof group, "librdkafka-bench-consume-%ld", suffix);
        printf("librdkafka %s: %d records of %d bytes, %d keys, %d partitions, broker %s\n", rd_kafka_version_str(), records, value_bytes, keys,
               partitions, bootstrap);
        produce(topic);
        consume(topic, group);
        return 0;
}
