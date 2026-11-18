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

package xkafka.benchmarks

import java.time.Duration
import java.util.Properties
import java.util.concurrent.atomic.AtomicLong

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

import cats.effect.IO
import cats.syntax.all.*
import org.apache.kafka.clients.admin.{Admin, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{Deserializer, LongDeserializer, Serdes, StringDeserializer, StringSerializer}
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig}
import org.apache.kafka.streams.kstream.{Consumed, Grouped, Materialized, Produced}
import org.apache.kafka.streams.state.Stores

/** The same measurements with the Java client and Java's Kafka Streams used directly, which xkafka drives on the JVM. Kafka Streams keeps its
  * store in memory and its record cache off, so it writes each update of a count as xkafka's in-memory store does.
  */
private[benchmarks] object Baselines:
  def run(run: Run): IO[Unit] =
    IO.blocking(measure(run)).flatMap(_.traverse_((client, name, timing) => timing.report(client, name, run)))

  private def measure(run: Run): List[(String, String, Timing)] =
    val input  = s"kafka-bench-input-${run.suffix}"
    val plain  = s"kafka-bench-stateless-${run.suffix}"
    val counts = s"kafka-bench-counts-${run.suffix}"
    val admin  = Admin.create(properties(run, Map.empty))
    try admin.createTopics(List(input, plain, counts).map(new NewTopic(_, run.partitions, 1.toShort)).asJava).all().get()
    finally admin.close()
    List(
      ("kafka-clients", "produce", produce(run, input)),
      ("kafka-clients", "consume", readUntil(run, input, s"kafka-bench-consume-${run.suffix}", new StringDeserializer)(0L)((n, _, _) => n + 1)(_ >= run.records)),
      ("kafka-streams", "streams without a store", stateless(run, input, plain)),
      ("kafka-streams", "streams counting in a store", stateful(run, input, counts))
    )

  private def produce(run: Run, topic: String): Timing =
    val producer = new KafkaProducer[String, String](properties(run, Map.empty), new StringSerializer, new StringSerializer)
    try
      val start  = now()
      val failed = new AtomicLong
      (0 until run.records).foreach(n =>
        producer.send(new ProducerRecord(topic, s"key-${n % run.keys}", run.value), (_, error) => if error != null then failed.incrementAndGet(): Unit)
      )
      producer.flush()
      if failed.get > 0 then throw new IllegalStateException(s"${failed.get} records were not delivered")
      val end = now()
      Timing(start, start, end)
    finally producer.close()

  private def stateless(run: Run, from: String, to: String): Timing =
    val builder = new StreamsBuilder
    builder.stream(from, Consumed.`with`(Serdes.String(), Serdes.String())).filter((_, value) => value.nonEmpty).mapValues(value => value.take(10))
      .to(to, Produced.`with`(Serdes.String(), Serdes.String()))
    streamed(run, builder, s"kafka-bench-stateless-${run.suffix}"):
      readUntil(run, to, s"kafka-bench-stateless-read-${run.suffix}", new StringDeserializer)(0L)((n, _, _) => n + 1)(_ >= run.records)

  private def stateful(run: Run, from: String, to: String): Timing =
    val builder = new StreamsBuilder
    builder.stream(from, Consumed.`with`(Serdes.String(), Serdes.String())).groupByKey(Grouped.`with`(Serdes.String(), Serdes.String()))
      .count(Materialized.as[String, java.lang.Long](Stores.inMemoryKeyValueStore("counts")).withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()))
      .toStream.to(to, Produced.`with`(Serdes.String(), Serdes.Long()))
    streamed(run, builder, s"kafka-bench-stateful-${run.suffix}"):
      readUntil(run, to, s"kafka-bench-stateful-read-${run.suffix}", new LongDeserializer)(0)((finished, key, value) =>
        if run.perKey.get(key).contains(value.longValue) then finished + 1 else finished
      )(_ >= run.perKey.size)

  /** Runs the program while `read` waits for its output. */
  private def streamed(run: Run, builder: StreamsBuilder, application: String)(read: => Timing): Timing =
    val directory = java.nio.file.Files.createTempDirectory("kafka-bench")
    val streams   =
      new KafkaStreams(
        builder.build(),
        properties(
          run,
          Map(
            StreamsConfig.APPLICATION_ID_CONFIG           -> application,
            StreamsConfig.COMMIT_INTERVAL_MS_CONFIG       -> run.commitEvery.toMillis.toString,
            StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG -> "0",
            StreamsConfig.STATE_DIR_CONFIG                -> directory.toString
          )
        )
      )
    streams.start()
    try read
    finally streams.close(Duration.ofSeconds(30)): Unit

  /** Reads `from` with a new group until `done` holds of what `step` has gathered from the records so far. */
  private def readUntil[V, A](run: Run, from: String, group: String, values: Deserializer[V])(initial: A)(step: (A, String, V) => A)(
      done: A => Boolean
  ): Timing =
    val consumer =
      new KafkaConsumer[String, V](
        properties(run, Map(ConsumerConfig.GROUP_ID_CONFIG -> group, ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest")),
        new StringDeserializer,
        values
      )
    @scala.annotation.tailrec
    def poll(first: Option[FiniteDuration], gathered: A): (Option[FiniteDuration], A) =
      if done(gathered) then (first, gathered)
      else
        val polled = consumer.poll(Duration.ofMillis(100)).asScala
        poll(first.orElse(Option.when(polled.nonEmpty)(now())), polled.foldLeft(gathered)((sofar, record) => step(sofar, record.key, record.value)))
    try
      consumer.subscribe(List(from).asJava)
      val start = now()
      Timing(start, poll(None, initial)._1.getOrElse(start), now())
    finally consumer.close()

  private def properties(run: Run, more: Map[String, String]): Properties =
    val all = new Properties
    all.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, run.bootstrap)
    more.foreach((name, value) => all.put(name, value))
    all

  private def now(): FiniteDuration = System.nanoTime().nanos
