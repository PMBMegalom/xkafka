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

package xkafka.fixture

import java.nio.file.Files
import java.util.Properties

import scala.jdk.CollectionConverters.*
import scala.util.Try

import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewTopic}
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig}

/** A Kafka Streams application for the streams group tests to join, one instance on each broker `XKAFKA_STREAMS_BROKERS` lists (as
  * `version=host:port`, comma separated) for each backend named in the arguments. Each copies `xkafka-streams-<backend>-input`, which it makes with
  * four partitions, to `xkafka-streams-<backend>-output`, as the group `xkafka-streams-<backend>`, until the process is stopped.
  */
object StreamsFixture:
  val Partitions = 4

  def main(backends: Array[String]): Unit =
    val brokers   = sys.env.getOrElse("XKAFKA_STREAMS_BROKERS", sys.error("XKAFKA_STREAMS_BROKERS is not set")).split(',').toList
    val instances =
      for
        broker  <- brokers.map(_.split('=')(1))
        backend <- backends.toList
      yield start(broker, s"xkafka-streams-$backend")
    sys.addShutdownHook(instances.foreach(_.close()))
    println(s"streams fixture: ${instances.size} instances started")
    Thread.currentThread.join()

  private def start(bootstrap: String, application: String): KafkaStreams =
    val admin = Admin.create(Map[String, AnyRef](AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrap).asJava)
    try
      val topics = List(NewTopic(s"$application-input", Partitions, 1.toShort), NewTopic(s"$application-output", Partitions, 1.toShort))
      // A topic left by an earlier run is kept.
      Try(admin.createTopics(topics.asJava).all.get)
    finally admin.close()
    val builder = StreamsBuilder()
    builder.stream[Array[Byte], Array[Byte]](s"$application-input").to(s"$application-output")
    val properties = Properties()
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, application)
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    properties.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, "streams")
    properties.put(StreamsConfig.STATE_DIR_CONFIG, Files.createTempDirectory("xkafka-streams-fixture").toString)
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray.getClass)
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray.getClass)
    val streams = KafkaStreams(builder.build(), properties)
    streams.start()
    streams
