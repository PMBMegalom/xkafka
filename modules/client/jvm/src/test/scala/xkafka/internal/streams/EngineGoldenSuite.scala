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

package xkafka
package internal.streams

import java.util.Properties

import scala.concurrent.duration.*

import munit.FunSuite
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.streams.StreamsConfig
import xkafka.streams.ProcessingGuarantee

/** The engine's defaults, compared with what Java Streams configures for the same guarantee. */
final class EngineGoldenSuite extends FunSuite:
  private def java(guarantee: String): StreamsConfig =
    val properties = new Properties()
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "app")
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1")
    properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, guarantee)
    new StreamsConfig(properties)

  test("each guarantee commits at Java's interval for it"):
    assertEquals(
      Engine.commitInterval(ProcessingGuarantee.AtLeastOnce),
      java(StreamsConfig.AT_LEAST_ONCE).getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG).toLong.millis
    )
    assertEquals(
      Engine.commitInterval(ProcessingGuarantee.ExactlyOnce),
      java(StreamsConfig.EXACTLY_ONCE_V2).getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG).toLong.millis
    )

  test("a store taken away is kept as long as Java keeps a task's state directory"):
    assertEquals(Engine.StateCleanupDelay, java(StreamsConfig.AT_LEAST_ONCE).getLong(StreamsConfig.STATE_CLEANUP_DELAY_MS_CONFIG).toLong.millis)

  test("repartition topics are purged at Java's interval"):
    assertEquals(Engine.PurgeInterval, java(StreamsConfig.AT_LEAST_ONCE).getLong(StreamsConfig.REPARTITION_PURGE_INTERVAL_MS_CONFIG).toLong.millis)

  test("exactly once, a transaction may stay open as long as Java's producer lets one"):
    val producer = java(StreamsConfig.EXACTLY_ONCE_V2).getProducerConfigs("client")
    assertEquals(Engine.TransactionTimeout, producer.get(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG).toString.toLong.millis)
