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

import cats.effect.Async
import org.apache.kafka.clients.admin.ConfigEntry as JavaConfigEntry
import internal.driver.KafkaDriver
import internal.kafkaclients.{JavaClients, KafkaClientsDriver}

private[xkafka] object KafkaClientPlatform:
  def apply[F[_]: Async]: KafkaClient[F] = fromJava(JavaClients.default)

  /** The client over Java clients made some other way, such as the Java client's mocks. */
  private[xkafka] def fromJava[F[_]: Async](clients: JavaClients): KafkaClient[F] = new DriverClient[F](new KafkaClientsDriver[F](clients))

  def driver[F[_]: Async]: KafkaDriver[F] = new KafkaClientsDriver[F](JavaClients.default)

/** Reads the Java client's configuration entries into the portable ones. */
private[xkafka] object JavaConfiguration:
  def portableEntry(entry: JavaConfigEntry): ConfigurationEntry =
    ConfigurationEntry.reported(Option(entry.value), entry.isSensitive, portableSource(entry.source), entry.isReadOnly)

  private def portableSource(source: JavaConfigEntry.ConfigSource): ConfigurationSource =
    source match
      case JavaConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG          => ConfigurationSource.TopicOverride
      case JavaConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG         => ConfigurationSource.DynamicBroker
      case JavaConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG => ConfigurationSource.DynamicClusterDefault
      case JavaConfigEntry.ConfigSource.STATIC_BROKER_CONFIG          => ConfigurationSource.StaticBroker
      case JavaConfigEntry.ConfigSource.DEFAULT_CONFIG                => ConfigurationSource.Default
      case _                                                          => ConfigurationSource.Unknown
