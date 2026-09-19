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

import cats.data.NonEmptyList
import fs2.Stream

final case class ClientSettings(bootstrapServers: NonEmptyList[String], clientId: Option[String] = None, properties: Map[String, String] = Map.empty)

final case class ProducerSettings[F[_], K, V](
    client: ClientSettings,
    keySerializer: Serializer[F, K],
    valueSerializer: Serializer[F, V],
    properties: Map[String, String] = Map.empty
)

enum AutoOffsetReset:
  case Earliest, Latest

final case class ConsumerSettings[F[_], K, V](
    client: ClientSettings,
    groupId: ConsumerGroup,
    keyDeserializer: Deserializer[F, K],
    valueDeserializer: Deserializer[F, V],
    autoOffsetReset: AutoOffsetReset = AutoOffsetReset.Latest,
    properties: Map[String, String] = Map.empty
)

enum Subscription:
  case Topics(topics: NonEmptyList[Topic])

trait CommittableOffset[F[_]]:
  def topicPartition: TopicPartition

  /** The next offset to consume after this commit succeeds. */
  def nextOffset: Offset

  def commit: F[Unit]

final case class CommittableConsumerRecord[F[_], K, V](
    record: ConsumerRecord[K, V],
    offset: CommittableOffset[F]
)

trait KafkaProducer[F[_], K, V]:
  def produce(
      records: NonEmptyList[ProducerRecord[K, V]]
  ): F[ProducerResult[K, V]]

trait KafkaConsumer[F[_], K, V]:
  def records: Stream[F, CommittableConsumerRecord[F, K, V]]
