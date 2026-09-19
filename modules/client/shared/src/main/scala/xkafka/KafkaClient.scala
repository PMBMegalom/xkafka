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

import cats.effect.{Async, Resource}

private[xkafka] val ManagedProperties =
  Set("bootstrap.servers", "client.id", "group.id", "auto.offset.reset", "enable.auto.commit", "enable.auto.offset.store")

trait KafkaClient[F[_]]:
  def producer[K, V](settings: ProducerSettings[F, K, V]): Resource[F, KafkaProducer[F, K, V]]

  def consumer[K, V](settings: ConsumerSettings[F, K, V], subscription: Subscription): Resource[F, KafkaConsumer[F, K, V]]

object KafkaClient:
  def apply[F[_]: Async]: KafkaClient[F] = KafkaClientPlatform[F]
