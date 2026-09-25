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

import cats.arrow.FunctionK
import cats.effect.{Async, MonadCancelThrow, Resource}

trait KafkaClient[F[_]]:
  self =>

  def producer[K, V](settings: ProducerSettings[F, K, V]): Resource[F, KafkaProducer[F, K, V]]

  def consumer[K, V](settings: ConsumerSettings[F, K, V], selection: Selection): Resource[F, KafkaConsumer[F, K, V]]

  /** Administers topics. It reads and changes cluster metadata only, so it neither produces nor consumes. */
  def admin(settings: ClientSettings): Resource[F, KafkaAdminClient[F]]

  final def imapK[G[_]](fk: FunctionK[F, G])(gk: FunctionK[G, F])(using MonadCancelThrow[F], MonadCancelThrow[G]): KafkaClient[G] =
    new KafkaClient[G]:
      override def producer[K, V](settings: ProducerSettings[G, K, V]): Resource[G, KafkaProducer[G, K, V]] =
        self.producer(settings.mapK(gk)).map(_.mapK(fk)).mapK(fk)

      override def consumer[K, V](settings: ConsumerSettings[G, K, V], selection: Selection): Resource[G, KafkaConsumer[G, K, V]] =
        self.consumer(settings.mapK(gk), selection).map(_.mapK(fk)).mapK(fk)

      override def admin(settings: ClientSettings): Resource[G, KafkaAdminClient[G]] = self.admin(settings).map(_.mapK(fk)).mapK(fk)

object KafkaClient:
  def apply[F[_]: Async]: KafkaClient[F] = KafkaClientPlatform[F]
