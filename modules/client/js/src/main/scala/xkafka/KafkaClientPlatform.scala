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
import cats.syntax.all.*
import internal.driver.KafkaDriver
import internal.librdkafka.{JsLibrdkafka, MockCluster}

private[xkafka] object KafkaClientPlatform:
  def apply[F[_]: Async]: KafkaClient[F] = new DriverClient[F](driver[F])

  def driver[F[_]: Async]: KafkaDriver[F] = new JsLibrdkafka[F]

private[xkafka] object LibrdkafkaPlatform:
  def version: String = JsLibrdkafka.version

  /** A mock cluster made for a producer created with `properties` (see [[internal.librdkafka.MockCluster]]). */
  def mockCluster[F[_]: Async](properties: Map[String, String]): Resource[F, MockCluster[F]] =
    val driver = new JsLibrdkafka[F]
    Resource.make(driver.producer("localhost:9092", None, properties, 1000))(driver.destroyProducer(_, 1000)).evalMap: producer =>
      driver.mockBootstraps(producer).map: servers =>
        new MockCluster[F]:
          val bootstrapServers: String                             = servers
          def createTopic(name: String, partitions: Int): F[Unit]  = driver.mockCreateTopic(producer, name, partitions)
          def failRequests(apiKey: Int, codes: List[Int]): F[Unit] = driver.mockFailRequests(producer, apiKey, codes)
