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
package internal.librdkafka

/** librdkafka's in-process mock cluster, for xkafka's own tests. It is reached only through the client librdkafka made it for, with
  * `test.mock.num.brokers`; the shim refuses any other client, so nothing here can reach a real cluster.
  */
private[xkafka] trait MockCluster[F[_]]:
  /** Where clients reach the mock cluster. */
  def bootstrapServers: String

  def createTopic(name: String, partitions: Int): F[Unit]

  /** The next requests of API `apiKey` are answered with these error codes, one each. */
  def failRequests(apiKey: Int, codes: List[Int]): F[Unit]

private[xkafka] object MockCluster:
  /** The property that makes librdkafka create a mock cluster of that many brokers for a client. */
  val Brokers: (String, String) = "test.mock.num.brokers" -> "1"

  /** Kafka's `OffsetFetch` API, which reads a group's committed offsets. */
  val OffsetFetch = 9

  /** Kafka's `NOT_COORDINATOR` error. */
  val NotCoordinator = 16
