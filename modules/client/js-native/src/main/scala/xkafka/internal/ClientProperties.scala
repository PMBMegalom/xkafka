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
package internal

/** Backend property names for the client settings. */
private[xkafka] object ClientProperties:
  /** librdkafka refreshes on this timer and derives `metadata.max.age.ms` from it, so only the timer is set here. */
  def apply(settings: ClientSettings): Map[String, String] =
    Map("topic.metadata.refresh.interval.ms" -> settings.metadataRefreshInterval.toMillis.toString)

  /** librdkafka hashes keys with CRC32 by default; Java's murmur2 sends each key to the partition a Java producer would. */
  val producer: Map[String, String] = Map("partitioner" -> "murmur2_random")

  /** librdkafka bounds each call by the timeout it is given, so a consumer needs nothing more. */
  def consumer[F[_], K, V](@scala.annotation.unused settings: ConsumerSettings[F, K, V]): Map[String, String] = Map.empty

  def security(settings: SecuritySettings): Map[String, String] = internal.security.SecurityProperties.librdkafka(settings)

  /** librdkafka names the policy that refuses to reset `error`. */
  def autoOffsetReset(value: AutoOffsetReset): String =
    value match
      case AutoOffsetReset.Earliest => "earliest"
      case AutoOffsetReset.Latest   => "latest"
      case AutoOffsetReset.Fail     => "error"
