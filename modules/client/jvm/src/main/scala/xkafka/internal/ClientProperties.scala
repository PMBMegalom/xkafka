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

import scala.concurrent.duration.FiniteDuration

/** Backend property names for the client settings. */
private[xkafka] object ClientProperties:
  /** The Java client refreshes when its metadata cache reaches this age, so the age is also the refresh period. */
  def apply(settings: ClientSettings): Map[String, String] = Map("metadata.max.age.ms" -> settings.metadataRefreshInterval.toMillis.toString)

  /** The Java producer's own partitioner is the one every backend follows. */
  val producer: Map[String, String] = Map.empty

  /** The Java consumer bounds the calls given no timeout of their own, such as reading a position, by its default API timeout. */
  def consumer[F[_], K, V](settings: ConsumerSettings[F, K, V]): Map[String, String] =
    Map("default.api.timeout.ms" -> settings.requestTimeout.toMillis.toString) ++
      fetchWait(settings.fetchWait, settings.client.properties ++ settings.properties)

  /** The Java consumer gives up on a fetch after its request timeout, 30 s unless set, so a fetch the broker may hold for nearly that long or more is
    * given that long again past the wait, unless the properties set one.
    */
  private def fetchRequestTimeout(value: FiniteDuration, properties: Map[String, String]): Map[String, String] =
    val wait = value.toMillis
    val set  = properties.keys.exists(_.trim.equalsIgnoreCase("request.timeout.ms"))
    if set || wait + RequestMargin <= JavaRequestTimeout then Map.empty else Map("request.timeout.ms" -> (wait + JavaRequestTimeout).toString)

  private val JavaRequestTimeout = 30000L
  private val RequestMargin      = 5000L

  /** The Java consumer names the fetch wait `fetch.max.wait.ms`, and gives a fetch no longer than its request timeout, so a wait that long gets a
    * request timeout to match, where `properties` set none.
    */
  def fetchWait(value: FiniteDuration, properties: Map[String, String] = Map.empty): Map[String, String] =
    Map("fetch.max.wait.ms" -> value.toMillis.toString) ++ fetchRequestTimeout(value, properties)

  def security(settings: SecuritySettings): Map[String, String] = internal.security.SecurityProperties.javaClient(settings)

  /** The Java client names the policy that refuses to reset `none`. */
  def autoOffsetReset(value: AutoOffsetReset): String =
    value match
      case AutoOffsetReset.Earliest => "earliest"
      case AutoOffsetReset.Latest   => "latest"
      case AutoOffsetReset.Fail     => "none"
