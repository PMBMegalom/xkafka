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
package internal.confluent

import scala.scalajs.js
import scala.scalajs.js.annotation.JSImport
import scala.scalajs.js.typedarray.Uint8Array

private[xkafka] type SubscriptionTopic = String | js.RegExp

/** The librdkafka surface the package exports alongside its KafkaJS compatibility layer.
  *
  * Unlike that layer it reports one delivery report per message, carries an opaque through which a report is matched back to the record that produced
  * it, and represents headers as an ordered array of single-entry objects.
  */
@js.native
@JSImport("@confluentinc/kafka-javascript", JSImport.Namespace)
private[xkafka] object RdKafka extends js.Object:
  val Producer: js.Dynamic      = js.native
  val KafkaConsumer: js.Dynamic = js.native

@js.native
private[xkafka] trait RdProducer extends js.Object:
  def connect(metadataOptions: js.UndefOr[js.Any], callback: js.Function2[RdError | Null, js.Any, Unit]): this.type = js.native

  def disconnect(callback: js.Function2[RdError | Null, js.Any, Unit]): this.type = js.native

  def produce(
      topic: String,
      partition: js.UndefOr[Int] | Null,
      message: Uint8Array | Null,
      key: Uint8Array | Null,
      timestamp: js.UndefOr[Double] | Null,
      opaque: js.Any,
      headers: js.UndefOr[js.Array[RdHeader]]
  ): js.Any = js.native

  def setPollInterval(interval: Int): this.type = js.native

  def flush(timeout: js.UndefOr[Int], callback: js.Function1[RdError | Null, Unit]): this.type = js.native

  def on(event: String, listener: js.Function2[RdError | Null, RdDeliveryReport, Unit]): this.type = js.native

/** One header. librdkafka keeps these in an array, so duplicate names and their order both survive. */
private[xkafka] type RdHeader = js.Dictionary[Uint8Array | String]

@js.native
private[xkafka] trait RdConsumer extends js.Object:
  def connect(metadataOptions: js.UndefOr[js.Any], callback: js.Function2[RdError | Null, js.Any, Unit]): this.type = js.native

  def disconnect(callback: js.Function2[RdError | Null, js.Any, Unit]): this.type = js.native

  def subscribe(topics: js.Array[SubscriptionTopic]): this.type = js.native

  def consume(count: Int, callback: js.Function2[RdError | Null, js.Array[RdMessage], Unit]): Unit = js.native

  def commit(offsets: js.Array[RdTopicPartitionOffset]): this.type = js.native

  def assignments(): js.Array[RdTopicPartition] = js.native

  def pause(topicPartitions: js.Array[RdTopicPartition]): this.type = js.native

  def resume(topicPartitions: js.Array[RdTopicPartition]): this.type = js.native

  def committed(
      topicPartitions: js.Array[RdTopicPartition],
      timeout: Int,
      callback: js.Function2[RdError | Null, js.Array[RdTopicPartitionOffset], Unit]
  ): this.type = js.native

  def queryWatermarkOffsets(topic: String, partition: Int, timeout: Int, callback: js.Function2[RdError | Null, RdWatermarks, Unit]): js.Any =
    js.native

  def offsetsForTimes(
      topicPartitions: js.Array[RdTopicPartitionOffset],
      timeout: Int,
      callback: js.Function2[RdError | Null, js.Array[RdTopicPartitionOffset], Unit]
  ): Unit = js.native

  def seek(topicPartition: RdTopicPartitionOffset, timeout: js.Any, callback: js.Function1[RdError | Null, Unit]): this.type = js.native

  def getMetadata(options: js.Any, callback: js.Function2[RdError | Null, RdMetadata, Unit]): js.Any = js.native

  def setDefaultConsumeTimeout(timeoutMs: Int): Unit = js.native

  def on(event: String, listener: js.Function2[RdError | Null, js.Array[RdTopicPartition], Unit]): this.type = js.native

@js.native
private[xkafka] trait RdTopicPartition extends js.Object:
  val topic: String  = js.native
  val partition: Int = js.native

@js.native
private[xkafka] trait RdTopicPartitionOffset extends RdTopicPartition:
  /** Absent when librdkafka has no offset for the partition, which is how an uncommitted partition comes back. */
  val offset: js.UndefOr[Double] = js.native

@js.native
private[xkafka] trait RdWatermarks extends js.Object:
  val lowOffset: Double  = js.native
  val highOffset: Double = js.native

@js.native
private[xkafka] trait RdMessage extends js.Object:
  val topic: String                           = js.native
  val partition: Int                          = js.native
  val offset: Double                          = js.native
  val key: Uint8Array | Null                  = js.native
  val value: Uint8Array | Null                = js.native
  val timestamp: js.UndefOr[Double]           = js.native
  val headers: js.UndefOr[js.Array[RdHeader]] = js.native

@js.native
private[xkafka] trait RdMetadata extends js.Object:
  val topics: js.Array[RdTopicMetadata] = js.native

@js.native
private[xkafka] trait RdTopicMetadata extends js.Object:
  val name: String                              = js.native
  val partitions: js.Array[RdPartitionMetadata] = js.native

@js.native
private[xkafka] trait RdPartitionMetadata extends js.Object:
  val id: Int = js.native

@js.native
private[xkafka] trait RdError extends js.Object:
  val message: String                  = js.native
  val code: Int                        = js.native
  val isFatal: js.UndefOr[Boolean]     = js.native
  val isRetriable: js.UndefOr[Boolean] = js.native

@js.native
private[xkafka] trait RdDeliveryReport extends js.Object:
  val topic: String                 = js.native
  val partition: Int                = js.native
  val offset: js.UndefOr[Double]    = js.native
  val timestamp: js.UndefOr[Double] = js.native
  val opaque: js.UndefOr[Double]    = js.native

@js.native
@JSImport("buffer", "Buffer")
private[xkafka] object Buffer extends js.Object:
  def from(bytes: Uint8Array): Uint8Array = js.native

private[xkafka] object Values:
  /** `dr_cb` turns on the per-message delivery reports the batch API cannot provide. */
  def rdProducerConfig(brokers: js.Array[String], clientId: js.UndefOr[String], properties: Map[String, String]): js.Dictionary[js.Any] =
    val result = js.Dictionary.empty[js.Any]
    properties.foreach((name, value) => result(name) = value)
    result("bootstrap.servers") = brokers.mkString(",")
    clientId.foreach(value => result("client.id") = value)
    result("dr_cb") = true
    result

  def rdConsumerConfig(
      brokers: js.Array[String],
      clientId: js.UndefOr[String],
      groupId: String,
      autoOffsetReset: AutoOffsetReset,
      properties: Map[String, String]
  ): js.Dictionary[js.Any] =
    val result = js.Dictionary.empty[js.Any]
    properties.foreach((name, value) => result(name) = value)
    result("bootstrap.servers") = brokers.mkString(",")
    clientId.foreach(value => result("client.id") = value)
    result("group.id") = groupId
    result("enable.auto.commit") = false
    // node-rdkafka only wires the rebalance event when this is set, and the boolean form keeps its own
    // assign and unassign, including the cooperative protocol split.
    result("rebalance_cb") = true
    result("auto.offset.reset") = (
      autoOffsetReset match
        case AutoOffsetReset.Earliest => "earliest"
        case AutoOffsetReset.Latest   => "latest"
    )
    result

  def rdTopicPartition(topic: String, partition: Int): RdTopicPartition =
    js.Dynamic.literal(topic = topic, partition = partition).asInstanceOf[RdTopicPartition]

  def rdTopicPartitionOffset(topic: String, partition: Int, offset: Double): RdTopicPartitionOffset =
    js.Dynamic.literal(topic = topic, partition = partition, offset = offset).asInstanceOf[RdTopicPartitionOffset]

  def rdMetadataOptions(topic: js.UndefOr[String]): js.Any =
    val result = js.Dictionary.empty[js.Any]
    topic.foreach(value => result("topic") = value)
    result("allTopics") = topic.isEmpty
    result("timeout") = 10000
    result

  def rdHeader(name: String, value: Uint8Array | Null): RdHeader =
    val result = js.Dictionary.empty[Uint8Array | String]
    result(name) = if value == null then "" else value.asInstanceOf[Uint8Array]
    result
