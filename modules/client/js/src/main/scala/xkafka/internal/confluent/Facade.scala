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

private[xkafka] type JsHeaders         = js.Dictionary[js.Any]
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
@JSImport("@confluentinc/kafka-javascript", "KafkaJS")
private[xkafka] object KafkaJS extends js.Object:
  val Kafka: js.Dynamic = js.native

@js.native
private[xkafka] trait Kafka extends js.Object:
  def producer(config: ProducerConfig): Producer = js.native
  def consumer(config: ConsumerConfig): Consumer = js.native

@js.native
private[xkafka] trait KafkaConfig extends js.Object

@js.native
private[xkafka] trait ProducerConfig extends js.Object

@js.native
private[xkafka] trait ConsumerConfig extends js.Object

@js.native
private[xkafka] trait Producer extends js.Object:
  def connect(): js.Promise[Unit]                                           = js.native
  def disconnect(): js.Promise[Unit]                                        = js.native
  def sendBatch(batch: ProducerBatch): js.Promise[js.Array[RecordMetadata]] = js.native

@js.native
private[xkafka] trait Consumer extends js.Object:
  def connect(): js.Promise[Unit]                                                                      = js.native
  def disconnect(): js.Promise[Unit]                                                                   = js.native
  def subscribe(subscription: ConsumerSubscribe): js.Promise[Unit]                                     = js.native
  def run(config: ConsumerRunConfig): js.Promise[Unit]                                                 = js.native
  def commitOffsets(offsets: js.Array[TopicPartitionOffset]): js.Promise[Unit]                         = js.native
  def committed(topicPartitions: js.Array[TopicPartition]): js.Promise[js.Array[TopicPartitionOffset]] = js.native
  def seek(topicPartitionOffset: TopicPartitionOffset): Unit                                           = js.native
  def assignment(): js.Array[TopicPartition]                                                           = js.native
  def dependentAdmin(): Admin                                                                          = js.native

@js.native
private[xkafka] trait Admin extends js.Object:
  def connect(): js.Promise[Unit]                                                                                = js.native
  def disconnect(): js.Promise[Unit]                                                                             = js.native
  def fetchTopicMetadata(options: TopicMetadataOptions): js.Promise[js.Array[TopicMetadata]]                     = js.native
  def fetchTopicOffsets(topic: String): js.Promise[js.Array[TopicOffsets]]                                       = js.native
  def fetchTopicOffsetsByTimestamp(topic: String, timestamp: Double): js.Promise[js.Array[TopicPartitionOffset]] = js.native

@js.native
private[xkafka] trait TopicMetadataOptions extends js.Object

@js.native
private[xkafka] trait TopicMetadata extends js.Object:
  val name: String                            = js.native
  val partitions: js.Array[PartitionMetadata] = js.native

@js.native
private[xkafka] trait PartitionMetadata extends js.Object:
  val partitionId: Int = js.native

@js.native
private[xkafka] trait ProducerBatch extends js.Object

@js.native
private[xkafka] trait TopicMessages extends js.Object

@js.native
private[xkafka] trait Message extends js.Object

@js.native
private[xkafka] trait RecordMetadata extends js.Object:
  val topicName: String                 = js.native
  val partition: Int                    = js.native
  val errorCode: Int                    = js.native
  val offset: js.UndefOr[String]        = js.native
  val timestamp: js.UndefOr[String]     = js.native
  val baseOffset: js.UndefOr[String]    = js.native
  val logAppendTime: js.UndefOr[String] = js.native

@js.native
private[xkafka] trait ConsumerSubscribe extends js.Object

@js.native
private[xkafka] trait ConsumerRunConfig extends js.Object

@js.native
private[xkafka] trait EachBatchPayload extends js.Object:
  val batch: ConsumerBatch                = js.native
  def isRunning(): Boolean                = js.native
  def isStale(): Boolean                  = js.native
  def resolveOffset(offset: String): Unit = js.native

@js.native
private[xkafka] trait ConsumerBatch extends js.Object:
  val topic: String                    = js.native
  val partition: Int                   = js.native
  val messages: js.Array[KafkaMessage] = js.native

@js.native
private[xkafka] trait KafkaMessage extends js.Object:
  val key: Uint8Array | Null         = js.native
  val value: Uint8Array | Null       = js.native
  val timestamp: String              = js.native
  val attributes: Int                = js.native
  val offset: String                 = js.native
  val size: Int                      = js.native
  val headers: js.UndefOr[JsHeaders] = js.native

@js.native
private[xkafka] trait TopicPartition extends js.Object:
  val topic: String  = js.native
  val partition: Int = js.native

@js.native
private[xkafka] trait TopicPartitionOffset extends TopicPartition:
  val offset: String | Null = js.native

@js.native
private[xkafka] trait TopicOffsets extends js.Object:
  val partition: Int = js.native
  val offset: String = js.native
  val high: String   = js.native
  val low: String    = js.native

@js.native
@JSImport("buffer", "Buffer")
private[xkafka] object Buffer extends js.Object:
  def from(bytes: Uint8Array): Uint8Array = js.native

private[xkafka] object Values:
  def kafka(config: KafkaConfig): Kafka = js.Dynamic.newInstance(KafkaJS.Kafka)(config).asInstanceOf[Kafka]

  def kafkaConfig(brokers: js.Array[String], clientId: js.UndefOr[String], properties: Map[String, String]): KafkaConfig =
    val result = configuration(properties)
    result.updateDynamic("bootstrap.servers")(brokers.mkString(","))
    clientId.foreach(value => result.updateDynamic("client.id")(value))
    result.asInstanceOf[KafkaConfig]

  def producerConfig(properties: Map[String, String]): ProducerConfig = configuration(properties).asInstanceOf[ProducerConfig]

  /** Config for the librdkafka producer. `dr_cb` turns on the per-message delivery reports the batch layer cannot give us. */
  def rdProducerConfig(brokers: js.Array[String], clientId: js.UndefOr[String], properties: Map[String, String]): js.Dictionary[js.Any] =
    val result = js.Dictionary.empty[js.Any]
    properties.foreach((name, value) => result(name) = value)
    result("bootstrap.servers") = brokers.mkString(",")
    clientId.foreach(value => result("client.id") = value)
    result("dr_cb") = true
    result

  def rdHeader(name: String, value: Uint8Array | Null): RdHeader =
    val result = js.Dictionary.empty[Uint8Array | String]
    result(name) = if value == null then "" else value.asInstanceOf[Uint8Array]
    result

  def consumerConfig(groupId: String, autoOffsetReset: AutoOffsetReset, properties: Map[String, String]): ConsumerConfig =
    val result = configuration(properties)
    result.updateDynamic("group.id")(groupId)
    result.updateDynamic("enable.auto.commit")(false)
    result.updateDynamic("auto.offset.reset")(
      autoOffsetReset match
        case AutoOffsetReset.Earliest => "earliest"
        case AutoOffsetReset.Latest   => "latest"
    )
    result.asInstanceOf[ConsumerConfig]

  private def configuration(properties: Map[String, String]): js.Dynamic =
    val result = js.Dynamic.literal()
    properties.foreach:
      case (key, value) => result.updateDynamic(key)(value)
    result

  def message(
      key: Uint8Array | Null,
      value: Uint8Array | Null,
      partition: js.UndefOr[Int],
      timestamp: js.UndefOr[String],
      headers: JsHeaders
  ): Message =
    val result = js.Dynamic.literal(key = key, value = value, headers = headers)
    partition.foreach(value => result.updateDynamic("partition")(value))
    timestamp.foreach(value => result.updateDynamic("timestamp")(value))
    result.asInstanceOf[Message]

  def topicMessages(topic: String, messages: js.Array[Message]): TopicMessages =
    js.Dynamic.literal(topic = topic, messages = messages).asInstanceOf[TopicMessages]

  def producerBatch(topicMessages: js.Array[TopicMessages]): ProducerBatch =
    js.Dynamic.literal(topicMessages = topicMessages).asInstanceOf[ProducerBatch]

  def subscription(topics: js.Array[SubscriptionTopic]): ConsumerSubscribe = js.Dynamic.literal(topics = topics).asInstanceOf[ConsumerSubscribe]

  def consumerRun(eachBatch: js.Function1[EachBatchPayload, js.Promise[Unit]]): ConsumerRunConfig =
    js.Dynamic.literal(eachBatchAutoResolve = false, eachBatch = eachBatch).asInstanceOf[ConsumerRunConfig]

  def topicPartitionOffset(topic: String, partition: Int, offset: String): TopicPartitionOffset =
    js.Dynamic.literal(topic = topic, partition = partition, offset = offset).asInstanceOf[TopicPartitionOffset]

  def topicPartition(topic: String, partition: Int): TopicPartition =
    js.Dynamic.literal(topic = topic, partition = partition).asInstanceOf[TopicPartition]

  def topicMetadataOptions(topics: js.UndefOr[js.Array[String]]): TopicMetadataOptions =
    val result = js.Dynamic.literal()
    topics.foreach(result.updateDynamic("topics")(_))
    result.asInstanceOf[TopicMetadataOptions]
