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

import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import cats.data.NonEmptyList
import cats.effect.Async
import cats.effect.Resource
import cats.effect.std.Semaphore
import cats.syntax.all.*
import fs2.Chunk
import fs2.Stream
import xkafka.internal.librdkafka.Bindings

private[xkafka] object KafkaClientPlatform:
  def apply[F[_]: Async]: KafkaClient[F] = new LibrdkafkaClient[F]

private[xkafka] object LibrdkafkaPlatform:
  def version: String = fromCString(Bindings.xkafka_version_str())

private final class LibrdkafkaClient[F[_]](using F: Async[F]) extends KafkaClient[F]:
  private val ErrorBufferSize     = 512
  private val PollTimeoutMillis   = 100
  private val UnassignedPartition = -1

  override def producer[K, V](settings: ProducerSettings[F, K, V]): Resource[F, KafkaProducer[F, K, V]] =
    for
      semaphore <- Resource.eval(Semaphore[F](1))
      handle <- Resource.make(createProducer(settings))(producer => semaphore.permit.use(_ => F.blocking(Bindings.xkafka_producer_destroy(producer))))
    yield new LibrdkafkaProducer(handle, semaphore, settings)

  override def consumer[K, V](settings: ConsumerSettings[F, K, V], subscription: Subscription): Resource[F, KafkaConsumer[F, K, V]] =
    for
      semaphore <- Resource.eval(Semaphore[F](1))
      handle <- Resource.make(createConsumer(settings))(consumer => semaphore.permit.use(_ => F.blocking(Bindings.xkafka_consumer_destroy(consumer))))
      _      <- Resource.eval(
        semaphore.permit.use(_ => F.blocking(subscribe(handle, subscription)))
      )
    yield new LibrdkafkaConsumer(handle, semaphore, settings)

  private def createProducer[K, V](settings: ProducerSettings[F, K, V]): F[CVoidPtr] =
    F.blocking {
      Zone.acquire { zone =>
        given Zone                        = zone
        val error                         = stackalloc[CChar](ErrorBufferSize)
        val (names, values, propertySize) = nativeProperties(settings.client.properties ++ settings.properties)
        val producer                      = Bindings.xkafka_producer_new(
          toCString(settings.client.bootstrapServers.toList.mkString(",")),
          settings.client.clientId.fold[CString](null)(toCString),
          names,
          values,
          propertySize,
          error,
          ErrorBufferSize.toUSize
        )
        if producer == null then throw nativeError(error)
        producer
      }
    }

  private def createConsumer[K, V](settings: ConsumerSettings[F, K, V]): F[CVoidPtr] =
    F.blocking {
      Zone.acquire { zone =>
        given Zone                        = zone
        val error                         = stackalloc[CChar](ErrorBufferSize)
        val (names, values, propertySize) = nativeProperties(settings.client.properties ++ settings.properties)
        val consumer                      = Bindings.xkafka_consumer_new(
          toCString(settings.client.bootstrapServers.toList.mkString(",")),
          settings.client.clientId.fold[CString](null)(toCString),
          toCString(settings.groupId.value),
          toCString(
            settings.autoOffsetReset match
              case AutoOffsetReset.Earliest => "earliest"
              case AutoOffsetReset.Latest   => "latest"
          ),
          names,
          values,
          propertySize,
          error,
          ErrorBufferSize.toUSize
        )
        if consumer == null then throw nativeError(error)
        consumer
      }
    }

  private def nativeProperties(properties: Map[String, String])(using Zone): (Ptr[CString], Ptr[CString], CSize) =
    val entries = properties.removedAll(ManagedProperties).toVector
    if entries.isEmpty then (null, null, 0.toUSize)
    else
      val names  = alloc[CString](entries.size)
      val values = alloc[CString](entries.size)
      entries.iterator.zipWithIndex.foreach { case ((name, value), index) =>
        names(index) = toCString(name)
        values(index) = toCString(value)
      }
      (names, values, entries.size.toUSize)

  private def subscribe(
      consumer: CVoidPtr,
      subscription: Subscription
  ): Unit =
    Zone.acquire { zone =>
      given Zone = zone
      val topics = subscription match
        case Subscription.Topics(values) => values
      val nativeSubscription =
        Bindings.xkafka_subscription_new(topics.length.toUSize)
      if nativeSubscription == null then throw new LibrdkafkaException("could not allocate a subscription")

      try
        topics.toList.foreach(topic =>
          Bindings.xkafka_subscription_add(
            nativeSubscription,
            toCString(topic.value)
          )
        )
        val error  = stackalloc[CChar](ErrorBufferSize)
        val result = Bindings.xkafka_consumer_subscribe(
          consumer,
          nativeSubscription,
          error,
          ErrorBufferSize.toUSize
        )
        if result != 0 then throw nativeError(error)
      finally Bindings.xkafka_subscription_destroy(nativeSubscription)
    }

  private def nativeError(error: CString): LibrdkafkaException =
    new LibrdkafkaException(fromCString(error))

  private def invalidBackendValue(
      field: String,
      value: Any,
      error: ValidationError
  ): LibrdkafkaException =
    new LibrdkafkaException(
      s"librdkafka returned an invalid $field '$value': $error"
    )

  private def cBytes(
      value: Option[Chunk[Byte]]
  )(using Zone): (CVoidPtr, CSize) =
    value.fold[(CVoidPtr, CSize)]((null, 0.toUSize)) { bytes =>
      val pointer = alloc[CChar](math.max(1, bytes.size))
      bytes.iterator.zipWithIndex.foreach { case (byte, index) =>
        pointer(index) = byte
      }
      (pointer, bytes.size.toUSize)
    }

  private def chunk(pointer: CVoidPtr, size: CSize): Chunk[Byte] =
    val bytes = pointer.asInstanceOf[Ptr[Byte]]
    Chunk.array(Array.tabulate(size.toInt)(bytes(_)))

  private final class LibrdkafkaProducer[K, V](
      handle: CVoidPtr,
      semaphore: Semaphore[F],
      settings: ProducerSettings[F, K, V]
  ) extends KafkaProducer[F, K, V]:

    override def produce(
        records: NonEmptyList[ProducerRecord[K, V]]
    ): F[ProducerResult[K, V]] =
      records.toList
        .traverse(produceRecord)
        .map(metadata => ProducerResult(records, metadata))

    private def produceRecord(
        record: ProducerRecord[K, V]
    ): F[RecordMetadata] =
      for
        key <- settings.keySerializer.serialize(
          record.topic,
          record.headers,
          record.key
        )
        value <- settings.valueSerializer.serialize(
          record.topic,
          record.headers,
          record.value
        )
        metadata <- semaphore.permit.use(_ => F.blocking(send(record, key, value)))
      yield metadata

    private def send(
        record: ProducerRecord[K, V],
        key: Option[Chunk[Byte]],
        value: Option[Chunk[Byte]]
    ): RecordMetadata =
      Zone.acquire { zone =>
        given Zone                    = zone
        val error                     = stackalloc[CChar](ErrorBufferSize)
        val (keyPointer, keySize)     = cBytes(key)
        val (valuePointer, valueSize) = cBytes(value)
        val resultPartition           = stackalloc[CInt]()
        val resultOffset              = stackalloc[CLongLong]()
        val resultTimestamp           = stackalloc[CLongLong]()
        val topic                     = toCString(record.topic.value)
        val partition                 = record.partition.fold(UnassignedPartition)(_.value)
        val timestamp                 = record.timestamp.fold(-1L)(_.epochMillis)
        val headers                   = Bindings.xkafka_headers_new(record.headers.values.size.toUSize)
        if headers == null then throw new LibrdkafkaException("could not allocate message headers")

        try
          record.headers.values.foreach { header =>
            val (headerValue, headerSize) = cBytes(header.value)
            val result                    = Bindings.xkafka_headers_add(
              headers,
              toCString(header.key),
              headerValue,
              headerSize,
              error,
              ErrorBufferSize.toUSize
            )
            if result != 0 then throw nativeError(error)
          }
        catch
          case error: Throwable =>
            Bindings.xkafka_headers_destroy(headers)
            throw error

        val result = Bindings.xkafka_producer_send(
          handle,
          topic,
          partition,
          timestamp,
          keyPointer,
          keySize,
          valuePointer,
          valueSize,
          headers,
          resultPartition,
          resultOffset,
          resultTimestamp,
          error,
          ErrorBufferSize.toUSize
        )
        if result != 0 then throw nativeError(error)

        val portablePartition = Partition
          .from(!resultPartition)
          .fold(
            error =>
              throw invalidBackendValue(
                "partition",
                !resultPartition,
                error
              ),
            identity
          )
        val offsetValue    = !resultOffset
        val timestampValue = !resultTimestamp
        val portableOffset = Option.when(offsetValue >= 0L)(
          Offset
            .from(offsetValue)
            .fold(
              error => throw invalidBackendValue("offset", offsetValue, error),
              identity
            )
        )

        RecordMetadata(
          TopicPartition(record.topic, portablePartition),
          portableOffset,
          Option
            .when(timestampValue >= 0L)(timestampValue)
            .map(Timestamp.fromEpochMillis)
        )
      }

  private final case class NativeRecord(
      topic: String,
      partition: Int,
      offset: Long,
      timestamp: Option[Long],
      key: Option[Chunk[Byte]],
      value: Option[Chunk[Byte]],
      headers: Headers
  )

  private final class LibrdkafkaConsumer[K, V](
      handle: CVoidPtr,
      semaphore: Semaphore[F],
      settings: ConsumerSettings[F, K, V]
  ) extends KafkaConsumer[F, K, V]:

    override val records: Stream[F, CommittableConsumerRecord[F, K, V]] =
      Stream
        .repeatEval(
          semaphore.permit.use(_ => F.blocking(poll()))
        )
        .unNone
        .evalMap(decode)

    private def poll(): Option[NativeRecord] =
      Zone.acquire { _ =>
        val status  = stackalloc[CInt]()
        val error   = stackalloc[CChar](ErrorBufferSize)
        val message = Bindings.xkafka_consumer_poll(
          handle,
          PollTimeoutMillis,
          status,
          error,
          ErrorBufferSize.toUSize
        )

        if !status < 0 then throw nativeError(error)
        else if !status == 0 then None
        else
          try Some(copyMessage(message))
          finally Bindings.xkafka_message_destroy(message)
      }

    private def copyMessage(message: CVoidPtr): NativeRecord =
      val keyPointer   = Bindings.xkafka_message_key(message)
      val valuePointer = Bindings.xkafka_message_value(message)
      val timestamp    = Bindings.xkafka_message_timestamp(message)
      NativeRecord(
        topic = fromCString(Bindings.xkafka_message_topic(message)),
        partition = Bindings.xkafka_message_partition(message),
        offset = Bindings.xkafka_message_offset(message),
        timestamp = Option.when(timestamp >= 0L)(timestamp),
        key = Option(keyPointer).map(pointer => chunk(pointer, Bindings.xkafka_message_key_size(message))),
        value = Option(valuePointer).map(pointer => chunk(pointer, Bindings.xkafka_message_value_size(message))),
        headers = copyHeaders(message)
      )

    private def copyHeaders(message: CVoidPtr): Headers =
      val count  = Bindings.xkafka_message_header_count(message)
      val values = Vector.tabulate(count.toInt) { index =>
        val name      = stackalloc[CString]()
        val value     = stackalloc[Ptr[Byte]]()
        val valueSize = stackalloc[CSize]()
        val hasValue  = stackalloc[CInt]()
        val result    = Bindings.xkafka_message_header_at(
          message,
          index.toUSize,
          name,
          value,
          valueSize,
          hasValue
        )
        if result != 0 then
          throw new LibrdkafkaException(
            s"could not read message header at index $index"
          )
        Header(
          fromCString(!name),
          Option.when(!hasValue != 0)(chunk(!value, !valueSize))
        )
      }
      Headers.fromVector(values)

    private def decode(
        source: NativeRecord
    ): F[CommittableConsumerRecord[F, K, V]] =
      for
        topic <- F.fromEither(
          Topic
            .from(source.topic)
            .leftMap(error => invalidBackendValue("topic", source.topic, error))
        )
        partition <- F.fromEither(
          Partition
            .from(source.partition)
            .leftMap(error => invalidBackendValue("partition", source.partition, error))
        )
        offset <- F.fromEither(
          Offset
            .from(source.offset)
            .leftMap(error => invalidBackendValue("offset", source.offset, error))
        )
        portableNextOffset <- F.fromEither(
          offset.next.leftMap(error => invalidBackendValue("next offset", source.offset, error))
        )
        key <- settings.keyDeserializer.deserialize(
          topic,
          source.headers,
          source.key
        )
        value <- settings.valueDeserializer.deserialize(
          topic,
          source.headers,
          source.value
        )
      yield
        val topicPartition = TopicPartition(topic, partition)
        val record         = ConsumerRecord(
          topicPartition,
          offset,
          source.timestamp.map(Timestamp.fromEpochMillis),
          key,
          value,
          source.headers
        )
        val committable = new CommittableOffset[F]:
          override val topicPartition: TopicPartition =
            TopicPartition(topic, partition)

          override val nextOffset: Offset = portableNextOffset

          override def commit: F[Unit] =
            semaphore.permit.use(_ => F.blocking(commitOffset(topic, partition, portableNextOffset)))

        CommittableConsumerRecord(record, committable)

    private def commitOffset(
        topic: Topic,
        partition: Partition,
        offset: Offset
    ): Unit =
      Zone.acquire { zone =>
        given Zone = zone
        val error  = stackalloc[CChar](ErrorBufferSize)
        val result = Bindings.xkafka_consumer_commit(
          handle,
          toCString(topic.value),
          partition.value,
          offset.value,
          error,
          ErrorBufferSize.toUSize
        )
        if result != 0 then throw nativeError(error)
      }

private final class LibrdkafkaException(message: String) extends RuntimeException(message)
