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

import scala.compiletime.testing.typeCheckErrors
import scala.concurrent.duration.*

import cats.arrow.FunctionK
import cats.data.NonEmptyList
import cats.effect.{IO, SyncIO}
import cats.syntax.all.*
import cats.tagless.FunctorK
import fs2.{Chunk, Stream}
import munit.FunSuite

final class ClientSuite extends FunSuite:
  private type ErrorOr[A] = Either[String, A]

  private val topic          = Topic.from("events").toOption.get
  private val partition      = Partition.from(0).toOption.get
  private val offset         = Offset.from(0L).toOption.get
  private val nextOffset     = Offset.from(1L).toOption.get
  private val timestamp      = Timestamp.fromEpochMillis(1234L)
  private val group          = ConsumerGroup.from("workers").toOption.get
  private val clientSettings = ClientSettings.from(NonEmptyList.one("localhost:9092")).toOption.get
  private val producerRecord = ProducerRecord(topic, "key", "value")
  private val consumerRecord = ConsumerRecord(TopicPartition(topic, partition), offset, None, "key", "value", Headers.empty)

  private val optionToErrorOr: FunctionK[Option, ErrorOr] =
    new FunctionK[Option, ErrorOr]:
      override def apply[A](value: Option[A]): ErrorOr[A] = value.toRight("empty")

  private val optionToSyncIO: FunctionK[Option, SyncIO] =
    new FunctionK[Option, SyncIO]:
      override def apply[A](value: Option[A]): SyncIO[A] = value.fold(SyncIO.raiseError(new NoSuchElementException("empty")))(SyncIO.pure)

  test("valid settings succeed"):
    val settings = ClientSettings.from(NonEmptyList.one("localhost:9092"), properties = Map("ssl.key.password" -> ""))

    assert(settings.isValid)

  test("settings cannot bypass their validated constructors"):
    assert(typeCheckErrors("new xkafka.ClientSettings(???, ???, ???)").nonEmpty)
    assert(typeCheckErrors("new xkafka.ProducerSettings(???, ???, ???, ???)").nonEmpty)
    assert(typeCheckErrors("new xkafka.ConsumerSettings(???, ???, ???, ???, ???, ???)").nonEmpty)
    assert(typeCheckErrors("xkafka.ClientSettings(???, ???, ???)").nonEmpty)
    assert(typeCheckErrors("xkafka.ProducerSettings(???, ???, ???, ???)").nonEmpty)
    assert(typeCheckErrors("xkafka.ConsumerSettings(???, ???, ???, ???, ???, ???)").nonEmpty)
    assert(typeCheckErrors("(??? : xkafka.ClientSettings).copy()").nonEmpty)

  test("settings retain structural case-class semantics without exposing property values"):
    val firstClient =
      ClientSettings.from(NonEmptyList.one("localhost:9092"), properties = Map("ssl.keystore.password" -> "client-secret")).toOption.get
    val secondClient =
      ClientSettings.from(NonEmptyList.one("localhost:9092"), properties = Map("ssl.keystore.password" -> "client-secret")).toOption.get
    val serializer     = Serializer.const[IO, String](None)
    val firstProducer  = ProducerSettings.from(firstClient, serializer, serializer, Map("ssl.key.password" -> "producer-secret")).toOption.get
    val secondProducer = ProducerSettings.from(firstClient, serializer, serializer, Map("ssl.key.password" -> "producer-secret")).toOption.get
    val deserializer   = Deserializer.utf8[IO]
    val firstConsumer  =
      ConsumerSettings.from(firstClient, group, deserializer, deserializer, properties = Map("ssl.key.password" -> "consumer-secret")).toOption.get
    val secondConsumer =
      ConsumerSettings.from(firstClient, group, deserializer, deserializer, properties = Map("ssl.key.password" -> "consumer-secret")).toOption.get
    val renderedSettings = List(firstClient.toString, firstProducer.toString, firstConsumer.toString)

    assertEquals(firstClient, secondClient)
    assertEquals(firstClient.hashCode, secondClient.hashCode)
    assertEquals(firstClient.productPrefix, "ClientSettings")
    assertEquals(firstProducer, secondProducer)
    assertEquals(firstProducer.hashCode, secondProducer.hashCode)
    assertEquals(firstProducer.productPrefix, "ProducerSettings")
    assertEquals(firstConsumer, secondConsumer)
    assertEquals(firstConsumer.hashCode, secondConsumer.hashCode)
    assertEquals(firstConsumer.productPrefix, "ConsumerSettings")
    assert(renderedSettings.forall(_.contains("<redacted>")))
    assert(renderedSettings.forall(!_.contains("-secret")))

  test("client settings accumulate portable validation errors"):
    val settings = ClientSettings.from(NonEmptyList.of("", " ", "localhost:9092"), properties = Map("" -> "value"))

    assertEquals(
      settings.toEither,
      Left(NonEmptyList.of(
        SettingsError.BlankBootstrapServer(0),
        SettingsError.BlankBootstrapServer(1),
        SettingsError.BlankPropertyName(SettingsError.PropertyScope.Client)
      ))
    )

  test("producer settings validate producer properties during construction"):
    val serializer = Serializer.const[IO, String](None)
    val settings   = ProducerSettings.from(clientSettings, serializer, serializer, properties = Map(" " -> "value"))

    assertEquals(settings.toEither, Left(NonEmptyList.one(SettingsError.BlankPropertyName(SettingsError.PropertyScope.Producer))))

  test("consumer settings validate consumer properties during construction"):
    val deserializer = Deserializer.utf8[IO]
    val settings     = ConsumerSettings.from(clientSettings, group, deserializer, deserializer, properties = Map("" -> "value"))

    assertEquals(settings.toEither, Left(NonEmptyList.one(SettingsError.BlankPropertyName(SettingsError.PropertyScope.Consumer))))

  test("settings reject properties xkafka manages itself"):
    val client = ClientSettings.from(NonEmptyList.one("localhost:9092"), properties = Map("group.id" -> "mine", "bootstrap.servers" -> "other"))

    assertEquals(
      client.toEither,
      Left(NonEmptyList.of(
        SettingsError.ManagedProperty("bootstrap.servers", SettingsError.PropertyScope.Client),
        SettingsError.ManagedProperty("group.id", SettingsError.PropertyScope.Client)
      ))
    )

  test("managed property rejection is scoped to the settings that declared it"):
    val serializer = Serializer.const[IO, String](None)
    val producer   = ProducerSettings.from(clientSettings, serializer, serializer, Map("enable.auto.commit" -> "true"))
    val consumer   =
      ConsumerSettings.from(clientSettings, group, Deserializer.utf8[IO], Deserializer.utf8[IO], properties = Map("AUTO.OFFSET.RESET" -> "earliest"))

    assertEquals(producer.toEither, Left(NonEmptyList.one(SettingsError.ManagedProperty("enable.auto.commit", SettingsError.PropertyScope.Producer))))
    assertEquals(consumer.toEither, Left(NonEmptyList.one(SettingsError.ManagedProperty("AUTO.OFFSET.RESET", SettingsError.PropertyScope.Consumer))))

  test("bootstrap servers must be host:port"):
    val invalid = ClientSettings.from(NonEmptyList.of("localhost", "localhost:0", "localhost:70000", "host:port"))

    assertEquals(
      invalid.toEither,
      Left(NonEmptyList.of(
        SettingsError.InvalidBootstrapServer(0, "localhost"),
        SettingsError.InvalidBootstrapServer(1, "localhost:0"),
        SettingsError.InvalidBootstrapServer(2, "localhost:70000"),
        SettingsError.InvalidBootstrapServer(3, "host:port")
      ))
    )
    assert(ClientSettings.from(NonEmptyList.of("localhost:9092", "[::1]:9092", "10.0.0.1:1")).isValid)

  test("TLS settings reject a blank certificate authority"):
    assertEquals(TlsSettings.from(CertificateAuthority.PemFile("  ")).toEither, Left(NonEmptyList.one(SettingsError.BlankCertificateAuthority)))
    assertEquals(TlsSettings.from(CertificateAuthority.Pem("")).toEither, Left(NonEmptyList.one(SettingsError.BlankCertificateAuthority)))
    assert(TlsSettings.from(CertificateAuthority.SystemDefault).isValid)

  test("SASL settings accumulate blank credential errors"):
    assertEquals(
      SaslSettings.from(SaslMechanism.ScramSha256, " ", "").toEither,
      Left(NonEmptyList.of(SettingsError.BlankSaslUsername, SettingsError.BlankSaslPassword))
    )

  test("SASL settings keep the password out of toString"):
    val sasl = SaslSettings.from(SaslMechanism.ScramSha256, "user", "secret").toOption.get
    assert(sasl.toString.contains("user"))
    assert(!sasl.toString.contains("secret"))

  test("security settings carry only the parts their protocol uses"):
    val tls  = TlsSettings.from().toOption.get
    val sasl = SaslSettings.from(SaslMechanism.Plain, "user", "secret").toOption.get

    assertEquals(SecuritySettings.Plaintext.tlsSettings, None)
    assertEquals(SecuritySettings.Plaintext.saslSettings, None)
    assertEquals(SecuritySettings.Tls(tls).tlsSettings, Some(tls))
    assertEquals(SecuritySettings.Tls(tls).saslSettings, None)
    assertEquals(SecuritySettings.SaslPlaintext(sasl).tlsSettings, None)
    assertEquals(SecuritySettings.SaslPlaintext(sasl).saslSettings, Some(sasl))
    assertEquals(SecuritySettings.SaslTls(tls, sasl).tlsSettings, Some(tls))
    assertEquals(SecuritySettings.SaslTls(tls, sasl).saslSettings, Some(sasl))

  test("settings reject the security properties the typed model owns"):
    val settings =
      ClientSettings.from(NonEmptyList.one("localhost:9092"), properties = Map("sasl.password" -> "secret", "ssl.ca.location" -> "ca.pem"))

    assertEquals(
      settings.toEither,
      Left(NonEmptyList.of(
        SettingsError.ManagedProperty("sasl.password", SettingsError.PropertyScope.Client),
        SettingsError.ManagedProperty("ssl.ca.location", SettingsError.PropertyScope.Client)
      ))
    )

  test("client settings carry security through the withers"):
    val tls    = TlsSettings.from(CertificateAuthority.PemFile("ca.pem")).toOption.get
    val secure = clientSettings.withSecurity(SecuritySettings.Tls(tls))

    assertEquals(secure.security, SecuritySettings.Tls(tls))
    assertEquals(secure.withClientId("id").security, SecuritySettings.Tls(tls))
    assertEquals(secure.withProperty("linger.ms", "5").toOption.get.security, SecuritySettings.Tls(tls))

  test("the consumer request timeout defaults and is carried by its wither"):
    val deserializer = Deserializer.utf8[IO]
    val settings     = ConsumerSettings.from(clientSettings, group, deserializer, deserializer).toOption.get

    assertEquals(settings.requestTimeout, ConsumerSettings.DefaultRequestTimeout)
    assertEquals(settings.withRequestTimeout(5.seconds).requestTimeout, 5.seconds)
    assertEquals(settings.withRequestTimeout(5.seconds).withPollTimeout(25.millis).requestTimeout, 5.seconds)
    assertEquals(settings.withRequestTimeout(5.seconds).withProperty("fetch.min.bytes", "1").toOption.get.requestTimeout, 5.seconds)

  test("settings reject the api timeout the request timeout owns"):
    val deserializer = Deserializer.utf8[IO]
    val settings     = ConsumerSettings.from(clientSettings, group, deserializer, deserializer, properties = Map("default.api.timeout.ms" -> "1000"))

    assertEquals(
      settings.toEither,
      Left(NonEmptyList.one(SettingsError.ManagedProperty("default.api.timeout.ms", SettingsError.PropertyScope.Consumer)))
    )

  test("the consumer poll timeout defaults and is carried by its wither"):
    val deserializer = Deserializer.utf8[IO]
    val settings     = ConsumerSettings.from(clientSettings, group, deserializer, deserializer).toOption.get

    assertEquals(settings.pollTimeout, ConsumerSettings.DefaultPollTimeout)
    assertEquals(settings.withPollTimeout(25.millis).pollTimeout, 25.millis)
    assertEquals(settings.withPollTimeout(25.millis).withGroupId(group).pollTimeout, 25.millis)
    assertEquals(settings.withPollTimeout(25.millis).withProperty("fetch.min.bytes", "1").toOption.get.pollTimeout, 25.millis)

  test("withers revalidate and preserve the remaining settings"):
    val updated = clientSettings.withClientId("probe").withProperty("linger.ms", "5")

    assertEquals(updated.toEither.map(_.clientId), Right(Some("probe")))
    assertEquals(updated.toEither.map(_.properties), Right(Map("linger.ms" -> "5")))
    assertEquals(
      clientSettings.withProperty("group.id", "mine").toEither,
      Left(NonEmptyList.one(SettingsError.ManagedProperty("group.id", SettingsError.PropertyScope.Client)))
    )
    assertEquals(clientSettings.withClientId("probe").withoutClientId.clientId, None)

  test("producer and consumer settings support natural transformations"):
    val serializer     = Serializer.instance[Option, String]((_, _, value) => Some(Some(Chunk.array(value.getBytes("UTF-8")))))
    val deserializer   = Deserializer.instance[Option, String]((_, _, bytes) => bytes.map(chunk => new String(chunk.toArray, "UTF-8")))
    val producer       = ProducerSettings.from(clientSettings, serializer, serializer).toOption.get
    val consumer       = ConsumerSettings.from(clientSettings, group, deserializer, deserializer).toOption.get
    val mappedProducer = FunctorK[[F[_]] =>> ProducerSettings[F, String, String]].mapK(producer)(optionToErrorOr)
    val mappedConsumer = FunctorK[[F[_]] =>> ConsumerSettings[F, String, String]].mapK(consumer)(optionToErrorOr)
    val bytes          = Some(Chunk.array("value".getBytes("UTF-8")))

    assertEquals(mappedProducer.valueSerializer.serialize(topic, Headers.empty, "value"), Right(bytes))
    assertEquals(mappedConsumer.valueDeserializer.deserialize(topic, Headers.empty, bytes), Right("value"))

  test("committable offsets and records support natural transformations"):
    val source       = committableOffset
    val record       = CommittableConsumerRecord(consumerRecord, source)
    val mapped       = FunctorK[CommittableOffset].mapK(source)(optionToErrorOr)
    val mappedRecord = FunctorK[[F[_]] =>> CommittableConsumerRecord[F, String, String]].mapK(record)(optionToErrorOr)

    assertEquals(mapped.topicPartition, source.topicPartition)
    assertEquals(mapped.nextOffset, source.nextOffset)
    assertEquals(mapped.commit, Right(()))
    assertEquals(mappedRecord.record, consumerRecord)
    assertEquals(mappedRecord.offset.commit, Right(()))

  test("partition records support natural transformations"):
    val sourceRecord = CommittableConsumerRecord(consumerRecord, committableOffset)
    val source       = PartitionRecords(consumerRecord.topicPartition, Stream.emit(sourceRecord).covary[Option])
    val mapped       = FunctorK[[F[_]] =>> PartitionRecords[F, String, String]].mapK(source)(optionToSyncIO)

    assertEquals(mapped.topicPartition, consumerRecord.topicPartition)
    assertEquals(mapped.records.compile.toList.unsafeRunSync().map(_.record), List(consumerRecord))

  test("offset batches retain the greatest next offset per topic-partition"):
    val otherPartition = Partition.from(1).toOption.get
    val laterOffset    = Offset.from(2L).toOption.get
    val batch          =
      CommittableOffsetBatch.fromFoldable(NonEmptyList.of(committableOffset, offsetAt(partition, laterOffset), offsetAt(otherPartition, nextOffset)))

    assertEquals(batch.size, 2)
    assertEquals(
      batch.offsets(optionCommitter),
      Map(TopicPartition(topic, partition) -> laterOffset, TopicPartition(topic, otherPartition) -> nextOffset)
    )
    assertEquals(batch.commit, Some(()))

  test("offset batches support natural transformations"):
    val batch  = CommittableOffsetBatch.fromFoldable(NonEmptyList.one(committableOffset))
    val mapped = FunctorK[CommittableOffsetBatch].mapK(batch)(optionToErrorOr)

    assertEquals(mapped.commit, Right(()))

  test("individually transformed offsets retain their shared committer"):
    val laterOffset = Offset.from(2L).toOption.get
    val offsets     = NonEmptyList.of(committableOffset, offsetAt(partition, laterOffset)).map(_.mapK(optionToErrorOr))
    val batch       = CommittableOffsetBatch.fromFoldable(offsets)

    assertEquals(batch.offsets.size, 1)
    assertEquals(batch.size, 1)
    assertEquals(batch.offsets.valuesIterator.flatMap(_.valuesIterator).toList, List(laterOffset))
    assertEquals(batch.commit, Right(()))

  test("KafkaProducer transforms both the enqueue and the acknowledgement"):
    val result = ProducerResult(NonEmptyList.one(producerRecord -> None))
    val source =
      new KafkaProducer[Option, String, String]:
        override def produce(records: NonEmptyList[ProducerRecord[String, String]]): Option[Option[ProducerResult[String, String]]] =
          Some(Some(ProducerResult(records.map(_ -> None))))
    val mapped = source.mapK(optionToErrorOr)

    assertEquals(mapped.produce(NonEmptyList.one(producerRecord)), Right(Right(result)))
    assertEquals(mapped.produceAndAwait(NonEmptyList.one(producerRecord)), Right(result))

  test("KafkaConsumer transforms both its stream and committable offsets"):
    val sourceRecord = CommittableConsumerRecord(consumerRecord, committableOffset)
    val source       =
      new KafkaConsumer[Option, String, String]:
        override val records: Stream[Option, CommittableConsumerRecord[Option, String, String]] = Stream.emit(sourceRecord).covary[Option]
        override def assignment: Option[Set[TopicPartition]]                                    = Some(Set(consumerRecord.topicPartition))
        override val assignmentChanges: Stream[Option, Set[TopicPartition]]                     = Stream.emit(Set(consumerRecord.topicPartition))
        override def committed(topicPartitions: Set[TopicPartition]): Option[Map[TopicPartition, Option[Offset]]] =
          Some(topicPartitions.map(_ -> Some(nextOffset)).toMap)
        override def beginningOffsets(topicPartitions: Set[TopicPartition]): Option[Map[TopicPartition, Offset]] =
          Some(topicPartitions.map(_ -> offset).toMap)
        override def endOffsets(topicPartitions: Set[TopicPartition]): Option[Map[TopicPartition, Offset]] =
          Some(topicPartitions.map(_ -> nextOffset).toMap)
        override def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Timestamp]): Option[Map[TopicPartition, Option[Offset]]] =
          Some(timestampsToSearch.keys.map(_ -> Some(offset)).toMap)
        override def partitionsFor(topic: Topic): Option[Set[Partition]]                 = Some(Set(partition))
        override def listTopics: Option[Map[Topic, Set[Partition]]]                      = Some(Map(topic -> Set(partition)))
        override def seek(topicPartition: TopicPartition, offset: Offset): Option[Unit]  = Some(())
        override def seekToBeginning(topicPartitions: Set[TopicPartition]): Option[Unit] = Some(())
        override def seekToEnd(topicPartitions: Set[TopicPartition]): Option[Unit]       = Some(())
        override def position(topicPartition: TopicPartition): Option[Option[Offset]]    = Some(Some(nextOffset))
    val mapped = FunctorK[[F[_]] =>> KafkaConsumer[F, String, String]].mapK(source)(optionToSyncIO)

    val result =
      (
        mapped.records.compile.lastOrError,
        mapped.assignment,
        mapped.committed(Set(consumerRecord.topicPartition)),
        mapped.beginningOffsets(Set(consumerRecord.topicPartition)),
        mapped.endOffsets(Set(consumerRecord.topicPartition)),
        mapped.offsetsForTimes(Map(consumerRecord.topicPartition -> timestamp)),
        mapped.seek(consumerRecord.topicPartition, offset)
      ).flatMapN((record, assignment, committed, beginning, end, timed, _) =>
        record.offset.commit.as((record.record, assignment, committed, beginning, end, timed))
      ).unsafeRunSync()

    assertEquals(
      result,
      (
        consumerRecord,
        Set(consumerRecord.topicPartition),
        Map(consumerRecord.topicPartition -> Some(nextOffset)),
        Map(consumerRecord.topicPartition -> offset),
        Map(consumerRecord.topicPartition -> nextOffset),
        Map(consumerRecord.topicPartition -> Some(offset))
      )
    )
    assertEquals(mapped.partitionsFor(topic).unsafeRunSync(), Set(partition))
    assertEquals(mapped.listTopics.unsafeRunSync(), Map(topic -> Set(partition)))
    assertEquals(mapped.position(consumerRecord.topicPartition).unsafeRunSync(), Some(nextOffset))
    assertEquals(mapped.seekToBeginning(Set(consumerRecord.topicPartition)).unsafeRunSync(), ())
    assertEquals(mapped.seekToEnd(Set(consumerRecord.topicPartition)).unsafeRunSync(), ())

  test("KafkaConsumer keeps backend pausing after mapK"):
    val pausedPartitions = scala.collection.mutable.ListBuffer.empty[Set[TopicPartition]]
    val source           =
      new KafkaConsumer[Option, String, String]:
        override val records: Stream[Option, CommittableConsumerRecord[Option, String, String]] = Stream.empty
        override def assignment: Option[Set[TopicPartition]]                                    = Some(Set(consumerRecord.topicPartition))
        override val assignmentChanges: Stream[Option, Set[TopicPartition]]                     = Stream.emit(Set(consumerRecord.topicPartition))
        override def committed(topicPartitions: Set[TopicPartition]): Option[Map[TopicPartition, Option[Offset]]] = Some(Map.empty)
        override def beginningOffsets(topicPartitions: Set[TopicPartition]): Option[Map[TopicPartition, Offset]]  = Some(Map.empty)
        override def endOffsets(topicPartitions: Set[TopicPartition]): Option[Map[TopicPartition, Offset]]        = Some(Map.empty)
        override def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Timestamp]): Option[Map[TopicPartition, Option[Offset]]] =
          Some(Map.empty)
        override def partitionsFor(topic: Topic): Option[Set[Partition]]                 = Some(Set(partition))
        override def listTopics: Option[Map[Topic, Set[Partition]]]                      = Some(Map.empty)
        override def seek(topicPartition: TopicPartition, offset: Offset): Option[Unit]  = Some(())
        override def seekToBeginning(topicPartitions: Set[TopicPartition]): Option[Unit] = Some(())
        override def seekToEnd(topicPartitions: Set[TopicPartition]): Option[Unit]       = Some(())
        override def position(topicPartition: TopicPartition): Option[Option[Offset]]    = Some(Some(nextOffset))
        override val pausing: PartitionPausing[Option]                                   =
          new PartitionPausing.Backend[Option]:
            override def pause(topicPartitions: Set[TopicPartition]): Option[Unit] =
              pausedPartitions += topicPartitions
              Some(())

            override def resume(topicPartitions: Set[TopicPartition]): Option[Unit] = Some(())

    val mapped = FunctorK[[F[_]] =>> KafkaConsumer[F, String, String]].mapK(source)(optionToSyncIO)
    mapped.pausing.orNoop.pause(Set(consumerRecord.topicPartition)).unsafeRunSync()

    assertEquals(pausedPartitions.toList, List(Set(consumerRecord.topicPartition)))

  private def committableOffset: CommittableOffset[Option] = offsetAt(partition, nextOffset)

  private val optionCommitter: OffsetCommitter[Option] =
    new OffsetCommitter[Option]:
      override def commit(offsets: Map[TopicPartition, Offset]): Option[Unit] = Some(())

  private def offsetAt(partition: Partition, nextOffsetValue: Offset): CommittableOffset[Option] =
    new CommittableOffset[Option]:
      override val topicPartition: TopicPartition     = TopicPartition(topic, partition)
      override val nextOffset: Offset                 = nextOffsetValue
      override val committer: OffsetCommitter[Option] = optionCommitter
