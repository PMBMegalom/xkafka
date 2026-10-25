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

import scala.concurrent.duration.*

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.IO
import cats.syntax.all.*
import munit.{CatsEffectSuite, TestOptions}

/** What the streams engine needs from each platform's driver beyond what the clients use, asserted identically on each one against the broker. */
final class StreamsDriverSuite extends CatsEffectSuite:
  override val munitIOTimeout: Duration = 3.minutes

  private val backend          = PlatformKafkaClient.name
  private val bootstrapServers = PlatformKafkaClient.integrationBootstrapServers
  private val driver           = KafkaClientPlatform.driver[IO]

  private def broker(name: String): TestOptions = if bootstrapServers.isEmpty then TestOptions(name).ignore else TestOptions(name)

  private def withBroker(run: String => IO[Unit]): IO[Unit] = bootstrapServers.fold(IO.unit)(run)

  test(broker("a transaction records offsets under a group identity built from its parts")):
    withBroker: server =>
      val topic = uniqueTopic("identity")
      val group = uniqueGroup("identity")

      for
        _      <- transactionUnder(server, topic, group, Identity(-1, ""), 7L)
        stored <- groupOffsets(server, group)
      yield assertEquals(stored, Map(partitionOf(topic) -> 7L))

  test(broker("a transaction under a member the group does not have is refused, and leaves the group's offsets as they were")):
    withBroker: server =>
      val topic = uniqueTopic("stranger")
      val group = uniqueGroup("stranger")

      for
        _       <- transactionUnder(server, topic, group, Identity(-1, ""), 7L)
        refused <- transactionUnder(server, topic, group, Identity(5, "stranger-member"), 9L).attempt
        stored  <- groupOffsets(server, group)
      yield
        assertEquals(
          refused.left.toOption.collect { case failure: KafkaException.BackendFailure => failure.code }.flatten,
          Some(ErrorCode.UnknownMemberId)
        )
        assertEquals(stored, Map(partitionOf(topic) -> 7L))

  test(broker("a transaction under a member the group has, at a generation it is not at, is refused for its generation")):
    withBroker: server =>
      val topic = uniqueTopic("generation")
      val group = uniqueGroup("generation")

      for
        client   <- ClientSettings.from(NonEmptyList.one(server)).liftTo[IO]
        newTopic <- NewTopic.from(topic, partitions = 1, replicationFactor = 1).liftTo[IO]
        _        <- PlatformKafkaClient().admin(client).use(_.createTopics(NonEmptySet.one(newTopic)))
        settings <- ConsumerSettings.from(client, group, Deserializer.utf8[IO], Deserializer.utf8[IO], AutoOffsetReset.Earliest).liftTo[IO]
        refused  <-
          PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic))).use: consumer =>
            consumer.assignmentChanges.filter(_.nonEmpty).head.compile.drain.timeout(60.seconds) *> memberOf(client, group)
              .flatMap(member => transactionUnder(server, topic, group, Identity(1000000, member), 9L).attempt)
        stored <- groupOffsets(server, group)
      yield
        assertEquals(codeOf(refused), Some(ErrorCode.IllegalGeneration))
        assertEquals(stored, Map.empty)

  private def codeOf(outcome: Either[Throwable, Unit]): Option[ErrorCode] =
    outcome.left.toOption.collect { case failure: KafkaException.BackendFailure => failure.code }.flatten

  /** The id of the group's only member. */
  private def memberOf(client: ClientSettings, group: ConsumerGroup): IO[String] =
    PlatformKafkaClient().admin(client).use(_.describeConsumerGroups(NonEmptySet.one(group)))
      .map(_(group).flatMap(_.toOption).toList.flatMap(_.members)).flatMap:
        case List(member) => IO.pure(member.memberId)
        case other        => IO.raiseError(new IllegalStateException(s"expected one member, found $other"))

  private final case class Identity(generation: Int, memberId: String)

  /** Writes one record and records `next` for the topic's only partition in the same transaction, under the given identity. */
  private def transactionUnder(server: String, topic: Topic, group: ConsumerGroup, as: Identity, next: Long): IO[Unit] =
    val named =
      GroupMembership.Backend[IO](
        driver.groupMetadataFor(group, as.generation, as.memberId).widen[GroupHandle],
        handle => driver.asGroupMetadata(handle).traverse_(driver.destroyGroupMetadata)
      )
    val offset =
      new CommittableOffset[IO]:
        override val topicPartition: TopicPartition                  = partitionOf(topic)
        override val nextOffset: Offset                              = Offset.from(next).fold(error => fail(error.message), identity)
        override val committer: OffsetCommitter[IO]                  = NoCommitter
        override private[xkafka] def membership: GroupMembership[IO] = named

    for
      client   <- ClientSettings.from(NonEmptyList.one(server)).liftTo[IO]
      settings <- TransactionalProducerSettings.from(client, uniqueTransactionalId(), Serializer.utf8[IO], Serializer.utf8[IO]).liftTo[IO]
      _        <-
        PlatformKafkaClient().transactionalProducer(settings).use: producer =>
          producer.transactionally: transaction =>
            transaction.produce(NonEmptyList.one(ProducerRecord(topic, "key", "value"))) *>
              transaction.commitOffsets(CommittableOffsetBatch.fromFoldable(List(offset)))
    yield ()

  /** Offsets here are only ever recorded by a transaction. */
  private object NoCommitter extends OffsetCommitter[IO]:
    override def commitWithMetadata(offsets: Map[TopicPartition, OffsetAndMetadata]): IO[Unit] = IO.raiseError(new IllegalStateException("unused"))

  private def groupOffsets(server: String, group: ConsumerGroup): IO[Map[TopicPartition, Long]] =
    ClientSettings.from(NonEmptyList.one(server)).liftTo[IO].flatMap: client =>
      PlatformKafkaClient().admin(client).use(_.listConsumerGroupOffsets(group)).map(_.view.mapValues(_.offset.value).toMap)

  private def partitionOf(topic: Topic): TopicPartition = TopicPartition(topic, Partition.from(0).fold(error => fail(error.message), identity))

  private def uniqueTopic(label: String): Topic =
    Topic.from(s"xkafka-streams-driver-$label-$backend-${System.nanoTime()}").fold(error => fail(error.message), identity)

  private def uniqueGroup(label: String): ConsumerGroup =
    ConsumerGroup.from(s"xkafka-streams-driver-$label-$backend-${System.nanoTime()}").fold(error => fail(error.message), identity)

  private def uniqueTransactionalId(): TransactionalId =
    TransactionalId.from(s"xkafka-streams-driver-$backend-${System.nanoTime()}").fold(error => fail(error.message), identity)
