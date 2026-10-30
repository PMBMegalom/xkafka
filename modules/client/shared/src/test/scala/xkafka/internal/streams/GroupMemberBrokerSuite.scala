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
package internal.streams

import scala.concurrent.duration.*

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{IO, Ref, Resource}
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import munit.CatsEffectSuite
import internal.group.{Connections, PlatformCrypto, PlatformSockets}
import internal.protocol.{OffsetCommitRequest, OffsetFetchRequest, StreamsGroup}

/** Streams group members joining the Java Streams application `scripts/integration-test.sh` runs, on each broker release it lists. */
final class GroupMemberBrokerSuite extends CatsEffectSuite:
  override val munitIOTimeout: Duration = 3.minutes

  private val application = s"xkafka-streams-${PlatformKafkaClient.name}"
  private val topology    = StreamsGroup.Topology(0, List(StreamsGroup.Subtopology("0", List(s"$application-input"), Nil, Nil, Nil, Nil, Nil)))

  /** A member whose engine runs what it is told, and gives back tasks only if `obliging`, with what it runs and every change it was told of. */
  private final class Running(
      val member: GroupMember[IO],
      val memberId: String,
      val owned: Ref[IO, Assignment],
      val told: Ref[IO, Vector[MemberAction]]
  ):
    def active: IO[Set[TaskId]] = owned.get.map(_.active)

  private def connections(server: String): Connections[IO] =
    val settings = ClientSettings.from(NonEmptyList.one(server), Some("xkafka-streams-member")).orFail
    Connections(PlatformSockets[IO], PlatformCrypto[IO], settings, 30.seconds)

  private def running(
      server: String,
      rebalanceTimeout: FiniteDuration = 30.seconds,
      obliging: Boolean = true,
      group: String = application,
      declared: StreamsGroup.Topology = topology
  ): Resource[IO, Running] =
    for
      memberId  <- Resource.eval(GroupMember.memberId[IO])
      processId <- Resource.eval(UUIDGen[IO].randomUUID.map(_.toString))
      identity = MemberIdentity(group, memberId, processId, declared, rebalanceTimeout.toMillis.toInt, None, Nil, None)
      member <- GroupMember(connections(server).coordinator(group), identity, 10.seconds)
      owned  <- Resource.eval(IO.ref(Assignment.empty))
      told   <- Resource.eval(IO.ref(Vector.empty[MemberAction]))
      engine =
        member.changes.evalMap: change =>
          told.update(_ :+ change) *>
            (change match
              case MemberAction.Assign(round, assignment)        => owned.set(assignment) *> member.assigned(round)
              case MemberAction.Revoke(round, tasks) if obliging => owned.update(o => o.copy(active = o.active -- tasks)) *> member.revoked(round)
              case MemberAction.LoseAll                          => owned.set(Assignment.empty)
              case _                                             => IO.unit)
      _ <- engine.compile.drain.background
    yield Running(member, memberId, owned, told)

  /** Waits up to a minute for `condition` to hold. */
  private def eventually(what: String)(condition: IO[Boolean]): IO[Unit] =
    def loop(left: Int): IO[Unit] =
      condition.flatMap: holds =>
        if holds then IO.unit
        else if left <= 0 then IO.raiseError(new AssertionError(s"$what did not happen within a minute"))
        else IO.sleep(250.millis) *> loop(left - 1)
    loop(240)

  private val brokers =
    PlatformKafkaClient.environment("XKAFKA_STREAMS_BROKERS").toList.flatMap(_.split(',').toList).map: entry =>
      val Array(release, server) = entry.split('=')
      (release, server)

  /** Offsets here are only ever recorded by a transaction. */
  private object Unused extends OffsetCommitter[IO]:
    override def commitWithMetadata(offsets: Map[TopicPartition, OffsetAndMetadata]): IO[Unit] = IO.raiseError(new IllegalStateException("unused"))

  /** A member of a new group of its own, without Java, reading a new topic of two partitions, once the coordinator has taken it in. */
  private def alone(server: String)(test: (Running, String, String) => IO[Unit]): IO[Unit] =
    Names.fresh:
      val group    = summon[Names](s"xkafka-streams-offsets-${PlatformKafkaClient.name}")
      val topic    = s"$group-input"
      val made     = NewTopic.from(Topic.from(topic).orFail, 2, 1).orFail
      val admin    = PlatformKafkaClient().admin(ClientSettings.from(NonEmptyList.one(server)).orFail)
      val declared = StreamsGroup.Topology(0, List(StreamsGroup.Subtopology("0", List(topic), Nil, Nil, Nil, Nil, Nil)))
      admin.use(_.createTopics(NonEmptySet.one(made))) *> running(server, group = group, declared = declared).use: one =>
        eventually("the coordinator taking the member in")(one.member.epoch.map(_ > 0)) *> test(one, group, topic) *> one.member.leave

  brokers.foreach: (release, server) =>
    test(s"$release: a member commits offsets under its epoch and reads them back"):
      alone(server): (one, _, topic) =>
        import OffsetCommitRequest.{Answer, Partition, Topic as Committed}
        import OffsetFetchRequest.{Stored, Topic as Asked}
        for
          answers <- one.member.commit(List(Committed(topic, List(Partition(0, 5L, -1, Some("first")), Partition(1, 7L, 3, None)))))
          stored  <- one.member.committed(List(Asked(topic, List(0, 1))))
        yield
          assertEquals(answers, List(Answer(topic, 0, 0), Answer(topic, 1, 0)))
          assertEquals(stored, OffsetFetchRequest.Answer(List(Stored(topic, 0, 5L, -1, Some("first"), 0), Stored(topic, 1, 7L, 3, Some(""), 0)), 0))

    test(s"$release: offsets a transaction records for the member are read only once it commits"):
      alone(server): (one, group, topic) =>
        val driver    = KafkaClientPlatform.driver[IO]
        val asked     = List(OffsetFetchRequest.Topic(topic, List(0)))
        val committed = one.member.committed(asked).map(_.partitions.map(stored => (stored.offset, stored.errorCode)))
        for
          epoch <- one.member.epoch
          named =
            GroupMembership.Backend[IO](
              driver.groupMetadataFor(ConsumerGroup.from(group).orFail, epoch, one.memberId).widen[GroupHandle],
              handle => driver.asGroupMetadata(handle).traverse_(driver.destroyGroupMetadata)
            )
          offset =
            new CommittableOffset[IO]:
              override val topicPartition: TopicPartition                  = TopicPartition(Topic.from(topic).orFail, Partition.from(0).orFail)
              override val nextOffset: Offset                              = Offset.from(4L).orFail
              override val committer: OffsetCommitter[IO]                  = Unused
              override private[xkafka] def membership: GroupMembership[IO] = named
          client   = ClientSettings.from(NonEmptyList.one(server)).orFail
          settings =
            TransactionalProducerSettings.from(client, TransactionalId.from(s"$group-transaction").orFail, Serializer.utf8[IO], Serializer.utf8[IO])
              .orFail
          pending <-
            PlatformKafkaClient().transactionalProducer(settings).use: producer =>
              producer.transactionally(transaction => transaction.commitOffsets(CommittableOffsetBatch.fromFoldable(List(offset))) *> committed)
          // The coordinator writes the transaction's markers after the commit returns.
          _ <- eventually("the committed offset being read")(committed.map(_ == List((4L, 0.toShort))))
        // UNSTABLE_OFFSET_COMMIT, with no offset.
        yield assertEquals(pending, List((-1L, 88.toShort)))

    test(s"$release: a commit under an epoch the member is not at, or an id the group does not have, is refused and stores nothing"):
      alone(server): (one, group, topic) =>
        import OffsetCommitRequest.{Answer, Partition, Topic as Committed}
        def commit(memberId: String, epoch: Int) =
          connections(server).coordinator(group)
            .use(_.send(OffsetCommitRequest(group, memberId, epoch, None, List(Committed(topic, List(Partition(0, 9L, -1, None)))))))
        for
          epoch   <- one.member.epoch
          ahead   <- commit(one.memberId, epoch + 1)
          behind  <- commit(one.memberId, epoch - 1)
          unknown <- commit("stranger", epoch)
          stored  <- one.member.committed(List(OffsetFetchRequest.Topic(topic, List(0))))
        yield
          // STALE_MEMBER_EPOCH either way, and UNKNOWN_MEMBER_ID.
          assertEquals((ahead, behind, unknown), (List(Answer(topic, 0, 113)), List(Answer(topic, 0, 113)), List(Answer(topic, 0, 25))))
          assertEquals(stored.partitions.map(_.offset), List(-1L))

    test(s"$release: a member joins beside a Java member, takes half the tasks and leaves"):
      running(server).use: one =>
        eventually("a member taking half the tasks")(one.active.map(_.size == 2)) *> one.member.leave

    test(s"$release: a member gives back a task to members that join, and takes tasks back once they leave"):
      running(server).use: first =>
        for
          _ <- eventually("the first member taking half the tasks")(first.active.map(_.size == 2))
          _ <-
            (running(server), running(server)).tupled.use: (second, third) =>
              val members = List(first, second, third)
              eventually("the four members running one task each")(
                members.traverse(_.active).map(tasks => tasks.forall(_.size == 1) && tasks.combineAll.size == 3)
              ) *>
                first.told.get.map(told => assert(told.exists(_.isInstanceOf[MemberAction.Revoke]), s"the first member gave nothing back: $told")) *>
                second.member.leave *> third.member.leave
          _ <- eventually("the first member taking tasks back")(first.active.map(_.size == 2))
          _ <- first.member.leave
        yield ()

    test(s"$release: a member that holds on to its tasks past its rebalance timeout is fenced, drops them and joins again"):
      running(server, rebalanceTimeout = 3.seconds, obliging = false).use: stubborn =>
        for
          _ <- eventually("the member taking half the tasks")(stubborn.active.map(_.size == 2))
          // With four members on four tasks, the stubborn member is asked for one of its two back.
          _ <-
            (running(server), running(server)).tupled.use: (second, third) =>
              eventually("the member dropping its tasks and getting new ones")(
                stubborn.told.get.map(told => told.dropWhile(_ != MemberAction.LoseAll).exists(_.isInstanceOf[MemberAction.Assign]))
              ) *> second.member.leave *> third.member.leave
        yield ()
