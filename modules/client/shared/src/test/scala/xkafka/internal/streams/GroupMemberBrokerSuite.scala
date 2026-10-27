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

import cats.data.NonEmptyList
import cats.effect.{IO, Ref, Resource}
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import munit.CatsEffectSuite
import internal.group.{Connections, PlatformCrypto, PlatformSockets}
import internal.protocol.StreamsGroup

/** Streams group members joining the Java Streams application `scripts/integration-test.sh` runs, on each broker release it lists. */
final class GroupMemberBrokerSuite extends CatsEffectSuite:
  override val munitIOTimeout: Duration = 3.minutes

  private val application = s"xkafka-streams-${PlatformKafkaClient.name}"
  private val topology    = StreamsGroup.Topology(0, List(StreamsGroup.Subtopology("0", List(s"$application-input"), Nil, Nil, Nil, Nil, Nil)))

  /** A member whose engine runs what it is told, and gives back tasks only if `obliging`, with what it runs and every change it was told of. */
  private final class Running(val member: GroupMember[IO], val owned: Ref[IO, Assignment], val told: Ref[IO, Vector[MemberAction]]):
    def active: IO[Set[TaskId]] = owned.get.map(_.active)

  private def running(server: String, rebalanceTimeout: FiniteDuration = 30.seconds, obliging: Boolean = true): Resource[IO, Running] =
    val settings    = ClientSettings.from(NonEmptyList.one(server), Some("xkafka-streams-member")).orFail
    val connections = Connections(PlatformSockets[IO], PlatformCrypto[IO], settings, 30.seconds)
    for
      memberId  <- Resource.eval(GroupMember.memberId[IO])
      processId <- Resource.eval(UUIDGen[IO].randomUUID.map(_.toString))
      identity = MemberIdentity(application, memberId, processId, topology, rebalanceTimeout.toMillis.toInt, None, Nil, None)
      member <- GroupMember(connections.coordinator(application), identity, 10.seconds)
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
    yield Running(member, owned, told)

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

  brokers.foreach: (release, server) =>
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
