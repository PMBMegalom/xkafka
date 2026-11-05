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
package streams

import scala.concurrent.duration.*

import cats.arrow.FunctionK
import cats.effect.IO
import cats.effect.std.Console
import munit.CatsEffectSuite
import internal.streams.{Task, TaskInput, TaskOutput, TaskState}

final class BranchingSuite extends CatsEffectSuite:
  private val text  = Serde.utf8[IO]
  private val count = Serde.long[IO]
  private val out   = Topic.from("out").orFail

  private def topic(name: String)                               = Topic.from(name).orFail
  private def read(name: String)                                = KStream(Named(name), topic(name), text, text)
  private def refused(first: Pipeline[IO], rest: Pipeline[IO]*) = Topology.of(first, rest*).left.toOption.map(_.toList)
  private def bytes(value: String)                              = Some(fs2.Chunk.array(value.getBytes))
  private def written(from: TaskOutput)                         = (from.topic.value, from.value.map(v => String(v.toArray)))

  test("the same stream written twice reads its topic twice, which is refused for into or fork"):
    val in = read("in")
    assert(
      refused(in.to(Named("a"), topic("a"), text, text), in.to(Named("b"), topic("b"), text, text))
        .exists(_.contains(TopologyError.DuplicateSource(topic("in"))))
    )
    assert(TopologyError.DuplicateSource(topic("in")).message.contains("into or fork"))

  test("a step of a stream written twice is refused as two steps of one name"):
    val in    = read("in").filter(Named("kept"))((_, _) => true)
    val found = refused(in.to(Named("a"), topic("a"), text, text), in.to(Named("b"), topic("b"), text, text))
    assert(found.exists(_.contains(TopologyError.DuplicateName("kept"))), clue(found))

  test("one node reached down two paths is one step, not two of one name"):
    val shared = read("in").flow.filter(Named("kept"))((_, _) => true).to(Named("write"), out, text, text)
    assert(Topology.of(read("in").into(shared, read("in").flow.mapValues(Named("m"))(identity).into(shared))).isRight)

  test("two different steps of one name in a flow are still refused"):
    val found =
      refused(read("in").fork(
        _.filter(Named("same"))((_, _) => true).to(Named("a"), topic("a"), text, text),
        _.mapValues(Named("same"))(identity).to(Named("b"), topic("b"), text, text)
      ))
    assertEquals(found, Some(List(TopologyError.DuplicateName("same"))))

  test("split branch names are checked as the steps they make"):
    val found =
      refused(
        read("in").split(Named("route-")).branch(Named("a"))((_, _) => true)(_.to(Named("route-b"), topic("a"), text, text))
          .branch(Named("b"))((_, _) => true)(_.to(Named("w"), topic("b"), text, text)).noDefaultBranch
      )
    assertEquals(found, Some(List(TopologyError.DuplicateName("route-b"))))

  test("a table made from a flow that goes nowhere cannot be read, and says which"):
    val counts = Flow[IO, String, String].groupByKey(Repartition("r"), text, text).count(Named("counted"), Store("counts"))
    val found  = refused(read("in").join(Named("joined"), counts)((v, n) => s"$v$n").to(Named("write"), out, text, text))
    assertEquals(found, Some(List(TopologyError.UnfedTable("counted"))))
    assert(TopologyError.UnfedTable("counted").message.contains("'counted'"))

  test("two tables with one store are refused as such, a table only being left to the program's own reading of it for the same topic"):
    val own   = KTable(Named("own"), topic("own"), text, text, Store("shared"))
    val read  = KTable(Named("read"), topic("read"), text, text, Store("shared"))
    val found =
      refused(
        own.toStream(Named("t")).to(Named("a"), topic("a"), text, text.option),
        this.read("in").join(Named("j"), read)(_ + _).to(Named("b"), topic("b"), text, text)
      )
    assertEquals(found, Some(List(TopologyError.DuplicateStore("shared"))))

  test("a table fed only by what a join of it makes is refused as unfed"):
    val feed: KTable.Flowing[IO, String, String, Option, Into[IO, String, String, KeyState]] =
      Flow[IO, String, String].toTable(Named("table"), Store("table"), text, text)
    val found = refused(read("in").join(Named("joined"), feed)(_ + _).into(feed.consumer))
    assert(found.exists(_.contains(TopologyError.UnfedTable("table"))), clue(found))

  test("a flow that repartitions for records that all arrive keyed as they were read is refused, and one a changed key reaches keeps it"):
    val plain = Flow[IO, String, String].groupByKey(Repartition("r"), text, text).count(Named("counted"), Store("counts")).consumer
    assertEquals(refused(read("in").into(plain)), Some(List(TopologyError.UnusedRepartition("r"))))
    val rekeyed = Flow[IO, String, String].selectKey(Named("sk"))((_, v) => v).into(plain)
    assertEquals(refused(read("in").into(plain, rekeyed)), None)
    assert(TopologyError.UnusedRepartition("r").message.contains("Flow.asRead"))

  test("a flow's other joins and tables are refused the same way when no changed key reaches them"):
    val users  = KTable(Named("users"), topic("users"), text, text, Store("users"))
    val joined = Flow[IO, String, String].join(Named("j"), users, text, text)(_ + _).to(Named("write"), out, text, text)
    assertEquals(refused(read("in").into(joined)), Some(List(TopologyError.UnusedRepartition("j"))))
    val tabled = Flow[IO, String, String].toTable(Named("t"), Store("t"), text, text).consumer
    assertEquals(refused(read("in").into(tabled)), Some(List(TopologyError.UnusedRepartition("t"))))
    val pairs =
      Flow[IO, String, String]
        .join(Named("p"), Flow[IO, String, String], JoinWindows(1.second, grace = 0.seconds), Store("p"), text, text, text)(_ + _)
        .to(Named("write"), out, text, text)
    assertEquals(refused(read("in").into(pairs)), Some(List(TopologyError.UnusedRepartition("p-left"), TopologyError.UnusedRepartition("p-right"))))
    assertEquals(refused(read("in").selectKey(Named("sk"))((_, v) => v).into(pairs)), None)
    assertEquals(refused(read("in").filter(Named("f"))((_, _) => true).selectKey(Named("sk"))((_, v) => v).into(joined, tabled)), None)

  test("a repartition after one that settled the key is refused where nothing changed it since, and one the program asks for itself is not"):
    val again = Flow[IO, String, String].groupByKey(Repartition("r2"), text, text).count(Named("counted"), Store("counts")).consumer
    assertEquals(
      refused(read("in").selectKey(Named("sk"))((_, v) => v).repartition(Repartition("r1"), text, text).into(again)),
      Some(List(TopologyError.UnusedRepartition("r2")))
    )
    assertEquals(refused(read("in").repartition(Repartition("r1"), text, text).to(Named("write"), out, text, text)), None)

  test("into takes any number of flows, and refuses in words one that cannot take the records, wherever it is, and none at all"):
    def path(n: Int) = Flow.asRead[IO, String, String].to(Named(s"w$n"), topic(s"t$n"), text, text)
    // format: off
    val many = read("in").into(path(1), path(2), path(3), path(4), path(5), path(6), path(7), path(8), path(9), path(10), path(11), path(12), path(13), path(14), path(15), path(16), path(17), path(18), path(19), path(20), path(21), path(22), path(23), path(24), path(25))
    // format: on
    assertEquals(refused(many), None)
    assertEquals(Topology.of(many).orFail.describe.toString.split("Sink: ").length - 1, 25)
    val asRead = Flow.asRead[IO, String, String].groupByKey(text, text).count(Named("counted"), Store("counts")).consumer
    val any    = Flow[IO, String, String].to(Named("any"), out, text, text)
    assertEquals(refused(read("in").selectKey(Named("sk"))((_, v) => v).into(any)), None)
    assertEquals(refused(read("in").into(asRead)), None)
    assert(
      compileErrors("""read("in").selectKey(Named("sk"))((_, v) => v).into(asRead, any, any, any, any)""")
        .contains("this flow takes them keyed as they were read")
    )
    assert(
      compileErrors("""read("in").selectKey(Named("sk"))((_, v) => v).into(any, any, asRead, any, any)""")
        .contains("this flow takes them keyed as they were read")
    )
    assert(
      compileErrors("""read("in").selectKey(Named("sk"))((_, v) => v).into(any, any, any, any, asRead)""")
        .contains("this flow takes them keyed as they were read")
    )
    assert(compileErrors("""read("in").into(Flow[IO, Int, String], any, any, any, any)""").contains("into can only send records to a flow"))
    assert(compileErrors("""read("in").into(any, any, Flow[IO, Int, String], any, any)""").contains("into can only send records to a flow"))
    assert(compileErrors("""read("in").into(any, any, any, any, Flow[IO, Int, String])""").contains("into can only send records to a flow"))
    assert(compileErrors("""read("in").into(EmptyTuple)""").contains("into can only send records to a flow"))
    val table = KTable(Named("users"), topic("users"), text, text, Store("users"))
    assert(compileErrors("""table.into(EmptyTuple)""").contains("into can only send records to a flow"))
    def changes(n: Int) = Flow.asRead[IO, String, Option[String]].to(Named(s"c$n"), topic(s"c$n"), text, text.option)
    assertEquals(refused(table.into(changes(1), changes(2), changes(3), changes(4), changes(5))), None)

  test("a flow says what key its records may have, and into refuses what cannot go to it, in words"):
    val asRead = Flow.asRead[IO, String, String].groupByKey(text, text).count(Named("counted"), Store("counts")).consumer
    assertEquals(refused(read("in").into(asRead)), None)
    assert(compileErrors("""read("in").selectKey(Named("sk"))((_, v) => v).into(asRead)""").contains("this flow takes them keyed as they were read"))
    assert(
      compileErrors("""read("in").into(Flow[IO, Int, String].to(Named("w"), out, Serde.utf8[IO].contramap(_.toString, _.toInt), text))""").nonEmpty
    )
    assert(compileErrors("""read("in").into(Flow[IO, Int, String])""").contains("into can only send records to a flow"))
    assert(compileErrors("""Flow[IO, String, String].groupByKey(text, text)""").contains("so they go through a repartition topic first"))
    assert(
      compileErrors("""Flow.asRead[IO, String, String].groupByKey(Repartition("r"), text, text)""").contains("so they need no repartition topic")
    )

  test("a merge is keyed as read only if both sides are"):
    assertEquals(
      refused(
        read("a").merge(Named("m"), read("b")).groupByKey(text, text).count(Named("c"), Store("s")).toStream(Named("t"))
          .to(Named("w"), out, text, count)
      ),
      None
    )
    val found = compileErrors("""read("a").selectKey(Named("sk"))((_, v) => v).merge(Named("m"), read("b")).groupByKey(text, text)""")
    assert(found.contains("so they go through a repartition topic first"), clue(found))
    val other = compileErrors("""read("a").merge(Named("m"), read("b").selectKey(Named("sk"))((_, v) => v)).groupByKey(text, text)""")
    assert(other.contains("so they go through a repartition topic first"), clue(other))
    assertEquals(
      refused(
        read("a").merge(Named("m"), read("b").selectKey(Named("sk"))((_, v) => v)).groupByKey(Repartition("r"), text, text)
          .count(Named("c"), Store("s")).toStream(Named("t")).to(Named("w"), out, text, count)
      ),
      None
    )

  test("a forkMerge is keyed as read only if each path is, and is built to repartition if any path changes the key"):
    def merged = read("in").forkMerge(Named("back"))(_.filter(Named("a"))((_, _) => true), _.mapValues(Named("b"))(identity))
    assert(Topology.of(merged.groupByKey(text, text).count(Named("c"), Store("s")).toStream(Named("t")).to(Named("w"), out, text, count)).isRight)
    def rekeyed = read("in").forkMerge(Named("back"))(_.filter(Named("a"))((_, _) => true), _.selectKey(Named("b"))((_, v) => v))
    assert(compileErrors("""rekeyed.groupByKey(text, text)""").contains("so they go through a repartition topic first"))
    val tabled = rekeyed.toTable(Named("table"), Store("table"), text, text).toStream(Named("t")).to(Named("w"), out, text, text.option)
    assertEquals(Topology.of(tabled).map(_.subtopologies.size), Right(2))

  test("a topology mapped to another effect describes and runs the same, its joins' two sides one node"):
    val left   = read("left")
    val right  = read("right")
    val joined =
      left.join(Named("p"), right, JoinWindows(1.second, grace = 0.seconds), Store("p"), text, text, text)(_ + _)
        .forkMerge(Named("back"))(_.filter(Named("a"))((_, _) => true), _.mapValues(Named("b"))(identity)).to(Named("write"), out, text, text)
    val topology = Topology.of(joined).orFail
    assertEquals(topology.mapK(FunctionK.id[IO]).describe.toString, topology.describe.toString)
    val mapped = Topology.of(joined.mapK(FunctionK.id[IO])).orFail
    assertEquals(mapped.describe.toString, topology.describe.toString)
    assertEquals(mapped.subtopologies.flatMap(_.stores), topology.subtopologies.flatMap(_.stores))
    val task = Task[IO]("app", topology.mapK(FunctionK.id[IO]).subtopologies.head, 0)
    for
      first  <- task.process(TaskState.empty, TaskInput(bytes("k"), bytes("l"), 1000L, Headers.empty, "left"), 0L)
      second <- task.process(first._1, TaskInput(bytes("k"), bytes("r"), 1000L, Headers.empty, "right"), 0L)
    yield assertEquals(second._2.filter(_.topic == out).map(written), Vector(("out", Some("lr")), ("out", Some("lr"))))

  test("many forks that merge back in a row are walked once, whether mapped or not, however many paths lead through them"):
    val depth = 40
    val tail  = Flow.asRead[IO, String, String].to(Named("write"), out, text, text)
    val chain = (1 to depth).foldLeft(tail): (next, n) =>
      val back = Flow.asRead[IO, String, String].merged(Named(s"back-$n")).into(next)
      Flow.asRead[IO, String, String].split(Named(s"s-$n-")).branch(Named("a"))((_, v) => v.startsWith("a"))(_.into(back))
        .defaultBranch(Named("b"))(_.into(back))
    val topology = Topology.of(read("in").into(chain)).orFail
    assertEquals(topology.subtopologies.head.nodes.size, 2 + 4 * depth)
    val mapped = Topology.of(read("in").into(chain).mapK(FunctionK.id[IO])).orFail
    assertEquals(mapped.subtopologies.head.nodes.size, 2 + 4 * depth)
    Task[IO]("app", topology.mapK(FunctionK.id[IO]).subtopologies.head, 0)
      .process(TaskState.empty, TaskInput(bytes("k"), bytes("a"), 0L, Headers.empty, "in"), 0L).map: (_, written) =>
        assertEquals(written.map(_.topic), Vector(out))

  test("a program a flow prints in, labelled by its own name with no step before it, and by the step before where there is one"):
    given Console[IO] = Console.make[IO]
    val named         = Flow.asRead[IO, String, String].print(Named("shown"))
    val after         = Flow.asRead[IO, String, String].filter(Named("kept"))((_, _) => true).print(Named("shown-after"))
    assert(Topology.of(read("in").into(named, after)).isRight)
