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

import cats.Show
import cats.data.Validated
import cats.effect.IO
import cats.effect.std.UUIDGen
import cats.syntax.show.*
import munit.{Assertions, Location}

extension [E: Show, A](result: Either[E, A])
  /** The value a test built, or a failure at the caller's line saying why the value is invalid. */
  def orFail(using Location): A = result.fold(error => Assertions.fail(s"invalid test value: ${error.show}"), identity)

extension [E: Show, A](result: Validated[E, A])
  /** The value a test built, or a failure at the caller's line saying why the value is invalid. */
  def orFail(using Location): A = result.toEither.orFail

/** What makes the names one test gives its topics, groups and transactions its own, so tests that share a broker never meet: a random id drawn as the
  * test starts.
  */
final case class Names(id: String):
  /** `label`, made this test's own. */
  def apply(label: String): String = s"$label-$id"

object Names:
  /** Names of their own, drawn when the effect runs. */
  val draw: IO[Names] = UUIDGen[IO].randomUUID.map(uuid => Names(uuid.toString))

  /** Runs `test` with names of its own. */
  def fresh[A](test: Names ?=> IO[A]): IO[A] = draw.flatMap(names => test(using names))
