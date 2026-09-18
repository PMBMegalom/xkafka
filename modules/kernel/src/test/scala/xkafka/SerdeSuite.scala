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

import cats.Contravariant
import cats.Functor
import cats.arrow.FunctionK
import cats.tagless.FunctorK
import fs2.Chunk
import munit.FunSuite

final class SerdeSuite extends FunSuite:
  private type ErrorOr[A] = Either[String, A]

  private val topic = Topic.from("events").toOption.get
  private val bytes = Some(Chunk.array(Array[Byte](1, 2, 3)))

  private val optionToErrorOr: FunctionK[Option, ErrorOr] =
    new FunctionK[Option, ErrorOr]:
      override def apply[A](value: Option[A]): ErrorOr[A] =
        value.toRight("empty")

  test("Serializer is contravariant in its input") {
    val serializer = Serializer.instance[Option, Int]((_, _, value) => Some(Some(Chunk.singleton(value.toByte))))
    val strings    = Contravariant[[A] =>> Serializer[Option, A]]
      .contramap(serializer)((value: String) => value.length)

    assertEquals(
      strings.serialize(topic, Headers.empty, "abc").map(_.map(_.toList)),
      Some(Some(List[Byte](3)))
    )
  }

  test("Deserializer is covariant in its output") {
    val deserializer = Deserializer.instance[Option, Int]((_, _, value) => value.map(_.size))
    val strings      = Functor[[A] =>> Deserializer[Option, A]]
      .map(deserializer)(_.toString)

    assertEquals(
      strings.deserialize(topic, Headers.empty, bytes),
      Some("3")
    )
  }

  test("Serializer supports natural transformations of its effect") {
    val serializer  = Serializer.instance[Option, String]((_, _, _) => Some(bytes))
    val transformed = FunctorK[[F[_]] =>> Serializer[F, String]]
      .mapK(serializer)(optionToErrorOr)

    assertEquals(
      transformed.serialize(topic, Headers.empty, "value").map(_.map(_.toList)),
      Right(Some(List[Byte](1, 2, 3)))
    )
  }

  test("Deserializer supports natural transformations of its effect") {
    val deserializer = Deserializer.instance[Option, Int]((_, _, value) => value.map(_.size))
    val transformed  = FunctorK[[F[_]] =>> Deserializer[F, Int]]
      .mapK(deserializer)(optionToErrorOr)

    assertEquals(
      transformed.deserialize(topic, Headers.empty, bytes),
      Right(3)
    )
  }
