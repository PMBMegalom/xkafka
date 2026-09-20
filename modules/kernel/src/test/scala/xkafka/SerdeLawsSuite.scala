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

import cats.{Eq, Id}
import cats.laws.discipline.{ContravariantTests, FunctorTests}
import fs2.Chunk
import munit.DisciplineSuite
import org.scalacheck.{Arbitrary, Cogen}

/** Serializers and deserializers are function-like, so their `Eq` compares behaviour over a fixed sample of inputs. */
final class SerdeLawsSuite extends DisciplineSuite:
  private val topics        = List("events", "other").map(Topic.from(_).toOption.get)
  private val headerSamples = List(Headers.empty, Headers(Header("trace", Some(Chunk.array(Array[Byte](1))))), Headers(Header("trace", None)))
  private val valueSamples  = List("", "a", "value", "unicode-éè")
  private val byteSamples   = List(None, Some(Chunk.empty), Some(Chunk.array(Array[Byte](1, 2, 3))))

  given Arbitrary[Chunk[Byte]] = Arbitrary(Arbitrary.arbitrary[Array[Byte]].map(Chunk.array))
  given Cogen[Chunk[Byte]]     = Cogen[List[Byte]].contramap(_.toList)

  given [A: Cogen]: Arbitrary[Serializer[Id, A]] =
    Arbitrary(Arbitrary.arbitrary[A => Option[Chunk[Byte]]].map(f => Serializer.instance[Id, A]((_, _, value) => f(value))))

  given Eq[Serializer[Id, String]] =
    Eq.instance: (x, y) =>
      val inputs = for topic <- topics; headers <- headerSamples; value <- valueSamples yield (topic, headers, value)
      inputs.forall((topic, headers, value) => x.serialize(topic, headers, value) == y.serialize(topic, headers, value))

  given [A: Arbitrary]: Arbitrary[Deserializer[Id, A]] =
    Arbitrary(Arbitrary.arbitrary[Option[Chunk[Byte]] => A].map(f => Deserializer.instance[Id, A]((_, _, bytes) => f(bytes))))

  given [A: Eq]: Eq[Deserializer[Id, A]] =
    Eq.instance: (x, y) =>
      val inputs = for topic <- topics; headers <- headerSamples; bytes <- byteSamples yield (topic, headers, bytes)
      inputs.forall((topic, headers, bytes) => Eq[A].eqv(x.deserialize(topic, headers, bytes), y.deserialize(topic, headers, bytes)))

  checkAll("Contravariant[Serializer[Id, *]]", ContravariantTests[[A] =>> Serializer[Id, A]].contravariant[String, String, String])
  checkAll("Functor[Deserializer[Id, *]]", FunctorTests[[A] =>> Deserializer[Id, A]].functor[String, String, String])
