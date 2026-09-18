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
