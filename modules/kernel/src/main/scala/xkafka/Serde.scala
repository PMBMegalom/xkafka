package xkafka

import cats.Applicative
import cats.Contravariant
import cats.Functor
import cats.arrow.FunctionK
import cats.tagless.FunctorK
import fs2.Chunk

trait Serializer[F[_], -A]:
  self =>

  def serialize(
      topic: Topic,
      headers: Headers,
      value: A
  ): F[Option[Chunk[Byte]]]

  final def contramap[B](f: B => A): Serializer[F, B] =
    new Serializer[F, B]:
      override def serialize(
          topic: Topic,
          headers: Headers,
          value: B
      ): F[Option[Chunk[Byte]]] =
        self.serialize(topic, headers, f(value))

object Serializer:
  given [F[_]]: Contravariant[[A] =>> Serializer[F, A]] with
    override def contramap[A, B](serializer: Serializer[F, A])(
        f: B => A
    ): Serializer[F, B] =
      serializer.contramap(f)

  given [A]: FunctorK[[F[_]] =>> Serializer[F, A]] with
    override def mapK[F[_], G[_]](serializer: Serializer[F, A])(
        fk: FunctionK[F, G]
    ): Serializer[G, A] =
      instance((topic, headers, value) => fk(serializer.serialize(topic, headers, value)))

  def apply[F[_], A](using serializer: Serializer[F, A]): Serializer[F, A] =
    serializer

  def instance[F[_], A](
      f: (Topic, Headers, A) => F[Option[Chunk[Byte]]]
  ): Serializer[F, A] =
    new Serializer[F, A]:
      override def serialize(
          topic: Topic,
          headers: Headers,
          value: A
      ): F[Option[Chunk[Byte]]] =
        f(topic, headers, value)

  def const[F[_]: Applicative, A](bytes: Option[Chunk[Byte]]): Serializer[F, A] =
    instance((_, _, _) => Applicative[F].pure(bytes))

trait Deserializer[F[_], A]:
  self =>

  def deserialize(
      topic: Topic,
      headers: Headers,
      bytes: Option[Chunk[Byte]]
  ): F[A]

  final def map[B](f: A => B)(using F: Functor[F]): Deserializer[F, B] =
    new Deserializer[F, B]:
      override def deserialize(
          topic: Topic,
          headers: Headers,
          bytes: Option[Chunk[Byte]]
      ): F[B] =
        F.map(self.deserialize(topic, headers, bytes))(f)

object Deserializer:
  given [F[_]: Functor]: Functor[[A] =>> Deserializer[F, A]] with
    override def map[A, B](deserializer: Deserializer[F, A])(
        f: A => B
    ): Deserializer[F, B] =
      deserializer.map(f)

  given [A]: FunctorK[[F[_]] =>> Deserializer[F, A]] with
    override def mapK[F[_], G[_]](deserializer: Deserializer[F, A])(
        fk: FunctionK[F, G]
    ): Deserializer[G, A] =
      instance((topic, headers, bytes) => fk(deserializer.deserialize(topic, headers, bytes)))

  def apply[F[_], A](using deserializer: Deserializer[F, A]): Deserializer[F, A] =
    deserializer

  def instance[F[_], A](
      f: (Topic, Headers, Option[Chunk[Byte]]) => F[A]
  ): Deserializer[F, A] =
    new Deserializer[F, A]:
      override def deserialize(
          topic: Topic,
          headers: Headers,
          bytes: Option[Chunk[Byte]]
      ): F[A] =
        f(topic, headers, bytes)
