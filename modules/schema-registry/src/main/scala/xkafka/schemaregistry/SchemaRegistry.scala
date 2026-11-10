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

package xkafka.schemaregistry

import java.nio.charset.StandardCharsets

import scala.concurrent.duration.FiniteDuration

import cats.~>
import cats.effect.{Ref, Temporal}
import cats.syntax.all.*
import cats.tagless.FunctorK
import fs2.Chunk
import io.circe.{parser, Decoder, Json}

/** The schema registry's operations, as Confluent's REST API offers them, with every answer that cannot change kept once given. */
trait SchemaRegistry[F[_]]:
  self =>

  /** The schema registered as `id`, as records written with it are read. */
  def schema(id: SchemaId): F[Schema]

  /** The id of `schema` as a version of `subject`, which registers it as the subject's next version where it is not one yet, and fails as
    * [[RegistryException.Refused]] with error 409 where the subject's compatibility rule refuses it.
    */
  def register(subject: Subject, schema: Schema): F[SchemaId]

  /** `schema` as registered under `subject`, where it is one of its versions. */
  def lookup(subject: Subject, schema: Schema): F[Option[Registered]]

  /** The latest version of `subject`, where it has one. */
  def latest(subject: Subject): F[Option[Registered]]

  /** Version `version` of `subject`, where it has one. */
  def version(subject: Subject, version: Int): F[Option[Registered]]

  /** Whether `schema` could be registered as the next version of `subject`; a subject without versions takes any schema. */
  def compatibility(subject: Subject, schema: Schema): F[Compatibility]

  final def mapK[G[_]](fk: F ~> G): SchemaRegistry[G] =
    new SchemaRegistry[G]:
      override def schema(id: SchemaId): G[Schema]                                   = fk(self.schema(id))
      override def register(subject: Subject, schema: Schema): G[SchemaId]           = fk(self.register(subject, schema))
      override def lookup(subject: Subject, schema: Schema): G[Option[Registered]]   = fk(self.lookup(subject, schema))
      override def latest(subject: Subject): G[Option[Registered]]                   = fk(self.latest(subject))
      override def version(subject: Subject, version: Int): G[Option[Registered]]    = fk(self.version(subject, version))
      override def compatibility(subject: Subject, schema: Schema): G[Compatibility] = fk(self.compatibility(subject, schema))

object SchemaRegistry:
  given FunctorK[SchemaRegistry] with
    override def mapK[F[_], G[_]](registry: SchemaRegistry[F])(fk: F ~> G): SchemaRegistry[G] = registry.mapK(fk)

  /** The registry `settings` locate, reached through `transport`. */
  def of[F[_]](settings: RegistrySettings, transport: Transport[F])(using F: Temporal[F]): F[SchemaRegistry[F]] =
    Ref.of[F, Kept](Kept(Map.empty, Map.empty, Map.empty, Map.empty)).map(new Client(settings, transport, _))

  /** Answers already given: schemas by id, ids and versions of schemas by subject, and each subject's latest version with when it was asked. */
  private final case class Kept(
      schemas: Map[SchemaId, Schema],
      ids: Map[(Subject, Schema), SchemaId],
      found: Map[(Subject, Schema), Registered],
      latest: Map[Subject, (Registered, FiniteDuration)]
  )

  // The registry's error codes for a subject, a version and a schema it does not have.
  private val SubjectNotFound = 40401
  private val VersionNotFound = 40402
  private val SchemaNotFound  = 40403

  private val headers =
    List(
      "Accept"       -> "application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json; qs=0.9, application/json; qs=0.5",
      "Content-Type" -> "application/vnd.schemaregistry.v1+json"
    )

  private final class Client[F[_]](settings: RegistrySettings, transport: Transport[F], kept: Ref[F, Kept])(using F: Temporal[F])
      extends SchemaRegistry[F]:
    override def schema(id: SchemaId): F[Schema] =
      kept.get.map(_.schemas.get(id)).flatMap:
        case Some(schema) => F.pure(schema)
        case None         => send(Method.Get, s"/schemas/ids/${id.value}", None).flatMap(answer(_)(using schemaAnswer)).flatTap: schema =>
            kept.update(state => state.copy(schemas = state.schemas.updated(id, schema)))

    override def register(subject: Subject, schema: Schema): F[SchemaId] =
      kept.get.map(_.ids.get((subject, schema))).flatMap:
        case Some(id) => F.pure(id)
        case None     => send(Method.Post, s"/subjects/${segment(subject.value)}/versions", Some(written(schema)))
            .flatMap(answer(_)(using Decoder[Int].at("id").map(SchemaId(_)))).flatTap(id =>
              kept.update(state => state.copy(ids = state.ids.updated((subject, schema), id), schemas = state.schemas.updated(id, schema)))
            )

    override def lookup(subject: Subject, schema: Schema): F[Option[Registered]] =
      kept.get.map(_.found.get((subject, schema))).flatMap:
        case Some(registered) => F.pure(Some(registered))
        case None             => send(Method.Post, s"/subjects/${segment(subject.value)}", Some(written(schema)))
            .flatMap(found(_, Set(SubjectNotFound, SchemaNotFound))(using registeredAnswer))
            .flatTap(_.traverse_(registered => kept.update(state => state.copy(found = state.found.updated((subject, schema), registered)))))

    override def latest(subject: Subject): F[Option[Registered]] =
      (kept.get, F.monotonic).flatMapN: (state, now) =>
        state.latest.get(subject).filter((_, asked) => settings.latestCachedFor.forall(now - asked < _)) match
          case Some((registered, _)) => F.pure(Some(registered))
          case None                  => send(Method.Get, s"/subjects/${segment(subject.value)}/versions/latest", None)
              .flatMap(found(_, Set(SubjectNotFound, VersionNotFound))(using registeredAnswer))
              .flatTap(_.traverse_(registered => kept.update(state => state.copy(latest = state.latest.updated(subject, (registered, now))))))

    override def version(subject: Subject, version: Int): F[Option[Registered]] =
      send(Method.Get, s"/subjects/${segment(subject.value)}/versions/$version", None)
        .flatMap(found(_, Set(SubjectNotFound, VersionNotFound))(using registeredAnswer))

    override def compatibility(subject: Subject, schema: Schema): F[Compatibility] =
      send(Method.Post, s"/compatibility/subjects/${segment(subject.value)}/versions/latest?verbose=true", Some(written(schema)))
        .flatMap(found(_, Set(SubjectNotFound, VersionNotFound))(using compatibilityAnswer)).map(_.getOrElse(Compatibility.Compatible))

    /** Sends the request to each URL in turn until one can be reached. */
    private def send(method: Method, path: String, body: Option[Json]): F[RegistryResponse] =
      val bytes = body.map(json => Chunk.array(json.noSpaces.getBytes(StandardCharsets.UTF_8)))
      settings.urls.map(url => transport(RegistryRequest(method, url + path, headers, bytes)))
        .reduceLeft((tried, next) => tried.handleErrorWith(_ => next)).adaptError { case error =>
          new RegistryException.Unreachable(settings.urls, error)
        }

    private def answer[A](response: RegistryResponse)(using decoder: Decoder[A]): F[A] =
      if response.status / 100 == 2 then F.fromEither(read(response, decoder)) else F.raiseError(refusal(response))

    /** The answer, or none where the registry refuses the request with one of `missing`, its codes for something it does not have. */
    private def found[A](response: RegistryResponse, missing: Set[Int])(using decoder: Decoder[A]): F[Option[A]] =
      if response.status / 100 == 2 then F.fromEither(read(response, decoder)).map(Some(_))
      else
        refusal(response) match
          case refused: RegistryException.Refused if missing.contains(refused.code) => F.pure(None)
          case refused                                                              => F.raiseError(refused)

    private def read[A](response: RegistryResponse, decoder: Decoder[A]): Either[RegistryException, A] =
      parser.decode(text(response))(using decoder).leftMap(error => new RegistryException.Unreadable(response.status, error.getMessage))

    private def refusal(response: RegistryResponse): RegistryException =
      parser.decode(text(response))(using refusalAnswer).fold(
        _ => new RegistryException.Unreadable(response.status, text(response).take(200)),
        (code, reason) => new RegistryException.Refused(response.status, code, reason)
      )

  private def text(response: RegistryResponse): String = new String(response.body.toArray, StandardCharsets.UTF_8)

  /** A subject in a URL's path, where every byte of it but a letter, a digit, `-`, `.`, `_` and `~` is written as `%` and two hex digits. */
  private def segment(value: String): String =
    value.getBytes(StandardCharsets.UTF_8).toList.map: byte =>
      val char = (byte & 0xff).toChar
      if char.isLetterOrDigit && char < 128 || "-._~".contains(char) then char.toString else f"%%${byte & 0xff}%02X"
    .mkString

  private def written(schema: Schema): Json =
    Json.fromFields(
      List("schema" -> Json.fromString(schema.text)) ++
        Option.when(schema.schemaType != SchemaType.Avro)("schemaType" -> Json.fromString(schema.schemaType.name)) ++
        Option.when(schema.references.nonEmpty)(
          "references" -> Json.fromValues(
            schema.references.map: reference =>
              Json.obj(
                "name"    -> Json.fromString(reference.name),
                "subject" -> Json.fromString(reference.subject.value),
                "version" -> Json.fromInt(reference.version)
              )
          )
        )
    )

  private val referenceAnswer: Decoder[SchemaReference] =
    Decoder.forProduct3[(String, String, Int), String, String, Int]("name", "subject", "version")((_, _, _)).emap: (name, subject, version) =>
      Subject.from(subject).bimap(_.message, SchemaReference(name, _, version))

  /** A schema as the registry gives it, where an absent type is Avro, the registry's first and default language. */
  private val schemaAnswer: Decoder[Schema] =
    Decoder.instance: cursor =>
      for
        text       <- cursor.get[String]("schema")
        named      <- cursor.get[Option[String]]("schemaType")
        references <-
          cursor.get[Option[List[SchemaReference]]]("references")(using Decoder.decodeOption(using Decoder.decodeList(using referenceAnswer)))
        schemaType <-
          named.fold(
            Right(SchemaType.Avro)
          )(name => SchemaType.named(name).toRight(io.circe.DecodingFailure(s"unknown schema type '$name'", cursor.downField("schemaType").history)))
      yield Schema(text, schemaType, references.getOrElse(Nil))

  private val registeredAnswer: Decoder[Registered] =
    Decoder.instance: cursor =>
      for
        subject <-
          cursor.get[String]("subject")
            .flatMap(name => Subject.from(name).leftMap(error => io.circe.DecodingFailure(error.message, cursor.downField("subject").history)))
        version <- cursor.get[Int]("version")
        id      <- cursor.get[Int]("id")
        schema  <- schemaAnswer(cursor)
      yield Registered(SchemaId(id), subject, version, schema)

  private val compatibilityAnswer: Decoder[Compatibility] =
    Decoder.instance: cursor =>
      for
        compatible <- cursor.get[Boolean]("is_compatible")
        messages   <- cursor.get[Option[List[String]]]("messages")
      yield if compatible then Compatibility.Compatible else Compatibility.Incompatible(messages.getOrElse(Nil))

  private val refusalAnswer: Decoder[(Int, String)] = Decoder.forProduct2[(Int, String), Int, String]("error_code", "message")((_, _))
