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

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import fs2.Chunk
import io.circe.{parser, Json}

/** A registry in memory answering the part of Confluent's REST API the client uses, as Confluent's answers them, and keeping every request. A schema
  * is incompatible with a subject where `incompatible` says so of its text.
  */
final class FakeRegistry(state: Ref[IO, FakeRegistry.State], val requests: Ref[IO, Vector[RegistryRequest]], incompatible: String => Boolean):
  import FakeRegistry.*

  /** Answers at `base`, and refuses to connect anywhere else. */
  def transport(base: String): Transport[IO] =
    request =>
      requests.update(_ :+ request) >>
        (if request.url.startsWith(base + "/") then answer(request.method, request.url.drop(base.length), request.body)
         else IO.raiseError(new java.io.IOException(s"connection refused: ${request.url}")))

  /** Registers `text` under `subject` as the registry does on a request, giving its id. */
  def registered(subject: String, text: String, schemaType: String = "AVRO"): IO[Int] =
    state.modify(_.register(subject, Stored(text, schemaType, Nil)))

  private def answer(method: Method, target: String, body: Option[Chunk[Byte]]): IO[RegistryResponse] =
    val (path, query) = target.span(_ != '?')
    val parts         = path.split('/').toList.drop(1).map(decode)
    def stored        = body.flatMap(bytes => parser.parse(new String(bytes.toArray, StandardCharsets.UTF_8)).toOption).flatMap(Stored.from)
    (method, parts) match
      case (Method.Get, List("schemas", "ids", id)) => state.get
          .map(_.byId.get(id.toInt).fold(refused(404, 40403, "Schema not found"))(schema => ok(schema.json)))
      case (Method.Post, List("subjects", subject, "versions")) => stored.fold(IO.pure(refused(422, 42201, "Invalid schema"))): schema =>
          if incompatible(schema.text) then IO.pure(refused(409, 409, "Schema being registered is incompatible with an earlier schema"))
          else state.modify(_.register(subject, schema)).map(id => ok(Json.obj("id" -> Json.fromInt(id))))
      case (Method.Post, List("subjects", subject)) => stored.fold(IO.pure(refused(422, 42201, "Invalid schema"))): schema =>
          state.get.map: current =>
            current.subjects.get(subject) match
              case None           => refused(404, 40401, s"Subject '$subject' not found.")
              case Some(versions) => versions.zipWithIndex.find((id, _) => current.byId(id) == schema)
                  .fold(refused(404, 40403, "Schema not found"))((id, index) => ok(current.version(subject, id, index + 1)))
      case (Method.Get, List("subjects", subject, "versions", which)) => state.get.map: current =>
          current.subjects.get(subject) match
            case None           => refused(404, 40401, s"Subject '$subject' not found.")
            case Some(versions) =>
              val index = if which == "latest" then versions.size - 1 else which.toInt - 1
              versions.lift(index).fold(refused(404, 40402, s"Version $which not found."))(id => ok(current.version(subject, id, index + 1)))
      case (Method.Post, List("compatibility", "subjects", subject, "versions", "latest")) if query == "?verbose=true" =>
        stored.fold(IO.pure(refused(422, 42201, "Invalid schema"))): schema =>
          state.get.map: current =>
            if !current.subjects.contains(subject) then refused(404, 40401, s"Subject '$subject' not found.")
            else if incompatible(schema.text) then
              ok(Json.obj("is_compatible" -> Json.False, "messages" -> Json.arr(Json.fromString("field removed"))))
            else ok(Json.obj("is_compatible" -> Json.True, "messages" -> Json.arr()))
      case _ => IO.pure(refused(404, 404, s"HTTP 404 Not Found: $method $target"))

object FakeRegistry:
  def apply(incompatible: String => Boolean = _ => false): IO[FakeRegistry] =
    (Ref.of[IO, State](State(Map.empty, Map.empty)), Ref.of[IO, Vector[RegistryRequest]](Vector.empty)).mapN(new FakeRegistry(_, _, incompatible))

  final case class Stored(text: String, schemaType: String, references: List[Json]):
    def json: Json =
      Json.fromFields(
        List("schema" -> Json.fromString(text)) ++ Option.when(schemaType != "AVRO")("schemaType" -> Json.fromString(schemaType)) ++
          Option.when(references.nonEmpty)("references" -> Json.fromValues(references))
      )

  object Stored:
    def from(json: Json): Option[Stored] =
      val cursor = json.hcursor
      cursor.get[String]("schema").toOption.map: text =>
        Stored(text, cursor.get[String]("schemaType").getOrElse("AVRO"), cursor.get[List[Json]]("references").getOrElse(Nil))

  final case class State(byId: Map[Int, Stored], subjects: Map[String, Vector[Int]]):
    /** The id of `schema`, the one it already has wherever it is registered, added as the subject's next version where it is not one yet. */
    def register(subject: String, schema: Stored): (State, Int) =
      val id       = byId.collectFirst { case (id, stored) if stored == schema => id }.getOrElse(byId.size + 1)
      val versions = subjects.getOrElse(subject, Vector.empty)
      (State(byId.updated(id, schema), if versions.contains(id) then subjects else subjects.updated(subject, versions :+ id)), id)

    def version(subject: String, id: Int, version: Int): Json =
      Json.obj("subject" -> Json.fromString(subject), "version" -> Json.fromInt(version), "id" -> Json.fromInt(id)).deepMerge(byId(id).json)

  private def ok(json: Json): RegistryResponse = RegistryResponse(200, Chunk.array(json.noSpaces.getBytes(StandardCharsets.UTF_8)))

  private def refused(status: Int, code: Int, message: String): RegistryResponse =
    RegistryResponse(
      status,
      Chunk.array(Json.obj("error_code" -> Json.fromInt(code), "message" -> Json.fromString(message)).noSpaces.getBytes(StandardCharsets.UTF_8))
    )

  /** A path segment with its `%` escapes decoded as UTF-8. */
  private def decode(segment: String): String =
    def bytes(rest: List[Char]): List[Byte] =
      rest match
        case '%' :: high :: low :: tail => Integer.parseInt(s"$high$low", 16).toByte :: bytes(tail)
        case char :: tail               => char.toString.getBytes(StandardCharsets.UTF_8).toList ++ bytes(tail)
        case Nil                        => Nil
    new String(bytes(segment.toList).toArray, StandardCharsets.UTF_8)
