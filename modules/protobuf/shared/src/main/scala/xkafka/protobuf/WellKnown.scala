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

package xkafka.protobuf

/** The files of Protobuf's well-known types, which every Protobuf library and the registry know, so a schema imports them without references. */
private[protobuf] object WellKnown:
  private def file(body: String): String = s"syntax = \"proto3\";\npackage google.protobuf;\n$body"

  private val wrappers =
    List("Double" -> "double", "Float" -> "float", "Int64" -> "int64", "UInt64" -> "uint64", "Int32" -> "int32", "UInt32" -> "uint32")
      .appendedAll(List("Bool" -> "bool", "String" -> "string", "Bytes" -> "bytes"))
      .map((name, scalar) => s"message ${name}Value { $scalar value = 1; }").mkString("\n")

  val texts: Map[String, String] =
    Map(
      "google/protobuf/any.proto"        -> file("message Any { string type_url = 1; bytes value = 2; }"),
      "google/protobuf/duration.proto"   -> file("message Duration { int64 seconds = 1; int32 nanos = 2; }"),
      "google/protobuf/empty.proto"      -> file("message Empty {}"),
      "google/protobuf/field_mask.proto" -> file("message FieldMask { repeated string paths = 1; }"),
      "google/protobuf/struct.proto"     -> file("""message Struct { map<string, Value> fields = 1; }
        |message Value {
        |  oneof kind {
        |    NullValue null_value = 1;
        |    double number_value = 2;
        |    string string_value = 3;
        |    bool bool_value = 4;
        |    Struct struct_value = 5;
        |    ListValue list_value = 6;
        |  }
        |}
        |enum NullValue { NULL_VALUE = 0; }
        |message ListValue { repeated Value values = 1; }""".stripMargin),
      "google/protobuf/timestamp.proto" -> file("message Timestamp { int64 seconds = 1; int32 nanos = 2; }"),
      "google/protobuf/wrappers.proto"  -> file(wrappers)
    )

  /** The files, read once. */
  lazy val files: Map[String, ProtobufSchema.File] =
    texts.toList.flatMap((name, text) => ProtobufSchemaText.parse(text).toOption.map(name -> _)).toMap
