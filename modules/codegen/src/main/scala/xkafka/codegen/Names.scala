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

package xkafka.codegen

/** Scala names for Avro's, which are letters, digits and underscores, and Scala text for strings. */
private[codegen] object Names:
  private val keywords =
    Set(
      "abstract",
      "case",
      "catch",
      "class",
      "def",
      "do",
      "else",
      "enum",
      "export",
      "extends",
      "false",
      "final",
      "finally",
      "for",
      "given",
      "if",
      "implicit",
      "import",
      "lazy",
      "match",
      "new",
      "null",
      "object",
      "override",
      "package",
      "private",
      "protected",
      "return",
      "sealed",
      "super",
      "then",
      "throw",
      "trait",
      "true",
      "try",
      "type",
      "val",
      "var",
      "while",
      "with",
      "yield"
    )

  // A case class's own members, which a field of that name would clash with.
  private val members =
    Set(
      "copy",
      "equals",
      "hashCode",
      "toString",
      "productArity",
      "productElement",
      "productElementName",
      "productElementNames",
      "productIterator",
      "productPrefix",
      "canEqual",
      "getClass",
      "notify",
      "notifyAll",
      "wait",
      "clone",
      "finalize"
    )

  // An enum's own members, which a case of that name would clash with.
  private val enumMembers = members ++ Set("values", "valueOf", "fromOrdinal", "ordinal")

  private val qualifiedBuiltins =
    Map(
      "Boolean"    -> "_root_.scala.Boolean",
      "Byte"       -> "_root_.scala.Byte",
      "Int"        -> "_root_.scala.Int",
      "Long"       -> "_root_.scala.Long",
      "Float"      -> "_root_.scala.Float",
      "Double"     -> "_root_.scala.Double",
      "String"     -> "_root_.scala.Predef.String",
      "List"       -> "scala.collection.immutable.List",
      "Map"        -> "scala.collection.immutable.Map",
      "Option"     -> "_root_.scala.Option",
      "Some"       -> "_root_.scala.Some",
      "None"       -> "_root_.scala.None",
      "Array"      -> "_root_.scala.Array",
      "BigDecimal" -> "scala.math.BigDecimal",
      "BigInt"     -> "scala.math.BigInt",
      "Set"        -> "scala.collection.immutable.Set",
      "Either"     -> "scala.util.Either"
    )

  /** A name of Scala's own, qualified where a type in the package being written has it too. */
  def builtin(name: String, neighbours: Set[String]): String = if neighbours.contains(name) then qualifiedBuiltins(name) else name

  /** A package's or type's name. */
  def term(name: String): String = if keywords.contains(name) then s"`$name`" else name

  def typeName(name: String): String = term(name)

  /** A field's Scala name, and the Avro name it is renamed from where that name is one of a case class's own. */
  def field(name: String): (String, Option[String]) = if members.contains(name) then (s"${name}_", Some(name)) else (term(name), None)

  /** An enum case's Scala name, and the symbol it is renamed from where that symbol is one of an enum's own members. */
  def symbol(name: String): (String, Option[String]) = if enumMembers.contains(name) then (s"${name}_", Some(name)) else (term(name), None)

  /** What goes between a parameter's name and its type: a name ending in an underscore would take a colon right after it as part of itself. */
  def colon(name: String): String = if name.endsWith("_") then " : " else ": "

  /** The colon that opens a type's body after its name, apart from a name ending in an underscore for the same reason. */
  def opening(name: String): String = if name.endsWith("_") then " :" else ":"

  private val identifier = """[\p{L}_][\p{L}\p{Nd}_]*""".r

  /** Whether `text` is a Scala identifier made of letters, digits and underscores, which a name may be as it is. */
  def isIdentifier(text: String): Boolean = text != "_" && identifier.matches(text)

  private def words(text: String): List[String] = text.split("[^\\p{L}\\p{Nd}]+").toList.filter(_.nonEmpty)

  /** A type's name made of the letters and digits of `text`, each word capitalized, as `line-item` makes `LineItem`. */
  def pascal(text: String): Option[String] =
    Option(words(text).map(_.capitalize).mkString).filter(_.nonEmpty).map(name => if name.head.isDigit then s"_$name" else name)

  /** A value's name made of the letters and digits of `text`, words after the first capitalized, as `first-name` makes `firstName`. */
  def camel(text: String): Option[String] =
    words(text) match
      case Nil           => None
      case first :: rest =>
        val head = if first.forall(!_.isLower) then first.toLowerCase else first.head.toLower +: first.tail
        Some((head :: rest.map(_.capitalize)).mkString).map(name => if name.head.isDigit then s"_$name" else name)

  /** `text` as a Scala string literal. */
  /** The Scala expression of `text`: one literal, or where it could be longer than the JVM holds in one constant (65535 bytes), literals of its parts
    * joined as the program runs, since the compiler would join literals added together back into one constant.
    */
  def text(value: String): String =
    if value.length <= ConstantChars then quote(value) else parts(value).map(quote).mkString("_root_.scala.List(", ", ", ").mkString")

  /** As many characters as one constant holds whatever they are: each takes at most three bytes there. */
  private val ConstantChars = 65535 / 3

  /** `value` in parts of at most `ConstantChars` characters, none of which splits a character written as two. */
  private def parts(value: String): List[String] =
    if value.length <= ConstantChars then List(value)
    else
      val cut = if value.charAt(ConstantChars - 1).isHighSurrogate then ConstantChars - 1 else ConstantChars
      value.take(cut) :: parts(value.drop(cut))

  def quote(text: String): String =
    text.flatMap:
      case '"'                                                   => "\\\""
      case '\\'                                                  => "\\\\"
      case '\n'                                                  => "\\n"
      case '\r'                                                  => "\\r"
      case '\t'                                                  => "\\t"
      case character if character < ' ' || character == '\u007f' => f"\\u${character.toInt}%04x"
      case character                                             => character.toString
    .mkString("\"", "", "\"")
