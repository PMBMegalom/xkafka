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

import java.nio.charset.StandardCharsets

import scala.annotation.tailrec

import cats.syntax.all.*
import xkafka.protobuf.ProtobufSchema.{Enum, Field, FieldType, File, Label, Message, Syntax}

/** A .proto file's tokens: names, numbers, quoted text and punctuation. */
private enum Token derives CanEqual:
  case Word(text: String)
  case Number(text: String)

  /** Quoted text, read, and as it is written, quotes and escapes and all. */
  case Text(value: String, written: String)
  case Symbol(char: Char)

/** A step of reading tokens from a position, giving a value and the position after it. */
private final case class Step[A](run: Int => Either[String, (A, Int)]):
  def map[B](f: A => B): Step[B]           = Step(at => run(at).map((value, next) => (f(value), next)))
  def flatMap[B](f: A => Step[B]): Step[B] = Step(at => run(at).flatMap((value, next) => f(value).run(next)))

/** Reads .proto files, in proto2 or proto3, into [[ProtobufSchema.File]]s, and writes them back. Options other than `packed` and `default` are
  * skipped, as are services; groups, extensions and editions are refused.
  */
object ProtobufSchemaText:
  def parse(text: String): Either[String, File] = tokens(text).flatMap(new Reader(_).file)

  def render(file: File): String =
    val syntax =
      file.syntax match
        case Syntax.Proto2 => "proto2"
        case Syntax.Proto3 => "proto3"
    val head =
      List(s"syntax = \"$syntax\";") ++ file.packageName.map(name => s"package $name;") ++
        Option.when(file.imports.nonEmpty)(file.imports.map(name => s"import \"${escape(name)}\";").mkString("\n"))
    (head ++ file.messages.map(message(_, "")) ++ file.enums.map(enumeration(_, ""))).mkString("", "\n\n", "\n")

  private def message(declared: Message, indent: String): String =
    val inner = indent + "  "
    // A oneof is written where its first field is.
    val fields =
      declared.fields.foldLeft(List.empty[String]): (lines, field) =>
        field.oneof match
          case None                                                                                 => lines :+ (inner + fieldLine(field))
          case Some(oneof) if declared.fields.takeWhile(_ != field).exists(_.oneof.contains(oneof)) => lines
          case Some(oneof)                                                                          =>
            val members = declared.fields.filter(_.oneof.contains(oneof)).map(member => s"$inner  ${fieldLine(member.copy(label = Label.Implicit))}")
            lines :+ s"${inner}oneof $oneof {\n${members.mkString("\n")}\n$inner}"
    val nested = declared.messages.map(message(_, inner)) ++ declared.enums.map(enumeration(_, inner))
    s"${indent}message ${declared.name} {\n${(fields ++ nested).map(_ + "\n").mkString}$indent}"

  private def fieldLine(field: Field): String =
    // A map is repeated without a label.
    val label =
      (field.label, field.fieldType) match
        case (_, FieldType.Map(_, _)) => ""
        case (Label.Implicit, _)      => ""
        case (Label.Optional, _)      => "optional "
        case (Label.Required, _)      => "required "
        case (Label.Repeated, _)      => "repeated "
    val options = field.packed.map(packed => s"packed = $packed").toList ++ field.default.map(value => s"default = $value")
    s"$label${typeName(field.fieldType)} ${field.name} = ${field.number}${if options.isEmpty then "" else options.mkString(" [", ", ", "]")};"

  private def typeName(fieldType: FieldType): String =
    fieldType match
      case FieldType.Named(name)     => name
      case FieldType.Map(key, value) => s"map<${typeName(key)}, ${typeName(value)}>"
      case scalar                    => scalar.toString.toLowerCase

  // protoc refuses two values of one number unless the enum allows aliases, which it says where it has them.
  private def enumeration(declared: Enum, indent: String): String =
    val aliased = declared.values.map(_._2).distinct.size < declared.values.size
    val option  = if aliased then s"$indent  option allow_alias = true;\n" else ""
    s"${indent}enum ${declared.name} {\n$option${declared.values.map((name, number) => s"$indent  $name = $number;\n").mkString}$indent}"

  private[protobuf] def escape(text: String): String =
    text.flatMap:
      case '"'  => "\\\""
      case '\\' => "\\\\"
      case '\n' => "\\n"
      case char => char.toString

  private def tokens(text: String): Either[String, Vector[Token]] =
    def wordChar(char: Char): Boolean = char.isLetterOrDigit || char == '_' || char == '.'
    // The escapes of control characters, by the letter after the backslash.
    val controls = Map('a' -> 7, 'b' -> 8, 'f' -> 12, 'n' -> 10, 'r' -> 13, 't' -> 9, 'v' -> 11).view.mapValues(_.toByte).toMap
    @tailrec
    def go(at: Int, read: Vector[Token]): Either[String, Vector[Token]] =
      if at >= text.length then Right(read)
      else
        val char = text(at)
        if char.isWhitespace then go(at + 1, read)
        else if text.startsWith("//", at) then
          go(
            text.indexOf('\n', at) match
              case -1  => text.length;
              case end => end
            ,
            read
          )
        else if text.startsWith("/*", at) then
          text.indexOf("*/", at + 2) match
            case -1  => Left("a comment is not closed")
            case end => go(end + 2, read)
        else if char.isLetter || char == '_' || (char == '.' && at + 1 < text.length && (text(at + 1).isLetter || text(at + 1) == '_')) then
          val end = (at + 1 until text.length).find(index => !wordChar(text(index))).getOrElse(text.length)
          go(end, read :+ Token.Word(text.substring(at, end)))
        else if char.isDigit then
          val end = (at + 1 until text.length).find: index =>
            val next = text(index)
            !(next.isLetterOrDigit || next == '.' || ((next == '-' || next == '+') && "eE".contains(text(index - 1)) && !text.startsWith("0x", at)))
          .getOrElse(text.length)
          go(end, read :+ Token.Number(text.substring(at, end)))
        else if char == '"' || char == '\'' then
          quoted(at + 1, char, Vector.empty) match
            case Right((value, end)) => go(end, read :+ Token.Text(value, text.substring(at, end)))
            case Left(reason)        => Left(reason)
        else go(at + 1, read :+ Token.Symbol(char))
    // The digits of `radix` at `at`, at most `most` of them.
    def digits(at: Int, radix: Int, most: Int): String = text.slice(at, at + most).takeWhile(char => Character.digit(char, radix) >= 0)
    def utf8(codePoint: Int): Vector[Byte]             = new String(Character.toChars(codePoint)).getBytes(StandardCharsets.UTF_8).toVector
    // Quoted text is bytes, as protobuf reads it: a character is its UTF-8, and an octal or hex escape one byte.
    @tailrec
    def quoted(at: Int, quote: Char, bytes: Vector[Byte]): Either[String, (String, Int)] =
      if at >= text.length then Left("quoted text is not closed")
      else
        text(at) match
          case `quote`                      => Right((new String(bytes.toArray, StandardCharsets.UTF_8), at + 1))
          case '\\' if at + 1 < text.length =>
            val escaped: Either[String, (Vector[Byte], Int)] =
              text(at + 1) match
                case char if controls.contains(char)       => Right((Vector(controls(char)), at + 2))
                case char if "\\'\"?".contains(char)       => Right((Vector(char.toByte), at + 2))
                case char if Character.digit(char, 8) >= 0 =>
                  val octal = digits(at + 1, 8, 3)
                  Right((Vector(Integer.parseInt(octal, 8).toByte), at + 1 + octal.length))
                case 'x' | 'X' => digits(at + 2, 16, 2) match
                    case ""  => Left("\\x is not followed by a hex digit")
                    case hex => Right((Vector(Integer.parseInt(hex, 16).toByte), at + 2 + hex.length))
                case 'u' | 'U' =>
                  val size = if text(at + 1) == 'u' then 4 else 8
                  digits(at + 2, 16, size) match
                    case hex if hex.length == size && java.lang.Long.parseLong(hex, 16) <= Character.MAX_CODE_POINT =>
                      Right((utf8(java.lang.Long.parseLong(hex, 16).toInt), at + 2 + size))
                    case _ => Left(s"\\${text(at + 1)} is not followed by $size hex digits of a character")
                case other => Left(s"'\\$other' is no escape")
            escaped match
              case Right((more, next)) => quoted(next, quote, bytes ++ more)
              case Left(reason)        => Left(reason)
          case _ =>
            val codePoint = text.codePointAt(at)
            quoted(at + Character.charCount(codePoint), quote, bytes ++ utf8(codePoint))
    go(0, Vector.empty)

  /** Reads a file's tokens, by recursive descent. */
  private final class Reader(tokens: Vector[Token]):
    private def peek(at: Int): Option[Token] = tokens.lift(at)

    private def describe(at: Int): String =
      peek(at) match
        case Some(Token.Word(text))       => s"'$text'"
        case Some(Token.Number(text))     => s"'$text'"
        case Some(Token.Text(_, written)) => written
        case Some(Token.Symbol(char))     => s"'$char'"
        case None                         => "the end of the file"

    private def fail[A](at: Int, expected: String): Either[String, A] = Left(s"expected $expected at token ${at + 1}, found ${describe(at)}")

    private def symbol(char: Char): Step[Unit] =
      Step(at => if peek(at).contains(Token.Symbol(char)) then Right(((), at + 1)) else fail(at, s"'$char'"))

    private val word: Step[String] =
      Step: at =>
        peek(at) match
          case Some(Token.Word(text)) => Right((text, at + 1))
          case _                      => fail(at, "a name")

    private val text: Step[String] =
      Step: at =>
        peek(at) match
          case Some(Token.Text(value, _)) => Right((value, at + 1))
          case _                          => fail(at, "quoted text")

    private val integer: Step[Int] =
      Step: at =>
        val (negative, start) = if peek(at).contains(Token.Symbol('-')) then (true, at + 1) else (false, at)
        peek(start) match
          case Some(Token.Number(digits)) =>
            val value =
              if digits.startsWith("0x") || digits.startsWith("0X") then scala.util.Try(BigInt(digits.drop(2), 16)).toOption
              else if digits.length > 1 && digits.startsWith("0") then scala.util.Try(BigInt(digits.drop(1), 8)).toOption
              else scala.util.Try(BigInt(digits)).toOption
            value.map(number => if negative then -number else number).filter(_.isValidInt) match
              case Some(number) => Right((number.toInt, start + 1))
              case None         => fail(at, "a whole number")
          case _ => fail(at, "a whole number")

    /** A constant, as an option's value is written, in its source form. */
    private val constant: Step[String] =
      Step: at =>
        peek(at) match
          case Some(Token.Word(text))       => Right((text, at + 1))
          case Some(Token.Number(text))     => Right((text, at + 1))
          case Some(Token.Text(_, written)) => Right((written, at + 1))
          case Some(Token.Symbol('-'))      => peek(at + 1) match
              case Some(Token.Number(text)) => Right((s"-$text", at + 2))
              case Some(Token.Word(text))   => Right((s"-$text", at + 2))
              case _                        => fail(at + 1, "a number")
          case _ => fail(at, "a constant")

    /** Skips tokens up to the next `end` outside any braces, brackets or parentheses, and past it. */
    private def skipPast(end: Char): Step[Unit] =
      Step: start =>
        @tailrec
        def go(at: Int, depth: Int): Either[String, (Unit, Int)] =
          peek(at) match
            case None                                                => fail(at, s"'$end'")
            case Some(Token.Symbol(`end`)) if depth == 0             => Right(((), at + 1))
            case Some(Token.Symbol('{' | '[' | '('))                 => go(at + 1, depth + 1)
            case Some(Token.Symbol('}' | ']' | ')')) if depth > 0    => go(at + 1, depth - 1)
            case Some(Token.Symbol('}')) if end == ';' && depth == 0 => fail(at, "';'")
            case _                                                   => go(at + 1, depth)
        go(start, 0)

    /** Items up to a closing brace, and past it. */
    private def items[A](item: Step[Option[A]]): Step[List[A]] =
      Step: start =>
        @tailrec
        def go(at: Int, read: List[A]): Either[String, (List[A], Int)] =
          peek(at) match
            case Some(Token.Symbol('}')) => Right((read.reverse, at + 1))
            case None                    => fail(at, "'}'")
            case _                       => item.run(at) match
                case Right((value, next)) => go(next, value.fold(read)(_ :: read))
                case Left(reason)         => Left(reason)
        go(start, Nil)

    def file: Either[String, File] =
      @tailrec
      def go(at: Int, file: File): Either[String, File] =
        if at >= tokens.length then Right(file.copy(imports = file.imports.reverse, messages = file.messages.reverse, enums = file.enums.reverse))
        else
          topLevel(file).run(at) match
            case Right((next, end)) => go(end, next)
            case Left(reason)       => Left(reason)
      go(0, File(Syntax.Proto2, None, Nil, Nil, Nil))

    private def topLevel(file: File): Step[File] =
      Step: at =>
        peek(at) match
          case Some(Token.Word("syntax")) => (for
              _      <- word
              _      <- symbol('=')
              syntax <- text
              _      <- symbol(';')
            yield syntax).run(at).flatMap:
              case ("proto2", next) => Right((file.copy(syntax = Syntax.Proto2), next))
              case ("proto3", next) => Right((file.copy(syntax = Syntax.Proto3), next))
              case (other, _)       => Left(s"syntax \"$other\" is not proto2 or proto3")
          case Some(Token.Word("edition")) => Left("the file is written in an edition of Protobuf, which xkafka does not read")
          case Some(Token.Word("package")) => (word >> word <* symbol(';')).run(at).map((name, next) => (file.copy(packageName = Some(name)), next))
          case Some(Token.Word("import"))  =>
            val start = if peek(at + 1).exists(token => token == Token.Word("public") || token == Token.Word("weak")) then at + 2 else at + 1
            (text <* symbol(';')).run(start).map((name, next) => (file.copy(imports = name :: file.imports), next))
          case Some(Token.Word("option"))  => skipPast(';').run(at).map((_, next) => (file, next))
          case Some(Token.Word("message")) => message(file.syntax, 1).run(at).map((read, next) => (file.copy(messages = read :: file.messages), next))
          case Some(Token.Word("enum"))    => enumeration.run(at).map((read, next) => (file.copy(enums = read :: file.enums), next))
          case Some(Token.Word("service")) => (word >> word >> symbol('{') >> skipPast('}')).run(at).map((_, next) => (file, next))
          case Some(Token.Word("extend"))  => Left("the file extends a message, and xkafka does not read extensions")
          case Some(Token.Symbol(';'))     => Right((file, at + 1))
          case _                           => fail(at, "a declaration")

    private enum Item:
      case Fields(fields: List[Field])
      case Nested(message: Message)
      case NestedEnum(declared: Enum)

    // Nesting is bounded as records' is, so a file nested past it is refused, not read until the stack runs out.
    private def message(syntax: Syntax, depth: Int): Step[Message] =
      for
        _ <-
          Step(at =>
            if depth > ProtobufWire.MaxDepth then Left(s"the file's messages are nested more than ${ProtobufWire.MaxDepth} deep") else Right(((), at))
          )
        _    <- word
        name <- word
        _    <- symbol('{')
        read <- items(messageItem(syntax, depth))
      yield Message(
        name,
        read.flatMap { case Item.Fields(fields) => fields; case _ => Nil },
        read.collect { case Item.Nested(nested) => nested },
        read.collect { case Item.NestedEnum(nested) => nested }
      )

    private def messageItem(syntax: Syntax, depth: Int): Step[Option[Item]] =
      Step: at =>
        peek(at) match
          case Some(Token.Word("message"))                            => message(syntax, depth + 1).map(read => Some(Item.Nested(read))).run(at)
          case Some(Token.Word("enum"))                               => enumeration.map(read => Some(Item.NestedEnum(read))).run(at)
          case Some(Token.Word("oneof"))                              => oneof(syntax).map(read => Some(Item.Fields(read))).run(at)
          case Some(Token.Word("option" | "reserved" | "extensions")) => skipPast(';').run(at).map((_, next) => (None, next))
          case Some(Token.Word("extend"))                             => Left("the file extends a message, and xkafka does not read extensions")
          case Some(Token.Symbol(';'))                                => Right((None, at + 1))
          case _                                                      => field(syntax, None).map(read => Some(Item.Fields(List(read)))).run(at)

    private def oneof(syntax: Syntax): Step[List[Field]] =
      for
        _    <- word
        name <- word
        _    <- symbol('{')
        read <-
          items(
            Step: at =>
              peek(at) match
                case Some(Token.Word("option")) => skipPast(';').run(at).map((_, next) => (None, next))
                case Some(Token.Symbol(';'))    => Right((None, at + 1))
                case _                          => field(syntax, Some(name)).map(Some(_)).run(at)
          )
      yield read

    private def field(syntax: Syntax, oneof: Option[String]): Step[Field] =
      Step: at =>
        val (label, start) =
          peek(at) match
            case Some(Token.Word("optional")) => (Some(Label.Optional), at + 1)
            case Some(Token.Word("required")) => (Some(Label.Required), at + 1)
            case Some(Token.Word("repeated")) => (Some(Label.Repeated), at + 1)
            case _                            => (None, at)
        val read =
          for
            fieldType <- fieldType
            name      <- word
            _         <- symbol('=')
            number    <- integer
            options   <- options
            _         <- symbol(';')
          yield
            val (packed, default) = options
            val labelled          =
              if oneof.isDefined then Label.Optional
              else
                fieldType match
                  case FieldType.Map(_, _) => Label.Repeated
                  case _                   => label.getOrElse(if syntax == Syntax.Proto3 then Label.Implicit else Label.Optional)
            Field(name, number, fieldType, labelled, oneof, packed, default)
        read.run(start).flatMap: (read: Field, next: Int) =>
          if read.number < 1 || read.number > ProtobufWire.MaxNumber then
            Left(s"field ${read.name} is numbered ${read.number}, which Protobuf does not allow")
          else Right((read, next))

    private val fieldType: Step[FieldType] =
      Step: at =>
        peek(at) match
          case Some(Token.Word("group"))                                           => Left("the file declares a group, which xkafka does not read")
          case Some(Token.Word("map")) if peek(at + 1).contains(Token.Symbol('<')) =>
            (for
              _     <- word
              _     <- symbol('<')
              key   <- word
              _     <- symbol(',')
              value <- fieldType
              _     <- symbol('>')
            yield (key, value)).run(at).flatMap:
              case ((_, FieldType.Map(_, _)), _) => Left("a map's values are a map, which Protobuf does not allow")
              case ((key, value), next)          => ProtobufSchema.scalars.get(key)
                  .filterNot(scalar => scalar == FieldType.Double || scalar == FieldType.Float || scalar == FieldType.Bytes) match
                  case Some(scalar) => Right((FieldType.Map(scalar, value), next))
                  case None         => Left(s"a map's keys are $key, which Protobuf does not allow")
          case Some(Token.Word(name)) => Right((ProtobufSchema.scalars.getOrElse(name, FieldType.Named(name)), at + 1))
          case _                      => fail(at, "a type")

    /** A field's options, of which `packed` and `default` are kept. */
    private val options: Step[(Option[Boolean], Option[String])] =
      Step: at =>
        if !peek(at).contains(Token.Symbol('[')) then Right(((None, None), at))
        else
          @tailrec
          def go(at: Int, packed: Option[Boolean], default: Option[String]): Either[String, ((Option[Boolean], Option[String]), Int)] =
            val option =
              peek(at) match
                // Of the options named plainly, `packed` and `default` are kept, and any other's value, which may be an aggregate, is skipped.
                case Some(Token.Word(name)) => (for
                    _     <- word
                    _     <- symbol('=')
                    value <- if name == "packed" || name == "default" then constant.map(Some(_)) else Step(skipOptionValue).map(_ => None)
                  yield value.map(name -> _)).run(at)
                // A custom option, named in parentheses, is skipped.
                case _ => skipPast('=').flatMap(_ => Step(skipOptionValue)).run(at).map((_, next) => (None, next))
            option match
              case Left(reason)        => Left(reason)
              case Right((pair, next)) =>
                val (nowPacked, nowDefault) =
                  pair match
                    case Some(("packed", value))  => (Some(value == "true"), default)
                    case Some(("default", value)) => (packed, Some(value))
                    case _                        => (packed, default)
                peek(next) match
                  case Some(Token.Symbol(',')) => go(next + 1, nowPacked, nowDefault)
                  case Some(Token.Symbol(']')) => Right(((nowPacked, nowDefault), next + 1))
                  case _                       => fail(next, "',' or ']'")
          go(at + 1, None, None)

    /** Skips an option's value: a constant, or a message's value in braces. */
    private def skipOptionValue(at: Int): Either[String, (Unit, Int)] =
      // An aggregate's braces may hold others, so they are counted to its end.
      @tailrec
      def closing(at: Int, open: Int): Either[String, (Unit, Int)] =
        peek(at) match
          case None                    => Left("an option's value is not closed")
          case Some(Token.Symbol('{')) => closing(at + 1, open + 1)
          case Some(Token.Symbol('}')) => if open == 1 then Right(((), at + 1)) else closing(at + 1, open - 1)
          case _                       => closing(at + 1, open)
      if peek(at).contains(Token.Symbol('{')) then closing(at + 1, 1) else constant.run(at).map((_, next) => ((), next))

    private val enumeration: Step[Enum] =
      for
        _      <- word
        name   <- word
        _      <- symbol('{')
        values <-
          items(
            Step: at =>
              peek(at) match
                case Some(Token.Word("option" | "reserved")) => skipPast(';').run(at).map((_, next) => (None, next))
                case Some(Token.Symbol(';'))                 => Right((None, at + 1))
                case _                                       => (for
                    value  <- word
                    _      <- symbol('=')
                    number <- integer
                    _      <- options
                    _      <- symbol(';')
                  yield Some(value -> number)).run(at)
          )
      yield Enum(name, values)

  private given cats.Monad[Step] with
    override def pure[A](value: A): Step[A]                                    = Step(at => Right((value, at)))
    override def flatMap[A, B](step: Step[A])(f: A => Step[B]): Step[B]        = step.flatMap(f)
    override def tailRecM[A, B](start: A)(f: A => Step[Either[A, B]]): Step[B] =
      Step: at =>
        @tailrec
        def go(value: A, at: Int): Either[String, (B, Int)] =
          f(value).run(at) match
            case Left(reason)              => Left(reason)
            case Right((Left(next), end))  => go(next, end)
            case Right((Right(done), end)) => Right((done, end))
        go(start, at)
