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

package xkafka.avro

import java.nio.charset.StandardCharsets

import cats.data.{EitherT, State}
import cats.syntax.all.*
import fs2.Chunk
import xkafka.schemaregistry.Shown

/** Avro's binary encoding, and reading what was written with one schema as another, as the specification resolves them. */
object AvroBinary:
  /** How deep records may hold records, as a list's nodes do, before a value is refused instead of read. */
  private[avro] val MaxDepth = 100

  /** `value` written as `schema` describes it, or why it does not fit the schema. */
  def encode(schema: AvroSchema, value: AvroValue): Either[String, Chunk[Byte]] =
    write(schema, value, AvroSchema.definitions(schema)).map(parts => Chunk.concat(parts.toList))

  /** What was written with `schema`, read back. */
  def decode(schema: AvroSchema, bytes: Chunk[Byte]): Either[String, AvroValue] = decoder(schema, schema).flatMap(_(bytes))

  /** How what was written with `writer` is read as `reader` describes it, or why it cannot be: the specification's schema resolution, which takes a
    * default for a field the writer did not write, skips a field the reader does not have, and widens numbers. A writer's union branch or enum symbol
    * the reader cannot take fails only a value that has it.
    */
  def decoder(writer: AvroSchema, reader: AvroSchema): Either[String, Chunk[Byte] => Either[String, AvroValue]] =
    Resolution(AvroSchema.definitions(writer), AvroSchema.definitions(reader)).plan(writer, reader).map: step =>
      bytes =>
        val array = bytes.toArray
        step(array, 0, 0).flatMap: (value, end) =>
          Either.cond(end == array.length, value, s"${array.length - end} bytes are left after the value")

  // Writing

  private type Defined = Map[FullName, AvroSchema]

  private def write(schema: AvroSchema, value: AvroValue, defined: Defined): Either[String, Vector[Chunk[Byte]]] =
    def mismatch = Left(s"${describe(value)} does not fit ${AvroSchemaJson.render(schema)}")
    (schema, value) match
      case (AvroSchema.Named(name), _)          => defined.get(name).toRight(s"'${name.value}' is not defined").flatMap(write(_, value, defined))
      case (AvroSchema.Logical(logicalType), _) => write(logicalType.underlying, value, defined)
      case (AvroSchema.Null, AvroValue.Null)    => Right(Vector.empty)
      case (AvroSchema.Boolean, AvroValue.Boolean(value)) => Right(Vector(Chunk.singleton(if value then 1.toByte else 0.toByte)))
      case (AvroSchema.Int, AvroValue.Int(value))         => Right(Vector(varLong(value.toLong)))
      case (AvroSchema.Long, AvroValue.Long(value))       => Right(Vector(varLong(value)))
      case (AvroSchema.Float, AvroValue.Float(value))     => Right(Vector(littleEndian(java.lang.Float.floatToRawIntBits(value).toLong, 4)))
      case (AvroSchema.Double, AvroValue.Double(value))   => Right(Vector(littleEndian(java.lang.Double.doubleToRawLongBits(value), 8)))
      case (AvroSchema.Bytes, AvroValue.Bytes(bytes))     => Right(Vector(varLong(bytes.size.toLong), bytes))
      case (AvroSchema.String, AvroValue.String(text))    =>
        val bytes = text.getBytes(StandardCharsets.UTF_8)
        Right(Vector(varLong(bytes.length.toLong), Chunk.array(bytes)))
      case (AvroSchema.Record(name, fields, _, _), AvroValue.Record(_, values)) => fields.traverse(field =>
          values.collectFirst { case (field.name, value) => value }.orElse(field.default)
            .toRight(s"record '${name.value}' has no field '${field.name}'").flatMap(write(field.schema, _, defined))
        ).map(_.toVector.flatten)
      case (AvroSchema.Enum(name, symbols, _, _, _), AvroValue.Enum(_, symbol)) => symbols.indexOf(symbol) match
          case -1    => Left(s"'$symbol' is not a symbol of '${name.value}'")
          case index => Right(Vector(varLong(index.toLong)))
      case (AvroSchema.Array(items), AvroValue.Array(values)) => blocks(values)(write(items, _, defined))
      case (AvroSchema.Map(schema), AvroValue.Map(entries))   =>
        blocks(entries)((key, value) => (write(AvroSchema.String, AvroValue.String(key), defined), write(schema, value, defined)).mapN(_ ++ _))
      case (AvroSchema.Union(branches), _) => branches.indexWhere(fits(_, value, defined)) match
          case -1    => mismatch
          case index => write(branches(index), value, defined).map(varLong(index.toLong) +: _)
      case (AvroSchema.Fixed(name, size, _), AvroValue.Fixed(_, bytes)) => Either
          .cond(bytes.size == size, Vector(bytes), s"fixed '${name.value}' is $size bytes, but the value is ${bytes.size}")
      case _ => mismatch

  /** Items written in one block of their count, then the empty block that ends them. */
  private def blocks[A](items: Vector[A])(item: A => Either[String, Vector[Chunk[Byte]]]): Either[String, Vector[Chunk[Byte]]] =
    if items.isEmpty then Right(Vector(varLong(0)))
    else items.flatTraverse(item).map(written => (varLong(items.size.toLong) +: written) :+ varLong(0))

  /** Whether `value` is one `schema` describes, which picks the union branch it is written as: the first that fits. */
  private[avro] def fits(schema: AvroSchema, value: AvroValue, defined: Map[FullName, AvroSchema]): Boolean =
    (schema, value) match
      case (AvroSchema.Named(name), _)                                    => defined.get(name).exists(fits(_, value, defined))
      case (AvroSchema.Logical(logicalType), _)                           => fits(logicalType.underlying, value, defined)
      case (AvroSchema.Null, AvroValue.Null)                              => true
      case (AvroSchema.Boolean, AvroValue.Boolean(_))                     => true
      case (AvroSchema.Int, AvroValue.Int(_))                             => true
      case (AvroSchema.Long, AvroValue.Long(_))                           => true
      case (AvroSchema.Float, AvroValue.Float(_))                         => true
      case (AvroSchema.Double, AvroValue.Double(_))                       => true
      case (AvroSchema.Bytes, AvroValue.Bytes(_))                         => true
      case (AvroSchema.String, AvroValue.String(_))                       => true
      case (AvroSchema.Record(name, _, _, _), AvroValue.Record(named, _)) => name == named
      case (AvroSchema.Enum(name, _, _, _, _), AvroValue.Enum(named, _))  => name == named
      case (AvroSchema.Fixed(name, _, _), AvroValue.Fixed(named, _))      => name == named
      case (AvroSchema.Array(_), AvroValue.Array(_))                      => true
      case (AvroSchema.Map(_), AvroValue.Map(_))                          => true
      case _                                                              => false

  /** A number as Avro writes `int` and `long`: zigzag, so small negative numbers are short, then seven bits a byte, least significant first. */
  private def varLong(value: Long): Chunk[Byte] =
    def unsigned(left: Long): List[Byte] = if (left & ~0x7fL) == 0 then List(left.toByte) else ((left & 0x7f) | 0x80).toByte :: unsigned(left >>> 7)
    Chunk.from(unsigned((value << 1) ^ (value >> 63)))

  private def littleEndian(bits: Long, size: Int): Chunk[Byte] = Chunk.from((0 until size).map(index => (bits >>> (8 * index)).toByte))

  private def describe(value: AvroValue): String =
    value match
      case AvroValue.Record(name, _) => s"a value of record '${name.value}'"
      case AvroValue.Enum(name, _)   => s"a value of enum '${name.value}'"
      case AvroValue.Fixed(name, _)  => s"a value of fixed '${name.value}'"
      case other                     => s"the value ${Shown(other)}"

  // Reading

  /** Reads a value from `bytes` at a position, inside as many records as the depth says, giving the position after it. */
  private type Step = (Array[Byte], Int, Int) => Either[String, (AvroValue, Int)]

  /** How many items of a type written as no bytes, such as `null`, an array may hold, since the bytes left cannot bound their count. */
  private[avro] val MaxEmptyItems = 1 << 20

  private def readLong(bytes: Array[Byte], at: Int, most: Int): Either[String, (Long, Int)] =
    def from(index: Int, shift: Int, sofar: Long): Either[String, (Long, Int)] =
      if index >= bytes.length then Left("the value ends before its last byte")
      else if index - at >= most then Left(s"a number takes more than $most bytes")
      else
        val byte  = bytes(index)
        val value = sofar | ((byte & 0x7fL) << shift)
        // The tenth byte holds the 64th bit alone.
        if shift == 63 && (byte & 0x7e) != 0 then Left("a number does not fit 64 bits")
        else if (byte & 0x80) == 0 then Right(((value >>> 1) ^ -(value & 1), index + 1))
        else from(index + 1, shift + 7, value)
    from(at, 0, 0L)

  private def readInt(bytes: Array[Byte], at: Int): Either[String, (Int, Int)] =
    readLong(bytes, at, 5).flatMap((value, end) => Either.cond(value.isValidInt, (value.toInt, end), s"$value is not an int"))

  private def readFixed(bytes: Array[Byte], at: Int, size: Int): Either[String, (Chunk[Byte], Int)] =
    Either.cond(
      size >= 0 && size <= bytes.length - at,
      (Chunk.array(java.util.Arrays.copyOfRange(bytes, at, at + size)), at + size),
      "the value ends before its last byte"
    )

  private def readBytes(bytes: Array[Byte], at: Int): Either[String, (Chunk[Byte], Int)] =
    readLong(bytes, at, 10).flatMap: (size, start) =>
      if size < 0 || size > bytes.length - start then Left(s"a length of $size does not fit the value") else readFixed(bytes, start, size.toInt)

  private def readLittleEndian(bytes: Array[Byte], at: Int, size: Int): Either[String, (Long, Int)] =
    Either.cond(
      at + size <= bytes.length,
      ((0 until size).foldLeft(0L)((sofar, index) => sofar | ((bytes(at + index) & 0xffL) << (8 * index))), at + size),
      "the value ends before its last byte"
    )

  /** Reads the blocks of an array or map, each item with `item`, until the empty block. A negative count is followed by the block's size in bytes.
    * Items `empty` says are written as no bytes are refused past `MaxEmptyItems`, and others past one for each byte left.
    */
  private def readBlocks[A](bytes: Array[Byte], at: Int, empty: Boolean)(
      item: (Array[Byte], Int) => Either[String, (A, Int)]
  ): Either[String, (Vector[A], Int)] =
    @scala.annotation.tailrec
    def items(left: Long, position: Int, sofar: Vector[A]): Either[String, (Vector[A], Int)] =
      if left == 0 then Right((sofar, position))
      else
        item(bytes, position) match
          case Right((value, next)) => items(left - 1, next, sofar :+ value)
          case Left(reason)         => Left(reason)
    @scala.annotation.tailrec
    def block(position: Int, sofar: Vector[A]): Either[String, (Vector[A], Int)] =
      val read =
        readLong(bytes, position, 10).flatMap: (count, after) =>
          if count == 0 then Right(None)
          else if count == Long.MinValue then Left(s"a count of $count items does not fit the value")
          else
            val start = if count < 0 then readLong(bytes, after, 10).map(_._2) else Right(after)
            start.flatMap: begin =>
              val most = if empty then MaxEmptyItems - sofar.size else bytes.length - begin
              if math.abs(count) > most then Left(s"a count of ${math.abs(count)} items does not fit the value")
              else items(math.abs(count), begin, sofar).map(Some(_))
      read match
        case Right(Some((more, next))) => block(next, more)
        case Right(None)               => readLong(bytes, position, 10).map((_, end) => (sofar, end))
        case Left(reason)              => Left(reason)
    block(at, Vector.empty)

  /** The record pairs planned so far, and those being planned, which a record that holds itself reads through. */
  private final case class Planned(done: Map[(FullName, FullName), Either[String, Step]], planning: Map[(FullName, FullName), Pending])

  private type Planning[A] = EitherT[[S] =>> State[Planned, S], String, A]

  /** The plans for reading one schema's values as another's, for the named types of each. Each pair of records is planned once, however often the
    * schemas use it.
    */
  private final case class Resolution(writers: Defined, readers: Defined):
    /** How what `writer` wrote is read as `reader`. */
    def plan(writer: AvroSchema, reader: AvroSchema): Either[String, Step] = planning(writer, reader).value.runA(Planned(Map.empty, Map.empty)).value

    private def planning(writer: AvroSchema, reader: AvroSchema): Planning[Step] =
      (writer, reader) match
        case (AvroSchema.Named(name), _) => found(writers.get(name).toRight(s"'${name.value}' is not defined")).flatMap(planning(_, reader))
        case (_, AvroSchema.Named(name)) => found(readers.get(name).toRight(s"'${name.value}' is not defined")).flatMap(planning(writer, _))
        // A decimal's bytes mean nothing without its scale, so they are read only as a decimal of that scale, with room for their digits.
        case (AvroSchema.Logical(Digits(precision, scale)), AvroSchema.Logical(Digits(readPrecision, readScale)))
            if scale != readScale || precision > readPrecision =>
          found(Left(s"a decimal of precision $precision and scale $scale is not read as one of precision $readPrecision and scale $readScale"))
        case (AvroSchema.Logical(logicalType), _) => planning(logicalType.underlying, reader)
        case (_, AvroSchema.Logical(logicalType)) => planning(writer, logicalType.underlying)
        case (AvroSchema.Union(branches), _)      =>
          // Each branch is planned now, and one the reader cannot take fails only a value written with it.
          EitherT.liftF(branches.toVector.traverse(branch => planning(branch, reader).value)).map: steps =>
            (bytes, at, depth) =>
              readLong(bytes, at, 10).flatMap: (index, after) =>
                steps.lift(index.toInt).filter(_ => index.isValidInt).toRight(s"union branch $index is not one of the writer's ${steps.size}").flatten
                  .flatMap(_(bytes, after, depth))
        case (_, AvroSchema.Union(branches)) => found(
            branches.find(same(writer, _)).orElse(branches.find(promotes(writer, _)))
              .toRight(s"no branch of ${render(reader)} reads ${render(writer)}")
          ).flatMap(planning(writer, _))
        case (written: AvroSchema.Record, read: AvroSchema.Record) =>
          if !named(written.name, read.name, read.aliases) then found(Left(s"record '${written.name.value}' is not read as '${read.name.value}'"))
          else recordOnce(written, read)
        case (AvroSchema.Array(written), AvroSchema.Array(read)) =>
          val empty = writesNothing(written)
          planning(written, read).map: item =>
            (bytes, at, depth) => readBlocks(bytes, at, empty)(item(_, _, depth)).map((items, end) => (AvroValue.Array(items), end))
        case (AvroSchema.Map(written), AvroSchema.Map(read)) => planning(written, read).map: value =>
            (bytes, at, depth) =>
              val entry =
                (bytes: Array[Byte], at: Int) =>
                  readBytes(bytes, at).flatMap: (key, after) =>
                    value(bytes, after, depth).map((read, end) => ((new String(key.toArray, StandardCharsets.UTF_8), read), end))
              readBlocks(bytes, at, empty = false)(entry).map((entries, end) => (AvroValue.Map(entries), end))
        case _ => found(single(writer, reader))

    private def found[A](result: Either[String, A]): Planning[A] = EitherT.fromEither(result)

    /** The plan of a pair of records: the one made before, or, for a pair being planned, one reading through it, or else a new one. */
    private def recordOnce(written: AvroSchema.Record, read: AvroSchema.Record): Planning[Step] =
      val pair = (written.name, read.name)
      EitherT:
        State: state =>
          state.done.get(pair) match
            case Some(step) => (state, step)
            case None       => state.planning.get(pair) match
                case Some(pending) => (state, Right(through(pending)))
                case None          =>
                  lazy val made: (Planned, Either[String, Step]) =
                    record(written, read).value.run(state.copy(planning = state.planning.updated(pair, pending))).value
                  lazy val pending: Pending = Pending(made._2)
                  val (after, step)         = made
                  val result                = step.map(_ => through(pending))
                  // Pairs planned meanwhile may read through this one, so a failure forgets them, to be planned again and fail as well.
                  val kept = if result.isLeft then state.done else after.done
                  (after.copy(done = kept.updated(pair, result), planning = state.planning), result)

    private def through(pending: Pending): Step = (bytes, at, depth) => pending.step.flatMap(_(bytes, at, depth))

    /** How a value of a type that holds no other is read, or why it cannot be. */
    private def single(writer: AvroSchema, reader: AvroSchema): Either[String, Step] =
      (writer, reader) match
        case (AvroSchema.Null, AvroSchema.Null)       => Right((_, at, _) => Right((AvroValue.Null, at)))
        case (AvroSchema.Boolean, AvroSchema.Boolean) =>
          Right((bytes, at, _) => Either.cond(at < bytes.length, (AvroValue.Boolean(bytes(at) != 0), at + 1), "the value ends before its last byte"))
        case (AvroSchema.Int, AvroSchema.Int)  => Right((bytes, at, _) => readInt(bytes, at).map((value, end) => (AvroValue.Int(value), end)))
        case (AvroSchema.Int, AvroSchema.Long) => Right((bytes, at, _) => readInt(bytes, at).map((value, end) => (AvroValue.Long(value.toLong), end)))
        case (AvroSchema.Int, AvroSchema.Float) =>
          Right((bytes, at, _) => readInt(bytes, at).map((value, end) => (AvroValue.Float(value.toFloat), end)))
        case (AvroSchema.Int, AvroSchema.Double) =>
          Right((bytes, at, _) => readInt(bytes, at).map((value, end) => (AvroValue.Double(value.toDouble), end)))
        case (AvroSchema.Long, AvroSchema.Long)  => Right((bytes, at, _) => readLong(bytes, at, 10).map((value, end) => (AvroValue.Long(value), end)))
        case (AvroSchema.Long, AvroSchema.Float) =>
          Right((bytes, at, _) => readLong(bytes, at, 10).map((value, end) => (AvroValue.Float(value.toFloat), end)))
        case (AvroSchema.Long, AvroSchema.Double) =>
          Right((bytes, at, _) => readLong(bytes, at, 10).map((value, end) => (AvroValue.Double(value.toDouble), end)))
        case (AvroSchema.Float, AvroSchema.Float)  => Right((bytes, at, _) => float(bytes, at).map((value, end) => (AvroValue.Float(value), end)))
        case (AvroSchema.Float, AvroSchema.Double) =>
          Right((bytes, at, _) => float(bytes, at).map((value, end) => (AvroValue.Double(value.toDouble), end)))
        case (AvroSchema.Double, AvroSchema.Double) =>
          Right((bytes, at, _) => readLittleEndian(bytes, at, 8).map((bits, end) => (AvroValue.Double(java.lang.Double.longBitsToDouble(bits)), end)))
        case (AvroSchema.Bytes | AvroSchema.String, AvroSchema.Bytes) =>
          Right((bytes, at, _) => readBytes(bytes, at).map((value, end) => (AvroValue.Bytes(value), end)))
        case (AvroSchema.Bytes | AvroSchema.String, AvroSchema.String) => Right((bytes, at, _) =>
            readBytes(bytes, at).map((value, end) => (AvroValue.String(new String(value.toArray, StandardCharsets.UTF_8)), end))
          )
        case (AvroSchema.Fixed(written, size, _), AvroSchema.Fixed(read, expected, aliases)) =>
          if !named(written, read, aliases) then Left(s"fixed '${written.value}' is not read as '${read.value}'")
          else if size != expected then Left(s"fixed '${read.value}' is $expected bytes, but '${written.value}' was written as $size")
          else Right((bytes, at, _) => readFixed(bytes, at, size).map((value, end) => (AvroValue.Fixed(read, value), end)))
        case (AvroSchema.Enum(written, symbols, _, _, _), AvroSchema.Enum(read, known, default, _, aliases)) =>
          if !named(written, read, aliases) then Left(s"enum '${written.value}' is not read as '${read.value}'")
          else
            val mapped =
              symbols.map(symbol =>
                Some(symbol).filter(known.contains).orElse(default).toRight(s"enum '${read.value}' has no symbol '$symbol' and no default")
              ).toVector
            Right: (bytes, at, _) =>
              readInt(bytes, at).flatMap: (index, end) =>
                mapped.lift(index).toRight(s"enum symbol $index is not one of the writer's ${mapped.size}").flatten
                  .map(symbol => (AvroValue.Enum(read, symbol), end))
        case _ => Left(s"${render(reader)} cannot read what ${render(writer)} wrote")

    private def record(written: AvroSchema.Record, read: AvroSchema.Record): Planning[Step] =
      def readerField(field: AvroField) = read.fields.find(candidate => candidate.name == field.name || candidate.aliases.contains(field.name))
      // A field the reader lacks is read only to be skipped, as the writer's own schema reads it, whatever the reader names the same.
      def skipped(field: AvroField) = found(Resolution(writers, writers).plan(field.schema, field.schema))
      val planned                   =
        written.fields.traverse(field => readerField(field).fold(skipped(field))(target => planning(field.schema, target.schema)).map(field -> _))
      planned.flatMap: steps =>
        val taken    = written.fields.flatMap(readerField).map(_.name).toSet
        val defaults =
          read.fields.filterNot(field => taken.contains(field.name)).traverse: field =>
            field.default
              .toRight(s"record '${read.name.value}' has no default for field '${field.name}', which '${written.name.value}' does not write")
              .map(field.name -> _)
        found(defaults).map: filled =>
          (bytes, at, depth) =>
            if depth >= MaxDepth then Left(s"the value is nested more than $MaxDepth deep")
            else
              steps.foldM((Map.empty[String, AvroValue], at)) { case ((values, position), (field, step)) =>
                step(bytes, position, depth + 1)
                  .map((value, next) => (readerField(field).fold(values)(target => values.updated(target.name, value)), next))
              }.map: (values, end) =>
                val all = values ++ filled
                (AvroValue.Record(read.name, read.fields.map(field => field.name -> all(field.name)).toVector), end)

    /** The writer's records written as no bytes, those whose fields all are: found by adding them until no more are, so a record that holds itself is
      * not one.
      */
    private lazy val silent: Set[FullName] =
      val records = writers.values.collect { case record: AvroSchema.Record => record }.toList
      @scala.annotation.tailrec
      def grow(found: Set[FullName]): Set[FullName] =
        val more = records.filter(record => !found(record.name) && record.fields.forall(field => writesNothing(field.schema, found))).map(_.name)
        if more.isEmpty then found else grow(found ++ more)
      grow(Set.empty)

    /** Whether values of `schema` are written as no bytes, as `null` and records of only such fields are. */
    private def writesNothing(schema: AvroSchema): Boolean = writesNothing(schema, silent)

    private def writesNothing(schema: AvroSchema, silent: Set[FullName]): Boolean =
      schema match
        case AvroSchema.Null                 => true
        case AvroSchema.Fixed(_, size, _)    => size == 0
        case AvroSchema.Logical(logicalType) => writesNothing(logicalType.underlying, silent)
        case AvroSchema.Named(name)          => writers.get(name).exists(writesNothing(_, silent))
        case record: AvroSchema.Record       => silent(record.name)
        case _                               => false

    private def float(bytes: Array[Byte], at: Int): Either[String, (Float, Int)] =
      readLittleEndian(bytes, at, 4).map((bits, end) => (java.lang.Float.intBitsToFloat(bits.toInt), end))

    /** Whether a reader named `read`, with `aliases`, takes a named type written as `written`: their names alone match, or an alias names it. */
    private def named(written: FullName, read: FullName, aliases: List[FullName]): Boolean =
      written.name == read.name || aliases.exists(alias => alias == written || alias.name == written.name && alias.namespace.isEmpty)

    /** Whether `reader` is the same kind of schema as `writer`, which is the union branch the reader takes for it before any it promotes to. */
    private def same(writer: AvroSchema, reader: AvroSchema): Boolean =
      (base(writer, writers), base(reader, readers)) match
        case (AvroSchema.Record(written, _, _, _), AvroSchema.Record(read, _, _, aliases))       => named(written, read, aliases)
        case (AvroSchema.Enum(written, _, _, _, _), AvroSchema.Enum(read, _, _, _, aliases))     => named(written, read, aliases)
        case (AvroSchema.Fixed(written, _, _), AvroSchema.Fixed(read, _, aliases))               => named(written, read, aliases)
        case (AvroSchema.Array(_), AvroSchema.Array(_)) | (AvroSchema.Map(_), AvroSchema.Map(_)) => true
        case (left, right)                                                                       => left == right

    private def promotes(writer: AvroSchema, reader: AvroSchema): Boolean =
      (base(writer, writers), base(reader, readers)) match
        case (AvroSchema.Int, AvroSchema.Long | AvroSchema.Float | AvroSchema.Double)      => true
        case (AvroSchema.Long, AvroSchema.Float | AvroSchema.Double)                       => true
        case (AvroSchema.Float, AvroSchema.Double)                                         => true
        case (AvroSchema.String, AvroSchema.Bytes) | (AvroSchema.Bytes, AvroSchema.String) => true
        case _                                                                             => false

    private def base(schema: AvroSchema, defined: Defined): AvroSchema =
      schema match
        case AvroSchema.Named(name)          => defined.get(name).fold(schema)(base(_, defined))
        case AvroSchema.Logical(logicalType) => base(logicalType.underlying, defined)
        case other                           => other

    private def render(schema: AvroSchema): String = AvroSchemaJson.render(schema)

  /** A record's plan, made once it is first needed, so a record that holds itself reads through the plan being made. */
  private final class Pending(make: => Either[String, Step]):
    lazy val step: Either[String, Step] = make

  /** The precision and scale of a decimal, whichever type holds its bytes. */
  private object Digits:
    def unapply(logicalType: LogicalType): Option[(Int, Int)] =
      logicalType match
        case LogicalType.Decimal(precision, scale)            => Some((precision, scale))
        case LogicalType.FixedDecimal(_, _, precision, scale) => Some((precision, scale))
        case _                                                => None
