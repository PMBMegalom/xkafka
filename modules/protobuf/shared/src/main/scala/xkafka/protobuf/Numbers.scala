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

import scala.quoted.*

/** The numbers `@ProtobufField` gives a type's fields and cases, read at compile time, so that a missing, repeated or disallowed number fails to
  * compile.
  */
private[protobuf] object Numbers:
  /** The numbers of a case class's fields, in order. */
  inline def fields[A]: List[Int] = ${ fieldsOf[A] }

  /** The number of a case of a sealed trait or enum. */
  inline def of[A]: Int = ${ caseOf[A] }

  /** Checks that every case of sealed trait or enum `A` has a number, that no two share one, and that an enum has a case numbered zero. */
  inline def cases[A]: Unit = ${ casesOf[A] }

  private def annotated(using Quotes)(symbol: quotes.reflect.Symbol): Option[Int] =
    import quotes.reflect.*
    val module = if symbol.flags.is(Flags.Module) && !symbol.moduleClass.isNoSymbol then symbol.moduleClass.annotations else Nil
    (symbol.annotations ++ module).filter(_.tpe.typeSymbol.fullName == "xkafka.protobuf.ProtobufField").map:
      case Apply(_, List(Literal(IntConstant(number)))) => number
      case other => report.errorAndAbort(s"the number @ProtobufField gives ${symbol.name} must be written as a literal", other.pos)
    .headOption

  private def position(using Quotes)(symbol: quotes.reflect.Symbol): quotes.reflect.Position =
    symbol.pos.getOrElse(quotes.reflect.Position.ofMacroExpansion)

  private def allowed(number: Int): Boolean = number >= 1 && number <= ProtobufWire.MaxNumber && (number < 19000 || number > 19999)

  private def check(using Quotes)(numbered: List[(quotes.reflect.Symbol, Int)], kind: String, owner: String, fields: Boolean): Unit =
    import quotes.reflect.*
    if fields then
      numbered.find((_, number) => !allowed(number)).foreach: (symbol, number) =>
        report.errorAndAbort(
          s"$kind ${symbol.name} of $owner is numbered $number, and Protobuf numbers run from 1 to ${ProtobufWire
              .MaxNumber}, leaving out 19000 to 19999",
          position(symbol)
        )
    numbered.groupBy(_._2).toList.sortBy(_._1).find(_._2.size > 1).foreach: (number, shared) =>
      report.errorAndAbort(s"${kind}s ${shared.map(_._1.name).mkString(" and ")} of $owner share number $number", position(shared.last._1))

  private def symbolOf(using Quotes)(tpe: quotes.reflect.TypeRepr): quotes.reflect.Symbol =
    import quotes.reflect.*
    tpe.dealias match
      case TermRef(_, _) => tpe.termSymbol
      case other         => other.typeSymbol

  private def fieldsOf[A: Type](using Quotes): Expr[List[Int]] =
    import quotes.reflect.*
    val tpe        = TypeRepr.of[A]
    val owner      = symbolOf(tpe).name
    val parameters =
      tpe.dealias match
        case TermRef(_, _) => Nil
        case other         => other.typeSymbol.primaryConstructor.paramSymss.flatten.filterNot(_.isTypeParam)
    val numbered =
      parameters.map: parameter =>
        parameter -> annotated(parameter).getOrElse(
          report.errorAndAbort(s"field ${parameter.name} of $owner has no @ProtobufField number, which Protobuf writes it under", position(parameter))
        )
    check(numbered, "field", owner, fields = true)
    Expr(numbered.map(_._2))

  private def caseOf[A: Type](using Quotes): Expr[Int] =
    import quotes.reflect.*
    val symbol = symbolOf(TypeRepr.of[A])
    Expr(annotated(symbol).getOrElse(report.errorAndAbort(s"case ${symbol.name} has no @ProtobufField number", position(symbol))))

  private def casesOf[A: Type](using Quotes): Expr[Unit] =
    import quotes.reflect.*
    val symbol   = TypeRepr.of[A].typeSymbol
    val owner    = symbol.name
    val numbered =
      symbol.children.map: child =>
        child -> annotated(child)
          .getOrElse(report.errorAndAbort(s"case ${child.name} of $owner has no @ProtobufField number, which Protobuf tells it by", position(child)))
    // Cases that are all objects are an enum, whose numbers may be any, one of them zero; others are a oneof's fields, numbered as fields are.
    val enumeration = symbol.children.forall(child => child.isTerm || child.flags.is(Flags.Module))
    if enumeration && !numbered.exists(_._2 == 0) then
      report.errorAndAbort(s"enum $owner has no case numbered 0, which Protobuf reads where a field of it is absent", position(symbol))
    check(numbered, "case", owner, fields = !enumeration)
    '{ () }
