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

import scala.quoted.*

/** What a derived schema takes from a Scala type at compile time: its name, namespace and annotations, its fields' annotations, and the defaults of
  * its constructor.
  */
private[avro] object Meta:
  /** A type's Avro name, namespace, documentation and aliases. */
  final case class Named(name: String, namespace: Option[String], doc: Option[String], aliases: List[String])

  /** A field's Avro name, documentation and aliases, and whether its Scala default is left out. */
  final case class Field(name: String, doc: Option[String], aliases: List[String], noDefault: Boolean)

  inline def named[A]: Named = ${ namedOf[A] }

  inline def fields[A]: List[Field] = ${ fieldsOf[A] }

  /** Each constructor parameter's default, written as its field's Avro value once asked for, in order. */
  inline def defaults[A]: List[Option[() => Either[String, AvroValue]]] = ${ defaultsOf[A] }

  private def text(using Quotes)(annotations: List[quotes.reflect.Term], annotation: String): List[String] =
    import quotes.reflect.*
    annotations.collect:
      case Apply(Select(New(tpt), _), List(Literal(StringConstant(value)))) if tpt.tpe.typeSymbol.fullName == s"xkafka.avro.$annotation" => value

  private def marked(using Quotes)(annotations: List[quotes.reflect.Term], annotation: String): Boolean =
    annotations.exists(_.tpe.typeSymbol.fullName == s"xkafka.avro.$annotation")

  /** The symbol whose name and annotations a type has: an enum's simple case's own, or the class's. */
  private def symbolOf(using Quotes)(tpe: quotes.reflect.TypeRepr): quotes.reflect.Symbol =
    import quotes.reflect.*
    tpe.dealias match
      case TermRef(_, _) => tpe.termSymbol
      case other         => other.typeSymbol

  /** The constructor parameters of a type's values; none for a singleton, such as an enum's simple case, whose class is the enum's. */
  private def parametersOf(using Quotes)(tpe: quotes.reflect.TypeRepr): List[quotes.reflect.Symbol] =
    import quotes.reflect.*
    tpe.dealias match
      case TermRef(_, _) => Nil
      case other         => other.typeSymbol.primaryConstructor.paramSymss.flatten.filterNot(_.isTypeParam)

  private def namedOf[A: Type](using Quotes): Expr[Named] =
    import quotes.reflect.*
    val tpe         = TypeRepr.of[A]
    val symbol      = symbolOf(tpe)
    val annotations = symbol.annotations
    val arguments   =
      tpe.dealias match
        case AppliedType(_, args) => args.map(arg => symbolOf(arg).name.stripSuffix("$"))
        case _                    => Nil
    // A generic type's name carries its type arguments, as avro4s names it, so Box[Int] and Box[String] are two records.
    val scalaName = symbol.name.stripSuffix("$") + (if arguments.isEmpty then "" else arguments.mkString("__", "_", ""))
    val packages  =
      Iterator.iterate(symbol.owner)(_.owner).takeWhile(owner => !owner.isNoSymbol && owner != defn.RootClass).filter(_.isPackageDef).map(_.name)
        .toList.reverse.filterNot(_ == "<empty>")
    val name: String              = text(annotations, "AvroName").headOption.getOrElse(scalaName)
    val namespace: Option[String] = text(annotations, "AvroNamespace").headOption.orElse(Option.when(packages.nonEmpty)(packages.mkString(".")))
    val doc: Option[String]       = text(annotations, "AvroDoc").headOption
    val aliases: List[String]     = text(annotations, "AvroAlias")
    '{ Named(${ Expr(name) }, ${ Expr(namespace) }, ${ Expr(doc) }, ${ Expr(aliases) }) }

  private def fieldsOf[A: Type](using Quotes): Expr[List[Field]] =
    import quotes.reflect.*
    Expr.ofList(
      parametersOf(TypeRepr.of[A]).map: parameter =>
        val annotations           = parameter.annotations
        val name: String          = text(annotations, "AvroName").headOption.getOrElse(parameter.name)
        val doc: Option[String]   = text(annotations, "AvroDoc").headOption
        val aliases: List[String] = text(annotations, "AvroAlias")
        val noDefault: Boolean    = marked(annotations, "AvroNoDefault")
        '{ Field(${ Expr(name) }, ${ Expr(doc) }, ${ Expr(aliases) }, ${ Expr(noDefault) }) }
    )

  private def defaultsOf[A: Type](using Quotes): Expr[List[Option[() => Either[String, AvroValue]]]] =
    import quotes.reflect.*
    val tpe       = TypeRepr.of[A]
    val symbol    = tpe.typeSymbol
    val arguments =
      tpe.dealias match
        case AppliedType(_, args) => args
        case _                    => Nil
    Expr.ofList(
      parametersOf(tpe).zipWithIndex.map: (parameter, index) =>
        tpe.memberType(symbol.fieldMember(parameter.name)).asType match
          case '[field] => symbol.companionClass.declaredMethod(s"$$lessinit$$greater$$default$$${index + 1}").headOption match
              case Some(method) if parameter.flags.is(Flags.HasDefault) =>
                val call  = Select(Ref(symbol.companionModule), method)
                val value = (if arguments.isEmpty then call else call.appliedToTypes(arguments)).asExprOf[field]
                '{ Some(() => AvroCodec.encodeDefault[field]($value)(using scala.compiletime.summonInline[Resolved[field]])) }
              case _ => '{ None }
    )
