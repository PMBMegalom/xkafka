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

import scala.quoted.*

/** How a schema language whose codecs are `C` writes a field's default of type `T` as one of its values, `Out`, for the schemas it derives. Each
  * schema language gives these in the companion of `C`; programs do not need to write one.
  */
trait DefaultOf[C[_], Out, T]:
  def write(value: T): Either[String, Out]

/** What a derived schema takes from a Scala type at compile time, whatever the schema language: its name, namespace and annotations, its fields'
  * annotations, and the defaults of its constructor.
  */
private[xkafka] object Meta:
  /** A type's name, namespace, documentation and aliases. */
  final case class Named(name: String, namespace: Option[String], doc: Option[String], aliases: List[String])

  /** A field's name, documentation and aliases, and whether its Scala default is left out. */
  final case class Field(name: String, doc: Option[String], aliases: List[String], noDefault: Boolean)

  inline def named[A]: Named = ${ namedOf[A] }

  inline def fields[A]: List[Field] = ${ fieldsOf[A] }

  /** Each constructor parameter's default, written as the format whose codecs are `C` writes it, once asked for, in order. */
  inline def defaults[A, C[_], Out]: List[Option[() => Either[String, Out]]] = ${ defaultsOf[A, C, Out] }

  private def text(using Quotes)(annotations: List[quotes.reflect.Term], annotation: String): List[String] =
    import quotes.reflect.*
    annotations.collect:
      case Apply(Select(New(tpt), _), List(Literal(StringConstant(value)))) if tpt.tpe.typeSymbol.fullName == s"xkafka.schemaregistry.$annotation" =>
        value

  private def marked(using Quotes)(annotations: List[quotes.reflect.Term], annotation: String): Boolean =
    annotations.exists(_.tpe.typeSymbol.fullName == s"xkafka.schemaregistry.$annotation")

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
    // A generic type's name carries its type arguments, so Box[Int] and Box[String] are two records.
    val scalaName = symbol.name.stripSuffix("$") + (if arguments.isEmpty then "" else arguments.mkString("__", "_", ""))
    val packages  =
      Iterator.iterate(symbol.owner)(_.owner).takeWhile(owner => !owner.isNoSymbol && owner != defn.RootClass).filter(_.isPackageDef).map(_.name)
        .toList.reverse.filterNot(_ == "<empty>")
    val name: String              = text(annotations, "SchemaName").headOption.getOrElse(scalaName)
    val namespace: Option[String] = text(annotations, "SchemaNamespace").headOption.orElse(Option.when(packages.nonEmpty)(packages.mkString(".")))
    val doc: Option[String]       = text(annotations, "SchemaDoc").headOption
    val aliases: List[String]     = text(annotations, "SchemaAlias")
    '{ Named(${ Expr(name) }, ${ Expr(namespace) }, ${ Expr(doc) }, ${ Expr(aliases) }) }

  private def fieldsOf[A: Type](using Quotes): Expr[List[Field]] =
    import quotes.reflect.*
    Expr.ofList(
      parametersOf(TypeRepr.of[A]).map: parameter =>
        val annotations           = parameter.annotations
        val name: String          = text(annotations, "SchemaName").headOption.getOrElse(parameter.name)
        val doc: Option[String]   = text(annotations, "SchemaDoc").headOption
        val aliases: List[String] = text(annotations, "SchemaAlias")
        val noDefault: Boolean    = marked(annotations, "SchemaNoDefault")
        '{ Field(${ Expr(name) }, ${ Expr(doc) }, ${ Expr(aliases) }, ${ Expr(noDefault) }) }
    )

  private def defaultsOf[A: Type, C[_]: Type, Out: Type](using Quotes): Expr[List[Option[() => Either[String, Out]]]] =
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
                '{ Some(() => scala.compiletime.summonInline[DefaultOf[C, Out, field]].write($value)) }
              case _ => '{ None }
    )
