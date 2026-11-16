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

  /** The constant an annotation's argument gives, in place or named: a literal, or anything the compiler folds to one, such as a `final val`. */
  private[xkafka] def constant(using Quotes)(argument: quotes.reflect.Term): Option[quotes.reflect.Constant] =
    import quotes.reflect.*
    argument match
      case NamedArg(_, value)    => constant(value)
      case Inlined(_, Nil, body) => constant(body)
      case Typed(value, _)       => constant(value)
      case Literal(value)        => Some(value)
      case _                     => argument.tpe.widenTermRefByName.dealias match
          case ConstantType(value) => Some(value)
          case _                   => None

  /** The text an annotation's argument gives as a constant. */
  private[xkafka] def constantText(using Quotes)(argument: quotes.reflect.Term): Option[String] =
    import quotes.reflect.*
    constant(argument).collect { case StringConstant(value) => value }

  /** The number an annotation's argument gives as a constant. */
  private[xkafka] def constantNumber(using Quotes)(argument: quotes.reflect.Term): Option[Int] =
    import quotes.reflect.*
    constant(argument).collect { case IntConstant(value) => value }

  /** The text each annotation `annotation` of these gives, in place or named; an argument that is no constant is refused as the code is compiled. */
  private[xkafka] def text(using Quotes)(annotations: List[quotes.reflect.Term], annotation: String): List[String] =
    import quotes.reflect.*
    annotations.collect:
      case applied @ Apply(Select(New(tpt), _), arguments) if tpt.tpe.typeSymbol.fullName == s"xkafka.schemaregistry.$annotation" =>
        arguments match
          case List(argument) => constantText(argument).getOrElse(report.errorAndAbort(s"@$annotation takes constant text", applied.pos))
          case _              => report.errorAndAbort(s"@$annotation takes constant text", applied.pos)

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

  /** The Scala names of the fields of `tpe`. */
  def fieldNamesOf(using Quotes)(tpe: quotes.reflect.TypeRepr): List[String] = parametersOf(tpe).map(_.name)

  /** The precision and scale [[SchemaDecimal]] gives each field of `tpe`, where it marks the field. */
  def decimalsOf(using Quotes)(tpe: quotes.reflect.TypeRepr): List[Option[(Int, Int)]] =
    import quotes.reflect.*
    parametersOf(tpe).map: parameter =>
      parameter.annotations.collectFirst:
        case annotation @ Apply(Select(New(tpt), _), arguments) if tpt.tpe.typeSymbol.fullName == "xkafka.schemaregistry.SchemaDecimal" =>
          val numbers =
            arguments
              .map(argument => constantNumber(argument).getOrElse(report.errorAndAbort("@SchemaDecimal takes constant numbers", annotation.pos)))
          numbers match
            case List(precision, scale) if precision >= 1 && scale >= 0 && scale <= precision => (precision, scale)
            case List(precision, scale)                                                       => report.errorAndAbort(
                s"@SchemaDecimal($precision, $scale) on ${parameter.name}: the precision must be at least 1 and the scale from 0 to the precision",
                annotation.pos
              )
            case _ => report.errorAndAbort("@SchemaDecimal takes a precision and a scale", annotation.pos)

  /** `tpe` with each `BigDecimal` in it a [[Decimals.Decimal]] of `decimal`'s precision and scale; `field` names it where it holds none. */
  def withDecimal(using Quotes)(tpe: quotes.reflect.TypeRepr, decimal: (Int, Int), field: String): quotes.reflect.TypeRepr =
    import quotes.reflect.*
    val (precision, scale) = decimal
    val decimalType        = TypeRepr.of[Decimals.Decimal].appliedTo(List(ConstantType(IntConstant(precision)), ConstantType(IntConstant(scale))))
    def replace(inside: TypeRepr): TypeRepr =
      if inside =:= TypeRepr.of[BigDecimal] then decimalType
      else
        inside.dealias match
          case AppliedType(constructor, arguments) => constructor.appliedTo(arguments.map(replace))
          case other                               => other
    val replaced = replace(tpe)
    if replaced =:= tpe then report.errorAndAbort(s"@SchemaDecimal on $field, which holds no BigDecimal") else replaced

  private def namedOf[A: Type](using Quotes): Expr[Named] =
    import quotes.reflect.*
    val tpe         = TypeRepr.of[A]
    val symbol      = symbolOf(tpe)
    val annotations = symbol.annotations
    // A generic type's name carries its type arguments in full, so Box[Int], Box[String] and Box[List[Int]] are three records.
    def written(of: TypeRepr): String =
      val name = symbolOf(of).name.stripSuffix("$")
      of.dealias match
        case AppliedType(_, args) => name + args.map(written).mkString("__", "_", "")
        case _                    => name
    val scalaName = written(tpe)
    // The namespace is the packages and the objects the type is in, so two types of one name in two objects are two types; the object Scala makes
    // around a file's top-level definitions is left out, and so is a package object, named `package`.
    def packageObject(name: String) = name == "package" || name.endsWith("$package")
    val packages                    =
      Iterator.iterate(symbol.owner)(_.owner).takeWhile(owner => !owner.isNoSymbol && owner != defn.RootClass)
        .filter(owner => owner.isPackageDef || owner.flags.is(Flags.Module) && !packageObject(owner.name.stripSuffix("$")))
        .map(_.name.stripSuffix("$")).toList.reverse.filterNot(_ == "<empty>")
    val name: String = text(annotations, "SchemaName").headOption.getOrElse(scalaName)
    // An empty namespace given is none, as a schema file that names none has.
    val namespace: Option[String] =
      text(annotations, "SchemaNamespace").headOption.fold(Option.when(packages.nonEmpty)(packages.mkString(".")))(Some(_).filter(_.nonEmpty))
    val doc: Option[String]   = text(annotations, "SchemaDoc").headOption
    val aliases: List[String] = text(annotations, "SchemaAlias")
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
      parametersOf(tpe).lazyZip(decimalsOf(tpe)).lazyZip(parametersOf(tpe).indices).map: (parameter, decimal, index) =>
        val declared = tpe.memberType(symbol.fieldMember(parameter.name))
        // A default of a field marked SchemaDecimal is written as the decimal its field is, not as any BigDecimal.
        (declared.asType, decimal.fold(declared)(withDecimal(declared, _, parameter.name)).asType) match
          case ('[field], '[derived]) => symbol.companionClass.declaredMethod(s"$$lessinit$$greater$$default$$${index + 1}").headOption match
              case Some(method) if parameter.flags.is(Flags.HasDefault) =>
                val call  = Select(Ref(symbol.companionModule), method)
                val value = (if arguments.isEmpty then call else call.appliedToTypes(arguments)).asExprOf[field]
                '{ Some(() => scala.compiletime.summonInline[DefaultOf[C, Out, derived]].write($value.asInstanceOf[derived])) }
              case _ => '{ None }
          // Every type matches a type pattern, which the compiler cannot see.
          case _ => '{ None }
    )
