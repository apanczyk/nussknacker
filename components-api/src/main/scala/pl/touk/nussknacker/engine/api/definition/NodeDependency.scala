package pl.touk.nussknacker.engine.api.definition

import cats.instances.list._
import cats.syntax.traverse._
import pl.touk.nussknacker.engine.api.context.transformation.{
  NodeDependencyValue,
  OutputVariableNameValue,
  TypedNodeDependencyValue
}
import pl.touk.nussknacker.engine.api.typed.MissingOutputVariableException
import pl.touk.nussknacker.engine.api.typed.typing.{Typed, TypingResult}
import pl.touk.nussknacker.engine.api.util.NotNothing
import pl.touk.nussknacker.engine.graph.expression.Expression

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

sealed trait NodeDependency

/**
 * This trait reduce boilerplate defining `GenericNodeTransformation` and reduce risk that definition of node dependencies
 * will desynchronize with implementation code using values
 */
trait ValueExtractor { self: NodeDependency =>
  type RuntimeValue

  def extract(values: List[NodeDependencyValue]): RuntimeValue
}

case class TypedNodeDependency[T](clazz: Class[_]) extends NodeDependency with ValueExtractor {
  override type RuntimeValue = T

  override def extract(values: List[NodeDependencyValue]): T = {
    values
      .collectFirst {
        case out: TypedNodeDependencyValue if clazz.isInstance(out.value) => out.value.asInstanceOf[T]
      }
      .getOrElse(throw new IllegalStateException(s"Missing node dependency of class: $clazz"))
  }

}

object TypedNodeDependency {

  def apply[T: ClassTag]: TypedNodeDependency[T] = new TypedNodeDependency[T](implicitly[ClassTag[T]].runtimeClass)

}

case object OutputVariableNameDependency extends NodeDependency with ValueExtractor {
  override type RuntimeValue = String

  override def extract(values: List[NodeDependencyValue]): String = {
    values
      .collectFirst { case out: OutputVariableNameValue =>
        out.name
      }
      .getOrElse(throw MissingOutputVariableException)
  }

}

object Parameter {

  def apply[T: TypeTag: NotNothing](name: String): Parameter = Parameter(name, Typed.fromDetailedType[T])

  // we want to have mandatory parameters by default because it can protect us from NPE in some cases)
  def apply(name: String, typ: TypingResult): Parameter =
    Parameter(name, typ, validators = List(MandatoryParameterValidator))

  def apply(name: String, typ: TypingResult, validators: List[ParameterValidator]): Parameter =
    Parameter(
      name,
      typ,
      editor = None,
      validators = validators,
      defaultValue = None,
      additionalVariables = Map.empty,
      variablesToHide = Set.empty,
      branchParam = false,
      isLazyParameter = false,
      scalaOptionParameter = false,
      javaOptionalParameter = false,
      hintText = None
    )

  def optional[T: TypeTag: NotNothing](name: String): Parameter =
    Parameter.optional(name, Typed.fromDetailedType[T])

  // Represents optional parameter annotated with @Nullable, if you want to emulate scala Option or java Optional,
  // you should redefine scalaOptionParameter and javaOptionalParameter
  def optional(name: String, typ: TypingResult): Parameter =
    Parameter(
      name,
      typ,
      editor = None,
      validators = List.empty,
      defaultValue = None,
      additionalVariables = Map.empty,
      variablesToHide = Set.empty,
      branchParam = false,
      isLazyParameter = false,
      scalaOptionParameter = false,
      javaOptionalParameter = false,
      hintText = None
    )

}

object NotBlankParameter {

  def apply(name: String, typ: TypingResult): Parameter =
    Parameter(name, typ, validators = List(NotBlankParameterValidator))

}

case class Parameter(
    name: String,
    typ: TypingResult,
    editor: Option[ParameterEditor],
    validators: List[ParameterValidator],
    defaultValue: Option[Expression],
    additionalVariables: Map[String, AdditionalVariable],
    variablesToHide: Set[String],
    branchParam: Boolean,
    isLazyParameter: Boolean,
    scalaOptionParameter: Boolean,
    javaOptionalParameter: Boolean,
    hintText: Option[String]
) extends NodeDependency {

  def copy(
      name: String,
      typ: TypingResult,
      editor: Option[ParameterEditor],
      validators: List[ParameterValidator],
      defaultValue: Option[Expression],
      additionalVariables: Map[String, AdditionalVariable],
      variablesToHide: Set[String],
      branchParam: Boolean,
      isLazyParameter: Boolean,
      scalaOptionParameter: Boolean,
      javaOptionalParameter: Boolean,
  ): Parameter = {
    copy(
      name,
      typ,
      editor,
      validators,
      defaultValue,
      additionalVariables,
      variablesToHide,
      branchParam,
      isLazyParameter,
      scalaOptionParameter,
      javaOptionalParameter,
      hintText = None
    )
  }

  def copy(
      name: String = this.name,
      typ: TypingResult = this.typ,
      editor: Option[ParameterEditor] = this.editor,
      validators: List[ParameterValidator] = this.validators,
      defaultValue: Option[Expression] = this.defaultValue,
      additionalVariables: Map[String, AdditionalVariable] = this.additionalVariables,
      variablesToHide: Set[String] = this.variablesToHide,
      branchParam: Boolean = this.branchParam,
      isLazyParameter: Boolean = this.isLazyParameter,
      scalaOptionParameter: Boolean = this.scalaOptionParameter,
      javaOptionalParameter: Boolean = this.javaOptionalParameter,
      hintText: Option[String] = this.hintText
  ): Parameter = {
    new Parameter(
      name,
      typ,
      editor,
      validators,
      defaultValue,
      additionalVariables,
      variablesToHide,
      branchParam,
      isLazyParameter,
      scalaOptionParameter,
      javaOptionalParameter,
      hintText
    )
  }

  def apply(
      name: String,
      typ: TypingResult,
      editor: Option[ParameterEditor],
      validators: List[ParameterValidator],
      defaultValue: Option[Expression],
      additionalVariables: Map[String, AdditionalVariable],
      variablesToHide: Set[String],
      branchParam: Boolean,
      isLazyParameter: Boolean,
      scalaOptionParameter: Boolean,
      javaOptionalParameter: Boolean,
      hintText: Option[String]
  ): Parameter = {
    new Parameter(
      name,
      typ,
      editor,
      validators,
      defaultValue,
      additionalVariables,
      variablesToHide,
      branchParam,
      isLazyParameter,
      scalaOptionParameter,
      javaOptionalParameter,
      hintText
    )
  }

  def apply(
      name: String,
      typ: TypingResult,
      editor: Option[ParameterEditor],
      validators: List[ParameterValidator],
      defaultValue: Option[Expression],
      additionalVariables: Map[String, AdditionalVariable],
      variablesToHide: Set[String],
      branchParam: Boolean,
      isLazyParameter: Boolean,
      scalaOptionParameter: Boolean,
      javaOptionalParameter: Boolean,
  ): Parameter = {
    new Parameter(
      name,
      typ,
      editor,
      validators,
      defaultValue,
      additionalVariables,
      variablesToHide,
      branchParam,
      isLazyParameter,
      scalaOptionParameter,
      javaOptionalParameter,
      hintText = None
    )
  }

  // we throw exception early, as it indicates that Component implementation is incorrect, this should not happen in running designer...
  if (isLazyParameter && additionalVariables.values.exists(_.isInstanceOf[AdditionalVariableWithFixedValue])) {
    throw new IllegalArgumentException(
      s"${classOf[AdditionalVariableWithFixedValue].getSimpleName} should not be used with LazyParameters"
    )
  } else if (!isLazyParameter && additionalVariables.values.exists(
      _.isInstanceOf[AdditionalVariableProvidedInRuntime]
    )) {
    throw new IllegalArgumentException(
      s"${classOf[AdditionalVariableProvidedInRuntime].getClass.getSimpleName} should be used only with LazyParameters"
    )
  }

  val isOptional: Boolean = !validators.contains(MandatoryParameterValidator)

}

// TODO: rename to AdditionalVariableDefinition
sealed trait AdditionalVariable {
  def typingResult: TypingResult
}

object AdditionalVariableProvidedInRuntime {

  def apply[T: TypeTag]: AdditionalVariableProvidedInRuntime = AdditionalVariableProvidedInRuntime(
    Typed.fromDetailedType[T]
  )

}

case class AdditionalVariableProvidedInRuntime(typingResult: TypingResult) extends AdditionalVariable

object AdditionalVariableWithFixedValue {
  def apply[T: TypeTag](value: T): AdditionalVariableWithFixedValue =
    AdditionalVariableWithFixedValue(value, Typed.fromDetailedType[T])
}

case class AdditionalVariableWithFixedValue(value: Any, typingResult: TypingResult) extends AdditionalVariable
