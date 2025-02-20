package pl.touk.nussknacker.ui.validation

import cats.data.{Validated, ValidatedNel}
import com.typesafe.config.ConfigValueFactory.{fromAnyRef, fromIterable}
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.Inside.inside
import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.matchers.{BeMatcher, MatchResult}
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.prop.TableDrivenPropertyChecks.forAll
import pl.touk.nussknacker.engine.api.component.ScenarioPropertyConfig
import pl.touk.nussknacker.engine.api.context.ProcessCompilationError
import pl.touk.nussknacker.engine.api.context.ProcessCompilationError._
import pl.touk.nussknacker.engine.api.definition._
import pl.touk.nussknacker.engine.api.displayedgraph.displayablenode.Edge
import pl.touk.nussknacker.engine.api.displayedgraph.{DisplayableProcess, ProcessProperties}
import pl.touk.nussknacker.engine.api.parameter.{ParameterValueCompileTimeValidation, ValueInputWithFixedValuesProvided}
import pl.touk.nussknacker.engine.api.process.{ProcessName, ProcessingType}
import pl.touk.nussknacker.engine.api.typed.typing
import pl.touk.nussknacker.engine.api.typed.typing.{Typed, TypedObjectTypingResult, Unknown}
import pl.touk.nussknacker.engine.api.{FragmentSpecificData, MetaData, ProcessAdditionalFields, StreamMetaData}
import pl.touk.nussknacker.engine.build.ScenarioBuilder
import pl.touk.nussknacker.engine.canonicalgraph.CanonicalProcess
import pl.touk.nussknacker.engine.canonicalgraph.canonicalnode.{FlatNode, SplitNode}
import pl.touk.nussknacker.engine.compile.ProcessValidator
import pl.touk.nussknacker.engine.graph.EdgeType.{NextSwitch, SwitchDefault}
import pl.touk.nussknacker.engine.graph.expression.Expression
import pl.touk.nussknacker.engine.graph.fragment.FragmentRef
import pl.touk.nussknacker.engine.graph.node.FragmentInputDefinition.{FragmentClazzRef, FragmentParameter}
import pl.touk.nussknacker.engine.graph.node._
import pl.touk.nussknacker.engine.graph.service.ServiceRef
import pl.touk.nussknacker.engine.graph.sink.SinkRef
import pl.touk.nussknacker.engine.graph.source.SourceRef
import pl.touk.nussknacker.engine.graph.variable.Field
import pl.touk.nussknacker.engine.graph.EdgeType
import pl.touk.nussknacker.engine.graph.evaluatedparam.{Parameter => NodeParameter}
import pl.touk.nussknacker.engine.management.FlinkStreamingPropertiesConfig
import pl.touk.nussknacker.engine.testing.ModelDefinitionBuilder
import pl.touk.nussknacker.engine.util.Implicits.RichScalaMap
import pl.touk.nussknacker.engine.{CustomProcessValidator, spel}
import pl.touk.nussknacker.restmodel.validation.ValidationResults.NodeValidationErrorType.{
  RenderNotAllowed,
  SaveAllowed,
  SaveNotAllowed
}
import pl.touk.nussknacker.restmodel.validation.ValidationResults.{
  NodeTypingData,
  NodeValidationError,
  NodeValidationErrorType,
  ValidationErrors,
  ValidationResult,
  ValidationWarnings
}
import pl.touk.nussknacker.restmodel.validation.{PrettyValidationErrors, ValidationResults}
import pl.touk.nussknacker.ui.api.helpers.TestFactory.possibleValues
import pl.touk.nussknacker.ui.api.helpers._
import pl.touk.nussknacker.ui.process.fragment.{FragmentDetails, FragmentResolver}
import pl.touk.nussknacker.ui.process.marshall.ProcessConverter
import pl.touk.nussknacker.ui.security.api.{AdminUser, LoggedUser}

import scala.jdk.CollectionConverters._

class UIProcessValidatorSpec extends AnyFunSuite with Matchers with TableDrivenPropertyChecks with OptionValues {

  import ProcessTestData._
  import TestCategories._
  import UIProcessValidatorSpec._
  import spel.Implicits._

  // TODO: tests for user privileges
  private implicit val user: LoggedUser = AdminUser("admin", "admin")

  test("check for not unique edge types") {
    val process = createProcess(
      List(
        Source("in", SourceRef(existingSourceFactory, List())),
        FragmentInput("subIn", FragmentRef("sub1", List())),
        Sink("out", SinkRef(existingSinkFactory, List())),
        Sink("out2", SinkRef(existingSinkFactory, List())),
        Sink("out3", SinkRef(existingSinkFactory, List()))
      ),
      List(
        Edge("in", "subIn", None),
        Edge("subIn", "out", Some(EdgeType.FragmentOutput("out1"))),
        Edge("subIn", "out2", Some(EdgeType.FragmentOutput("out2"))),
        Edge("subIn", "out3", Some(EdgeType.FragmentOutput("out2")))
      )
    )

    val result = configuredValidator.validate(process)

    result.errors.invalidNodes shouldBe Map(
      "subIn" -> List(
        NodeValidationError(
          "NonUniqueEdgeType",
          "Edges are not unique",
          "Node subIn has duplicate outgoing edges of type: FragmentOutput(out2), it cannot be saved properly",
          None,
          NodeValidationErrorType.SaveNotAllowed
        )
      )
    )
  }

  test("switch edges do not have to be unique") {
    val process = createProcess(
      List(
        Source("in", SourceRef(existingSourceFactory, List())),
        Switch("switch"),
        Sink("out", SinkRef(existingSinkFactory, List())),
        Sink("out2", SinkRef(existingSinkFactory, List())),
      ),
      List(
        Edge("in", "switch", None),
        Edge("switch", "out", Some(EdgeType.NextSwitch("true"))),
        Edge("switch", "out2", Some(EdgeType.NextSwitch("true"))),
      )
    )

    val result = configuredValidator.validate(process)
    result.errors.invalidNodes shouldBe Symbol("empty")
  }

  test("check for not unique edges") {
    val process = createProcess(
      List(
        Source("in", SourceRef(existingSourceFactory, List())),
        FragmentInput("subIn", FragmentRef("sub1", List())),
        Sink("out2", SinkRef(existingSinkFactory, List())),
      ),
      List(
        Edge("in", "subIn", None),
        Edge("subIn", "out2", Some(EdgeType.FragmentOutput("out1"))),
        Edge("subIn", "out2", Some(EdgeType.FragmentOutput("out2"))),
      )
    )

    val result = configuredValidator.validate(process)

    result.errors.invalidNodes shouldBe Map(
      "subIn" -> List(
        NodeValidationError(
          "NonUniqueEdge",
          "Edges are not unique",
          "Node subIn has duplicate outgoing edges to: out2, it cannot be saved properly",
          None,
          SaveNotAllowed
        )
      )
    )
  }

  test("check for loose nodes") {
    val process = createProcess(
      List(
        Source("in", SourceRef(existingSourceFactory, List())),
        Sink("out", SinkRef(existingSinkFactory, List())),
        Filter("loose", Expression.spel("true"))
      ),
      List(Edge("in", "out", None))
    )
    val result = configuredValidator.validate(process)

    result.errors.invalidNodes shouldBe Map(
      "loose" -> List(
        NodeValidationError(
          "LooseNode",
          "Loose node",
          "Node loose is not connected to source, it cannot be saved properly",
          None,
          SaveNotAllowed
        )
      )
    )
  }

  test("filter with only 'false' edge") {
    val process = createProcess(
      List(
        Source("in", SourceRef(existingSourceFactory, List())),
        Sink("out", SinkRef(existingSinkFactory, List())),
        Filter("filter", Expression.spel("true"))
      ),
      List(
        Edge("in", "filter", None),
        Edge("filter", "out", Some(EdgeType.FilterFalse)),
      ),
      additionalFields = Map(
        "requiredStringProperty" -> "test"
      )
    )
    val result = configuredValidator.validate(process)

    result.hasErrors shouldBe false
  }

  test("check for disabled nodes") {
    val process = createProcess(
      List(
        Source("in", SourceRef(existingSourceFactory, List())),
        Sink("out", SinkRef(existingSinkFactory, List())),
        Filter("filter", Expression.spel("true"), isDisabled = Some(true))
      ),
      List(
        Edge("in", "filter", None),
        Edge("filter", "out", Some(EdgeType.FilterTrue)),
      )
    )
    val result = configuredValidator.validate(process)

    result.warnings.invalidNodes shouldBe Map(
      "filter" -> List(
        NodeValidationError(
          "DisabledNode",
          "Node filter is disabled",
          "Deploying scenario with disabled node can have unexpected consequences",
          None,
          SaveAllowed
        )
      )
    )
  }

  test("check for duplicated ids") {
    val process = createProcess(
      List(
        Source("inID", SourceRef(existingSourceFactory, List())),
        Filter("inID", Expression.spel("''")),
        Sink("out", SinkRef(existingSinkFactory, List()))
      ),
      List(Edge("inID", "inID", None), Edge("inID", "out", None))
    )
    val result = configuredValidator.validate(process)

    result.errors.globalErrors shouldBe List(
      NodeValidationError(
        "DuplicatedNodeIds",
        "Two nodes cannot have same id",
        "Duplicate node ids: inID",
        None,
        RenderNotAllowed
      )
    )
  }

  test("check for duplicated ids when duplicated id is switch id") {
    val process = createProcess(
      List(
        Source("in", SourceRef(existingSourceFactory, List())),
        Switch("switchID"),
        Sink("out", SinkRef(existingSinkFactory, List())),
        Sink("switchID", SinkRef(existingSinkFactory, List()))
      ),
      List(
        Edge("in", "switchID", None),
        Edge("switchID", "out", Some(SwitchDefault)),
        Edge("switchID", "switch", Some(NextSwitch(Expression.spel("''"))))
      )
    )

    val result = configuredValidator.validate(process)

    result.errors.globalErrors shouldBe List(
      NodeValidationError(
        "DuplicatedNodeIds",
        "Two nodes cannot have same id",
        "Duplicate node ids: switchID",
        None,
        RenderNotAllowed
      )
    )
    result.errors.invalidNodes shouldBe empty
    result.warnings shouldBe ValidationWarnings.success
  }

  test("not allow required scenario fields") {
    val processValidator = TestFactory.processValidator.withScenarioPropertiesConfig(
      Map(
        "field1" -> ScenarioPropertyConfig(
          defaultValue = None,
          editor = None,
          validators = Some(List(MandatoryParameterValidator)),
          label = Some("label1")
        ),
        "field2" -> ScenarioPropertyConfig(
          defaultValue = None,
          editor = None,
          validators = None,
          label = Some("label2")
        )
      ) ++ FlinkStreamingPropertiesConfig.properties
    )

    processValidator.validate(
      validProcessWithFields(Map("field1" -> "a", "field2" -> "b"))
    ) shouldBe withoutErrorsAndWarnings

    processValidator.validate(validProcessWithFields(Map("field1" -> "a"))) shouldBe withoutErrorsAndWarnings

    processValidator
      .validate(validProcessWithFields(Map("field1" -> "", "field2" -> "b")))
      .errors
      .processPropertiesErrors should matchPattern {
      case List(
            NodeValidationError(
              "EmptyMandatoryParameter",
              _,
              _,
              Some("field1"),
              ValidationResults.NodeValidationErrorType.SaveAllowed
            )
          ) =>
    }
    processValidator
      .validate(validProcessWithFields(Map("field2" -> "b")))
      .errors
      .processPropertiesErrors should matchPattern {
      case List(
            NodeValidationError(
              "MissingRequiredProperty",
              _,
              _,
              Some("field1"),
              ValidationResults.NodeValidationErrorType.SaveAllowed
            )
          ) =>
    }
  }

  test("don't validate properties on fragment") {
    val processValidator = TestFactory.processValidator.withScenarioPropertiesConfig(
      Map(
        "field1" -> ScenarioPropertyConfig(
          defaultValue = None,
          editor = None,
          validators = Some(List(MandatoryParameterValidator)),
          label = Some("label1")
        ),
        "field2" -> ScenarioPropertyConfig(
          defaultValue = None,
          editor = None,
          validators = Some(List(MandatoryParameterValidator)),
          label = Some("label2")
        )
      ) ++ FlinkStreamingPropertiesConfig.properties
    )

    val process = validProcessWithFields(Map())
    val fragment = process.copy(properties =
      process.properties.copy(
        additionalFields = process.properties.additionalFields.copy(
          metaDataType = FragmentSpecificData.typeName
        )
      )
    )

    processValidator.validate(fragment) shouldBe withoutErrorsAndWarnings

  }

  test("validate type scenario field") {
    val possibleValues = List(FixedExpressionValue("true", "true"), FixedExpressionValue("false", "false"))
    val processValidator = TestFactory.processValidator.withScenarioPropertiesConfig(
      Map(
        "field1" -> ScenarioPropertyConfig(
          defaultValue = None,
          editor = Some(FixedValuesParameterEditor(possibleValues)),
          validators = Some(List(FixedValuesValidator(possibleValues))),
          label = Some("label")
        ),
        "field2" -> ScenarioPropertyConfig(
          defaultValue = None,
          editor = None,
          validators = Some(List(LiteralIntegerValidator)),
          label = Some("label")
        )
      ) ++ FlinkStreamingPropertiesConfig.properties
    )

    processValidator.validate(validProcessWithFields(Map("field1" -> "true"))) shouldBe withoutErrorsAndWarnings
    processValidator.validate(validProcessWithFields(Map("field1" -> "false"))) shouldBe withoutErrorsAndWarnings
    processValidator.validate(validProcessWithFields(Map("field1" -> "1"))) should not be withoutErrorsAndWarnings

    processValidator.validate(validProcessWithFields(Map("field2" -> "1"))) shouldBe withoutErrorsAndWarnings
    processValidator.validate(validProcessWithFields(Map("field2" -> "1.1"))) should not be withoutErrorsAndWarnings
    processValidator.validate(validProcessWithFields(Map("field2" -> "true"))) should not be withoutErrorsAndWarnings
  }

  test("handle unknown properties validation") {
    val processValidator = TestFactory.processValidator.withScenarioPropertiesConfig(
      Map(
        "field2" -> ScenarioPropertyConfig(
          defaultValue = None,
          editor = None,
          validators = Some(List(CompileTimeEvaluableValueValidator)),
          label = Some("label")
        )
      ) ++ FlinkStreamingPropertiesConfig.properties
    )

    val result = processValidator.validate(validProcessWithFields(Map("field1" -> "true")))

    result.errors.processPropertiesErrors should matchPattern {
      case List(NodeValidationError("UnknownProperty", _, _, Some("field1"), NodeValidationErrorType.SaveAllowed)) =>
    }
  }

  test("not allows save with incorrect characters in ids") {
    def process(nodeId: String) = createProcess(
      List(Source(nodeId, SourceRef(existingSourceFactory, List()))),
      List()
    )

    configuredValidator.validate(process("a\"s")).saveAllowed shouldBe false
    configuredValidator.validate(process("a's")).saveAllowed shouldBe false
    configuredValidator.validate(process("a.s")).saveAllowed shouldBe false
    configuredValidator.validate(process("as")).saveAllowed shouldBe true

  }

  test("fails validation if cannot resolve fragment parameter type while validating fragment") {
    val fragmentWithInvalidParam =
      CanonicalProcess(
        MetaData("sub1", FragmentSpecificData()),
        List(
          FlatNode(
            FragmentInputDefinition(
              "in",
              List(
                FragmentParameter(
                  "subParam1",
                  FragmentClazzRef("thisTypeDoesntExist"),
                  required = false,
                  initialValue = None,
                  hintText = None,
                  valueEditor = None,
                  valueCompileTimeValidation = None
                )
              )
            )
          ),
          FlatNode(
            FragmentOutputDefinition("out", "out1", List.empty)
          )
        ),
        List.empty
      )

    val displayableFragment =
      ProcessConverter.toDisplayable(fragmentWithInvalidParam, TestProcessingTypes.Streaming, Category1)

    val validationResult = configuredValidator.validate(displayableFragment)

    validationResult.errors should not be empty
    validationResult.errors.invalidNodes("in") should matchPattern {
      case List(
            NodeValidationError(
              "FragmentParamClassLoadError",
              "Invalid parameter type.",
              "Failed to load thisTypeDoesntExist",
              Some("$param.subParam1.$typ"),
              NodeValidationErrorType.SaveAllowed
            )
          ) =>
    }
  }

  test("validates fragment input definition while validating fragment") {
    val fragmentWithInvalidParam =
      CanonicalProcess(
        MetaData("sub1", FragmentSpecificData()),
        List(
          FlatNode(
            FragmentInputDefinition(
              "in",
              List(
                FragmentParameter(
                  "subParam1",
                  FragmentClazzRef[String],
                  required = false,
                  initialValue = Some(FixedExpressionValue("'outsidePreset'", "outsidePreset")),
                  hintText = None,
                  valueEditor = Some(
                    ValueInputWithFixedValuesProvided(
                      fixedValuesList = List(FixedExpressionValue("'someValue'", "someValue")),
                      allowOtherValue = false
                    )
                  ),
                  valueCompileTimeValidation = None
                ),
                FragmentParameter(
                  "subParam2",
                  FragmentClazzRef[java.lang.Boolean],
                  required = false,
                  initialValue = None,
                  hintText = None,
                  valueEditor = Some(
                    ValueInputWithFixedValuesProvided(
                      fixedValuesList = List(FixedExpressionValue("'someValue'", "someValue")),
                      allowOtherValue = false
                    )
                  ),
                  valueCompileTimeValidation = None
                ),
                FragmentParameter(
                  "subParam3",
                  FragmentClazzRef[java.lang.String],
                  required = false,
                  initialValue = None,
                  hintText = None,
                  valueEditor = None,
                  valueCompileTimeValidation =
                    Some(ParameterValueCompileTimeValidation(Expression.spel("'a' + 'b'"), Some("some failed message")))
                ),
                FragmentParameter(
                  "subParam4",
                  FragmentClazzRef[java.lang.String],
                  required = false,
                  initialValue = None,
                  hintText = None,
                  valueEditor = None,
                  valueCompileTimeValidation = Some(
                    ParameterValueCompileTimeValidation(
                      s"#${ValidationExpressionParameterValidator.variableName} < 7", // invalid operation (comparing string with int)
                      None
                    )
                  )
                )
              )
            )
          ),
          FlatNode(
            FragmentOutputDefinition("out", "out1", List.empty)
          )
        ),
        List.empty
      )

    val displayableFragment =
      ProcessConverter.toDisplayable(fragmentWithInvalidParam, TestProcessingTypes.Streaming, Category1)

    val validationResult = configuredValidator.validate(displayableFragment)

    validationResult.errors should not be empty
    validationResult.errors.invalidNodes("in") should matchPattern {
      case List(
            NodeValidationError(
              "InitialValueNotPresentInPossibleValues",
              "The initial value provided for parameter 'subParam1' is not present in the parameter's possible values list",
              _,
              Some("$param.subParam1.$initialValue"),
              NodeValidationErrorType.SaveAllowed
            ),
            NodeValidationError(
              "ExpressionParserCompilationErrorInFragmentDefinition",
              "Failed to parse expression: Bad expression type, expected: Boolean, found: String(someValue)",
              "There is a problem with expression: 'someValue'",
              Some("$param.subParam2.$fixedValuesList"),
              NodeValidationErrorType.SaveAllowed
            ),
            NodeValidationError(
              "InvalidValidationExpression",
              "Invalid validation expression: Bad expression type, expected: Boolean, found: String(ab)",
              "There is a problem with validation expression: 'a' + 'b'",
              Some("$param.subParam3.$validationExpression"),
              NodeValidationErrorType.SaveAllowed
            ),
            NodeValidationError(
              "InvalidValidationExpression",
              "Invalid validation expression: Wrong part types",
              "There is a problem with validation expression: #value < 7",
              Some("$param.subParam4.$validationExpression"),
              NodeValidationErrorType.SaveAllowed
            )
          ) =>
    }
  }

  test("validates fragment input definition while validating process that uses fragment") {
    val invalidFragment = CanonicalProcess(
      MetaData("sub1", FragmentSpecificData()),
      nodes = List(
        FlatNode(FragmentInputDefinition("in", List(FragmentParameter("param1", FragmentClazzRef[Long])))),
        FlatNode(Variable(id = "subVar", varName = "subVar", value = "#nonExistingVar")),
        FlatNode(FragmentOutputDefinition("out1", "output", List.empty))
      ),
      additionalBranches = List.empty
    )

    val process = createProcess(
      nodes = List(
        Source("in", SourceRef(sourceTypeName, List())),
        FragmentInput(
          "subIn",
          FragmentRef(invalidFragment.name.value, List(NodeParameter("param1", "'someString'"))),
          isDisabled = Some(false)
        ),
        Sink("out", SinkRef(sinkTypeName, List()))
      ),
      edges = List(
        Edge("in", "subIn", None),
        Edge("subIn", "out", Some(EdgeType.FragmentOutput("output")))
      )
    )

    val processValidator = mockedProcessValidator(invalidFragment)
    val validationResult = processValidator.validate(process)

    validationResult should matchPattern {
      case ValidationResult(ValidationErrors(invalidNodes, Nil, Nil), ValidationWarnings.success, _)
          if invalidNodes("subIn").size == 1 && invalidNodes("subIn-subVar").size == 1 =>
    }
  }

  test("validates FragmentInput parameters according to FragmentInputDefinition") {
    val fragment = CanonicalProcess(
      MetaData("sub1", FragmentSpecificData()),
      nodes = List(
        FlatNode(
          FragmentInputDefinition(
            "in",
            List(
              FragmentParameter(
                "subParam1",
                FragmentClazzRef[String],
                required = true,
                initialValue = None,
                hintText = None,
                valueEditor = Some(
                  ValueInputWithFixedValuesProvided(
                    fixedValuesList = List(FixedExpressionValue("'someValue'", "someValue")),
                    allowOtherValue = false
                  )
                ),
                valueCompileTimeValidation = None
              ),
            )
          )
        ),
        FlatNode(FragmentOutputDefinition("subOut1", "subOut1", List(Field("foo", "42L"))))
      ),
      additionalBranches = List.empty
    )

    val process = createProcess(
      nodes = List(
        Source("source", SourceRef(sourceTypeName, Nil)),
        FragmentInput(
          "subIn1",
          FragmentRef(
            fragment.name.value,
            List(
              NodeParameter("subParam1", "'outsideAllowedValues'"),
            )
          ),
          isDisabled = Some(false)
        ),
        FragmentInput(
          "subIn2",
          FragmentRef(
            fragment.name.value,
            List(
              NodeParameter("subParam1", ""),
            )
          ),
          isDisabled = Some(false)
        ),
        Sink("sink1", SinkRef(sinkTypeName, Nil)),
      ),
      edges = List(
        Edge("source", "subIn1", None),
        Edge("subIn1", "subIn2", Some(EdgeType.FragmentOutput("subOut1"))),
        Edge("subIn2", "sink1", Some(EdgeType.FragmentOutput("subOut1")))
      )
    )

    val processValidation = mockedProcessValidator(fragment)
    val validationResult  = processValidation.validate(process)

    validationResult.errors should not be empty
    validationResult.errors.invalidNodes("subIn1") should matchPattern {
      case List(
            NodeValidationError(
              "InvalidPropertyFixedValue",
              _,
              _,
              Some("subParam1"),
              NodeValidationErrorType.SaveAllowed
            )
          ) =>
    }
    validationResult.errors.invalidNodes("subIn2") should matchPattern {
      case List(
            NodeValidationError(
              "EmptyMandatoryParameter",
              _,
              _,
              Some("subParam1"),
              NodeValidationErrorType.SaveAllowed
            )
          ) =>
    }
  }

  test("validates disabled fragment with parameters") {
    val invalidFragment = CanonicalProcess(
      MetaData("sub1", FragmentSpecificData()),
      nodes = List(
        FlatNode(FragmentInputDefinition("sub1", List(FragmentParameter("param1", FragmentClazzRef[Long])))),
        FlatNode(Variable(id = "subVar", varName = "subVar", value = "#nonExistingVar")),
        FlatNode(FragmentOutputDefinition("out1", "output", List.empty))
      ),
      additionalBranches = List.empty
    )

    val process = createProcess(
      nodes = List(
        Source("in", SourceRef(sourceTypeName, List())),
        FragmentInput(
          "subIn",
          FragmentRef(invalidFragment.name.value, List(NodeParameter("param1", "'someString'"))),
          isDisabled = Some(true)
        ),
        Sink("out", SinkRef(sinkTypeName, List()))
      ),
      edges = List(
        Edge("in", "subIn", None),
        Edge("subIn", "out", Some(EdgeType.FragmentOutput("output")))
      )
    )

    val processValidator = mockedProcessValidator(invalidFragment)

    val validationResult = processValidator.validate(process)
    validationResult.errors.invalidNodes shouldBe Symbol("empty")
    validationResult.errors.globalErrors shouldBe Symbol("empty")
    validationResult.saveAllowed shouldBe true
  }

  test("validates and returns type info of fragment output fields") {
    val fragment = CanonicalProcess(
      MetaData("sub1", FragmentSpecificData()),
      nodes = List(
        FlatNode(FragmentInputDefinition("in", List(FragmentParameter("subParam1", FragmentClazzRef[String])))),
        SplitNode(
          Split("split"),
          List(
            List(FlatNode(FragmentOutputDefinition("subOut1", "subOut1", List(Field("foo", "42L"))))),
            List(FlatNode(FragmentOutputDefinition("subOut2", "subOut2", List(Field("bar", "'42'")))))
          )
        )
      ),
      additionalBranches = List.empty
    )

    val process = createProcess(
      nodes = List(
        Source("source", SourceRef(sourceTypeName, Nil)),
        FragmentInput(
          "subIn",
          FragmentRef(fragment.name.value, List(NodeParameter("subParam1", "'someString'"))),
          isDisabled = Some(false)
        ),
        Variable(id = "var1", varName = "var1", value = "#subOut1.foo"),
        Variable(id = "var2", varName = "var2", value = "#subOut2.bar"),
        Sink("sink1", SinkRef(sinkTypeName, Nil)),
        Sink("sink2", SinkRef(sinkTypeName, Nil))
      ),
      edges = List(
        Edge("source", "subIn", None),
        Edge("subIn", "var1", Some(EdgeType.FragmentOutput("subOut1"))),
        Edge("subIn", "var2", Some(EdgeType.FragmentOutput("subOut2"))),
        Edge("var1", "sink1", None),
        Edge("var2", "sink2", None)
      )
    )

    val processValidator = mockedProcessValidator(fragment)
    val validationResult = processValidator.validate(process)

    validationResult.errors.invalidNodes shouldBe Symbol("empty")
    validationResult.nodeResults("sink2").variableTypes("input") shouldBe typing.Unknown
    validationResult.nodeResults("sink2").variableTypes("var2") shouldBe Typed.fromInstance("42")
    validationResult.nodeResults("sink2").variableTypes("subOut2") shouldBe TypedObjectTypingResult(
      Map(
        "bar" -> Typed.fromInstance("42")
      )
    )
  }

  test("check for no expression found in mandatory parameter") {
    val process = createProcess(
      List(
        Source("inID", SourceRef(existingSourceFactory, List())),
        Enricher(
          "custom",
          ServiceRef("fooService3", List(NodeParameter("expression", Expression.spel("")))),
          "out"
        ),
        Sink("out", SinkRef(existingSinkFactory, List()))
      ),
      List(Edge("inID", "custom", None), Edge("custom", "out", None))
    )

    val result = configuredValidator.validate(process)

    result.errors.globalErrors shouldBe empty
    result.errors.invalidNodes.get("custom") should matchPattern {
      case Some(
            List(
              NodeValidationError(
                "EmptyMandatoryParameter",
                _,
                _,
                Some("expression"),
                NodeValidationErrorType.SaveAllowed
              )
            )
          ) =>
    }
    result.warnings shouldBe ValidationWarnings.success
  }

  test("check for wrong fixed expression value in node parameter") {
    val process: DisplayableProcess = createProcessWithParams(
      List(NodeParameter("expression", Expression.spel("wrong fixed value"))),
      Map.empty
    )

    val result = configuredValidator.validate(process)

    result.errors.globalErrors shouldBe empty
    result.errors.invalidNodes.get("custom") should matchPattern {
      case Some(
            List(
              NodeValidationError(
                "ExpressionParserCompilationError",
                _,
                _,
                Some("expression"),
                NodeValidationErrorType.SaveAllowed
              )
            )
          ) =>
    }
    result.warnings shouldBe ValidationWarnings.success
  }

  test("check for wrong fixed expression value in scenario property") {
    val process = createProcessWithParams(
      List.empty,
      Map(
        "numberOfThreads"        -> "wrong fixed value",
        "requiredStringProperty" -> "test"
      )
    )

    val result = configuredValidator.validate(process)

    result.errors.globalErrors shouldBe empty
    result.errors.processPropertiesErrors should matchPattern {
      case List(
            NodeValidationError(
              "InvalidPropertyFixedValue",
              _,
              _,
              Some("numberOfThreads"),
              NodeValidationErrorType.SaveAllowed
            )
          ) =>
    }
    result.warnings shouldBe ValidationWarnings.success
  }

  test("validates scenario with fragment within other processingType") {
    val fragment = CanonicalProcess(
      MetaData("sub1", FragmentSpecificData()),
      nodes = List(
        FlatNode(FragmentInputDefinition("in", List(FragmentParameter("subParam1", FragmentClazzRef[String])))),
        FlatNode(FragmentOutputDefinition("subOut1", "out", List(Field("foo", "42L"))))
      ),
      additionalBranches = List.empty
    )

    val process = createProcess(
      nodes = List(
        Source("source", SourceRef(sourceTypeName, Nil)),
        FragmentInput(
          "subIn",
          FragmentRef(fragment.name.value, List(NodeParameter("subParam1", "'someString'"))),
          isDisabled = Some(false)
        ),
        Sink("sink", SinkRef(sinkTypeName, Nil))
      ),
      edges = List(
        Edge("source", "subIn", None),
        Edge("subIn", "sink", Some(EdgeType.FragmentOutput("out")))
      )
    )

    val processValidator = mockedProcessValidator(fragment)

    val validationResult = processValidator.validate(process)
    validationResult.errors.invalidNodes shouldBe Symbol("empty")
    validationResult.errors.globalErrors shouldBe Symbol("empty")
    validationResult.saveAllowed shouldBe true

    val validationResultWithCategory2 =
      processValidator.validate(process.copy(processingType = TestProcessingTypes.Streaming2))
    validationResultWithCategory2.errors.invalidNodes shouldBe Map(
      "subIn" -> List(PrettyValidationErrors.formatErrorMessage(UnknownFragment(fragment.name.value, "subIn")))
    )
  }

  test("validates scenario with fragment parameters - P1 as mandatory param with some actual value") {
    val fragmentId = "fragment1"

    val configWithValidators: Config = defaultConfig
      .withValue(
        s"componentsUiConfig.$fragmentId.params.P1.validators",
        fromIterable(List(Map("type" -> "MandatoryParameterValidator").asJava).asJava)
      )

    val fragmentDefinition: CanonicalProcess =
      createFragmentDefinition(fragmentId, List(FragmentParameter("P1", FragmentClazzRef[Short])))
    val processWithFragment = createProcessWithFragmentParams(fragmentId, List(NodeParameter("P1", "123")))

    val processValidator = mockedProcessValidator(fragmentDefinition, configWithValidators)
    val result           = processValidator.validate(processWithFragment)
    result.hasErrors shouldBe false
    result.errors.invalidNodes shouldBe Symbol("empty")
    result.errors.globalErrors shouldBe Symbol("empty")
    result.saveAllowed shouldBe true
  }

  test("validates scenario with fragment parameters - P1 as mandatory param with with missing actual value") {
    val fragmentId = "fragment1"

    val fragmentDefinition: CanonicalProcess =
      createFragmentDefinition(fragmentId, List(FragmentParameter("P1", FragmentClazzRef[Short]).copy(required = true)))
    val processWithFragment = createProcessWithFragmentParams(fragmentId, List(NodeParameter("P1", "")))

    val processValidator = mockedProcessValidator(fragmentDefinition, defaultConfig)
    val result           = processValidator.validate(processWithFragment)

    result.hasErrors shouldBe true
    result.errors.globalErrors shouldBe empty
    result.errors.invalidNodes.get("subIn") should matchPattern {
      case Some(
            List(NodeValidationError("EmptyMandatoryParameter", _, _, Some("P1"), NodeValidationErrorType.SaveAllowed))
          ) =>
    }
  }

  test(
    "validates scenario with fragment parameters - P1 and P2 as mandatory params with missing actual values accumulated"
  ) {
    val fragmentId = "fragment1"

    val fragmentDefinition: CanonicalProcess = createFragmentDefinition(
      fragmentId,
      List(
        FragmentParameter("P1", FragmentClazzRef[Short]).copy(required = true),
        FragmentParameter("P2", FragmentClazzRef[String]).copy(required = true)
      )
    )

    val processWithFragment = createProcessWithFragmentParams(
      fragmentId,
      List(
        NodeParameter("P1", ""),
        NodeParameter("P2", "")
      )
    )

    val processValidator = mockedProcessValidator(fragmentDefinition, defaultConfig)
    val result           = processValidator.validate(processWithFragment)

    result.hasErrors shouldBe true
    result.errors.globalErrors shouldBe empty
    result.errors.invalidNodes.get("subIn") should matchPattern {
      case Some(
            List(
              NodeValidationError("EmptyMandatoryParameter", _, _, Some("P1"), NodeValidationErrorType.SaveAllowed),
              NodeValidationError("EmptyMandatoryParameter", _, _, Some("P2"), NodeValidationErrorType.SaveAllowed)
            )
          ) =>
    }
  }

  test("validates scenario with fragment parameters - with spel validation expression and valid value") {
    val fragmentId = "fragment1"
    val paramName  = "name"

    val fragmentDefinition: CanonicalProcess =
      createFragmentDefinition(
        fragmentId,
        List(
          FragmentParameter(
            paramName,
            FragmentClazzRef[java.lang.String],
            required = false,
            initialValue = None,
            hintText = None,
            valueEditor = None,
            valueCompileTimeValidation = Some(
              ParameterValueCompileTimeValidation(
                s"#${ValidationExpressionParameterValidator.variableName}.length() < 7",
                None
              )
            )
          )
        )
      )
    val processWithFragment =
      createProcessWithFragmentParams(fragmentId, List(NodeParameter(paramName, "\"Tomasz\"")))

    val processValidation = mockedProcessValidator(fragmentDefinition, defaultConfig)
    val result            = processValidation.validate(processWithFragment)

    result.hasErrors shouldBe false
    result.errors.invalidNodes shouldBe Symbol("empty")
    result.errors.globalErrors shouldBe Symbol("empty")
    result.saveAllowed shouldBe true
  }

  test("validates scenario with fragment parameters - with spel validation expression and invalid value") {
    val fragmentId = "fragment1"
    val paramName  = "name"

    val configWithValidators: Config = defaultConfig

    val fragmentDefinition: CanonicalProcess =
      createFragmentDefinition(
        fragmentId,
        List(
          FragmentParameter(
            paramName,
            FragmentClazzRef[java.lang.String],
            required = false,
            initialValue = None,
            hintText = None,
            valueEditor = None,
            valueCompileTimeValidation = Some(
              ParameterValueCompileTimeValidation(
                s"#${ValidationExpressionParameterValidator.variableName}.length() < 7",
                Some("some failed message")
              )
            )
          )
        )
      )
    val processWithFragment =
      createProcessWithFragmentParams(fragmentId, List(NodeParameter(paramName, "\"Barabasz\"")))

    val processValidation = mockedProcessValidator(fragmentDefinition, configWithValidators)
    val result            = processValidation.validate(processWithFragment)
    result.hasErrors shouldBe true
    result.errors.globalErrors shouldBe empty
    result.errors.invalidNodes.get("subIn") should matchPattern {
      case Some(
            List(
              NodeValidationError(
                "CustomParameterValidationError",
                "some failed message",
                "Please provide value that satisfies the validation expression '#value.length() < 7'",
                Some(paramName),
                NodeValidationErrorType.SaveAllowed
              )
            )
          ) =>
    }
  }

  test("validates with custom validator") {
    val process = ScenarioBuilder
      .streaming(SampleCustomProcessValidator.badName.value)
      .source("start", existingSourceFactory)
      .emptySink("sink", existingSinkFactory)

    val displayable = ProcessConverter.toDisplayable(process, TestProcessingTypes.Streaming, Category1)
    val result      = mockedProcessValidator(process).validate(displayable)

    result.errors.processPropertiesErrors shouldBe List(
      PrettyValidationErrors.formatErrorMessage(SampleCustomProcessValidator.badNameError)
    )
  }

  test("should validate invalid scenario id") {
    val blankValue     = ProcessName(" ")
    val testedScenario = UIProcessValidatorSpec.validFlinkProcess.copy(name = blankValue)
    val result         = TestFactory.flinkProcessValidator.validate(testedScenario).errors.processPropertiesErrors
    result shouldBe List(
      PrettyValidationErrors.formatErrorMessage(ScenarioNameError(BlankId, blankValue, isFragment = false))
    )
  }

  test("should validate invalid node id") {
    val blankValue = " "
    val testedScenario = createProcess(
      List(
        Source(blankValue, SourceRef(existingSourceFactory, List())),
        Sink("out", SinkRef(existingSinkFactory, List()))
      ),
      List(Edge(blankValue, "out", None))
    )
    val result = TestFactory.flinkProcessValidator.validate(testedScenario).errors.invalidNodes
    val nodeErrors =
      Map(blankValue -> List(PrettyValidationErrors.formatErrorMessage(NodeIdValidationError(BlankId, blankValue))))
    result shouldBe nodeErrors
  }

  test("should validate scenario id with error preventing canonized form") {
    val incompleteScenarioWithBlankIds = createProcess(
      List(
        Variable(id = " ", varName = "var", value = "")
      ),
      List.empty
    ).copy(name = ProcessName(" "))
    val result = TestFactory.flinkProcessValidator.validate(incompleteScenarioWithBlankIds)
    inside(result) { case ValidationResult(errors, _, _) =>
      inside(errors) { case ValidationErrors(nodeErrors, propertiesErrors, _) =>
        nodeErrors should contain key " "
        nodeErrors(" ") should contain(
          PrettyValidationErrors.formatErrorMessage(NodeIdValidationError(BlankId, " "))
        )
        propertiesErrors shouldBe List(
          PrettyValidationErrors.formatErrorMessage(ScenarioNameError(BlankId, ProcessName(" "), isFragment = false))
        )
      }
    }
  }

  test("be able to convert process ending not properly") {
    forAll(
      Table(
        "unexpectedEnd",
        Filter("e", Expression.spel("0")),
        Switch("e"),
        Enricher("e", ServiceRef("ref", List()), "out"),
        Split("e")
      )
    ) { unexpectedEnd =>
      val displayable = createProcess(
        List(Source("s", SourceRef("sourceRef", List())), unexpectedEnd),
        List(Edge("s", "e", None)),
      )

      val result = TestFactory.processValidator.validate(displayable)

      result.errors.invalidNodes.get(unexpectedEnd.id) should matchPattern {
        case Some(
              List(NodeValidationError("InvalidTailOfBranch", _, _, _, NodeValidationErrorType.SaveAllowed))
            ) =>
      }
    }
  }

  test("return variable type information for process that cannot be canonized") {
    val displayable = createProcess(
      List(
        Source("s", SourceRef("sourceRef", List())),
        Variable("v", "test", Expression.spel("''")),
        Filter("e", Expression.spel("''"))
      ),
      List(Edge("s", "v", None), Edge("v", "e", None)),
    )

    val result = TestFactory.processValidator.validate(displayable)
    result.hasErrors shouldBe true

    val nodeResults = result.nodeResults.mapValuesNow(_.variableTypes)
    nodeResults.get("s").value shouldEqual Map.empty
    nodeResults.get("v").value shouldEqual Map("input" -> Unknown)
    nodeResults.get("e").value shouldEqual Map("input" -> Unknown, "test" -> Typed.fromInstance(""))
  }

}

private object UIProcessValidatorSpec {

  import ProcessTestData._
  import TestCategories._

  val sourceTypeName: String = "processSource"
  val sinkTypeName: String   = "processSink"

  val defaultConfig: Config = List("genericParametersSource", "genericParametersSink", "genericTransformer")
    .foldLeft(ConfigFactory.empty())((c, n) =>
      c.withValue(s"componentsUiConfig.$n.params.par1.defaultValue", fromAnyRef("'realDefault'"))
    )

  val configuredValidator: UIProcessValidator = TestFactory.processValidator.withScenarioPropertiesConfig(
    Map(
      "requiredStringProperty" -> ScenarioPropertyConfig(
        defaultValue = None,
        editor = Some(StringParameterEditor),
        validators = Some(List(MandatoryParameterValidator)),
        label = Some("label")
      ),
      "numberOfThreads" -> ScenarioPropertyConfig(
        defaultValue = None,
        editor = Some(FixedValuesParameterEditor(possibleValues)),
        validators = Some(List(FixedValuesValidator(possibleValues))),
        label = None
      ),
      "maxEvents" -> ScenarioPropertyConfig(
        defaultValue = None,
        editor = None,
        validators = Some(List(CompileTimeEvaluableValueValidator)),
        label = Some("label")
      )
    ) ++ FlinkStreamingPropertiesConfig.properties
  )

  val validFlinkProcess: DisplayableProcess = createProcess(
    List(
      Source("in", SourceRef(existingSourceFactory, List())),
      Sink("out", SinkRef(existingSinkFactory, List()))
    ),
    List(Edge("in", "out", None))
  )

  val validFlinkFragment: DisplayableProcess = DisplayableProcess(
    ProcessName("test"),
    ProcessProperties.combineTypeSpecificProperties(
      StreamMetaData(),
      additionalFields = ProcessAdditionalFields(None, FragmentSpecificData().toMap, FragmentSpecificData.typeName)
    ),
    nodes = List(
      FragmentInputDefinition("in", List()),
      FragmentOutputDefinition("out", "outputName")
    ),
    edges = List(Edge("in", "out", None)),
    processingType = TestProcessingTypes.Streaming,
    category = Category1
  )

  def validProcessWithFields(fields: Map[String, String]): DisplayableProcess = {
    createProcess(
      List(
        Source("in", SourceRef(existingSourceFactory, List())),
        Sink("out", SinkRef(existingSinkFactory, List()))
      ),
      List(Edge("in", "out", None)),
      additionalFields = fields
    )
  }

  private def createProcessWithParams(
      nodeParams: List[NodeParameter],
      scenarioProperties: Map[String, String],
      category: String = Category1
  ): DisplayableProcess = {
    createProcess(
      List(
        Source("inID", SourceRef(existingSourceFactory, List())),
        Enricher("custom", ServiceRef(otherExistingServiceId4, nodeParams), "out"),
        Sink("out", SinkRef(existingSinkFactory, List()))
      ),
      List(Edge("inID", "custom", None), Edge("custom", "out", None)),
      TestProcessingTypes.Streaming,
      category,
      scenarioProperties
    )
  }

  private def createProcessWithFragmentParams(
      fragmentDefinitionId: String,
      nodeParams: List[NodeParameter]
  ): DisplayableProcess = {
    createProcess(
      nodes = List(
        Source("source", SourceRef(sourceTypeName, Nil)),
        FragmentInput("subIn", FragmentRef(fragmentDefinitionId, nodeParams), isDisabled = Some(false)),
        Sink("sink", SinkRef(sinkTypeName, Nil))
      ),
      edges = List(
        Edge("source", "subIn", None),
        Edge("subIn", "sink", Some(EdgeType.FragmentOutput("out1")))
      )
    )
  }

  private def createProcess(
      nodes: List[NodeData],
      edges: List[Edge],
      `type`: ProcessingType = TestProcessingTypes.Streaming,
      category: String = Category1,
      additionalFields: Map[String, String] = Map()
  ): DisplayableProcess = {
    DisplayableProcess(
      ProcessName("test"),
      ProcessProperties.combineTypeSpecificProperties(
        StreamMetaData(),
        additionalFields = ProcessAdditionalFields(None, additionalFields, StreamMetaData.typeName)
      ),
      nodes,
      edges,
      `type`,
      category
    )
  }

  private def createFragmentDefinition(
      fragmentDefinitionId: String,
      fragmentInputParams: List[FragmentParameter]
  ): CanonicalProcess = {
    CanonicalProcess(
      MetaData(fragmentDefinitionId, FragmentSpecificData()),
      List(
        FlatNode(FragmentInputDefinition("in", fragmentInputParams)),
        FlatNode(FragmentOutputDefinition("out", "out1", List(Field("strField", Expression("spel", "'value'"))))),
      ),
      additionalBranches = List.empty
    )
  }

  def mockedProcessValidator(
      fragment: CanonicalProcess,
      execConfig: Config = ConfigFactory.empty()
  ): UIProcessValidator = {
    import ModelDefinitionBuilder._

    val modelDefinition = ModelDefinitionBuilder.empty
      .withSourceFactory(sourceTypeName)
      .withSinkFactory(sinkTypeName)

    new UIProcessValidator(
      ProcessValidator.default(new StubModelDataWithModelDefinition(modelDefinition, execConfig)),
      FlinkStreamingPropertiesConfig.properties,
      List(SampleCustomProcessValidator),
      new FragmentResolver(
        new StubFragmentRepository(
          Map(TestProcessingTypes.Streaming -> List(FragmentDetails(fragment, Category1)))
        )
      )
    )
  }

  object SampleCustomProcessValidator extends CustomProcessValidator {
    val badName = ProcessName("badName")

    val badNameError: ScenarioNameValidationError = ScenarioNameValidationError("BadName", "BadName")

    override def validate(process: CanonicalProcess): ValidatedNel[ProcessCompilationError, Unit] = {
      Validated.condNel(process.name != badName, (), badNameError)
    }

  }

  class WithoutErrorsAndWarnings extends BeMatcher[ValidationResult] {

    override def apply(left: ValidationResult): MatchResult = {
      MatchResult(
        !left.hasErrors && !left.hasWarnings,
        "ValidationResult should has neither errors nor warnings",
        "ValidationResult should has either errors or warnings"
      )
    }

  }

  val withoutErrorsAndWarnings: WithoutErrorsAndWarnings = new WithoutErrorsAndWarnings()

}
