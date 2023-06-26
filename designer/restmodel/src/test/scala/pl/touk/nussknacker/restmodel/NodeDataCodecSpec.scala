package pl.touk.nussknacker.restmodel

import io.circe.{Decoder, Encoder, Json, parser}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import pl.touk.nussknacker.engine.api.{ProcessAdditionalFields, StreamMetaData}
import pl.touk.nussknacker.engine.graph.evaluatedparam.Parameter
import pl.touk.nussknacker.engine.graph.expression.Expression
import pl.touk.nussknacker.engine.graph.node.SubprocessInputDefinition.{SubprocessClazzRef, SubprocessParameter}
import pl.touk.nussknacker.engine.graph.node.{CustomNode, SubprocessInputDefinition, UserDefinedAdditionalNodeFields}
import pl.touk.nussknacker.restmodel.displayedgraph.displayablenode.Edge
import pl.touk.nussknacker.restmodel.displayedgraph.{DisplayableProcess, ProcessProperties}
import pl.touk.nussknacker.test.EitherValuesDetailedMessage

class NodeDataCodecSpec extends AnyFunSuite with Matchers with EitherValuesDetailedMessage {


  test("displayable process encode and decode") {
    val process = DisplayableProcess("", ProcessProperties.combineTypeSpecificProperties(
      StreamMetaData(), ProcessAdditionalFields(Some("a"), Map("field1" -> "value1"), StreamMetaData.typeName)
    ), List(
      SubprocessInputDefinition("proc1", List(SubprocessParameter("param1", SubprocessClazzRef[String]))),
      CustomNode("id", Some("out1"), "typ1", List(Parameter("name1", Expression.spel("11"))),
        Some(UserDefinedAdditionalNodeFields(Some("desc"), None)))
    ), List(
      Edge("from1", "to1", None)
    ), "", "category1")

    val encoded = Encoder[DisplayableProcess].apply(process)

    encoded.hcursor.downField("edges").focus.flatMap(_.asArray) shouldBe Some(List(Json.obj(
      "from" -> Json.fromString("from1"),
      "to" -> Json.fromString("to1"),
      "edgeType" -> Json.Null
    )))

    Decoder[DisplayableProcess].decodeJson(encoded).toOption shouldBe Some(process)
  }

  test("decode displayable process in legacy format with typeSpecificProperties") {
    val givenProcessName = "foo1"
    val givenProcessingType = "fooProcessingType"
    val givenCategory = "FooCategory"
    val givenParallelism = 10
    val legacyJsonWithNoFields =
      s"""{
         |  "id" : "$givenProcessName",
         |  "properties" : {
         |    "typeSpecificProperties" : {
         |      "parallelism" : $givenParallelism,
         |      "type" : "${StreamMetaData.typeName}"
         |    },
         |    "additionalFields" : null
         |  },
         |  "nodes" : [],
         |  "edges" : [],
         |  "processingType" : "$givenProcessingType",
         |  "category" : "$givenCategory"
         |}""".stripMargin

    val parsedLegacy = parser.parse(legacyJsonWithNoFields).rightValue

    val decoded = Decoder[DisplayableProcess].decodeJson(parsedLegacy).rightValue
    decoded shouldEqual DisplayableProcess(givenProcessName,
      ProcessProperties(ProcessAdditionalFields(None, Map(
        StreamMetaData.parallelismName -> givenParallelism.toString,
        StreamMetaData.spillStateToDiskName -> "true",
        StreamMetaData.useAsyncInterpretationName -> "",
        StreamMetaData.checkpointIntervalName -> ""
      ), StreamMetaData.typeName)),
      List.empty,
      List.empty,
      givenProcessingType,
      givenCategory)

  }

}
