package pl.touk.nussknacker.engine.schemedkafka.source.flink

import cats.data.NonEmptyList
import cats.data.Validated.Invalid
import com.typesafe.config.ConfigFactory
import io.confluent.kafka.schemaregistry.client.{SchemaRegistryClient => CSchemaRegistryClient}
import io.confluent.kafka.serializers.NonRecordContainer
import org.apache.avro.generic.{GenericData, GenericRecord}
import pl.touk.nussknacker.engine.api.component.SingleComponentConfig
import pl.touk.nussknacker.engine.api.context.ProcessCompilationError.{CustomNodeError, InvalidPropertyFixedValue}
import pl.touk.nussknacker.engine.api.context.ValidationContext
import pl.touk.nussknacker.engine.api.typed.typing.{Typed, TypedObjectTypingResult, Unknown}
import pl.touk.nussknacker.engine.api.{MetaData, NodeId, StreamMetaData, VariableConstants}
import pl.touk.nussknacker.engine.compile.nodecompilation.{DynamicNodeValidator, TransformationResult}
import pl.touk.nussknacker.engine.graph.evaluatedparam.{Parameter => NodeParameter}
import pl.touk.nussknacker.engine.graph.expression.Expression
import pl.touk.nussknacker.engine.kafka.source.InputMeta
import pl.touk.nussknacker.engine.schemedkafka.KafkaUniversalComponentTransformer.{
  SchemaVersionParamName,
  TopicParamName
}
import pl.touk.nussknacker.engine.schemedkafka.helpers.KafkaAvroSpecMixin
import pl.touk.nussknacker.engine.schemedkafka.schema._
import pl.touk.nussknacker.engine.schemedkafka.schemaregistry.{
  ExistingSchemaVersion,
  LatestSchemaVersion,
  SchemaRegistryClientFactory,
  SchemaVersionOption
}
import pl.touk.nussknacker.engine.spel.Implicits._
import pl.touk.nussknacker.engine.testing.LocalModelData

import java.nio.charset.StandardCharsets
import scala.collection.immutable.ListMap
import scala.jdk.CollectionConverters._

class KafkaAvroPayloadSourceFactorySpec extends KafkaAvroSpecMixin with KafkaAvroSourceSpecMixin {

  import KafkaAvroSourceMockSchemaRegistry._

  override protected def schemaRegistryClient: CSchemaRegistryClient = schemaRegistryMockClient

  override protected def schemaRegistryClientFactory: SchemaRegistryClientFactory = factory

  test("should read generated generic record in v1 with null key") {
    val givenObj = FullNameV1.createRecord("Jan", "Kowalski")

    roundTripKeyValueObject(
      universalSourceFactory,
      useStringForKey = true,
      RecordTopic,
      ExistingSchemaVersion(1),
      null,
      givenObj
    )
  }

  test("should read generated generic record in v1 with empty string key") {
    val givenObj = FullNameV1.createRecord("Jan", "Kowalski")

    roundTripKeyValueObject(
      universalSourceFactory,
      useStringForKey = true,
      RecordTopic,
      ExistingSchemaVersion(1),
      "",
      givenObj
    )
  }

  test("should read last generated generic record with logical types") {
    val givenObj = PaymentDate.record

    roundTripKeyValueObject(
      universalSourceFactory,
      useStringForKey = true,
      PaymentDateTopic,
      ExistingSchemaVersion(1),
      "",
      givenObj
    )
  }

  test("should read generated record in v2") {
    val givenObj = FullNameV2.createRecord("Jan", "Maria", "Kowalski")

    roundTripKeyValueObject(
      universalSourceFactory,
      useStringForKey = true,
      RecordTopic,
      ExistingSchemaVersion(2),
      null,
      givenObj
    )
  }

  test("should read generated record in last version") {
    val givenObj = FullNameV2.createRecord("Jan", "Maria", "Kowalski")

    roundTripKeyValueObject(
      universalSourceFactory,
      useStringForKey = true,
      RecordTopic,
      LatestSchemaVersion,
      null,
      givenObj
    )
  }

  test("should return validation errors when schema doesn't exist") {
    val givenObj = FullNameV2.createRecord("Jan", "Maria", "Kowalski")

    readLastMessageAndVerify(
      universalSourceFactory(useStringForKey = true),
      "fake-topic",
      ExistingSchemaVersion(1),
      null,
      givenObj
    ) should matchPattern {
      case Invalid(
            NonEmptyList(
              CustomNodeError(_, "Fetching schema error for topic: fake-topic, version: ExistingSchemaVersion(1)", _),
              _
            )
          ) =>
        ()
    }
  }

  test("should return validation errors when schema version doesn't exist") {
    val givenObj = FullNameV2.createRecord("Jan", "Maria", "Kowalski")

    readLastMessageAndVerify(
      universalSourceFactory(useStringForKey = true),
      RecordTopic,
      ExistingSchemaVersion(3),
      null,
      givenObj
    ) should matchPattern {
      case Invalid(
            NonEmptyList(
              CustomNodeError(
                _,
                "Fetching schema error for topic: testAvroRecordTopic1, version: ExistingSchemaVersion(3)",
                _
              ),
              _
            )
          ) =>
        ()
    }
  }

  test("should read last generated simple object without key schema") {
    val givenObj = 123123

    roundTripKeyValueObject(
      universalSourceFactory,
      useStringForKey = true,
      IntTopicNoKey,
      ExistingSchemaVersion(1),
      null,
      givenObj
    )
  }

  test("should ignore key schema and empty key value with string-as-key deserialization") {
    val givenObj = 123123

    roundTripKeyValueObject(
      universalSourceFactory,
      useStringForKey = true,
      IntTopicWithKey,
      ExistingSchemaVersion(1),
      null,
      givenObj
    )
  }

  test("should read last generated simple object with expected key schema and valid key") {
    val givenObj = 123123

    val serializedKey   = keySerializer.serialize(IntTopicWithKey, -1)
    val serializedValue = valueSerializer.serialize(IntTopicWithKey, givenObj)
    kafkaClient.sendRawMessage(IntTopicWithKey, serializedKey, serializedValue, Some(0))

    readLastMessageAndVerify(
      universalSourceFactory(useStringForKey = true),
      IntTopicWithKey,
      ExistingSchemaVersion(1),
      new String(serializedKey, StandardCharsets.UTF_8),
      givenObj
    )
  }

  test("should read object with invalid defaults") {
    val givenObj = new GenericData.Record(InvalidDefaultsSchema)
    givenObj.put("field1", "foo")

    roundTripKeyValueObject(
      universalSourceFactory,
      useStringForKey = true,
      InvalidDefaultsTopic,
      ExistingSchemaVersion(1),
      null,
      givenObj
    )
  }

  test("should read array of primitives on top level") {
    val topic        = ArrayOfNumbersTopic
    val arrayOfInts  = List(123).asJava
    val arrayOfLongs = List(123L).asJava
    val wrappedObj   = new NonRecordContainer(ArrayOfIntsSchema, arrayOfInts)
    pushMessageWithKey(null, wrappedObj, topic, useStringForKey = true)

    readLastMessageAndVerify(
      universalSourceFactory(useStringForKey = true),
      topic,
      ExistingSchemaVersion(2),
      null,
      arrayOfLongs
    )
  }

  test("should read array of records on top level") {
    val topic            = ArrayOfRecordsTopic
    val recordV1         = FullNameV1.createRecord("Jan", "Kowalski")
    val arrayOfRecordsV1 = List(recordV1).asJava
    val recordV2         = FullNameV2.createRecord("Jan", null, "Kowalski")
    val arrayOfRecordsV2 = List(recordV2).asJava
    val wrappedObj       = new NonRecordContainer(ArrayOfRecordsV1Schema, arrayOfRecordsV1)
    pushMessageWithKey(null, wrappedObj, topic, useStringForKey = true)

    readLastMessageAndVerify(
      universalSourceFactory(useStringForKey = true),
      topic,
      ExistingSchemaVersion(2),
      null,
      arrayOfRecordsV2
    )
  }

  test("should read last generated key-value object, simple type") {
    val givenKey   = 123
    val givenValue = 456

    val serializedKey   = keySerializer.serialize(IntTopicWithKey, givenKey)
    val serializedValue = valueSerializer.serialize(IntTopicWithKey, givenValue)
    kafkaClient.sendRawMessage(IntTopicWithKey, serializedKey, serializedValue, Some(0))

    roundTripKeyValueObject(
      universalSourceFactory,
      useStringForKey = false,
      IntTopicWithKey,
      ExistingSchemaVersion(1),
      givenKey,
      givenValue
    )
  }

  test("should read last generated key-value object, complex object") {
    val givenKey   = FullNameV1.record
    val givenValue = PaymentV1.record

    val serializedKey   = keySerializer.serialize(RecordTopicWithKey, givenKey)
    val serializedValue = valueSerializer.serialize(RecordTopicWithKey, givenValue)
    kafkaClient.sendRawMessage(RecordTopicWithKey, serializedKey, serializedValue, Some(0))

    roundTripKeyValueObject(
      universalSourceFactory,
      useStringForKey = false,
      RecordTopicWithKey,
      ExistingSchemaVersion(1),
      givenKey,
      givenValue
    )
  }

  test("Should validate specific version") {
    val result =
      validate(TopicParamName -> s"'${KafkaAvroSourceMockSchemaRegistry.RecordTopic}'", SchemaVersionParamName -> "'1'")

    result.errors shouldBe Nil
  }

  test("Should validate latest version") {
    val result = validate(
      TopicParamName         -> s"'${KafkaAvroSourceMockSchemaRegistry.RecordTopic}'",
      SchemaVersionParamName -> s"'${SchemaVersionOption.LatestOptionName}'"
    )

    result.errors shouldBe Nil
  }

  test("Should return sane error on invalid topic") {
    val result =
      validate(TopicParamName -> "'terefere'", SchemaVersionParamName -> s"'${SchemaVersionOption.LatestOptionName}'")

    result.errors shouldBe
      InvalidPropertyFixedValue(
        TopicParamName,
        None,
        "'terefere'",
        List(
          "",
          "'testArrayOfNumbersTopic'",
          "'testArrayOfRecordsTopic'",
          "'testAvroIntTopic1NoKey'",
          "'testAvroIntTopic1WithKey'",
          "'testAvroInvalidDefaultsTopic1'",
          "'testAvroRecordTopic1'",
          "'testAvroRecordTopic1WithKey'",
          "'testPaymentDateTopic'"
        ),
        "id"
      ) :: Nil
    result.outputContext shouldBe ValidationContext(
      Map(
        VariableConstants.InputVariableName     -> Unknown,
        VariableConstants.InputMetaVariableName -> InputMeta.withType(Unknown)
      )
    )
  }

  test("Should return sane error on invalid version") {
    val result = validate(
      TopicParamName         -> s"'${KafkaAvroSourceMockSchemaRegistry.RecordTopic}'",
      SchemaVersionParamName -> "'12345'"
    )

    result.errors shouldBe InvalidPropertyFixedValue(
      SchemaVersionParamName,
      None,
      "'12345'",
      List("'latest'", "'1'", "'2'"),
      "id"
    ) :: Nil
    result.outputContext shouldBe ValidationContext(
      Map(
        VariableConstants.InputVariableName     -> Unknown,
        VariableConstants.InputMetaVariableName -> InputMeta.withType(Unknown)
      )
    )
  }

  test("Should properly detect input type") {
    val result = validate(
      TopicParamName         -> s"'${KafkaAvroSourceMockSchemaRegistry.RecordTopic}'",
      SchemaVersionParamName -> s"'${SchemaVersionOption.LatestOptionName}'"
    )

    result.errors shouldBe Nil
    result.outputContext shouldBe ValidationContext(
      Map(
        VariableConstants.InputVariableName -> TypedObjectTypingResult(
          ListMap(
            "first"  -> AvroStringSettings.stringTypingResult,
            "middle" -> AvroStringSettings.stringTypingResult,
            "last"   -> AvroStringSettings.stringTypingResult
          ),
          Typed.typedClass[GenericRecord]
        ),
        VariableConstants.InputMetaVariableName -> InputMeta.withType(Typed[String])
      )
    )
  }

  private def validate(params: (String, Expression)*): TransformationResult = {
    val modelData = LocalModelData(ConfigFactory.empty(), List.empty)
    val validator = DynamicNodeValidator(modelData)

    implicit val meta: MetaData = MetaData("processId", StreamMetaData())
    implicit val nodeId: NodeId = NodeId("id")
    val paramsList              = params.toList.map(p => NodeParameter(p._1, p._2))
    validator
      .validateNode(
        universalSourceFactory(useStringForKey = true),
        paramsList,
        Nil,
        Some(VariableConstants.InputVariableName),
        SingleComponentConfig.zero
      )(ValidationContext())
      .toOption
      .get
  }

}
