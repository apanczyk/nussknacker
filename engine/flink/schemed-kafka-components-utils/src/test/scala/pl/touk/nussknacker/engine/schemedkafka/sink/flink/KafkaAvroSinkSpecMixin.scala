package pl.touk.nussknacker.engine.schemedkafka.sink.flink

import io.confluent.kafka.schemaregistry.client.{SchemaRegistryClient => CSchemaRegistryClient}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericContainer
import pl.touk.nussknacker.engine.api.LazyParameter
import pl.touk.nussknacker.engine.api.typed.typing
import pl.touk.nussknacker.engine.api.validation.ValidationMode
import pl.touk.nussknacker.engine.schemedkafka.encode.BestEffortAvroEncoder
import pl.touk.nussknacker.engine.schemedkafka.schema._
import pl.touk.nussknacker.engine.schemedkafka.schemaregistry.SchemaRegistryClientFactory
import pl.touk.nussknacker.engine.schemedkafka.schemaregistry.confluent.client.MockConfluentSchemaRegistryClientBuilder
import pl.touk.nussknacker.engine.schemedkafka.schemaregistry.universal.MockSchemaRegistryClientFactory
import pl.touk.nussknacker.engine.schemedkafka.typed.AvroSchemaTypeDefinitionExtractor

trait KafkaAvroSinkSpecMixin {

  final protected val avroEncoder = BestEffortAvroEncoder(ValidationMode.strict)

  protected def createLazyParam(schema: Schema, data: Map[String, Any]): LazyParameter[GenericContainer] = {
    val record = avroEncoder.encodeRecordOrError(data, schema)
    new LazyParameter[GenericContainer] {
      override def returnType: typing.TypingResult = AvroSchemaTypeDefinitionExtractor.typeDefinition(record.getSchema)
    }
  }

  object KafkaAvroSinkMockSchemaRegistry {

    val fullnameTopic: String          = "fullname"
    val generatedNewSchemaVersion: Int = 3

    val schemaRegistryMockClient: CSchemaRegistryClient = new MockConfluentSchemaRegistryClientBuilder()
      .register(fullnameTopic, FullNameV1.schema, 1, isKey = false)
      .register(fullnameTopic, FullNameV2.schema, 2, isKey = false)
      .register(fullnameTopic, PaymentV1.schema, 3, isKey = false)
      .build

    val factory: SchemaRegistryClientFactory = MockSchemaRegistryClientFactory.confluentBased(schemaRegistryMockClient)
  }

}
