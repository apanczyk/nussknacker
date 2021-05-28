package pl.touk.nussknacker.engine.kafka.source

import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import pl.touk.nussknacker.engine.api.process.ProcessObjectDependencies
import pl.touk.nussknacker.engine.flink.api.process.{FlinkContextInitializer, FlinkSource, FlinkSourceFactory}
import pl.touk.nussknacker.engine.flink.api.timestampwatermark.TimestampWatermarkHandler
import pl.touk.nussknacker.engine.kafka.serialization.KafkaDeserializationSchemaFactory
import pl.touk.nussknacker.engine.kafka._
import org.apache.flink.types.Nothing
import org.apache.kafka.clients.consumer.ConsumerRecord
import pl.touk.nussknacker.engine.api.context.{ProcessCompilationError, ValidationContext}
import pl.touk.nussknacker.engine.api.context.transformation.{DefinedEagerParameter, NodeDependencyValue, SingleInputGenericNodeTransformation}
import pl.touk.nussknacker.engine.api.definition._
import pl.touk.nussknacker.engine.api.typed.typing.Typed
import pl.touk.nussknacker.engine.kafka.validator.WithCachedTopicsExistenceValidator

import scala.reflect.ClassTag

/**
  * Base factory for Kafka sources with additional metadata variable.
  * It is based on [[pl.touk.nussknacker.engine.api.context.transformation.SingleInputGenericNodeTransformation]]
  * that allows custom ValidationContext and Context transformations, which are provided by [[pl.touk.nussknacker.engine.kafka.source.KafkaContextInitializer]]
  * Can be used for single- or multi- topic sources (as csv, see topicNameSeparator and extractTopics).
  *
  * Wrapper for [[org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer]]
  * Features:
  *   - fetch latest N records which can be later used to test process in UI
  * Fetching data is defined in [[pl.touk.nussknacker.engine.kafka.source.KafkaSource]] which
  * extends [[pl.touk.nussknacker.engine.api.process.TestDataGenerator]]. See [[pl.touk.nussknacker.engine.kafka.KafkaUtils#readLastMessages]]
  *   - reset Kafka's offset to latest value - `forceLatestRead` property, see [[pl.touk.nussknacker.engine.kafka.KafkaUtils#setOffsetToLatest]]
  *
  * @param deserializationSchemaFactory - produces KafkaDeserializationSchema for raw [[pl.touk.nussknacker.engine.kafka.source.KafkaSource]]
  * @param timestampAssigner            - provides timestampAsigner and WatermarkStrategy to KafkaSource
  * @param formatterFactory             - support for test data parser and generator
  * @param processObjectDependencies    - dependencies required by the component
  * @tparam K - type of key of kafka event that is generated by raw source (SourceFunction).
  * @tparam V - type of value of kafka event that is generated by raw source (SourceFunction).
  * */
class KafkaSourceFactory[K: ClassTag, V: ClassTag](deserializationSchemaFactory: KafkaDeserializationSchemaFactory[ConsumerRecord[K, V]],
                                                   timestampAssigner: Option[TimestampWatermarkHandler[ConsumerRecord[K, V]]],
                                                   formatterFactory: RecordFormatterFactory,
                                                   processObjectDependencies: ProcessObjectDependencies)
  extends FlinkSourceFactory[ConsumerRecord[K, V]] with SingleInputGenericNodeTransformation[FlinkSource[ConsumerRecord[K, V]]] with WithCachedTopicsExistenceValidator {

  protected val topicNameSeparator = ","

  protected val kafkaContextInitializer: KafkaContextInitializer[K, V, DefinedParameter, State] =
    new KafkaContextInitializer[K, V, DefinedParameter, State](Typed[K], Typed[V])

  override type State = Nothing

  // initialParameters should not expose raised exceptions.
  override def initialParameters: List[Parameter] =
    try {
      prepareInitialParameters
    } catch {
      case e: Exception => handleExceptionInInitialParameters
    }

  protected def handleExceptionInInitialParameters: List[Parameter] = Nil

  private def initialStep(context: ValidationContext, dependencies: List[NodeDependencyValue])(implicit nodeId: ProcessCompilationError.NodeId): NodeTransformationDefinition = {
    case step@TransformationStep(Nil, _) =>
      NextParameters(prepareInitialParameters)
  }

  protected def nextSteps(context: ValidationContext, dependencies: List[NodeDependencyValue])(implicit nodeId: ProcessCompilationError.NodeId): NodeTransformationDefinition = {
    case step@TransformationStep((KafkaSourceFactory.TopicParamName, DefinedEagerParameter(topic: String, _)) :: tailParams, None) => {
      val topics = topic.split(topicNameSeparator).map(_.trim).toList
      val preparedTopics = topics.map(KafkaUtils.prepareKafkaTopic(_, processObjectDependencies)).map(_.prepared)
      val topicValidationErrors = validateTopics(preparedTopics).swap.toList.map(_.toCustomNodeError(nodeId.id, Some(KafkaSourceFactory.TopicParamName)))
      FinalResults(
        finalContext = kafkaContextInitializer.validationContext(context, dependencies, step.parameters, step.state),
        errors = topicValidationErrors)
    }
  }

  /**
    * contextTransformation should handle exceptions raised by prepareInitialParameters
    */
  override def contextTransformation(context: ValidationContext, dependencies: List[NodeDependencyValue])(implicit nodeId: ProcessCompilationError.NodeId)
  : NodeTransformationDefinition =
    initialStep(context, dependencies) orElse
      nextSteps(context ,dependencies)

  /**
    * Common set of operations required to create basic KafkaSource.
    */
  override def implementation(params: Map[String, Any], dependencies: List[NodeDependencyValue], finalState: Option[Nothing]): FlinkSource[ConsumerRecord[K, V]] = {
    val topics = extractTopics(params)
    val preparedTopics = topics.map(KafkaUtils.prepareKafkaTopic(_, processObjectDependencies))
    val deserializationSchema = deserializationSchemaFactory.create(topics, kafkaConfig)
    val formatter = formatterFactory.create(deserializationSchema)
    createSource(params, dependencies, finalState, preparedTopics, kafkaConfig, deserializationSchema, formatter)
  }

  /**
    * Basic implementation of new source creation. Override this method to create custom KafkaSource.
    */
  protected def createSource(params: Map[String, Any],
                             dependencies: List[NodeDependencyValue],
                             finalState: Option[Nothing],
                             preparedTopics: List[PreparedKafkaTopic],
                             kafkaConfig: KafkaConfig,
                             deserializationSchema: KafkaDeserializationSchema[ConsumerRecord[K, V]],
                             formatter: RecordFormatter): FlinkSource[ConsumerRecord[K, V]] = {
    new KafkaSource[ConsumerRecord[K, V]](preparedTopics, kafkaConfig, deserializationSchema, timestampAssigner, formatter) {
      override val contextInitializer: FlinkContextInitializer[ConsumerRecord[K, V]] = kafkaContextInitializer
    }
  }

  /**
    * Basic implementation of definition of single topic parameter.
    * In case of fetching topics from external repository: return list of topics or raise exception.
    */
  protected def prepareInitialParameters: List[Parameter] = topicParameter.parameter :: Nil

  protected val topicParameter: ParameterWithExtractor[String] =
    ParameterWithExtractor.mandatory[String](
      KafkaSourceFactory.TopicParamName,
      _.copy(validators = List(MandatoryParameterValidator, NotBlankParameterValidator))
    )

  /**
    * Extracts topics from default topic parameter.
    */
  protected def extractTopics(params: Map[String, Any]): List[String] = {
    val paramValue = topicParameter.extractValue(params)
    paramValue.split(topicNameSeparator).map(_.trim).toList
  }

  override def nodeDependencies: List[NodeDependency] = Nil

  override protected val kafkaConfig: KafkaConfig = KafkaConfig.parseProcessObjectDependencies(processObjectDependencies)
}

object KafkaSourceFactory {
  final val TopicParamName = "topic"
}
