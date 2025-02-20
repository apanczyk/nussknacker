package pl.touk.nussknacker.engine.management.sample

import org.slf4j.LoggerFactory
import org.slf4j.event.Level
import pl.touk.nussknacker.engine.api._
import pl.touk.nussknacker.engine.api.editor.{SimpleEditor, SimpleEditorType}
import pl.touk.nussknacker.engine.api.process.ComponentUseCase
import pl.touk.nussknacker.engine.api.test.InvocationCollectors.ServiceInvocationCollector

import javax.annotation.Nullable
import scala.concurrent.{ExecutionContext, Future}

object LoggingService extends EagerService {

  private val rootLogger = "scenarios"

  @MethodToInvoke(returnType = classOf[Void])
  def invoke(
      @ParamName("logger") @Nullable loggerName: String,
      @ParamName("level") @DefaultValue("T(org.slf4j.event.Level).DEBUG") level: Level,
      @ParamName("message") @SimpleEditor(`type` = SimpleEditorType.SPEL_TEMPLATE_EDITOR) message: LazyParameter[String]
  )(implicit metaData: MetaData, nodeId: NodeId): ServiceInvoker =
    new ServiceInvoker {

      private lazy val logger = LoggerFactory.getLogger(
        (rootLogger :: metaData.name.value :: nodeId.id :: Option(loggerName).toList).filterNot(_.isBlank).mkString(".")
      )

      override def invokeService(params: Map[String, Any])(
          implicit ec: ExecutionContext,
          collector: ServiceInvocationCollector,
          contextId: ContextId,
          componentUseCase: ComponentUseCase
      ): Future[Any] = {
        val message = params("message").asInstanceOf[String]
        level match {
          case Level.TRACE => logger.trace(message)
          case Level.DEBUG => logger.debug(message)
          case Level.INFO  => logger.info(message)
          case Level.WARN  => logger.warn(message)
          case Level.ERROR => logger.error(message)
        }
        Future.successful(())
      }

    }

}
