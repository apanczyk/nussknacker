package pl.touk.nussknacker.ui.process.repository

import akka.http.scaladsl.model.HttpHeader
import com.typesafe.scalalogging.LazyLogging
import db.util.DBIOActionInstances._
import io.circe.generic.JsonCodec
import pl.touk.nussknacker.engine.ModelData
import pl.touk.nussknacker.engine.api.process.{ProcessId, ProcessIdWithName, ProcessName, VersionId}
import pl.touk.nussknacker.engine.canonicalgraph.CanonicalProcess
import pl.touk.nussknacker.engine.api.process.ProcessingType
import pl.touk.nussknacker.ui.db.entity.{CommentActions, ProcessEntityData, ProcessVersionEntityData}
import pl.touk.nussknacker.ui.db.{DbRef, NuTables}
import pl.touk.nussknacker.ui.listener.Comment
import pl.touk.nussknacker.ui.process.processingtypedata.ProcessingTypeDataProvider
import pl.touk.nussknacker.ui.process.repository.ProcessDBQueryRepository._
import pl.touk.nussknacker.ui.process.repository.ProcessRepository.{
  CreateProcessAction,
  ProcessCreated,
  ProcessUpdated,
  UpdateProcessAction
}
import pl.touk.nussknacker.ui.security.api.LoggedUser
import slick.dbio.DBIOAction

import java.sql.Timestamp
import java.time.Instant
import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.higherKinds

object ProcessRepository {

  @JsonCodec final case class RemoteUserName(name: String) extends AnyVal {
    def display: String = s"Remote[$name]"
  }

  object RemoteUserName {
    val headerName = "Remote-User-Name".toLowerCase

    def extractFromHeader: HttpHeader => Option[RemoteUserName] = {
      case HttpHeader(`headerName`, value) => Some(RemoteUserName(value))
      case _                               => None
    }

  }

  def create(dbRef: DbRef, modelData: ProcessingTypeDataProvider[ModelData, _]): DBProcessRepository =
    new DBProcessRepository(dbRef, modelData.mapValues(_.migrations.version))

  final case class CreateProcessAction(
      processName: ProcessName,
      category: String,
      canonicalProcess: CanonicalProcess,
      processingType: ProcessingType,
      isFragment: Boolean,
      forwardedUserName: Option[RemoteUserName]
  )

  final case class UpdateProcessAction(
      id: ProcessId,
      canonicalProcess: CanonicalProcess,
      comment: Option[Comment],
      increaseVersionWhenJsonNotChanged: Boolean,
      forwardedUserName: Option[RemoteUserName]
  )

  final case class ProcessUpdated(processId: ProcessId, oldVersion: Option[VersionId], newVersion: Option[VersionId])

  final case class ProcessCreated(processId: ProcessId, processVersionId: VersionId)
}

trait ProcessRepository[F[_]] {

  def saveNewProcess(action: CreateProcessAction)(implicit loggedUser: LoggedUser): F[Option[ProcessCreated]]

  def updateProcess(action: UpdateProcessAction)(implicit loggedUser: LoggedUser): F[ProcessUpdated]

  def updateCategory(processId: ProcessId, category: String)(implicit loggedUser: LoggedUser): F[Unit]

  def archive(processId: ProcessId, isArchived: Boolean): F[Unit]

  def deleteProcess(processId: ProcessId): F[Unit]

  def renameProcess(processId: ProcessIdWithName, newName: ProcessName)(implicit loggedUser: LoggedUser): F[Unit]

}

class DBProcessRepository(val dbRef: DbRef, val modelVersion: ProcessingTypeDataProvider[Int, _])
    extends ProcessRepository[DB]
    with NuTables
    with LazyLogging
    with CommentActions
    with ProcessDBQueryRepository[DB] {

  import profile.api._

  // FIXME: It's temporary way.. After merge and refactor process repositories we can remove it.
  override def run[R]: DB[R] => DB[R] = identity

  /**
   * These action should be done on transaction - move it to ProcessService.createProcess
   */
  def saveNewProcess(
      action: CreateProcessAction
  )(implicit loggedUser: LoggedUser): DB[Option[ProcessCreated]] = {
    val userName = action.forwardedUserName.map(_.display).getOrElse(loggedUser.username)
    val processToSave = ProcessEntityData(
      id = ProcessId(-1L),
      name = action.processName,
      processCategory = action.category,
      description = None,
      processingType = action.processingType,
      isFragment = action.isFragment,
      isArchived = false,
      createdAt = Timestamp.from(now),
      createdBy = userName
    )

    val insertNew =
      processesTable.returning(processesTable.map(_.id)).into { case (entity, newId) => entity.copy(id = newId) }

    logger.debug(s"Saving scenario ${action.processName} by user $userName")

    latestProcessVersionsNoJsonQuery(action.processName).result.headOption.flatMap {
      case Some(_) => DBIOAction.failed(ProcessAlreadyExists(action.processName.value))
      case None =>
        processesTable.filter(_.name === action.processName).result.headOption.flatMap {
          case Some(_) => DBIOAction.failed(ProcessAlreadyExists(action.processName.value))
          case None =>
            (insertNew += processToSave)
              .flatMap(entity =>
                updateProcessInternal(
                  entity.id,
                  action.canonicalProcess,
                  increaseVersionWhenJsonNotChanged = false,
                  userName = userName
                )
              )
              .map(res => res.newVersion.map(ProcessCreated(res.processId, _)))
        }
    }
  }

  def updateProcess(
      updateProcessAction: UpdateProcessAction
  )(implicit loggedUser: LoggedUser): DB[ProcessUpdated] = {
    val userName = updateProcessAction.forwardedUserName.map(_.display).getOrElse(loggedUser.username)

    def addNewCommentToVersion(processId: ProcessId, versionId: VersionId) = {
      newCommentAction(processId, versionId, updateProcessAction.comment)
    }

    updateProcessInternal(
      updateProcessAction.id,
      updateProcessAction.canonicalProcess,
      updateProcessAction.increaseVersionWhenJsonNotChanged,
      userName
    ).flatMap {
      // Comment should be added via ProcessService not to mix this repository responsibility.
      case updateProcessRes @ ProcessUpdated(processId, _, Some(newVersion)) =>
        addNewCommentToVersion(processId, newVersion).map(_ => updateProcessRes)
      case updateProcessRes @ ProcessUpdated(processId, Some(oldVersion), _) =>
        addNewCommentToVersion(processId, oldVersion).map(_ => updateProcessRes)
      case a => DBIO.successful(a)
    }
  }

  private def updateProcessInternal(
      processId: ProcessId,
      canonicalProcess: CanonicalProcess,
      increaseVersionWhenJsonNotChanged: Boolean,
      userName: String
  )(implicit loggedUser: LoggedUser): DB[ProcessUpdated] = {
    def createProcessVersionEntityData(version: VersionId, processingType: ProcessingType) = ProcessVersionEntityData(
      id = version,
      processId = processId,
      json = Some(canonicalProcess),
      createDate = Timestamp.from(now),
      user = userName,
      modelVersion = modelVersion.forType(processingType),
      componentsUsages = Some(ScenarioComponentsUsagesHelper.compute(canonicalProcess)),
    )

    def isLastVersionContainsSameProcess(lastVersion: ProcessVersionEntityData): Boolean =
      lastVersion.json.contains(canonicalProcess)

    def versionToInsert(latestProcessVersion: Option[ProcessVersionEntityData], processingType: ProcessingType) =
      latestProcessVersion match {
        case Some(version) if isLastVersionContainsSameProcess(version) && !increaseVersionWhenJsonNotChanged =>
          None
        case Some(version) =>
          Some(createProcessVersionEntityData(version.id.increase, processingType))
        case _ =>
          Some(createProcessVersionEntityData(VersionId.initialVersionId, processingType))
      }

    logger.debug(s"Updating scenario $processId by user $userName")
    for {
      maybeProcess <- processTableFilteredByUser.filter(_.id === processId).result.headOption
      process = maybeProcess.getOrElse(throw ProcessNotFoundError(processId.value.toString))
      latestProcessVersion <- fetchProcessLatestVersionsQuery(processId)(
        ScenarioShapeFetchStrategy.FetchDisplayable
      ).result.headOption
      newProcessVersionOpt = versionToInsert(latestProcessVersion, process.processingType)
      _ <- newProcessVersionOpt.map(processVersionsTable += _).getOrElse(dbMonad.pure(0))
    } yield ProcessUpdated(
      process.id,
      oldVersion = latestProcessVersion.map(_.id),
      newVersion = newProcessVersionOpt.map(_.id)
    )
  }

  def deleteProcess(processId: ProcessId): DB[Unit] =
    processesTable.filter(_.id === processId).delete.map {
      case 0 => throw ProcessNotFoundError(processId.value.toString)
      case 1 => ()
    }

  def archive(processId: ProcessId, isArchived: Boolean): DB[Unit] =
    processesTable.filter(_.id === processId).map(_.isArchived).update(isArchived).map {
      case 0 => throw ProcessNotFoundError(processId.value.toString)
      case 1 => ()
    }

  // accessible only from initializing scripts so far
  def updateCategory(processId: ProcessId, category: String)(implicit loggedUser: LoggedUser): DB[Unit] =
    processesTable.filter(_.id === processId).map(_.processCategory).update(category).map {
      case 0 => throw ProcessNotFoundError(processId.value.toString)
      case 1 => ()
    }

  def renameProcess(process: ProcessIdWithName, newName: ProcessName)(
      implicit loggedUser: LoggedUser
  ): DB[Unit] = {
    def updateNameInSingleProcessVersion(processVersion: ProcessVersionEntityData, process: ProcessEntityData) = {
      processVersion.json match {
        case Some(json) =>
          val updatedProcess        = json.copy(metaData = json.metaData.copy(id = newName.value))
          val updatedProcessVersion = processVersion.copy(json = Some(updatedProcess))
          processVersionsTableWithScenarioJson
            .filter(version => version.id === processVersion.id && version.processId === process.id)
            .update(updatedProcessVersion)
        case None => DBIO.successful(())
      }
    }

    val updateNameInProcessJson =
      processVersionsTableWithScenarioJson
        .filter(_.processId === process.id)
        .join(processesTable)
        .on { case (version, process) => version.processId === process.id }
        .result
        .flatMap { processVersions =>
          DBIO.seq(processVersions.map((updateNameInSingleProcessVersion _).tupled): _*)
        }

    val updateNameInProcess =
      processesTable.filter(_.id === process.id).map(_.name).update(newName)

    // Comment relates to specific version (in this case last version). Last version could be extracted in one of the
    // above queries, but for sake of readability we perform separate query for this matter
    // TODO: remove this comment in favour of process-audit-log
    val addCommentAction = processVersionsTableWithUnit
      .filter(_.processId === process.id)
      .sortBy(_.id.desc)
      .result
      .headOption
      .flatMap {
        case Some(version) =>
          newCommentAction(
            process.id,
            version.id,
            Some(UpdateProcessComment(s"Rename: [${process.name}] -> [$newName]"))
          )
        case None => DBIO.successful(())
      }

    val action = processesTable.filter(_.name === newName).result.headOption.flatMap {
      case Some(_) => DBIO.failed(ProcessAlreadyExists(newName.value))
      case None =>
        DBIO
          .seq[Effect.All](
            updateNameInProcess,
            updateNameInProcessJson,
            addCommentAction
          )
          .map(_ => ())
          .transactionally
    }

    action
  }

  // to override in tests
  protected def now: Instant = Instant.now()

  // We use it only on tests..
  def changeVersionId(processId: ProcessId, versionId: VersionId, versionIdToUpdate: VersionId) =
    processVersionsTableWithUnit
      .filter(v => v.id === versionId && v.processId === processId)
      .map(_.id)
      .update(versionIdToUpdate)

}
