package pl.touk.nussknacker.ui.process

import pl.touk.nussknacker.engine.api.displayedgraph.DisplayableProcess
import pl.touk.nussknacker.engine.api.process.ProcessId
import pl.touk.nussknacker.restmodel.scenariodetails.ScenarioWithDetails
import pl.touk.nussknacker.restmodel.validation.ValidatedDisplayableProcess
import pl.touk.nussknacker.ui.process.repository.ScenarioWithDetailsEntity

object ScenarioWithDetailsConversions {

  def fromEntity(details: ScenarioWithDetailsEntity[ValidatedDisplayableProcess]): ScenarioWithDetails =
    fromEntityIgnoringGraphAndValidationResult(details).withScenarioGraphAndValidationResult(
      details.json
    )

  def fromEntityWithScenarioGraph(
      details: ScenarioWithDetailsEntity[DisplayableProcess]
  ): ScenarioWithDetails =
    fromEntityIgnoringGraphAndValidationResult(details).withScenarioGraphAndValidationResult(
      ValidatedDisplayableProcess.withEmptyValidationResult(details.json)
    )

  def fromEntityIgnoringGraphAndValidationResult(
      details: ScenarioWithDetailsEntity[_]
  ): ScenarioWithDetails = {
    ScenarioWithDetails(
      name = details.name,
      processId = Some(details.processId),
      processVersionId = details.processVersionId,
      isLatestVersion = details.isLatestVersion,
      description = details.description,
      isArchived = details.isArchived,
      isFragment = details.isFragment,
      processingType = details.processingType,
      processCategory = details.processCategory,
      modificationDate = details.modificationDate,
      modifiedAt = details.modifiedAt,
      modifiedBy = details.modifiedBy,
      createdAt = details.createdAt,
      createdBy = details.createdBy,
      tags = details.tags,
      lastDeployedAction = details.lastDeployedAction,
      lastStateAction = details.lastStateAction,
      lastAction = details.lastAction,
      json = None,
      history = details.history,
      modelVersion = details.modelVersion,
      state = None
    )
  }

  implicit class Ops(scenarioWithDetails: ScenarioWithDetails) {

    // TODO: Instead of doing these conversions below, wee should pass around ScenarioWithDetails
    def toEntity: ScenarioWithDetailsEntity[Unit] = {
      toEntity(())
    }

    def toEntityWithScenarioGraphUnsafe: ScenarioWithDetailsEntity[DisplayableProcess] = {
      toEntity(scenarioWithDetails.scenarioGraphAndValidationResultUnsafe.toDisplayable(scenarioWithDetails.name))
    }

    private def toEntity[T](prepareJson: => T): ScenarioWithDetailsEntity[T] = {
      ScenarioWithDetailsEntity(
        name = scenarioWithDetails.name,
        // We can't just use processIdUnsafe because it is used also for testMigrations which gets ScenarioWithDetails
        // via REST API so this information will be missing
        processId = scenarioWithDetails.processId.getOrElse(ProcessId(-1)),
        processVersionId = scenarioWithDetails.processVersionId,
        isLatestVersion = scenarioWithDetails.isLatestVersion,
        description = scenarioWithDetails.description,
        isArchived = scenarioWithDetails.isArchived,
        isFragment = scenarioWithDetails.isFragment,
        processingType = scenarioWithDetails.processingType,
        processCategory = scenarioWithDetails.processCategory,
        modificationDate = scenarioWithDetails.modificationDate,
        modifiedAt = scenarioWithDetails.modifiedAt,
        modifiedBy = scenarioWithDetails.modifiedBy,
        createdAt = scenarioWithDetails.createdAt,
        createdBy = scenarioWithDetails.createdBy,
        tags = scenarioWithDetails.tags,
        lastDeployedAction = scenarioWithDetails.lastDeployedAction,
        lastStateAction = scenarioWithDetails.lastStateAction,
        lastAction = scenarioWithDetails.lastAction,
        json = prepareJson,
        history = scenarioWithDetails.history,
        modelVersion = scenarioWithDetails.modelVersion
      )
    }

  }

}
