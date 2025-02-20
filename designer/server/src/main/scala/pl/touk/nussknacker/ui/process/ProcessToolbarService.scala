package pl.touk.nussknacker.ui.process

import com.typesafe.config.Config
import io.circe.generic.JsonCodec
import pl.touk.nussknacker.engine.util.UriUtils
import pl.touk.nussknacker.ui.config.processtoolbar.ToolbarButtonConfigType.ToolbarButtonType
import pl.touk.nussknacker.ui.config.processtoolbar.ToolbarButtonsConfigVariant.ToolbarButtonVariant
import pl.touk.nussknacker.ui.config.processtoolbar.ToolbarPanelTypeConfig.ToolbarPanelType
import pl.touk.nussknacker.ui.config.processtoolbar._
import pl.touk.nussknacker.ui.process.repository.ScenarioWithDetailsEntity

trait ProcessToolbarService {
  def getProcessToolbarSettings(process: ScenarioWithDetailsEntity[_]): ProcessToolbarSettings
}

class ConfigProcessToolbarService(config: Config, getCategories: () => List[String]) extends ProcessToolbarService {

  private val categoriesProcessToolbarConfig: Map[String, ProcessToolbarsConfig] =
    getCategories()
      .map(category => category -> ProcessToolbarsConfigProvider.create(config, Some(category)))
      .toMap

  override def getProcessToolbarSettings(process: ScenarioWithDetailsEntity[_]): ProcessToolbarSettings = {
    val toolbarConfig = categoriesProcessToolbarConfig.getOrElse(
      process.processCategory,
      throw new IllegalArgumentException(
        s"Try to get scenario toolbar settings for not existing category: ${process.processCategory}. Available categories: ${categoriesProcessToolbarConfig.keys
            .mkString(",")}."
      )
    )

    ProcessToolbarSettings.fromConfig(toolbarConfig, process)
  }

}

object ProcessToolbarSettings {

  import ToolbarHelper._

  def fromConfig(
      processToolbarConfig: ProcessToolbarsConfig,
      process: ScenarioWithDetailsEntity[_]
  ): ProcessToolbarSettings =
    ProcessToolbarSettings(
      createProcessToolbarId(processToolbarConfig, process),
      processToolbarConfig.topLeft
        .filterNot(tp => verifyCondition(tp.hidden, process))
        .map(tp => ToolbarPanel.fromConfig(tp, process)),
      processToolbarConfig.bottomLeft
        .filterNot(tp => verifyCondition(tp.hidden, process))
        .map(tp => ToolbarPanel.fromConfig(tp, process)),
      processToolbarConfig.topRight
        .filterNot(tp => verifyCondition(tp.hidden, process))
        .map(tp => ToolbarPanel.fromConfig(tp, process)),
      processToolbarConfig.bottomRight
        .filterNot(tp => verifyCondition(tp.hidden, process))
        .map(tp => ToolbarPanel.fromConfig(tp, process))
    )

}

@JsonCodec
final case class ProcessToolbarSettings(
    id: String,
    topLeft: List[ToolbarPanel],
    bottomLeft: List[ToolbarPanel],
    topRight: List[ToolbarPanel],
    bottomRight: List[ToolbarPanel]
)

object ToolbarPanel {

  import ToolbarHelper._

  def apply(
      `type`: ToolbarPanelType,
      title: Option[String],
      buttonsVariant: Option[ToolbarButtonVariant],
      buttons: Option[List[ToolbarButton]]
  ): ToolbarPanel =
    ToolbarPanel(`type`.toString, title, buttonsVariant, buttons)

  def fromConfig(config: ToolbarPanelConfig, process: ScenarioWithDetailsEntity[_]): ToolbarPanel =
    ToolbarPanel(
      config.identity,
      config.title.map(t => fillByProcessData(t, process)),
      config.buttonsVariant,
      config.buttons.map(buttons =>
        buttons
          .filterNot(button => {
            verifyCondition(button.hidden, process)
          })
          .map(button => ToolbarButton.fromConfig(button, process))
      )
    )

}

@JsonCodec
final case class ToolbarPanel(
    id: String,
    title: Option[String],
    buttonsVariant: Option[ToolbarButtonVariant],
    buttons: Option[List[ToolbarButton]]
)

object ToolbarButton {

  import ToolbarHelper._

  def fromConfig(config: ToolbarButtonConfig, process: ScenarioWithDetailsEntity[_]): ToolbarButton = ToolbarButton(
    config.`type`,
    config.name.map(t => fillByProcessData(t, process)),
    config.title.map(t => fillByProcessData(t, process)),
    config.icon.map(i => fillByProcessData(i, process, urlOption = true)),
    config.url.map(th => fillByProcessData(th, process, urlOption = true)),
    disabled = verifyCondition(config.disabled, process)
  )

}

@JsonCodec
final case class ToolbarButton(
    `type`: ToolbarButtonType,
    name: Option[String],
    title: Option[String],
    icon: Option[String],
    url: Option[String],
    disabled: Boolean
)

private[process] object ToolbarHelper {

  def createProcessToolbarId(config: ProcessToolbarsConfig, process: ScenarioWithDetailsEntity[_]): String =
    s"${config.uuidCode}-${if (process.isArchived) "archived" else "not-archived"}-${if (process.isFragment) "fragment"
      else "scenario"}"

  def fillByProcessData(text: String, process: ScenarioWithDetailsEntity[_], urlOption: Boolean = false): String = {
    val processName = if (urlOption) UriUtils.encodeURIComponent(process.name.value) else process.name.value

    text
      .replace("$processName", processName)
      .replace("$processId", process.processId.value.toString)
  }

  def verifyCondition(condition: Option[ToolbarCondition], process: ScenarioWithDetailsEntity[_]): Boolean = {
    condition.nonEmpty && condition.exists(con => {
      if (con.shouldMatchAllOfConditions) {
        verifyFragmentCondition(con, process) && verifyArchivedCondition(con, process)
      } else {
        verifyFragmentCondition(con, process) || verifyArchivedCondition(con, process)
      }
    })
  }

  private def verifyFragmentCondition(condition: ToolbarCondition, process: ScenarioWithDetailsEntity[_]) =
    verifyCondition(process.isFragment, condition.fragment, condition.shouldMatchAllOfConditions)

  private def verifyArchivedCondition(condition: ToolbarCondition, process: ScenarioWithDetailsEntity[_]) =
    verifyCondition(process.isArchived, condition.archived, condition.shouldMatchAllOfConditions)

  // When we should match all conditions and expected condition is empty (not set) then we ignore this condition
  private def verifyCondition(
      toVerify: Boolean,
      expected: Option[Boolean],
      shouldMatchAllOfConditions: Boolean
  ): Boolean =
    (shouldMatchAllOfConditions && expected.isEmpty) || expected.contains(toVerify)

}
