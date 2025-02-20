package pl.touk.nussknacker.ui.process.repository

import pl.touk.nussknacker.engine.canonicalgraph.CanonicalProcess
import pl.touk.nussknacker.engine.node.ComponentInfoExtractor
import pl.touk.nussknacker.engine.util.Implicits.RichTupleList
import pl.touk.nussknacker.restmodel.component.ScenarioComponentsUsages

object ScenarioComponentsUsagesHelper {

  def compute(scenario: CanonicalProcess): ScenarioComponentsUsages = {
    val usagesList = for {
      node          <- scenario.collectAllNodes
      componentInfo <- ComponentInfoExtractor.fromScenarioNode(node)
    } yield {
      (componentInfo, node.id)
    }
    val usagesMap = usagesList.toGroupedMap
    ScenarioComponentsUsages(usagesMap)
  }

}
