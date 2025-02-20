package pl.touk.nussknacker.restmodel

import io.circe.{Decoder, Encoder}
import io.circe.generic.JsonCodec
import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import pl.touk.nussknacker.engine.api.component.ComponentType.ComponentType
import pl.touk.nussknacker.engine.api.component.{ComponentGroupName, ComponentInfo, SingleComponentConfig}
import pl.touk.nussknacker.engine.api.definition.ParameterEditor
import pl.touk.nussknacker.engine.api.deployment.CustomAction
import pl.touk.nussknacker.engine.api.typed.typing.TypingResult
import pl.touk.nussknacker.engine.graph.evaluatedparam.{Parameter => NodeParameter}
import pl.touk.nussknacker.engine.graph.expression.Expression
import pl.touk.nussknacker.engine.graph.node.NodeData
import pl.touk.nussknacker.engine.graph.EdgeType

import java.net.URI

package object definition {

  import pl.touk.nussknacker.engine.api.CirceUtil._

  @JsonCodec(encodeOnly = true) final case class UIProcessObjects(
      componentGroups: List[ComponentGroup],
      components: Map[ComponentInfo, UIComponentDefinition],
      classes: List[TypingResult],
      // TODO: remove it, use components field on the FE side instead
      componentsConfig: Map[String, SingleComponentConfig],
      scenarioPropertiesConfig: Map[String, UiScenarioPropertyConfig],
      edgesForNodes: List[NodeEdges],
      customActions: List[UICustomAction]
  )

  @JsonCodec(encodeOnly = true) final case class UIValueParameter(
      name: String,
      typ: TypingResult,
      expression: Expression
  )

  @JsonCodec(encodeOnly = true) final case class UIParameter(
      name: String,
      typ: TypingResult,
      editor: ParameterEditor,
      defaultValue: Expression,
      // additionalVariables and variablesToHide are served to FE because suggestions API requires full set of variables
      // and ScenarioWithDetails.json.validationResult.nodeResults is not enough
      additionalVariables: Map[String, TypingResult],
      variablesToHide: Set[String],
      // FE need this information because branch parameters aren't changed dynamically during node validation so they never
      // should be invalidated
      branchParam: Boolean,
      hintText: Option[String]
  )

  @JsonCodec(encodeOnly = true) final case class UIComponentDefinition(
      // These parameters are mostly used for method based, static components. For dynamic components, it is the last fallback
      // when scenario validation doesn't returned node results (e.g. when DisplayableProcess can't be translated to CanonicalProcess).
      // And node validation wasn't performed yet (e.g. just after node details modal open) or for branch parameters
      // which aren't handled dynamically. See getDynamicParameterDefinitions in selectors.tsx.
      parameters: List[UIParameter],
      // TODO: remove this field
      // We use it for two purposes:
      // 1. Because we have a special "Output variable name" parameter which is treated specially both in scenario format
      //    (see CustomNode.outputVar and Join.outputVar) and accordingly in the component definition
      //    We can easily move this parameter to normal parameters but in the join case, it will change the order parameters
      //    (it will be after branch parameters instead of before them)
      // 2. We have a heuristic that trying to figure out context of variables to pass to node validation and to suggestions
      //    (see. ProcessUtils.findAvailableVariables). This heuristic is used when DisplayableProcess can't be translated
      //    to CanonicalProcess. When we replace CanonicalProcess by DisplayableProcess, it won't be needed anymore
      returnType: Option[TypingResult],
      // For fragments only
      outputParameters: Option[List[String]]
  )

  @JsonCodec(encodeOnly = true) final case class UISourceParameters(sourceId: String, parameters: List[UIParameter])

  final case class NodeEdges(
      componentId: ComponentInfo,
      edges: List[EdgeType],
      canChooseNodes: Boolean,
      isForInputDefinition: Boolean
  )

  object NodeEdges {
    implicit val componentIdEncoder: Encoder[ComponentInfo] = Encoder.encodeString.contramap(_.toString)

    implicit val encoder: Encoder[NodeEdges] = deriveConfiguredEncoder
  }

  object ComponentNodeTemplate {

    def create(
        componentInfo: ComponentInfo,
        nodeTemplate: NodeData,
        categories: List[String],
        branchParametersTemplate: List[NodeParameter]
    ): ComponentNodeTemplate =
      ComponentNodeTemplate(
        componentInfo.`type`,
        componentInfo.name,
        nodeTemplate,
        categories,
        branchParametersTemplate
      )

  }

  @JsonCodec(encodeOnly = true) final case class ComponentNodeTemplate(
      // This field is used to generate unique key in DOM model on FE side (the label isn't unique)
      `type`: ComponentType,
      label: String,
      node: NodeData,
      // TODO: Remove it - it is not used on the FE code, only ComponentService use it and we can take it from
      //       the processingType property there
      categories: List[String],
      branchParametersTemplate: List[NodeParameter] = List.empty,
      // TODO: This field is added temporary to pick correct icon - we shouldn't use this class for other purposes than encoding to json
      isEnricher: Option[Boolean] = None
  ) {
    // TODO: This is temporary - we shouldn't use ComponentNodeTemplate class for other purposes than encoding to json
    def componentInfo: ComponentInfo = ComponentInfo(`type`, label)
  }

  @JsonCodec(encodeOnly = true) final case class ComponentGroup(
      name: ComponentGroupName,
      components: List[ComponentNodeTemplate]
  )

  @JsonCodec final case class UiScenarioPropertyConfig(
      defaultValue: Option[String],
      editor: ParameterEditor,
      label: Option[String]
  )

  object UIParameter {
    implicit def decoder(implicit typing: Decoder[TypingResult]): Decoder[UIParameter] =
      deriveConfiguredDecoder[UIParameter]
  }

  object UICustomAction {

    def apply(action: CustomAction): UICustomAction = UICustomAction(
      name = action.name,
      allowedStateStatusNames = action.allowedStateStatusNames,
      icon = action.icon,
      parameters = action.parameters.map(p => UICustomActionParameter(p.name, p.editor))
    )

  }

  @JsonCodec final case class UICustomAction(
      name: String,
      allowedStateStatusNames: List[String],
      icon: Option[URI],
      parameters: List[UICustomActionParameter]
  )

  @JsonCodec final case class UICustomActionParameter(name: String, editor: ParameterEditor)

}
