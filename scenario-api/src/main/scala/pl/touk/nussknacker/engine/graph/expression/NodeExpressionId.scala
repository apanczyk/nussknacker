package pl.touk.nussknacker.engine.graph.expression

import pl.touk.nussknacker.engine.api.NodeId

case class NodeExpressionId(nodeId: NodeId, expressionId: String)

object NodeExpressionId {

  def apply(expressionId: String)(implicit nodeId: NodeId): NodeExpressionId =
    NodeExpressionId(nodeId, expressionId)

  val DefaultExpressionId: String = "$expression"

  def branchParameterExpressionId(paramName: String, branch: String): String =
    paramName + "-" + branch

}
