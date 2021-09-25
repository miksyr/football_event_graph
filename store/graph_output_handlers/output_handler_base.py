from typing import Any, Dict, List

from datamodel.node_field import NodeField
from datamodel.node_ids import BaseNodeId
from datamodel.node_labels import NodeLabel
from datamodel.relations import BaseRelationType


class RelationOutputHandlerBase:
    def add(
        self,
        startNodeId: BaseNodeId,
        endNodeId: BaseNodeId,
        relationType: BaseRelationType,
    ) -> None:
        raise NotImplementedError(
            "Can't use RelationOutputHandlerBase as an output handler"
        )

    def close(self) -> None:
        raise NotImplementedError(
            "Can't use RelationOutputHandlerBase as an output handler"
        )


class NodeOutputHandlerBase:
    def add(
        self,
        nodeId: BaseNodeId,
        nodeLabels: List[NodeLabel],
        nodeProperties: Dict[NodeField, Any],
    ) -> None:
        raise NotImplementedError(
            "Can't use NodeOutputHandlerBase as an output handler"
        )

    def close(self) -> None:
        raise NotImplementedError(
            "Can't use NodeOutputHandlerBase as an output handler"
        )
