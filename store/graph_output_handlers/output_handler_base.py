class RelationOutputHandlerBase:

    def add(self, startNodeId, endNodeId, relationType):
        raise NotImplementedError("Can't use RelationOutputHandlerBase as an output handler")

    def close(self):
        raise NotImplementedError("Can't use RelationOutputHandlerBase as an output handler")


class NodeOutputHandlerBase:

    def add(self, nodeId, nodeLabels, nodeProperties):
        raise NotImplementedError("Can't use NodeOutputHandlerBase as an output handler")

    def close(self):
        raise NotImplementedError("Can't use NodeOutputHandlerBase as an output handler")
