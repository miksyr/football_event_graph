import csv
import gzip
import os
from pathlib import Path
from typing import Union

from datamodel.node_ids import BaseNodeId
from datamodel.relations import BaseRelationType, Relation
from store.graph_output_handlers.output_handler_base import RelationOutputHandlerBase


class RelationsFile(RelationOutputHandlerBase):
    def __init__(self, fileName: Union[str, Path]):
        self.fileName = fileName
        open(f"{os.path.dirname(self.fileName)}/relations.csv", "w").write(
            ",".join(Relation.ALL)
        )
        self.file = gzip.open(self.fileName, "wt")
        self.csv = csv.DictWriter(
            self.file,
            fieldnames=Relation.ALL,
            escapechar="\\",
            quotechar='"',
            quoting=csv.QUOTE_ALL,
        )

    def add(
        self,
        startNodeId: BaseNodeId,
        endNodeId: BaseNodeId,
        relationType: BaseRelationType,
    ) -> None:
        self.csv.writerow(
            {
                Relation.START_ID: str(startNodeId),
                Relation.END_ID: str(endNodeId),
                Relation.TYPE: relationType,
            }
        )

    def close(self) -> None:
        self.file.close()
