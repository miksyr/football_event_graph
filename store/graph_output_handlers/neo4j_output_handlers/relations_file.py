import csv
import gzip
import os

from datamodel.relations import Relation
from store.graph_output_handlers.output_handler_base import RelationOutputHandlerBase


class RelationsFile(RelationOutputHandlerBase):

    def __init__(self, fileName):
        self.fileName = fileName
        open(f'{os.path.dirname(self.fileName)}/relations.csv', 'w').write(','.join(Relation.ALL))
        self.file = gzip.open(self.fileName, 'wt')
        self.csv = csv.DictWriter(self.file, fieldnames=Relation.ALL, escapechar='\\', quotechar='"', quoting=csv.QUOTE_ALL)

    def add(self, startNodeId, endNodeId, relationType):
        self.csv.writerow({Relation.START_ID: str(startNodeId), Relation.END_ID: str(endNodeId), Relation.TYPE: relationType})

    def close(self):
        self.file.close()
