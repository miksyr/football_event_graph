import csv
import gzip
import os

from datamodel.fields_and_labels import NodeField
from store.graph_output_handlers.output_handler_base import NodeOutputHandlerBase


class NodesFile(NodeOutputHandlerBase):

    def __init__(self, fileName):
        self.fileName = fileName
        open(f'{os.path.dirname(self.fileName)}/nodes.csv').write(','.join(NodeField.ALL))
        self.file = gzip.open(self.fileName, 'wt')
        self.csv = csv.DictWriter(self.file, fieldnames=NodeField.ALL, escapechar='\\', quotechar='"', quoting=csv.QUOTE_ALL)

    @staticmethod
    def _escape_backslashes(data):
        return {key: value.replace('\\', '\\\\') if isinstance(value, str) else value for key, value in data.items()}

    def add(self, nodeId, nodeLabels, nodeProperties):
        row = {NodeField.ID: str(nodeId), NodeField.LABEL: ', '.join((label for label in nodeLabels))}
        row.update(self._escape_backslashes(data=nodeProperties))
        self.csv.writerow(row)

    def close(self):
        self.file.close()
