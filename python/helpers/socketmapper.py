from pyflink.datastream import MapFunction
from elements.vertex import SimpleVertex
from elements.edge import SimpleEdge
from elements.element_feature.tensor_feature import TensorReplicableFeature
from elements import GraphQuery, Op
import torch
from typing import Dict,List


class EdgeListParser(MapFunction):
    def __init__(self, categories: List[str], *args, **kwargs):
        super(EdgeListParser, self).__init__(*args, **kwargs)
        self.one_hot_encoding:Dict[str, torch.tensor] = dict()
        eye = torch.eye(len(categories))
        for i, category in enumerate(categories):
            self.one_hot_encoding[category] = eye[i]


    def open(self, runtime_context: "RuntimeContext"):
        print(runtime_context.get_index_of_this_subtask())

    def map(self, value: str) -> GraphQuery:
        """
            Map Strings to GraphQueries
            Expects a list of <int,int> -> Edge
            or a list of  <int, string> -> Node Category Feature
        """
        values = value.split("\t")
        try:
            int(values[0])
            int(values[1])
            a = SimpleVertex(element_id=values[0])
            b = SimpleVertex(element_id=values[1])
            edge = SimpleEdge(src=a, dest=b)
            query = GraphQuery(Op.ADD, edge)
        except Exception:
            source_id = values[0]
            category = self.one_hot_encoding[values[1]]
            a = SimpleVertex(element_id=source_id)
            a['feature'] = TensorReplicableFeature(value=category)
            query = GraphQuery(Op.ADD, a['feature'])
        return query
