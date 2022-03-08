from pyflink.datastream import MapFunction, RuntimeContext
from elements.vertex import BaseVertex
from elements.edge import BaseEdge
from elements.element_feature.tensor_feature import VersionedTensorReplicableFeature, TensorReplicableFeature
from elements import GraphQuery, Op
import jax.numpy as jnp
from typing import Dict, List


class EdgeListParser(MapFunction):
    def __init__(self, categories: List[str], *args, **kwargs):
        super(EdgeListParser, self).__init__(*args, **kwargs)
        self.one_hot_encoding: Dict[str, "jnp.array"] = dict()
        eye = jnp.eye(len(categories))
        for i, category in enumerate(categories):
            self.one_hot_encoding[category] = eye[i]


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
            a = BaseVertex(element_id=values[0])
            b = BaseVertex(element_id=values[1])
            edge = BaseEdge(src=a, dest=b)
            query = GraphQuery(Op.ADDUPDATE, edge)
        except Exception:
            source_id = values[0]
            # category = self.one_hot_encoding[values[1]]
            vertex = BaseVertex(element_id=source_id)
            # vertex['feature'] = VersionedTensorReplicableFeature(value=category)
            query = GraphQuery(Op.ADDUPDATE, vertex)
        return query
