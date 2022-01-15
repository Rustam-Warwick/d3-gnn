import os
from pyflink.datastream import StreamExecutionEnvironment, DataStream, MapFunction
from elements.vertex import SimpleVertex
from elements.edge import SimpleEdge
from elements.element_feature.tensor_feature import TensorReplicableFeature
from elements import GraphQuery, Op
import torch

class EdgeListParser(MapFunction):
    def open(self, runtime_context: "RuntimeContext"):
        print(runtime_context.get_index_of_this_subtask())

    def map(self, value: str) -> GraphQuery:
        values = value.split("\t")
        a = SimpleVertex(element_id=values[0])
        a['feature'] = TensorReplicableFeature(value=torch.randint(0, 100, (16, 16)))
        b = SimpleVertex(element_id=values[1])
        b['feature'] = TensorReplicableFeature(value=torch.randint(0, 100, (16, 16)))
        edge = SimpleEdge(src=a, dest=b)
        query = GraphQuery(Op.ADD, edge)
        return query
