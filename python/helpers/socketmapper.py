import os
from pyflink.datastream import StreamExecutionEnvironment, DataStream, MapFunction
from elements.vertex import SimpleVertex
from elements.edge import SimpleEdge
from elements import GraphQuery, Op


class EdgeListParser(MapFunction):
    def open(self, runtime_context: "RuntimeContext"):
        print(runtime_context.get_index_of_this_subtask())

    def map(self, value: str) -> GraphQuery:
        import logging
        values = value.split("\t")
        a = SimpleVertex(element_id=values[0])
        b = SimpleVertex(element_id=values[1])
        edge = SimpleEdge(src=a, dest=b)
        query = GraphQuery(Op.ADD, edge)
        logging.log(logging.INFO,query)
        return query
