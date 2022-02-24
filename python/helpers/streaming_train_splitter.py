from pyflink.datastream import MapFunction
from elements import GraphQuery, Op, ElementTypes
from random import random


class StreamingTrainSplitter(MapFunction):
    """ Map Function that splits the stream of some Op types to train and test set
        based on the pre-defined renadom paramenter \lambda
    """

    def __init__(self, l=0.2):
        self.l = l

    def map(self, value: "GraphQuery") -> "GraphQuery":
        if value.op is Op.ADDUPDATE and value.element.element_type is ElementTypes.VERTEX:
            n = random()
            if n < self.l:
                # Interpret this as training data
                value.op = Op.AGG
                value.aggregator_name = '3trainer'
        return value
