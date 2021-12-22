from elements.graphelement import GraphElement


class BaseVertex(GraphElement):
    def __init__(self, *args, **kwargs):
        super(BaseVertex, self).__init__(args, kwargs)
