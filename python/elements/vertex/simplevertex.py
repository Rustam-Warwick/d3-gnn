from elements.vertex import BaseVertex


class SimpleVertex(BaseVertex):
    def __init__(self, *args, **kwargs):
        super(SimpleVertex, self).__init__(args, kwargs)
