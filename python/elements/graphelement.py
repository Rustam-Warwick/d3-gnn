class GraphElement:
    """GraphElement is the main parent class of all Vertex, Edge, Feature classes"""

    def __init__(self, element_id: str, storage) -> None:
        self.id = element_id
        self.storage = storage

    def __init__(self, element_id: str) -> None:
        self.id = element_id
        self.storage = None

    def __eq__(self, other):
        return self.id == other.id
