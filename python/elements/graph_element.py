import abc
from abc import ABCMeta
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from storage import BaseStorage


class ElementTypes(Enum):
    VERTEX = 0
    EDGE = 1
    FEATURE = 2


class GraphElement(metaclass=ABCMeta):
    """GraphElement is the main parent class of all Vertex, Edge, Feature classes"""

    def __init__(self, element_id: str, part_id=None, storage: "BaseStorage" = None) -> None:
        self.id: str = element_id
        self.storage: "BaseStorage" = storage
        self.part_id: int = part_id

    def __eq__(self, other):
        return self.id == other.id

    def pre_add_storage_callback(self, storage: "BaseStorage"):
        self.storage = storage
        self.part_id = storage.part_id

    def post_add_storage_callback(self, storage: "BaseStorage"):
        pass

    @property
    @abc.abstractmethod
    def element_type(self) -> ElementTypes:
        pass

    @property
    def is_replicable(self) -> bool:
        return False
