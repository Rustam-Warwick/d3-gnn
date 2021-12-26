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
        [x.pre_add_storage_callback(storage) for x in self.__get_child_elements]

    def post_add_storage_callback(self, storage: "BaseStorage"):
        [x.post_add_storage_callback(storage) for x in self.__get_child_elements]
        pass

    @property
    @abc.abstractmethod
    def element_type(self) -> ElementTypes:
        pass

    @property
    def is_replicable(self) -> bool:
        return False

    def __setstate__(self, state: dict):
        if "storage" not in state: state['storage'] = None
        for i in state.values():
            #  Add element to feature
            if isinstance(i, GraphElement) and i.element_type == ElementTypes.FEATURE:
                i.element = self
        self.__dict__.update(state)

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['storage']
        return state


    @property
    def __get_child_elements(self):
        from elements.feature import Feature
        a = list()
        if isinstance(self, Feature): return a  # Stop at Feature since circular reference otherwise
        for i in self.__dict__.values():
            if isinstance(i, GraphElement):
                # If this feature if GraphElement and there is no circular reference add
                a.append(i)
        return a
