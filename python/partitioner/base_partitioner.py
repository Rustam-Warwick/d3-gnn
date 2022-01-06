import abc

from pyflink.datastream import MapFunction, Partitioner as Pr, KeySelector as Ky
from typing import TYPE_CHECKING, Any
from abc import ABCMeta

if TYPE_CHECKING:
    from elements import GraphQuery


class BasePartitioner(MapFunction, metaclass=ABCMeta):
    def __init__(self, partitions=3, *args, **kwargs):
        super(BasePartitioner, self).__init__(*args, **kwargs)
        self.partitions = partitions

    @abc.abstractmethod
    def is_parallel(self):
        return False


class Partitioner(Pr):
    def partition(self, key: Any, num_partitions: int) -> int:
        return key


class KeySelector(Ky):
    def get_key(self, value: "GraphQuery"):
        return value.part
