from pyflink.datastream import MapFunction
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from elements import GraphQuery

class BasePartitioner(MapFunction):
    pass
