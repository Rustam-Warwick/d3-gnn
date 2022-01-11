import abc

from elements.element_feature import ElementFeature
from decorators import rpc
from abc import ABCMeta
import torch


class TensorFeatureMixin:
    @rpc
    def add(self, t):
        torch.add(self.value, t, out=self.value)
        return True


class TensorReplicableElementFeature(ElementFeature, TensorFeatureMixin):
    def __init__(self, value: torch.tensor = None, *args, **kwargs):
        if value is None: value = torch.zeros(1)
        super(TensorReplicableElementFeature, self).__init__(*args, value=value, **kwargs)

    def _eq(self, old_value, new_value) -> bool:
        return torch.equal(old_value, new_value)


class AggregatorReplicableElementFeature(ElementFeature, metaclass=ABCMeta):
    def __init__(self, value: torch.tensor = None, *args, **kwargs):
        if value is None: value = torch.zeros(1)
        super(AggregatorReplicableElementFeature, self).__init__(*args, value=value, **kwargs)

    @rpc
    @abc.abstractmethod
    def reduce(self, new_element: torch.tensor):
        pass

    @rpc
    @abc.abstractmethod
    def revert(self, deleted_tensor: torch.tensor):
        pass

    @rpc
    @abc.abstractmethod
    def replace(self, old_tensor: torch.tensor, new_tensor: torch.tensor):
        pass


class MeanAggregatorReplicableFeature(AggregatorReplicableElementFeature):
    def __init__(self, value: torch.tensor = None, *args, **kwargs):
        stored_value = (value, 0)  # Represents tensor and number of elements
        super(MeanAggregatorReplicableFeature, self).__init__(*args, value=stored_value, **kwargs)

    def reduce(self, new_element: torch.tensor):
        self._value[0] = ((self._value[0] * self._value[1]) + new_element) / (self._value[1] + 1)
        self._value[1] += 1
        return True

    def revert(self, deleted_tensor: torch.tensor):
        pass

    def replace(self, old_tensor: torch.tensor, new_tensor: torch.tensor):
        pass

    def _eq(self, old_value, new_value) -> bool:
        return torch.equal(old_value[0], new_value[0]) and old_value[1] == new_value[1]
