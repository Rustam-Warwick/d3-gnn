import abc

from elements.element_feature import ReplicableFeature
from decorators import rpc
from typing import Tuple, List
import torch


class TensorFeatureMixin:
    @rpc
    def add(self, t):
        torch.add(self.value, t, out=self.value)
        return True


class AggregatorFeatureMixin:
    """ Base class for all feature aggregators """

    @abc.abstractmethod
    def reduce(self, new_element: object):
        """ Add New Element to the aggregator """
        pass

    @abc.abstractmethod
    def bulk_reduce(self, *new_elements: List[torch.tensor]):
        """ Given list of elements reduce them all at once if possible, so RPC is sent only once"""
        pass

    @abc.abstractmethod
    def revert(self, deleted_tensor: object):
        """ Given tensor remove it from the aggregation """
        pass

    @abc.abstractmethod
    def replace(self, new_tensor: object, old_tensor: object):
        """ Given old value and new value of the tensor update the aggregator on the fly """
        pass

    @abc.abstractmethod
    def bulk_replace(self, *elements: List[Tuple[torch.tensor, torch.tensor]]):
        pass


class TensorReplicableFeature(ReplicableFeature, TensorFeatureMixin):
    def __init__(self, value: torch.tensor = None, *args, **kwargs):
        if value is None: value = torch.zeros(1)
        super(TensorReplicableFeature, self).__init__(*args, value=value, **kwargs)

    def _value_eq_(self, old_value, new_value) -> bool:
        return torch.equal(old_value, new_value)


class MeanAggregatorReplicableFeature(ReplicableFeature, AggregatorFeatureMixin):
    def __init__(self, value: Tuple[torch.tensor, int] = None, tensor: torch.tensor = None, *args, **kwargs):
        if value:
            # If value exists use it directly
            super(MeanAggregatorReplicableFeature, self).__init__(*args, value=value, **kwargs)
            return

        # Otherwise, append 0 to the tensor
        stored_value = [tensor, 0, dict()]  # Represents (tensor, number_of_elements, parameters for training)
        super(MeanAggregatorReplicableFeature, self).__init__(*args, value=stored_value, **kwargs)

    @rpc
    def reduce(self, new_element: torch.tensor, count=1):
        """ Supports Bulk Reduce with 1 sync. step """
        self.value[0] = ((self.value[0] * self.value[1]) + new_element) / (self.value[1] + count)
        self.value[1] += count
        return True

    def bulk_reduce(self, *new_elements: Tuple[torch.tensor]):
        if len(new_elements) == 0:
            return
        my_sum = 0
        for el in new_elements:
            my_sum += el
        self.reduce(my_sum, len(new_elements))  # This one is the RPC Call

    @rpc
    def revert(self, deleted_tensor: torch.tensor):
        pass

    @rpc
    def replace(self, new_tensor: torch.tensor, old_tensor: torch.tensor):
        self.value[0] += (new_tensor - old_tensor) / self.value[1]
        return True

    def bulk_replace(self, *elements: List[Tuple[torch.tensor, torch.tensor]]):
        if len(elements) == 0:
            return
        common_new_tensor, common_old_tensor = 0, 0
        for new_tensor, old_tensor in elements:
            common_new_tensor += new_tensor
            common_old_tensor += old_tensor
        self.replace(common_new_tensor, common_old_tensor)

    def _value_eq_(self, old_value: Tuple[torch.tensor, int], new_value: Tuple[torch.tensor, int]) -> bool:
        return torch.equal(old_value[0], new_value[0]) and old_value[1] == new_value[1]
