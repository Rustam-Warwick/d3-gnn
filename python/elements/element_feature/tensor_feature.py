import abc
import jax.numpy as jnp
from elements.element_feature import ReplicableFeature
from decorators import rpc
from typing import Tuple, List


class TensorFeatureMixin:
   pass


class AggregatorFeatureMixin:
    """ Base class for all feature aggregators """

    @abc.abstractmethod
    def reduce(self, new_element: object):
        """ Add New Element to the aggregator """
        pass

    @abc.abstractmethod
    def bulk_reduce(self, *new_elements: List[jnp.array]):
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
    def bulk_replace(self, *elements: List[Tuple[jnp.array, jnp.array]]):
        pass


class TensorReplicableFeature(ReplicableFeature, TensorFeatureMixin):
    def __init__(self, value: "jnp.array" = None, *args, **kwargs):
        if value is None: value = jnp.zeros(1)
        super(TensorReplicableFeature, self).__init__(*args, value=value, **kwargs)

    def _value_eq_(self, old_value: "jnp.array", new_value: "jnp.array") -> bool:
        return jnp.array_equal(old_value, new_value)


class MeanAggregatorReplicableFeature(ReplicableFeature, AggregatorFeatureMixin):
    def __init__(self, value: Tuple[jnp.array, int] = None, tensor: "jnp.array" = None, *args, **kwargs):
        if value:
            # If value exists use it directly
            super(MeanAggregatorReplicableFeature, self).__init__(*args, value=value, **kwargs)
            return

        # Otherwise, append 0 to the tensor
        stored_value = [tensor, 0]  # Represents (tensor, number_of_elements)
        super(MeanAggregatorReplicableFeature, self).__init__(*args, value=stored_value, **kwargs)

    @rpc
    def reduce(self, new_element: "jnp.array", count=1):
        """ Supports Bulk Reduce with 1 sync. step """
        self.value[0] = ((self.value[0] * self.value[1]) + new_element) / (self.value[1] + count)
        self.value[1] += count
        return True

    def bulk_reduce(self, *new_elements: Tuple[jnp.array]):
        if len(new_elements) == 0:
            return
        my_sum = 0
        for el in new_elements:
            my_sum += el
        self.reduce(my_sum, len(new_elements))  # This one is the RPC Call

    @rpc
    def revert(self, deleted_tensor: "jnp.array"):
        pass

    @rpc
    def replace(self, new_tensor: jnp.array, old_tensor: jnp.array):

        self.value[0] += (new_tensor - old_tensor) / self.value[1]
        return True

    def bulk_replace(self, *elements: List[Tuple[jnp.array, jnp.array]]):
        if len(elements) == 0:
            return
        common_new_tensor, common_old_tensor = 0, 0
        for new_tensor, old_tensor in elements:
            common_new_tensor += new_tensor
            common_old_tensor += old_tensor
        self.replace(common_new_tensor, common_old_tensor) # This one is the RPC call

    def _value_eq_(self, old_value: Tuple[jnp.array, int], new_value: Tuple[jnp.array, int]) -> bool:
        return jnp.array_equal(old_value[0], new_value[0]) and old_value[1] == new_value[1]
