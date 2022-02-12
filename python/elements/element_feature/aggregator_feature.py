import abc
import jax.numpy as jnp
from elements.element_feature import ReplicableFeature
import jax
from decorators import rpc
from typing import Tuple, List, TYPE_CHECKING

if TYPE_CHECKING:
    from jax.experimental.maps import FrozenDict


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

    @abc.abstractmethod
    def grad(self, grad_agg: jnp.array):
        """ Given dloss/dagg output dloss/dmsg. This usually does not change """
        pass


class JACMeanAggregatorReplicableFeature(ReplicableFeature, AggregatorFeatureMixin):
    def __init__(self, value: Tuple[jnp.array, int, "FrozenDict"] = None, tensor: "jnp.array" = None, *args, **kwargs):
        if value:
            # If value exists use it directly
            super(JACMeanAggregatorReplicableFeature, self).__init__(*args, value=value, **kwargs)
            return

        # Otherwise, append 0 to the tensor
        stored_value = [tensor, 0]  # Represents (tensor, number_of_elements)
        super(JACMeanAggregatorReplicableFeature, self).__init__(*args, value=stored_value, **kwargs)

    def fn(self, elements: jnp.array):
        summed_aggregations = jnp.sum(elements, axis=0) / elements.shape[0]
        return summed_aggregations

    @rpc()
    def reduce(self, new_element: "jnp.array", count=1):
        """ Supports Bulk Reduce with 1 sync. step """
        if self._value[1] == 0:
            # First element addition
            self._value[1] += count
            self._value[0] = new_element
        else:
            # Subsequent element additions
            self._value[0] = ((self._value[0] * self._value[1]) + new_element) / (self._value[1] + count)
            self._value[1] += count

        return True

    def bulk_reduce(self, *new_elements: List[jnp.array]):
        if len(new_elements) == 0:
            return

        stacked_aggregations = jnp.vstack(new_elements)
        summed_aggregations = jnp.sum(stacked_aggregations, axis=0)
        self.reduce(summed_aggregations, len(new_elements))  # This one is the RPC Call

    @rpc()
    def revert(self, deleted_tensor: "jnp.array"):
        pass

    @rpc()
    def replace(self, new_tensor: jnp.array, old_tensor: jnp.array):
        if self._value[1] == 0:
            print("Something wrong replace came with empty aggregator")
            return False

        self._value[0] += (new_tensor - old_tensor) / self._value[1]
        return True

    def bulk_replace(self, *elements: List[Tuple[jnp.array]]):
        """ Input is [((new_array, new_jac), (old_array, old_jack)), ... ] """
        if len(elements) == 0:
            return
        elif len(elements) == 1:
            self.replace(*elements[0])
        else:
            new_array_sum = jnp.sum(jnp.vstack([x[0] for x in elements]), axis=0)
            old_array_sum = jnp.sum(jnp.vstack([x[1] for x in elements]), axis=0)
            self.replace(new_array_sum, old_array_sum)  # This one is the RPC call

    def _value_eq_(self, old_value: Tuple[jnp.array, int], new_value: Tuple[jnp.array, int]) -> bool:
        return jnp.array_equal(old_value[0], new_value[0]) and old_value[1] == new_value[1]

    def grad(self, grad_agg: jnp.array):
        return grad_agg / self._value[1]

#
# class MeanAggregatorReplicableFeature(ReplicableFeature, AggregatorFeatureMixin):
#     def __init__(self, value: Tuple[jnp.array, int] = None, tensor: "jnp.array" = None, *args, **kwargs):
#         if value:
#             # If value exists use it directly
#             super(MeanAggregatorReplicableFeature, self).__init__(*args, value=value, **kwargs)
#             return
#
#         # Otherwise, append 0 to the tensor
#         stored_value = [tensor, 0]  # Represents (tensor, number_of_elements)
#         super(MeanAggregatorReplicableFeature, self).__init__(*args, value=stored_value, **kwargs)
#
#     @rpc
#     def reduce(self, new_element: "jnp.array", count=1):
#         """ Supports Bulk Reduce with 1 sync. step """
#         self.value[0] = ((self.value[0] * self.value[1]) + new_element) / (self.value[1] + count)
#         self.value[1] += count
#         return True
#
#     def bulk_reduce(self, *new_elements: Tuple[jnp.array]):
#         if len(new_elements) == 0:
#             return
#         stacked = jnp.vstack(new_elements)
#         summed = jnp.sum(stacked, axis=0)
#         self.reduce(summed, len(new_elements))  # This one is the RPC Call
#
#     @rpc
#     def revert(self, deleted_tensor: "jnp.array"):
#         pass
#
#     @rpc
#     def replace(self, new_tensor: jnp.array, old_tensor: jnp.array):
#         if self.value[1] == 0:
#             print(new_tensor, old_tensor)
#             return True
#         self.value[0] += (new_tensor - old_tensor) / self.value[1]
#         return True
#
#     def bulk_replace(self, *elements: List[Tuple[jnp.array, jnp.array]]):
#         if len(elements) == 0:
#             return
#         new_tensors = list(map(lambda x: x[0], elements))
#         old_tensors = list(map(lambda x: x[1], elements))
#         stacked_new_tensors = jnp.vstack(new_tensors)
#         stacked_old_tensors = jnp.vstack(old_tensors)
#         summed_new = jnp.sum(stacked_new_tensors, axis=0)
#         summed_old = jnp.sum(stacked_old_tensors, axis=0)
#
#         self.replace(summed_new, summed_old)  # This one is the RPC call
#
#     def _value_eq_(self, old_value: Tuple[jnp.array, int], new_value: Tuple[jnp.array, int]) -> bool:
#         return jnp.array_equal(old_value[0], new_value[0]) and old_value[1] == new_value[1]
