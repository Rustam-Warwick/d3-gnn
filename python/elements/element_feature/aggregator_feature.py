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
    def reduce(self, new_element: object, count=1, part_id=None):
        """ Add New Element to the aggregator """
        pass

    @abc.abstractmethod
    def bulk_reduce(self, *new_elements: List[jnp.array], part_id):
        """ Given list of elements reduce them all at once if possible, so RPC is sent only once"""
        pass

    @abc.abstractmethod
    def revert(self, deleted_tensor: object, part_id):
        """ Given tensor remove it from the aggregation """
        pass

    @abc.abstractmethod
    def replace(self, new_tensor: object, old_tensor: object, part_id):
        """ Given old value and new value of the tensor update the aggregator on the fly """
        pass

    @abc.abstractmethod
    def bulk_replace(self, *elements: List[Tuple[jnp.array, jnp.array]], part_id):
        pass

    @abc.abstractmethod
    def grad(self, grad_agg: jnp.array, part_id):
        """ Given dloss/dagg output dloss/dmsg. This usually does not change """
        pass

    @abc.abstractmethod
    def is_ready(self) -> bool:
        pass

    @abc.abstractmethod
    def reset(self):
        pass


class JACMeanAggregatorReplicableFeature(ReplicableFeature, AggregatorFeatureMixin):
    def __init__(self, value: Tuple[jnp.array, int, set] = None, tensor: "jnp.array" = None, *args, **kwargs):
        if value:
            # If value exists use it directly
            super(JACMeanAggregatorReplicableFeature, self).__init__(*args, value=value, **kwargs)
            return

        # Otherwise, append 0 to the tensor
        stored_value = [tensor, 0, dict()]  # Represents (tensor, number_of_elements)
        super(JACMeanAggregatorReplicableFeature, self).__init__(*args, value=stored_value, **kwargs)

    @rpc()
    def reduce(self, new_element: "jnp.array", part_id, part_version, count=1):
        """ Supports Bulk Reduce with 1 sync. step """
        was_ready = self.is_ready
        self._value[2][part_id] = part_version
        is_ready = self.is_ready
        if was_ready and not is_ready:
            # New versions are coming in reset the values
            self.reset()

        self._value[0] = ((self._value[0] * self._value[1]) + new_element) / (self._value[1] + count)
        self._value[1] += count

        return True

    def bulk_reduce(self, *new_elements: List[jnp.array]):
        if len(new_elements) == 0:
            return

        stacked_aggregations = jnp.vstack(new_elements)
        summed_aggregations = jnp.sum(stacked_aggregations, axis=0)
        self.reduce(new_element=summed_aggregations, count=len(new_elements))  # This one is the RPC Call

    @rpc()
    def revert(self, deleted_tensor: "jnp.array", part_id, part_version):
        pass

    @rpc()
    def replace(self, new_tensor: jnp.array, old_tensor: jnp.array, part_id, part_version):
        self._value[2].add(part_id)
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

    @property
    def is_ready(self) -> bool:
        """
            All accumulated messages should be from same version of this storage part
        """
        # max_seen_version = self._value[2].get(self.part_id, self.storage.part_version)
        seen_versions = self._value[2].values()
        min_seen = min([self.storage.part_version, *seen_versions])
        max_seen = max([self.storage.part_version, *seen_versions])
        return min_seen == max_seen

    def reset(self):
        self._value[0] = jax.numpy.zeros_like(self._value[0])
        self._value[1] = 0

    def _value_eq_(self, old_value: Tuple[jnp.array, int, set], new_value: Tuple[jnp.array, int, set]) -> bool:
        return jnp.array_equal(old_value[0], new_value[0]) and old_value[1] == new_value[1] and old_value[2] == \
               new_value[2]

    def grad(self, grad_agg: jnp.array):
        if self._value[1] == 0:
            return jax.numpy.zeros_like(grad_agg)
        return grad_agg / self._value[1]
