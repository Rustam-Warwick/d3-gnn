from . import ReplicableFeature
from flax.core.frozen_dict import FrozenDict
from decorators import rmi
import jax


class JaxParamMixin:
    @rmi()
    def update(self, grads, part_id, part_version):
        """ Subtract the new gradients from the current one """
        self._value = jax.tree_multimap(lambda x, y: jax.numpy.asarray(x - y), self._value, grads)
        return True

    def batch_update(self, *grads):
        """ Update given a list of grads """
        if len(grads) == 0:
            return
        sum_grads = jax.tree_multimap(lambda *x: jax.numpy.sum(jax.numpy.vstack(x), axis=0), *grads)

        return self.update(sum_grads)


class JaxParamsFeature(ReplicableFeature, JaxParamMixin):
    """ Represents parameters of JAX Models a.k.a FrozenDict """

    def __init__(self, value: "FrozenDict" = None, *args, **kwargs):
        super(JaxParamsFeature, self).__init__(*args, value=value, **kwargs)

    def equals(self, old_value: "FrozenDict", new_value: "FrozenDict") -> bool:
        """ This is the actual compare function """
        this_leaves = jax.tree_leaves(old_value)
        old_leaves = jax.tree_leaves(new_value)
        for i, arr in enumerate(this_leaves):
            if not jax.numpy.array_equal(arr, old_leaves):
                return False

        return True

    def _value_eq_(self, old_value: "FrozenDict", new_value: "FrozenDict") -> bool:
        """ Update should always happen to trigger the model version update """
        return False
