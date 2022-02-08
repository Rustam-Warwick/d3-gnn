from . import ReplicableFeature
from flax.core.frozen_dict import FrozenDict
from decorators import rpc
import jax


class JaxParamMixin:
    @rpc
    def update(self, grads):
        """ Subtract the new gradients from the current one """
        self._value = jax.tree_multimap(lambda x, y: jax.numpy.asarray(x - y), self._value, grads)
        return True


class JaxParamsFeature(ReplicableFeature, JaxParamMixin):
    """ Represents parameters of JAX Models a.k.a FrozenDict """
    def __init__(self, value: "FrozenDict" = None, *args, **kwargs):
        super(JaxParamsFeature, self).__init__(*args, value=value, **kwargs)

    def _value_eq_(self, old_value: "FrozenDict", new_value: "FrozenDict") -> bool:
        return False
