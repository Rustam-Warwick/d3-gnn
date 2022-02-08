import jax.numpy as jnp
from elements.element_feature import ReplicableFeature


class TensorFeatureMixin:
    pass


class TensorReplicableFeature(ReplicableFeature, TensorFeatureMixin):
    def __init__(self, value: "jnp.array" = None, *args, **kwargs):
        if value is None: value = jnp.zeros(1)
        super(TensorReplicableFeature, self).__init__(*args, value=value, **kwargs)

    def _value_eq_(self, old_value: "jnp.array", new_value: "jnp.array") -> bool:
        return jnp.array_equal(old_value, new_value)
