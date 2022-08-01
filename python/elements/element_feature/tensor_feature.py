import jax.numpy as jnp
from elements.element_feature import ReplicableFeature
from typing import Tuple


class TensorFeatureMixin:
    pass


class TensorReplicableFeature(ReplicableFeature, TensorFeatureMixin):
    def __init__(self, value: "jnp.array" = None, *args, **kwargs):
        if value is None: value = jnp.zeros(1)
        super(TensorReplicableFeature, self).__init__(*args, value=value, **kwargs)

    def _value_eq_(self, old_value: "jnp.array", new_value: "jnp.array") -> bool:
        return jnp.array_equal(old_value, new_value)


class VersionedTensorReplicableFeature(ReplicableFeature, TensorFeatureMixin):
    def __init__(self, value: Tuple["jnp.array", int] = None, version=1, *args, **kwargs):
        self.version = version
        super(VersionedTensorReplicableFeature, self).__init__(*args, value=value, **kwargs)

    def _value_eq_(self, old_value: "jnp.array", new_value: "jnp.array") -> bool:
        return jnp.array_equal(old_value, new_value)

    @property
    def is_ready(self) -> bool:
        """ First layer does not have versioning requirements, hence it is always true """
        return True
        if self.storage.is_first:
            return True
        return self.storage.part_version == self.version

    def __deepcopy__(self, memodict={}):
        element = super(VersionedTensorReplicableFeature, self).__copy__()
        element.__dict__.update({
            "version": self.version
        })
        return element

    def __copy__(self):
        element = super(VersionedTensorReplicableFeature, self).__copy__()
        element.__dict__.update({
            "version": self.version

        })
        return element

    def __getstate__(self):
        """ Fill in from the state """
        state = super(VersionedTensorReplicableFeature, self).__getstate__()
        state.update({
            "version": self.version

        })
        return state
