from elements.feature import Feature
from decorators import rpc
import torch


class TensorFeatureMixin:

    @rpc
    def add(self, t):
        torch.add(self.value, t, out=self.value)
        return True


class TensorReplicableFeature(Feature, TensorFeatureMixin):
    def __init__(self, value: torch.tensor = None, *args, **kwargs):
        if value is None: value = torch.zeros(1)
        super(TensorReplicableFeature, self).__init__(*args, value=value, **kwargs)

    def _eq(self, old_value, new_value) -> bool:
        return torch.equal(old_value, new_value)
