from elements.feature import ReplicableFeature
from decorators import wrap_to_rpc
import torch


class TensorFeatureMixin:
    pass


class TensorReplicableFeature(ReplicableFeature, TensorFeatureMixin):
    def __init__(self, value: torch.tensor = None, *args, **kwargs):
        if not value: value = torch.zeros((1))
        super(TensorReplicableFeature, self).__init__(*args, value=value, **kwargs)
