from elements.feature import Feature
from decorators import wrap_to_rpc
import torch


class TensorFeatureMixin:
    pass


class TensorReplicableFeature(Feature, TensorFeatureMixin):
    def __init__(self, value: torch.tensor = None, *args, **kwargs):
        if value is None: value = torch.zeros(1)
        super(TensorReplicableFeature, self).__init__(*args, value=value, **kwargs)
