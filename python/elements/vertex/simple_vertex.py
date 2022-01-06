import torch

from elements.vertex import BaseVertex
from elements.feature.tensor_feature import TensorReplicableFeature
from torch import randint,zeros


class SimpleVertex(BaseVertex):
    def __init__(self, *args, **kwargs):
        super(SimpleVertex, self).__init__(*args, **kwargs)
        self.image = TensorReplicableFeature(value=randint(100, (16, 16),dtype=torch.float32,requires_grad=False), field_name="image", element=self)
        self.agg = TensorReplicableFeature(value=zeros((16, 16), dtype=torch.float32,requires_grad=False), field_name="agg", element=self)
