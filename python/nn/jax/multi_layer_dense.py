from flax.linen import Module, Dense, relu
from typing import Sequence, Literal


class MultiLayerDense(Module):
    features: Sequence[int]

    def setup(self):
        self.layers = [Dense(i) for i in self.features]

    def __call__(self, inputs):
        x = inputs
        for i, lyr in enumerate(self.layers):
            x = lyr(x)
            x = relu(x)
        return x
