from flax.linen import Module, Dense, relu, softmax
from typing import Sequence, Literal



class MultiLayerDense(Module):
    features: Sequence[int]
    activations: Sequence[str]

    def setup(self):
        self.layers = [Dense(i) for i in self.features]

    def __call__(self, inputs):
        x = inputs
        for i, lyr in enumerate(self.layers):
            x = lyr(x)
            if self.activations[i]:
                x = self.activations[i](x)
        return x
