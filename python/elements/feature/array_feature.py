from elements.feature import ReplicableFeature
from decorators import wrap_to_rpc


class ArrayFeatureMixin:

    @wrap_to_rpc
    def append(self, element):
        pass

    def _append(self, element):
        self.value.append(element)


class ArrayReplicableFeature(ReplicableFeature, ArrayFeatureMixin):
    def __init__(self, value: list = list(), *args, **kwargs):
        super(ArrayReplicableFeature, self).__init__(*args, value=value, **kwargs)
