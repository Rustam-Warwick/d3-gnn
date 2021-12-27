from elements.feature import ReplicableFeature
from decorators import wrap_to_rpc


class ArrayFeatureMixin:

    @wrap_to_rpc
    def append(self, element):
        pass

    def _append(self, element) -> bool:
        is_updated = False
        if element in self.value: ret = True
        self.value.append(element)
        return is_updated

class ArrayReplicableFeature(ReplicableFeature, ArrayFeatureMixin):
    def __init__(self, value: list = None, *args, **kwargs):
        if not value: value = list()
        super(ArrayReplicableFeature, self).__init__(*args, value=value, **kwargs)
