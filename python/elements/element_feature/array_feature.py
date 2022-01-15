from elements.element_feature import ReplicableFeature
from decorators import rpc


class ArrayFeatureMixin:



    @rpc
    def append(self, element) -> bool:
        is_updated = False
        if element in self.value: ret = True
        self.value.append(element)
        return is_updated


class ArrayReplicableReplicableFeature(ReplicableFeature, ArrayFeatureMixin):
    def __init__(self, value: list = None, *args, **kwargs):
        if not value: value = list()
        super(ArrayReplicableReplicableFeature, self).__init__(*args, value=value, **kwargs)

    def _value_eq_(self, old_value: list, new_value: list) -> bool:
        return old_value == new_value
