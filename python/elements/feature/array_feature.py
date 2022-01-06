from elements.feature import Feature
from decorators import rpc


class ArrayFeatureMixin:



    @rpc
    def append(self, element) -> bool:
        is_updated = False
        if element in self.value: ret = True
        self.value.append(element)
        return is_updated


class ArrayReplicableFeature(Feature, ArrayFeatureMixin):
    def __init__(self, value: list = None, *args, **kwargs):
        if not value: value = list()
        super(ArrayReplicableFeature, self).__init__(*args, value=value, **kwargs)

    def _eq(self, old_value: list, new_value: list) -> bool:
        return old_value == new_value
