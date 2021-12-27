from elements.feature import ReplicableFeature
from decorators import wrap_to_rpc


class SetFeatureMixin:
    @wrap_to_rpc
    def add(self, element):
        pass

    def _add(self, element) -> bool:
        my_set: set = self.value
        is_changed = False
        if element not in my_set: is_changed = True
        my_set.add(element)
        return is_changed

    @wrap_to_rpc
    def remove(self, element):
        pass

    def _remove(self, element) -> bool:
        my_set: set = self.value
        is_changed = False
        if element in is_changed: is_changed = True
        my_set.remove(element)
        return is_changed


class SetReplicableFeature(ReplicableFeature, SetFeatureMixin):
    def __init__(self, value: set = None, *args, **kwargs):
        if not value: value = set()
        super(SetReplicableFeature, self).__init__(*args, value=value, **kwargs)
