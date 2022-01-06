from decorators import rpc
from elements.feature import Feature
import collections


class SetFeatureMixin:

    @rpc
    def add(self, element) -> bool:
        my_set: set = self.value
        is_changed = False
        if element not in my_set: is_changed = True
        my_set.add(element)
        return is_changed


    @rpc
    def remove(self, element) -> bool:
        my_set: set = self.value
        is_changed = False
        if element in is_changed: is_changed = True
        my_set.remove(element)
        return is_changed


class SetReplicableFeature(Feature, SetFeatureMixin):
    def __init__(self, value: set = None, *args, **kwargs):
        if value is None : value = set()
        super(SetReplicableFeature, self).__init__(*args, value=value, **kwargs)

    def _eq(self, old_value: set, new_value: set) -> bool:
        return collections.Counter(old_value) == collections.Counter(new_value)

class PartSetFeature(Feature, SetFeatureMixin):
    def __init__(self, value: set = None, *args, **kwargs):
        if not value: value = set()
        super(PartSetFeature, self).__init__(*args, value=value, **kwargs)

    def _eq(self, old_value: set, new_value: set) -> bool:
        return collections.Counter(old_value) == collections.Counter(new_value)

    def sync_replicas(self):
        """ This is exceptional PartSet Feature which syncs the parent GraphElement when it is modified
            Logic is that if part set is modifiied a new replica of self.element is added so it need a full sync
         """
        self.element.sync_replicas()
