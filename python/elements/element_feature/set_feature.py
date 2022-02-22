from decorators import rpc
from elements.element_feature import ReplicableFeature
import collections


class SetFeatureMixin:

    @rpc()
    def add(self, element, part_id, part_version) -> bool:
        my_set: set = self.value
        is_changed = False
        if element not in my_set: is_changed = True
        my_set.add(element)
        return is_changed

    @rpc()
    def remove(self, element, part_id=None) -> bool:
        my_set: set = self.value
        is_changed = False
        if element in is_changed: is_changed = True
        my_set.remove(element)
        return is_changed


class SetReplicatedFeature(ReplicableFeature, SetFeatureMixin):
    """ RPC Wrapper for python built-in set classes  """
    def __init__(self, value: set = None, *args, **kwargs):
        if value is None: value = set()
        super(SetReplicatedFeature, self).__init__(*args, value=value, **kwargs)

    def _value_eq_(self, old_value: set, new_value: set) -> bool:
        return collections.Counter(old_value) == collections.Counter(new_value)