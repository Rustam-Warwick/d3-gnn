from elements.feature import ReplicableFeature


class ArrayFeatureMixin:
    def append(self, element):
        fn = lambda x: x.append(element)
        self(fn)


class ArrayReplicableFeature(ReplicableFeature, ArrayFeatureMixin):
    def __init__(self, value: list = list(), *args, **kwargs):
        super(ArrayReplicableFeature, self).__init__(*args, value=value, **kwargs)
