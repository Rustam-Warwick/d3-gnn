class NotSupported(Exception):
    pass


class GraphElementNotFound(Exception):
    pass


class OldVersionException(Exception):
    pass


class NotUsedOnReplicaException(Exception):
    pass


class AggregatorExistsException(Exception):
    pass


class FeatureExistsException(Exception):
    pass


class NestedFeaturesException(Exception):
    pass


class CreateElementFailException(Exception):
    pass
