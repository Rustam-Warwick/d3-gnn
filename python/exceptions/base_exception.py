class NotSupported(BaseException):
    pass


class GraphElementNotFound(BaseException):
    pass


class OldVersionException(BaseException):
    pass


class NotUsedOnReplicaException(BaseException):
    pass


class AggregatorExistsException(BaseException):
    pass