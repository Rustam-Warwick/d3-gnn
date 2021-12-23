import abc
from abc import ABCMeta
from typing import TYPE_CHECKING
from elements import GraphElement
if TYPE_CHECKING:
    from elements import ElementTypes


class Feature(GraphElement, metaclass=ABCMeta):
    def __init__(self, field_name: str, element: "GraphElement", value: object, *args, **kwargs):
        feature_id = "%s:%s:%s" % (element.element_type.value, element.id, field_name)
        self.value = value
        self.element: "GraphElement" = element
        super(Feature, self).__init__(element_id=feature_id, *args, **kwargs)

    def __call__(self, lambda_fn):
        """When passed a lambda function call function executes it on the Feature value"""
        lambda_fn(self)

    @property
    def element_type(self):
        return ElementTypes.FEATURE
