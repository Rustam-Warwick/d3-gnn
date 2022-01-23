import abc
from abc import ABCMeta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from storage.process_fn import GraphStorageProcess
    from elements import GraphElement


class BaseAggregator(metaclass=ABCMeta):
    def __init__(self, ident: str, storage: "GraphStorageProcess" = None):
        self.storage: "GraphStorageProcess" = storage
        self.id: str = ident

    @abc.abstractmethod
    def open(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def run(self, *args, **kwargs):
        """ External run of this function @notimplemented"""
        pass

    @abc.abstractmethod
    def add_element_callback(self, element: "GraphElement"):
        """ Callback after Graph Element is added """
        pass

    @abc.abstractmethod
    def update_element_callback(self, element: "GraphElement", old_element: "GraphElement"):
        """ When external update comes and updated GraphElement """
        pass
