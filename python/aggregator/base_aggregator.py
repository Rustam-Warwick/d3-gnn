import abc
from abc import ABCMeta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from storage import BaseStorage
    from elements import GraphElement


class BaseAggregator(metaclass=ABCMeta):
    def __init__(self, ident: str, storage: "BaseStorage" = None):
        self.storage = storage
        self.id = ident

    @abc.abstractmethod
    def open(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def run(self, *args, **kwargs):
        """ External run of this function """
        pass

    @abc.abstractmethod
    def add_element_callback(self, element: "GraphElement"):
        """ Callback after element is added """
        pass

    @abc.abstractmethod
    def commit_element_callback(self, element:"GraphElement"):
        """ Update is being commited to the storage, called before update,so storage can safely be interacted afterwards, sync and rpc element calls """
        pass

    @abc.abstractmethod
    def update_element_callback(self, element: "GraphElement", old_element: "GraphElement"):
        """ When external update comes and updated  GraphElement """
        pass

    @abc.abstractmethod
    def sync_element_callback(self, element: "GraphElement", old_element: "GraphElement"):
        """ When master syncs values to this GraphElement """
        pass

    @abc.abstractmethod
    def rpc_element_callback(self, element: "GraphElement"):
        """ When master syncs values to this GraphElement """
        pass
