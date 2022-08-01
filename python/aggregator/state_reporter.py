from aggregator import BaseAggregator
from typing import TYPE_CHECKING
from elements import ElementTypes

if TYPE_CHECKING:
    from elements import GraphElement


class StateReporter(BaseAggregator):

    def __init__(self, ident: str = "state_reporter", storage: "BaseStorage" = None):
        super().__init__(ident, storage)
        self.elements = ["13"]

    def report(self, element: "GraphElement"):
        str = "Element %s, with clock %s, in part %s and master %s has following features\n" % (
        element.id, element.integer_clock, self.storage.part_id, element.master_part)
        for name, feature in element.features:
            if name != "parts":continue
            str += "%s %s \n" % (name, feature._value)
        print(str)

    def run(self, *args, **kwargs):
        pass

    def add_element_callback(self, element: "GraphElement"):

        pass

    def update_element_callback(self, element: "GraphElement", old_element: "GraphQuery"):
        pass

    def sync_element_callback(self, element: "GraphElement", old_element: "GraphQuery"):
        if element.element_type is ElementTypes.VERTEX and element.id in self.elements:
            self.report(element)
        pass

    def open(self, *args, **kwargs):
        pass

    def rpc_element_callback(self, element: "GraphElement"):
        pass
