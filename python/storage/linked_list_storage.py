import re
from typing import Literal, Iterator
from exceptions.base_exception import GraphElementNotFound
from storage import BaseStorage
from typing import Dict, List, TYPE_CHECKING
from elements import ElementTypes
from elements.element_feature import ReplicableFeature
from elements.edge import BaseEdge
from elements.vertex import BaseVertex


class LinkedListStorage(BaseStorage):
    def __init__(self, *args, **kwargs):
        self.translation_table: Dict[str, str] = dict()  # Table storing translated ids for memory efficiency
        self.vertex_table: Dict[str, List[str]] = dict()  # Source Vertex to destinations mapping table
        self.feature_table: Dict[str,
                            Dict:[str,
                                  "ReplicableFeature"]] = dict()  # Source vertex to dict of field_name and feature objects
        self.meta_data: Dict[str, dict] = dict()  # Meta-data such as replication stuff per each graph element
        self.last_translated_id = 0
        self.feature_classes: Dict[str, object] = dict()

    def add_feature(self, feature: "ReplicableFeature") -> bool:
        if feature.element.element_type is ElementTypes.VERTEX:
            vertex_id = self.translation_table[feature.element.id]
            if vertex_id not in self.feature_table: self.feature_table[vertex_id] = dict()
            features = self.feature_table[vertex_id]
            if feature.field_name in features:
                return False

            features[feature.field_name] = feature.value
            self.feature_classes[feature.field_name] = type(feature)
        elif feature.element.element_type is ElementTypes.EDGE:
            # @todo not yet implemented since edges do not have features
            pass

        return True

    def add_vertex(self, vertex: "BaseVertex") -> bool:
        if vertex.id in self.translation_table: return False  # Already in there
        my_id = str(self.last_translated_id)
        self.last_translated_id += 1
        self.translation_table[vertex.id] = my_id
        self.vertex_table[my_id] = list()
        meta_data = vertex.__getstate__()
        if "_features" in meta_data: del meta_data["_features"]  # Features will come
        if "id" in meta_data: del meta_data["id"]  # id is not needed
        self.meta_data[my_id] = meta_data
        return True

    def add_edge(self, edge: "BaseEdge") -> bool:
        """ Since we allow multi-edges it always returns True unless some runtime error happens """
        source_id = self.translation_table[edge.source.id]
        dest_id = self.translation_table[edge.destination.id]
        self.vertex_table[source_id].append(dest_id)
        return True

    def update_feature(self, feature: "ReplicableFeature") -> bool:
        """ No need to do anything since objects are already in memory """
        if feature.element.element_type is ElementTypes.VERTEX:
            internal_id = self.translation_table[feature.element.id]
            self.feature_table[internal_id][feature.field_name] = feature.value
            return True
        pass

    def update_vertex(self, vertex: "BaseVertex") -> bool:
        try:
            int_id = self.translation_table[vertex.id]
            meta_data = vertex.__getstate__()
            if "_features" in meta_data: del meta_data["_features"]  # Features will come
            if "id" in meta_data: del meta_data["id"]  # id is not needed
            self.meta_data[int_id] = meta_data
            return True
        except KeyError:
            raise GraphElementNotFound
        pass

    def update_edge(self, vertex: "BaseVertex") -> bool:
        pass

    def get_vertex(self, element_id: str, with_features=False) -> "BaseVertex":
        if element_id not in self.translation_table: raise GraphElementNotFound
        int_id = self.translation_table[element_id]
        vertex = BaseVertex(element_id=element_id)
        vertex.__setstate__(self.meta_data[int_id])
        if with_features:
            # Add the cache of all the features
            pass
        vertex.attach_storage(self)
        return vertex

    def get_edge(self, element_id: str, with_features=False) -> "BaseEdge":
        pass

    def get_feature(self, element_id: str) -> "ReplicableFeature":
        try:
            feature_match = re.search("(?P<type>\w+):(?P<element_id>\w+):(?P<feature_name>\w+)", element_id)
            el_type = int(feature_match['type'])
            if el_type == ElementTypes.VERTEX.value:
                internal_id = self.translation_table[feature_match['element_id']]
                feature_value = self.feature_table[internal_id][feature_match['feature_name']]
                feature_class = self.feature_classes[feature_match['feature_name']]
                feature = feature_class(value=feature_value, element_id=element_id, element=self.get_vertex(feature_match['element_id']))
                feature.attach_storage(self)
                return feature
            elif el_type == ElementTypes.EDGE.value:
                # @todo Implement edge features later
                pass
        except KeyError:
            raise GraphElementNotFound

    def get_features(self, element_type: "ElementTypes", element_id: str) -> Dict[str, "ReplicableFeature"]:
        res = {}
        try:
            if element_type is ElementTypes.VERTEX:
                internal_id = self.translation_table[element_id]
                features: Dict[str, "ReplicableFeature"] = self.feature_table[internal_id]
                vertex = self.get_vertex(element_id)
                for name,value in features.items():
                    feature_class = self.feature_classes[name]
                    feature = feature_class(value=value, element_id=element_id,
                                            element=vertex)
                    feature.attach_storage(self)
                    res[name] = feature
                return res

            elif element_type is ElementTypes.EDGE:
                #   @todo Implement later edge features
                pass
        except KeyError:
            raise GraphElementNotFound

    def get_incident_edges(self, vertex: "BaseVertex", edge_type: Literal['in', 'out', 'both'] = "in") -> Iterator[
        "BaseEdge"]:

        edge_list: ['BaseEdge'] = list()
        if vertex.id not in self.translation_table: raise GraphElementNotFound
        int_id = self.translation_table[vertex.id]
        if edge_type in ['in','both']:
            # Edges where vertex is destination
            for _id, edges in self.vertex_table.items():
                if int_id in edges:
                    real_id = next(key for key, value in self.translation_table.items() if value == _id)
                    src_vertex = self.get_vertex(real_id)
                    edge = BaseEdge(src=src_vertex, dest=vertex)
                    edge.attach_storage(self)
                    edge_list.append(edge)
        if edge_type in ['out','both']:
            # Edge where vertex is the source
            out_vertices = self.vertex_table[int_id]
            for _id in out_vertices:
                real_id = next(key for key, value in self.translation_table.items() if value == _id)
                dest_vertex = self.get_vertex(real_id)
                edge = BaseEdge(src=vertex, dest=dest_vertex)
                edge.attach_storage(self)
                edge_list.append(edge)
        return edge_list

