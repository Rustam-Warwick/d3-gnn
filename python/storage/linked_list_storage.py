import re
from typing import Literal, Iterator
from exceptions.base_exception import GraphElementNotFound
from storage import BaseStorage
from aggregator import BaseAggregator
from typing import Dict, List, TYPE_CHECKING, Set
from elements import ElementTypes, GraphElement
from elements.element_feature import ReplicableFeature
from elements.edge import BaseEdge
from elements.vertex import BaseVertex


class LinkedListStorage(BaseStorage):
    def __init__(self, *args, **kwargs):
        self.translation_table: Dict[str, int] = dict()  # Table storing translated ids for memory efficiency
        self.reverse_translation_table: Dict[int, str] = dict()  # Reverse Lookup
        self.vertex_table: Dict[int, Dict] = dict()  # Source Vertex to destinations mapping table
        self.feature_table: Dict[int, Dict] = dict()  # Feature and their meta-data
        self.agg_table: Dict[int, "BaseAggregator"] = dict()  # Feature and their meta-data
        self.element_features: Dict[int, Set[int]] = dict()  # Element.id -> List of Feature ids
        self.vertex_out_edges: Dict[int, List[int]] = dict()  # Vertex.id -> List of out vertex ids
        self.vertex_in_edges: Dict[int, List[int]] = dict()  # Vertex.id -> List of in vertex ids
        self.last_translated_id = 0  # Internal id counter
        self.feature_classes: Dict[str, object] = dict()  # Feature.field name -> Class

    def add_feature(self, feature: "ReplicableFeature") -> bool:
        if feature.id in self.translation_table: return False  # If exists return False
        self.translation_table[feature.id] = self.last_translated_id
        self.reverse_translation_table[self.last_translated_id] = feature.id
        data = feature.__get_save_data__()
        self.feature_table[self.last_translated_id] = data
        if feature.field_name not in self.feature_classes:
            # Add Feature classes
            self.feature_classes[feature.field_name] = type(feature)

        if feature.attached_to[0] is not ElementTypes.NONE:
            # If this feature belongs to some other element
            attached_id = self.translation_table[feature.attached_to[1]]
            if attached_id not in self.element_features: self.element_features[attached_id] = set()
            self.element_features[attached_id].add(self.last_translated_id)

        self.last_translated_id += 1
        return True

    def add_vertex(self, vertex: "BaseVertex") -> bool:
        if vertex.id in self.translation_table: return False  # Already in there
        self.translation_table[vertex.id] = self.last_translated_id
        self.reverse_translation_table[self.last_translated_id] = vertex.id
        data = vertex.__get_save_data__()
        self.vertex_table[self.last_translated_id] = data
        self.last_translated_id += 1
        return True

    def add_edge(self, edge: "BaseEdge") -> bool:
        """ Since we allow multi-edges it always returns True unless some runtime error happens """
        source_id = self.translation_table[edge.source.id]
        dest_id = self.translation_table[edge.destination.id]
        if source_id not in self.vertex_out_edges: self.vertex_out_edges[source_id] = list()
        if dest_id not in self.vertex_in_edges: self.vertex_in_edges[dest_id] = list()
        self.vertex_out_edges[source_id].append(dest_id)
        self.vertex_in_edges[dest_id].append(source_id)
        return True

    def add_aggregator(self, agg: "BaseAggregator") -> bool:
        if agg.id in self.translation_table: return False  # Already in there
        self.translation_table[agg.id] = self.last_translated_id
        self.reverse_translation_table[self.last_translated_id] = agg.id
        self.agg_table[self.last_translated_id] = agg
        self.last_translated_id += 1
        return True

    def update_feature(self, feature: "ReplicableFeature") -> bool:
        int_id = self.translation_table[feature.id]
        data = feature.__get_save_data__()
        self.feature_table[int_id] = data
        return True

    def update_vertex(self, vertex: "BaseVertex") -> bool:
        int_id = self.translation_table[vertex.id]
        meta_data = vertex.__get_save_data__()
        self.vertex_table[int_id] = meta_data
        return True

    def update_edge(self, vertex: "BaseVertex") -> bool:
        pass

    def get_vertex(self, element_id: str, with_features=False) -> "BaseVertex":
        try:
            int_id = self.translation_table[element_id]
            vertex = BaseVertex()
            vertex.id = element_id
            vertex.__setstate__(self.vertex_table[int_id])
            if with_features:
                # Add the cache of all the features
                pass
            vertex.attach_storage(self)
            return vertex
        except KeyError:
            raise GraphElementNotFound

    def get_aggregator(self, element_id: str, with_features: bool = False) -> "BaseAggregator":
        try:
            int_id = self.translation_table[element_id]
            agg = self.agg_table[int_id]
            if with_features:
                # Add the cache of all the features
                pass
            return agg
        except KeyError:
            raise GraphElementNotFound

    def get_aggregators(self, with_features: bool = False) -> Iterator["BaseAggregator"]:
        return self.agg_table.values()

    def get_edge(self, element_id: str, with_features=False) -> "BaseEdge":
        return None

    def get_feature(self, element_id: str, with_element=True) -> "ReplicableFeature":
        try:
            int_id = self.translation_table[element_id]
            feature_match = re.search("(\w+:)*(?P<feature_name>\w+)$", element_id[1:])
            # el_type = int(feature_match['type'])
            feature_class = self.feature_classes[feature_match['feature_name']]
            feature: "ReplicableFeature" = feature_class()
            feature.id = element_id
            feature.__setstate__(self.feature_table[int_id])
            if with_element:
                if feature.attached_to[0] == ElementTypes.VERTEX:
                    element = self.get_vertex(feature.attached_to[1])
                    feature.element = element
                if feature.attached_to[0] == ElementTypes.EDGE.value:
                    # @todo Implement edge features later
                    pass

            feature.attach_storage(self)
            return feature
        except KeyError:
            raise GraphElementNotFound

    def get_features(self, element_type: "ElementTypes", element_id: str) -> Dict[str, "ReplicableFeature"]:
        try:
            internal_id = self.translation_table[element_id]
            feature_ids = self.element_features.get(internal_id, list())
            res: Dict[str, "ReplicableFeature"] = dict()
            for f_id in feature_ids:
                feat = self.get_feature(self.reverse_translation_table[f_id], with_element=False)
                res[feat.field_name] = feat
            return res
        except KeyError:
            raise GraphElementNotFound

    def get_incident_edges(self, vertex: "BaseVertex", edge_type: Literal['in', 'out', 'both'] = "in") -> Iterator[
        "BaseEdge"]:

        edge_list: ['BaseEdge'] = list()
        if vertex.id not in self.translation_table: raise GraphElementNotFound
        int_id = self.translation_table[vertex.id]  # Internal Vertex Id
        if edge_type in ('in', 'both'):
            # Edges where vertex is destination
            in_vertices = self.vertex_in_edges.get(int_id, list())
            for _id in in_vertices:
                real_id = self.reverse_translation_table[_id]
                source_vertex = self.get_vertex(real_id)
                edge = BaseEdge(src=source_vertex, dest=vertex, storage=self)
                edge_list.append(edge)
        if edge_type in ('out', 'both'):
            # Edge where vertex is the source
            out_vertices = self.vertex_out_edges.get(int_id, list())
            for _id in out_vertices:
                real_id = self.reverse_translation_table[_id]
                dest_vertex = self.get_vertex(real_id)
                edge = BaseEdge(src=vertex, dest=dest_vertex, storage=self)
                edge_list.append(edge)
        return edge_list
