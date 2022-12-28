package storage;

import elements.*;
import elements.enums.CacheFeatureContext;
import elements.enums.EdgeType;
import elements.enums.ElementType;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.taskshared.TaskSharedKeyedStateBackend;
import org.apache.flink.runtime.state.taskshared.TaskSharedState;
import org.apache.flink.runtime.state.taskshared.TaskSharedStateDescriptor;
import org.jetbrains.annotations.Nullable;

public class ListStorage extends BaseStorage {

    @Override
    public boolean addAttachedFeature(Feature<?, ?> feature) {
        return false;
    }

    @Override
    public boolean addStandaloneFeature(Feature<?, ?> feature) {
        return false;
    }

    @Override
    public boolean addVertex(Vertex vertex) {
        return false;
    }

    @Override
    public boolean addEdge(DirectedEdge directedEdge) {
        return false;
    }

    @Override
    public boolean addHyperEdge(HyperEdge hyperEdge) {
        return false;
    }

    @Override
    public boolean updateAttachedFeature(Feature<?, ?> feature, Feature<?, ?> memento) {
        return false;
    }

    @Override
    public boolean updateStandaloneFeature(Feature<?, ?> feature, Feature<?, ?> memento) {
        return false;
    }

    @Override
    public boolean updateVertex(Vertex vertex, Vertex memento) {
        return false;
    }

    @Override
    public boolean updateEdge(DirectedEdge directedEdge, DirectedEdge memento) {
        return false;
    }

    @Override
    public boolean updateHyperEdge(HyperEdge hyperEdge, HyperEdge memento) {
        return false;
    }

    @Override
    public boolean deleteAttachedFeature(Feature<?, ?> feature) {
        return false;
    }

    @Override
    public boolean deleteStandaloneFeature(Feature<?, ?> feature) {
        return false;
    }

    @Override
    public boolean deleteVertex(Vertex vertex) {
        return false;
    }

    @Override
    public boolean deleteEdge(DirectedEdge directedEdge) {
        return false;
    }

    @Override
    public boolean deleteHyperEdge(HyperEdge hyperEdge) {
        return false;
    }

    @Override
    public @Nullable Vertex getVertex(String vertexId) {
        return null;
    }

    @Override
    public Iterable<Vertex> getVertices() {
        return null;
    }

    @Override
    public @Nullable DirectedEdge getEdge(Tuple3<String, String, String> ids) {
        return null;
    }

    @Override
    public Iterable<DirectedEdge> getIncidentEdges(Vertex vertex, EdgeType edge_type) {
        return null;
    }

    @Override
    public @Nullable HyperEdge getHyperEdge(String hyperEdgeId) {
        return null;
    }

    @Override
    public Iterable<HyperEdge> getIncidentHyperEdges(Vertex vertex) {
        return null;
    }

    @Override
    public @Nullable Feature<?, ?> getAttachedFeature(Tuple3<ElementType, Object, String> ids) {
        return null;
    }

    @Override
    public @Nullable Feature<?, ?> getStandaloneFeature(Tuple3<ElementType, Object, String> ids) {
        return null;
    }

    @Override
    public boolean containsVertex(String vertexId) {
        return false;
    }

    @Override
    public boolean containsAttachedFeature(Tuple3<ElementType, Object, String> ids) {
        return false;
    }

    @Override
    public boolean containsStandaloneFeature(Tuple3<ElementType, Object, String> ids) {
        return false;
    }

    @Override
    public boolean containsEdge(Tuple3<String, String, String> ids) {
        return false;
    }

    @Override
    public boolean containsHyperEdge(String hyperEdgeId) {
        return false;
    }

    @Override
    public void cacheFeatures(GraphElement element, CacheFeatureContext context) {

    }

    @Override
    public void clear() {

    }
}
