package storage;

import elements.Edge;
import elements.GraphElement;
import elements.Vertex;
import elements.features.Feature;

import java.util.Map;
import java.util.stream.Stream;

public class HashMapStorage extends BaseStorage{
    @Override
    public boolean addFeature(Feature feature) {
        return false;
    }

    @Override
    public boolean addVertex(Vertex vertex) {
        return false;
    }

    @Override
    public boolean addEdge(Edge edge) {
        return false;
    }

    @Override
    public boolean updateFeature(Feature feature) {
        return false;
    }

    @Override
    public boolean updateVertex(Vertex vertex) {
        return false;
    }

    @Override
    public boolean updateEdge(Edge edge) {
        return false;
    }

    @Override
    public Vertex getVertex(String id) {
        return null;
    }

    @Override
    public Stream<Vertex> getVertices() {
        return null;
    }

    @Override
    public Edge getEdge(String id) {
        return null;
    }

    @Override
    public Stream<Edge> getIncidentEdges(Vertex vertex, String edge_type) {
        return null;
    }

    @Override
    public Feature getFeature(String id) {
        return null;
    }

    @Override
    public Map<String, Feature> getFeatures(GraphElement e) {
        return null;
    }
}
