package storage;

import elements.Edge;
import elements.GraphElement;
import elements.Vertex;
import elements.features.Feature;

import java.util.Map;
import java.util.stream.Stream;

abstract public class BaseStorage {
    public abstract boolean addFeature(Feature feature);
    public abstract boolean addVertex(Vertex vertex);
    public abstract boolean addEdge(Edge edge);
    public abstract boolean updateFeature(Feature feature);
    public abstract boolean updateVertex(Vertex vertex);
    public abstract boolean updateEdge(Edge edge);
    public abstract Vertex getVertex(String id);
    public abstract Stream<Vertex> getVertices();
    public abstract Edge getEdge(String id);
    public abstract Stream<Edge> getIncidentEdges(Vertex vertex, String edge_type);
    public abstract Feature getFeature(String id);
    public abstract Map<String, Feature> getFeatures(GraphElement e);


    public boolean addElement(GraphElement element){
        switch (element.elementType()){
            case VERTEX:
                return this.addVertex((Vertex) element);
            case EDGE:
                return this.addEdge((Edge) element);
            case FEATURE:
                return this.addFeature((Feature<?>) element);
            default:
                return false;
        }
    }


}
