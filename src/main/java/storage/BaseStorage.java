package storage;

import elements.*;
import elements.Feature;

import java.util.Map;
import java.util.stream.Stream;

abstract public class BaseStorage {
    public short partId = -1;

    public abstract boolean addFeature(Feature feature);
    public abstract boolean addVertex(Vertex vertex);
    public abstract boolean addEdge(Edge edge);
    public abstract boolean addAggregator(Aggregator agg);
    public abstract boolean updateFeature(Feature feature);
    public abstract boolean updateVertex(Vertex vertex);
    public abstract boolean updateEdge(Edge edge);
    public abstract Vertex getVertex(String id);
    public abstract Stream<Vertex> getVertices();
    public abstract Edge getEdge(String id);
    public abstract Stream<Edge> getIncidentEdges(Vertex vertex, String edge_type);
    public abstract Feature getFeature(String id);
    public abstract Map<String, Feature> getFeatures(GraphElement e);
    public abstract Aggregator getAggregator(String id);
    public abstract Stream<Aggregator> getAggregators();
    public abstract void message(GraphOp op);

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

    public boolean updateElement(GraphElement element){
        switch (element.elementType()){
            case VERTEX:
                return this.updateVertex((Vertex) element);
            case EDGE:
                return this.updateEdge((Edge) element);
            case FEATURE:
                return this.updateFeature((Feature) element);
            default:
                return false;
        }
    }

    public GraphElement getElement(GraphElement element){
        switch (element.elementType()){
            case VERTEX:
                return this.getVertex(element.id);
            case FEATURE:
                return this.getFeature(element.id);
            case EDGE:
                return this.getEdge(element.id);
            default:
                return null;
        }
    }

    public GraphElement getElement(String id, ElementType t){
        switch (t){
            case VERTEX:
                return this.getVertex(id);
            case FEATURE:
                return this.getFeature(id);
            case EDGE:
                return this.getEdge(id);
            default:
                return null;
        }
    }

}
