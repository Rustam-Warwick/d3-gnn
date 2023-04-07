package storage;

import elements.*;
import elements.enums.ElementType;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.operators.graph.interfaces.GraphRuntimeContext;

import java.util.ArrayList;

/**
 * An operator-local view of the graph object that stores the {@link GraphRuntimeContext} inside
 * @implNote All the get methods assume that contains is checked before
 */
abstract public class GraphView {

    /**
     * Store the context here as this view is Operator-local
     */
    protected final GraphRuntimeContext runtimeContext;

    public GraphView(GraphRuntimeContext runtimeContext) {
        this.runtimeContext = runtimeContext;
    }

    /**
     * {@link VerticesView} of the graph
     */
    public abstract VerticesView getVertices();

    /**
     * {@link EdgesView} of the graph
     */
    public abstract EdgesView getEdges();

    /**
     * {@link FeaturesView} of standalone features in the graph
     */
    public abstract FeaturesView getStandaloneFeatures();

    /**
     * Return an instance of {@link ObjectPoolScope} object and open that scope
     */
    public abstract ObjectPoolScope openObjectPoolScope();

    // HELPER METHODS
    public final void addElement(GraphElement element) {
        switch (element.getType()) {
            case VERTEX:
                getVertices().put((String) element.getId(), (Vertex) element);
                break;
            case EDGE:
                getEdges().add((DirectedEdge) element);
                break;
            case ATTACHED_FEATURE:
                Feature attachedFeature = (Feature) element;
                switch (attachedFeature.getAttachedElementType()) {
                    case VERTEX:
                        getVertices().getFeatures((String) attachedFeature.getAttachedElementId()).put(attachedFeature.getName(), attachedFeature);
                        break;
                    default:
                        throw new NotImplementedException("Attached feature only possible in vertices for now");
                }
                break;
            case STANDALONE_FEATURE:
                Feature standaloneFeature = (Feature) element;
                getStandaloneFeatures().put(standaloneFeature.getName(), standaloneFeature);
                break;
            default:
                throw new IllegalStateException("Could not find the element type");
        }
    }

    public final void deleteElement(GraphElement element) {
        throw new NotImplementedException("Deletions not implemented");
    }

    public final void updateElement(GraphElement element, GraphElement memento) {
        switch (element.getType()) {
            case VERTEX:
                break;
            case EDGE:
                break;
            case ATTACHED_FEATURE:
                Feature attachedFeature = (Feature) element;
                switch (attachedFeature.getAttachedElementType()) {
                    case VERTEX:
                        getVertices().getFeatures((String) attachedFeature.getAttachedElementId()).put(attachedFeature.getName(), attachedFeature);
                        break;
                    default:
                        throw new NotImplementedException("Attached feature only possible in vertices for now");
                }
                break;
            case STANDALONE_FEATURE:
                Feature standaloneFeature = (Feature) element;
                getStandaloneFeatures().put(standaloneFeature.getName(), standaloneFeature);
                break;
            default:
                throw new IllegalStateException("Could not find the element type");
        }
    }

    public final boolean containsElement(Object id, ElementType type) {
        switch (type) {
            case VERTEX:
                return getVertices().containsKey((String) id);
            case EDGE:
                Tuple3<String, String, String> directedEdgeId = (Tuple3<String, String, String>) id;
                return getEdges().contains(directedEdgeId.f0, directedEdgeId.f1, directedEdgeId.f2);
            case ATTACHED_FEATURE:
                Tuple3<ElementType, Object, String> attachedFeatureId = (Tuple3<ElementType, Object, String>) id;
                switch (attachedFeatureId.f0) {
                    case VERTEX:
                        return getVertices().getFeatures((String) attachedFeatureId.f1).containsKey(attachedFeatureId.f2);
                    default:
                        throw new NotImplementedException("Attached feature only possible in vertices for now");
                }
            case STANDALONE_FEATURE:
                Tuple3<ElementType, Object, String> standaloneFeatureId = (Tuple3<ElementType, Object, String>) id;
                return getStandaloneFeatures().containsKey(standaloneFeatureId.f2);
            case PLUGIN:
                return getRuntimeContext().getPlugin((String) id) != null;
            default:
                throw new IllegalStateException("Could not find the element type");
        }
    }

    public final GraphElement getElement(Object id, ElementType type) {
        switch (type) {
            case VERTEX:
                return getVertices().get((String) id);
            case EDGE:
                Tuple3<String, String, String> directedEdgeId = (Tuple3<String, String, String>) id;
                return getEdges().get(directedEdgeId.f0, directedEdgeId.f1, directedEdgeId.f2);
            case ATTACHED_FEATURE:
                Tuple3<ElementType, Object, String> attachedFeatureId = (Tuple3<ElementType, Object, String>) id;
                switch (attachedFeatureId.f0) {
                    case VERTEX:
                        return getVertices().getFeatures((String) attachedFeatureId.f1).get(attachedFeatureId.f2);
                    default:
                        throw new NotImplementedException("Attached feature only possible in vertices for now");
                }
            case STANDALONE_FEATURE:
                Tuple3<ElementType, Object, String> standaloneFeatureId = (Tuple3<ElementType, Object, String>) id;
                return getStandaloneFeatures().get(standaloneFeatureId.f2);
            case PLUGIN:
                return getRuntimeContext().getPlugin((String) id);
            default:
                throw new IllegalStateException("Could not find the element type");
        }
    }

    public final boolean containsElement(GraphElement element) {
        switch (element.getType()) {
            case VERTEX:
                return getVertices().containsKey((String) element.getId());
            case EDGE:
                Tuple3<String, String, String> directedEdgeId = (Tuple3<String, String, String>) element.getId();
                return getEdges().contains(directedEdgeId.f0, directedEdgeId.f1, directedEdgeId.f2);
            case ATTACHED_FEATURE:
                Tuple3<ElementType, Object, String> attachedFeatureId = (Tuple3<ElementType, Object, String>) element.getId();
                switch (attachedFeatureId.f0) {
                    case VERTEX:
                        return getVertices().getFeatures((String) attachedFeatureId.f1).containsKey(attachedFeatureId.f2);
                    default:
                        throw new NotImplementedException("Attached feature only possible in vertices for now");
                }
            case STANDALONE_FEATURE:
                Tuple3<ElementType, Object, String> standaloneFeatureId = (Tuple3<ElementType, Object, String>) element.getId();
                return getStandaloneFeatures().containsKey(standaloneFeatureId.f2);
            case PLUGIN:
                return getRuntimeContext().getPlugin((String) element.getId()) != null;
            default:
                throw new IllegalStateException("Could not find the element type");
        }
    }

    public final GraphElement getElement(GraphElement element) {
        switch (element.getType()) {
            case VERTEX:
                return getVertices().get((String) element.getId());
            case EDGE:
                Tuple3<String, String, String> directedEdgeId = (Tuple3<String, String, String>) element.getId();
                return getEdges().get(directedEdgeId.f0, directedEdgeId.f1, directedEdgeId.f2);
            case ATTACHED_FEATURE:
                Tuple3<ElementType, Object, String> attachedFeatureId = (Tuple3<ElementType, Object, String>) element.getId();
                switch (attachedFeatureId.f0) {
                    case VERTEX:
                        return getVertices().getFeatures((String) attachedFeatureId.f1).get(attachedFeatureId.f2);
                    default:
                        throw new NotImplementedException("Attached feature only possible in vertices for now");
                }
            case STANDALONE_FEATURE:
                Tuple3<ElementType, Object, String> standaloneFeatureId = (Tuple3<ElementType, Object, String>) element.getId();
                return getStandaloneFeatures().get(standaloneFeatureId.f2);
            case PLUGIN:
                return getRuntimeContext().getPlugin((String) element.getId());
            default:
                throw new IllegalStateException("Could not find the element type");
        }
    }

    /**
     * Method that create a <strong>dummy</strong> {@link GraphElement} if possible
     * Assuming that this is the master part
     */
    public final GraphElement getDummyElementAsMaster(Object id, ElementType elementType) {
        switch (elementType) {
            case VERTEX:
                return new Vertex((String) id, getRuntimeContext().getCurrentPart());
            case HYPEREDGE:
                return new HyperEdge((String) id, new ArrayList<>(), getRuntimeContext().getCurrentPart());
        }
        throw new IllegalStateException("Dummy element can only be created for VERTEX and HYPEREDGE");
    }

    /**
     * Return {@link GraphRuntimeContext} operating in this {@link Thread}
     */
    public final GraphRuntimeContext getRuntimeContext() {
        return runtimeContext;
    }
}
