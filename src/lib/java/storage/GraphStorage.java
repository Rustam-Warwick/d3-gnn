package storage;

import elements.*;
import elements.enums.CacheFeatureContext;
import elements.enums.EdgeType;
import elements.enums.ElementType;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.PartNumber;
import org.apache.flink.runtime.state.taskshared.TaskSharedKeyedStateBackend;
import org.apache.flink.runtime.state.taskshared.TaskSharedState;
import org.apache.flink.streaming.api.operators.graph.interfaces.GraphRuntimeContext;
import org.apache.flink.util.Preconditions;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.function.Supplier;

/**
 * Base Class for all Graph Storage States
 * <p>
 * Graph Storage state is different from other {@link org.apache.flink.runtime.state.internal.InternalKvState} as it holds many types of elements
 * Features, Vertices, Edges, HyperEdges
 * In order to facilitate such logic, graph storage is recursive. It is a state holding itself
 * Graph Storage can decide to publish its stored vertices or edges as a KV state as well
 * It is also not part of default {@link org.apache.flink.runtime.state.heap.StateMap} or {@link org.apache.flink.runtime.state.heap.StateTable} logic
 * BaseStorage is self-sustaining state
 * </p>
 */
abstract public class GraphStorage extends TaskSharedState implements Serializable {

    /**
     * Logger
     */
    protected static Logger LOG = LoggerFactory.getLogger(GraphStorage.class);

    // ------------------------ ABSTRACT METHODS -------------------------------------

    public abstract boolean addAttachedFeature(Feature<?, ?> feature);

    public abstract boolean addStandaloneFeature(Feature<?, ?> feature);

    public abstract boolean addVertex(Vertex vertex);

    public abstract boolean addEdge(DirectedEdge directedEdge);

    public abstract boolean addHyperEdge(HyperEdge hyperEdge);

    public abstract boolean updateAttachedFeature(Feature<?, ?> feature, Feature<?, ?> memento);

    public abstract boolean updateStandaloneFeature(Feature<?, ?> feature, Feature<?, ?> memento);

    public abstract boolean updateVertex(Vertex vertex, Vertex memento);

    public abstract boolean updateEdge(DirectedEdge directedEdge, DirectedEdge memento);

    public abstract boolean updateHyperEdge(HyperEdge hyperEdge, HyperEdge memento);

    public abstract boolean deleteAttachedFeature(Feature<?, ?> feature);

    public abstract boolean deleteStandaloneFeature(Feature<?, ?> feature);

    public abstract boolean deleteVertex(Vertex vertex);

    public abstract boolean deleteEdge(DirectedEdge directedEdge);

    public abstract boolean deleteHyperEdge(HyperEdge hyperEdge);

    @Nullable
    public abstract Vertex getVertex(String vertexId);

    public abstract Iterable<Vertex> getVertices();

    @Nullable
    public abstract DirectedEdge getEdge(Tuple3<String, String, String> ids);

    public abstract Iterable<DirectedEdge> getIncidentEdges(Vertex vertex, EdgeType edge_type);

    @Nullable
    public abstract HyperEdge getHyperEdge(String hyperEdgeId);

    public abstract Iterable<HyperEdge> getIncidentHyperEdges(Vertex vertex);

    @Nullable
    public abstract Feature<?, ?> getAttachedFeature(Tuple3<ElementType, Object, String> ids);

    @Nullable
    public abstract Feature<?, ?> getStandaloneFeature(Tuple3<ElementType, Object, String> ids);

    public abstract boolean containsVertex(String vertexId);

    public abstract boolean containsAttachedFeature(Tuple3<ElementType, Object, String> ids);

    public abstract boolean containsStandaloneFeature(Tuple3<ElementType, Object, String> ids);

    public abstract boolean containsEdge(Tuple3<String, String, String> ids);

    public abstract boolean containsHyperEdge(String hyperEdgeId);

    /**
     * Given a {@link GraphElement} aggregate all its available {@link Feature}
     *
     * @implNote This method is only called in <code>sync()</code> {@link GraphElement}
     */
    public abstract void cacheFeatures(GraphElement element, CacheFeatureContext context);

    // -------------------- HELPER METHODS ----------------

    public boolean addElement(GraphElement element) {
        switch (element.getType()) {
            case VERTEX:
                return addVertex((Vertex) element);
            case EDGE:
                return addEdge((DirectedEdge) element);
            case ATTACHED_FEATURE:
                return addAttachedFeature((Feature<?, ?>) element);
            case STANDALONE_FEATURE:
                return addStandaloneFeature((Feature<?, ?>) element);
            case HYPEREDGE:
                return addHyperEdge((HyperEdge) element);
            default:
                return false;
        }
    }

    public boolean deleteElement(GraphElement element) {
        switch (element.getType()) {
            case VERTEX:
                return deleteVertex((Vertex) element);
            case EDGE:
                return deleteEdge((DirectedEdge) element);
            case ATTACHED_FEATURE:
                return deleteAttachedFeature((Feature<?, ?>) element);
            case STANDALONE_FEATURE:
                return deleteStandaloneFeature((Feature<?, ?>) element);
            case HYPEREDGE:
                return deleteHyperEdge((HyperEdge) element);
            default:
                return false;
        }
    }

    public boolean updateElement(GraphElement element, GraphElement memento) {
        switch (element.getType()) {
            case VERTEX:
                return updateVertex((Vertex) element, (Vertex) memento);
            case EDGE:
                return updateEdge((DirectedEdge) element, (DirectedEdge) element);
            case ATTACHED_FEATURE:
                return updateAttachedFeature((Feature<?, ?>) element, (Feature<?, ?>) memento);
            case STANDALONE_FEATURE:
                return updateStandaloneFeature((Feature<?, ?>) element, (Feature<?, ?>) memento);
            case HYPEREDGE:
                return updateHyperEdge((HyperEdge) element, (HyperEdge) memento);
            default:
                return false;
        }
    }

    public boolean containsElement(Object id, ElementType type) {
        switch (type) {
            case VERTEX:
                return containsVertex((String) id);
            case EDGE:
                return containsEdge((Tuple3<String, String, String>) id);
            case ATTACHED_FEATURE:
                return containsAttachedFeature((Tuple3<ElementType, Object, String>) id);
            case STANDALONE_FEATURE:
                return containsStandaloneFeature((Tuple3<ElementType, Object, String>) id);
            case HYPEREDGE:
                return containsHyperEdge((String) id);
            case PLUGIN:
                return getRuntimeContext().getPlugin((String) id) != null;
            default:
                return false;
        }
    }

    public GraphElement getElement(Object id, ElementType t) {
        switch (t) {
            case VERTEX:
                return getVertex((String) id);
            case ATTACHED_FEATURE:
                return getAttachedFeature((Tuple3<ElementType, Object, String>) id);
            case STANDALONE_FEATURE:
                return getStandaloneFeature((Tuple3<ElementType, Object, String>) id);
            case EDGE:
                return getEdge((Tuple3<String, String, String>) id);
            case HYPEREDGE:
                return getHyperEdge((String) id);
            case PLUGIN:
                return getRuntimeContext().getPlugin((String) id);
            default:
                return null;
        }
    }

    public boolean containsElement(GraphElement element) {
        switch (element.getType()) {
            case VERTEX:
                return containsVertex((String) element.getId());
            case ATTACHED_FEATURE:
                return containsAttachedFeature((Tuple3<ElementType, Object, String>) element.getId());
            case STANDALONE_FEATURE:
                return containsStandaloneFeature((Tuple3<ElementType, Object, String>) element.getId());
            case EDGE:
                return containsEdge((Tuple3<String, String, String>) element.getId());
            case HYPEREDGE:
                return containsHyperEdge((String) element.getId());
            default:
                return false;
        }
    }

    public GraphElement getElement(GraphElement element) {
        switch (element.getType()) {
            case VERTEX:
                return getVertex((String) element.getId());
            case ATTACHED_FEATURE:
                return getAttachedFeature((Tuple3<ElementType, Object, String>) element.getId());
            case STANDALONE_FEATURE:
                return getStandaloneFeature((Tuple3<ElementType, Object, String>) element.getId());
            case EDGE:
                return getEdge((Tuple3<String, String, String>) element.getId());
            case HYPEREDGE:
                return getHyperEdge((String) element.getId());
            default:
                return null;
        }
    }

    public final GraphElement getDummyElement(Object id, ElementType elementType) {
        switch (elementType) {
            case VERTEX:
                return new Vertex((String) id, getRuntimeContext().getCurrentPart());
            case HYPEREDGE:
                return new HyperEdge((String) id, new ArrayList<>(), getRuntimeContext().getCurrentPart());
        }
        throw new IllegalStateException("Dummy element can only be created for VERTEX and HYPEREDGE");
    }

    final public GraphRuntimeContext getRuntimeContext() {
        return GraphRuntimeContext.CONTEXT_THREAD_LOCAL.get();
    }

    @Override
    public synchronized void register(TaskSharedKeyedStateBackend<?> taskSharedStateBackend) {
        Preconditions.checkNotNull(getRuntimeContext(), "Graph Storage can only be used in GraphStorage Operators. GraphRuntimeContext is not detected");
        Preconditions.checkState(taskSharedStateBackend.getKeySerializer().createInstance() instanceof PartNumber, "GraphStorage can only be used with partitioned keyed streams");
        super.register(taskSharedStateBackend);
    }

    /**
     * Provider pattern for GraphStorage
     */
    public interface GraphStorageProvider extends Supplier<GraphStorage>, Serializable {
    }

    /**
     * Default provider using {@link DefaultStorage}
     */
    public static class DefaultGraphStorageProvider implements GraphStorageProvider {
        @Override
        public GraphStorage get() {
            return new DefaultStorage();
        }
    }

}
