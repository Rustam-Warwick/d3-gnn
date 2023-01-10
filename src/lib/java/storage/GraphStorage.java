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
abstract public class GraphStorage extends TaskSharedState {

    /**
     * Logger
     */
    protected static Logger LOG = LoggerFactory.getLogger(GraphStorage.class);

    /**
     * Add {@link Feature} that is attached to element
     */
    public abstract boolean addAttachedFeature(Feature feature);

    /**
     * Add {@link Feature} that is not attached to any element
     */
    public abstract boolean addStandaloneFeature(Feature feature);

    /**
     * Add {@link Vertex}
     */
    public abstract boolean addVertex(Vertex vertex);

    /**
     * Add {@link DirectedEdge}
     */
    public abstract boolean addEdge(DirectedEdge directedEdge);

    /**
     * Add {@link HyperEdge}
     */
    public abstract boolean addHyperEdge(HyperEdge hyperEdge);

    /**
     * Update {@link Feature} that is attached to element
     */
    public abstract boolean updateAttachedFeature(Feature feature, Feature memento);

    /**
     * Update {@link Feature} that is not attached to element
     */
    public abstract boolean updateStandaloneFeature(Feature feature, Feature memento);

    /**
     * Update {@link Vertex}
     */
    public abstract boolean updateVertex(Vertex vertex, Vertex memento);

    /**
     * Update {@link DirectedEdge}
     */
    public abstract boolean updateEdge(DirectedEdge directedEdge, DirectedEdge memento);

    /**
     * Update {@link HyperEdge}
     */
    public abstract boolean updateHyperEdge(HyperEdge hyperEdge, HyperEdge memento);

    /**
     * Delte {@link Feature} that is attached to element
     */
    public abstract boolean deleteAttachedFeature(Feature feature);

    /**
     * Update {@link Feature} that is not attached to any element
     */
    public abstract boolean deleteStandaloneFeature(Feature feature);

    /**
     * Delete {@link Vertex}
     */
    public abstract boolean deleteVertex(Vertex vertex);

    /**
     * Delete {@link DirectedEdge}
     */
    public abstract boolean deleteEdge(DirectedEdge directedEdge);

    /**
     * Delete {@link HyperEdge}
     */
    public abstract boolean deleteHyperEdge(HyperEdge hyperEdge);

    /**
     * Get {@link Vertex} by its String key
     */
    @Nullable
    public abstract Vertex getVertex(String vertexId);

    /**
     * Get all {@link Vertex} in this storage
     */
    public abstract Iterable<Vertex> getVertices();

    /**
     * Get {@link DirectedEdge} by its full id
     */
    @Nullable
    public abstract DirectedEdge getEdge(Tuple3<String, String, String> ids);

    /**
     * Get incided edges of {@link Vertex}
     */
    public abstract Iterable<DirectedEdge> getIncidentEdges(Vertex vertex, EdgeType edge_type);

    /**
     * Get {@link HyperEdge} by its String id
     */
    @Nullable
    public abstract HyperEdge getHyperEdge(String hyperEdgeId);

    /**
     * Get {@link HyperEdge} incident to {@link Vertex}
     */
    public abstract Iterable<HyperEdge> getIncidentHyperEdges(Vertex vertex);

    /**
     * Get attached {@link Feature} by its id
     */
    public abstract @Nullable Feature getAttachedFeature(Tuple3<ElementType, Object, String> ids);

    /**
     * Get all attached {@link Feature} of a given name
     */
    public abstract Iterable<Feature> getAttachedFeatures(ElementType elementType, String featureName);

    /**
     * Get standalone {@link Feature} given by its name
     */
    public abstract @Nullable Feature getStandaloneFeature(String featureName);

    /**
     * Get standalone {@link Feature} in this storage
     */
    public abstract Iterable<Feature> getStandaloneFeatures();

    /**
     * Does this storage contain a {@link Vertex} by its name
     */
    public abstract boolean containsVertex(String vertexId);

    /**
     * Does this storage contain {@link Feature} by its id
     */
    public abstract boolean containsAttachedFeature(Tuple3<ElementType, Object, String> ids);

    /**
     * Does this storage contain {@link Feature} by its id
     */
    public abstract boolean containsStandaloneFeature(String featureName);

    /**
     * Does this storage contain {@link DirectedEdge} by its id
     */
    public abstract boolean containsEdge(Tuple3<String, String, String> ids);

    /**
     * Does this torage contain {@link HyperEdge} by its id
     */
    public abstract boolean containsHyperEdge(String hyperEdgeId);

    /**
     * Given a {@link GraphElement} cache all its available {@link Feature}
     */
    public abstract void cacheFeatures(GraphElement element, CacheFeatureContext context);

    /**
     * Return an instance of {@link ReuseScope} object and open a new scope
     */
    public abstract ReuseScope withReuse();

    /**
     * Add {@link GraphElement}
     */
    public boolean addElement(GraphElement element) {
        switch (element.getType()) {
            case VERTEX:
                return addVertex((Vertex) element);
            case EDGE:
                return addEdge((DirectedEdge) element);
            case ATTACHED_FEATURE:
                return addAttachedFeature((Feature) element);
            case STANDALONE_FEATURE:
                return addStandaloneFeature((Feature) element);
            case HYPEREDGE:
                return addHyperEdge((HyperEdge) element);
            default:
                return false;
        }
    }

    /**
     * Delete {@link GraphElement}
     */
    public boolean deleteElement(GraphElement element) {
        switch (element.getType()) {
            case VERTEX:
                return deleteVertex((Vertex) element);
            case EDGE:
                return deleteEdge((DirectedEdge) element);
            case ATTACHED_FEATURE:
                return deleteAttachedFeature((Feature) element);
            case STANDALONE_FEATURE:
                return deleteStandaloneFeature((Feature) element);
            case HYPEREDGE:
                return deleteHyperEdge((HyperEdge) element);
            default:
                return false;
        }
    }

    /**
     * Update {@link GraphElement}
     */
    public boolean updateElement(GraphElement element, GraphElement memento) {
        switch (element.getType()) {
            case VERTEX:
                return updateVertex((Vertex) element, (Vertex) memento);
            case EDGE:
                return updateEdge((DirectedEdge) element, (DirectedEdge) element);
            case ATTACHED_FEATURE:
                return updateAttachedFeature((Feature) element, (Feature) memento);
            case STANDALONE_FEATURE:
                return updateStandaloneFeature((Feature) element, (Feature) memento);
            case HYPEREDGE:
                return updateHyperEdge((HyperEdge) element, (HyperEdge) memento);
            default:
                return false;
        }
    }

    /**
     * Does this storage contain {@link GraphElement}
     */
    public boolean containsElement(Object id, ElementType type) {
        switch (type) {
            case VERTEX:
                return containsVertex((String) id);
            case EDGE:
                return containsEdge((Tuple3<String, String, String>) id);
            case ATTACHED_FEATURE:
                return containsAttachedFeature((Tuple3<ElementType, Object, String>) id);
            case STANDALONE_FEATURE:
                return containsStandaloneFeature((String) id);
            case HYPEREDGE:
                return containsHyperEdge((String) id);
            case PLUGIN:
                return getRuntimeContext().getPlugin((String) id) != null;
            default:
                return false;
        }
    }

    /**
     * Get {@link GraphElement} given its id and {@link ElementType}
     */
    public GraphElement getElement(Object id, ElementType t) {
        switch (t) {
            case VERTEX:
                return getVertex((String) id);
            case ATTACHED_FEATURE:
                return getAttachedFeature((Tuple3<ElementType, Object, String>) id);
            case STANDALONE_FEATURE:
                return getStandaloneFeature((String) id);
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

    /**
     * Does this storage contain {@link GraphElement}
     */
    public boolean containsElement(GraphElement element) {
        switch (element.getType()) {
            case VERTEX:
                return containsVertex((String) element.getId());
            case ATTACHED_FEATURE:
                return containsAttachedFeature((Tuple3<ElementType, Object, String>) element.getId());
            case STANDALONE_FEATURE:
                return containsStandaloneFeature((String) element.getId());
            case EDGE:
                return containsEdge((Tuple3<String, String, String>) element.getId());
            case HYPEREDGE:
                return containsHyperEdge((String) element.getId());
            default:
                return false;
        }
    }

    /**
     * Get the {@link GraphElement} stored in this storage
     */
    public GraphElement getElement(GraphElement element) {
        switch (element.getType()) {
            case VERTEX:
                return getVertex((String) element.getId());
            case ATTACHED_FEATURE:
                return getAttachedFeature((Tuple3<ElementType, Object, String>) element.getId());
            case STANDALONE_FEATURE:
                return getStandaloneFeature((String) element.getId());
            case EDGE:
                return getEdge((Tuple3<String, String, String>) element.getId());
            case HYPEREDGE:
                return getHyperEdge((String) element.getId());
            default:
                return null;
        }
    }

    /**
     * Method that create a <strong>dummy</strong> {@link GraphElement} if possible
     */
    public final GraphElement getDummyElement(Object id, ElementType elementType) {
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
    final public GraphRuntimeContext getRuntimeContext() {
        return GraphRuntimeContext.CONTEXT_THREAD_LOCAL.get();
    }

    /**
     * {@inheritDoc}
     * <p>
     *     Will fail if the storage object is created outside of {@link GraphRuntimeContext} and non-part-number part
     * </p>
     */
    @Override
    public synchronized void register(TaskSharedKeyedStateBackend<?> taskSharedStateBackend) {
        Preconditions.checkNotNull(getRuntimeContext(), "Graph Storage can only be used in GraphStorage Operators. GraphRuntimeContext is not detected");
        Preconditions.checkState(taskSharedStateBackend.getKeySerializer().createInstance() instanceof PartNumber, "GraphStorage can only be used with partitioned keyed streams");
        super.register(taskSharedStateBackend);
    }

    /**
     * Provider pattern for {@link GraphStorage}
     */
    public interface GraphStorageProvider extends Supplier<GraphStorage>, Serializable {}

    /**
     * Default provider using {@link DefaultStorage}
     */
    public static class DefaultGraphStorageProvider implements GraphStorageProvider {
        @Override
        public GraphStorage get() {
            return new DefaultStorage();
        }
    }

    /**
     * <p>
     *     A special {@link AutoCloseable} object that should be opened when you want to access storage with reuse semantics
     *     reuse semantics depends to the implementation of storage (Some of not might not have such semantics whatsoever)
     *     However, generally reuse semantics makes use of shared objects to reduce allocation costs
     *     In such mode, UDF should not depend on storing the returned objects as they might change value later
     * </p>
     */
    public static class ReuseScope implements AutoCloseable {
        protected byte openCount;

        protected ReuseScope open(){
            openCount++;
            return this;
        }

        @Override
        public void close() {
            openCount--;
        }

        public boolean isOpen(){return openCount > 0;}

        public byte getOpenCount(){
             return openCount;
        }

    }

}
