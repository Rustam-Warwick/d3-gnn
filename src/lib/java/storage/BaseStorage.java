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
 *      This state is part of the TaskSharedState logic, as implementations might have overlaps between the versions
 *      To facilitate faster access across various shared operators a {@link GraphView} should be created holding the {@link GraphRuntimeContext}
 * </p>
 */
abstract public class BaseStorage extends TaskSharedState {

    /**
     * Logger
     */
    protected static Logger LOG = LoggerFactory.getLogger(BaseStorage.class);

    /**
     * {@inheritDoc}
     * <p>
     *     Will fail if the storage object is created outside of {@link GraphRuntimeContext} and non-part-number part
     * </p>
     */
    @Override
    public synchronized void register(TaskSharedKeyedStateBackend<?> taskSharedStateBackend) {
        Preconditions.checkNotNull(GraphRuntimeContext.CONTEXT_THREAD_LOCAL.get(), "Graph Storage can only be used in GraphStorage Operators. GraphRuntimeContext is not detected");
        Preconditions.checkState(taskSharedStateBackend.getKeySerializer().createInstance() instanceof PartNumber, "GraphStorage can only be used with partitioned keyed streams");
        super.register(taskSharedStateBackend);
    }

    /**
     * Create or get the {@link GraphView}
     */
    abstract public GraphView createGraphStorageView(GraphRuntimeContext runtimeContext);

    /**
     * A thread local view of the graph object
     */
    abstract public static class GraphView {

        protected final GraphRuntimeContext runtimeContext;

        public GraphView(GraphRuntimeContext runtimeContext){
            this.runtimeContext = runtimeContext;
        }

        /**
         * Add {@link Feature} that is attached to element
         */
        public abstract void addAttachedFeature(Feature feature);

        /**
         * Add {@link Feature} that is not attached to any element
         */
        public abstract void addStandaloneFeature(Feature feature);

        /**
         * Add {@link Vertex}
         */
        public abstract void addVertex(Vertex vertex);

        /**
         * Add {@link DirectedEdge}
         */
        public abstract void addEdge(DirectedEdge directedEdge);

        /**
         * Add {@link HyperEdge}
         */
        public abstract void addHyperEdge(HyperEdge hyperEdge);

        /**
         * Update {@link Feature} that is attached to element
         */
        public abstract void updateAttachedFeature(Feature feature, Feature memento);

        /**
         * Update {@link Feature} that is not attached to element
         */
        public abstract void updateStandaloneFeature(Feature feature, Feature memento);

        /**
         * Update {@link Vertex}
         */
        public abstract void updateVertex(Vertex vertex, Vertex memento);

        /**
         * Update {@link DirectedEdge}
         */
        public abstract void updateEdge(DirectedEdge directedEdge, DirectedEdge memento);

        /**
         * Update {@link HyperEdge}
         */
        public abstract void updateHyperEdge(HyperEdge hyperEdge, HyperEdge memento);

        /**
         * Delte {@link Feature} that is attached to element
         */
        public abstract void deleteAttachedFeature(Feature feature);

        /**
         * Update {@link Feature} that is not attached to any element
         */
        public abstract void deleteStandaloneFeature(Feature feature);

        /**
         * Delete {@link Vertex}
         */
        public abstract void deleteVertex(Vertex vertex);

        /**
         * Delete {@link DirectedEdge}
         */
        public abstract void deleteEdge(DirectedEdge directedEdge);

        /**
         * Delete {@link HyperEdge}
         */
        public abstract void deleteHyperEdge(HyperEdge hyperEdge);

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
        public abstract DirectedEdge getEdge(Tuple3<String, String, String> id);

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
        public abstract @Nullable Feature getAttachedFeature(Tuple3<ElementType, Object, String> id);

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
        public abstract boolean containsAttachedFeature(Tuple3<ElementType, Object, String> id);

        /**
         * Does this storage contain {@link Feature} by its id
         */
        public abstract boolean containsStandaloneFeature(String featureName);

        /**
         * Does this storage contain {@link DirectedEdge} by its id
         */
        public abstract boolean containsEdge(Tuple3<String, String, String> id);

        /**
         * Does this torage contain {@link HyperEdge} by its id
         */
        public abstract boolean containsHyperEdge(String hyperEdgeId);

        /**
         * Given a {@link GraphElement} cache all its available {@link Feature}
         */
        public abstract void cacheAttachedFeatures(GraphElement element, CacheFeatureContext context);

        /**
         * Return an instance of {@link ObjectPoolScope} object and open that scope
         */
        public abstract ObjectPoolScope openObjectPoolScope();

        /**
         * Add {@link GraphElement}
         */
        public void addElement(GraphElement element) {
            switch (element.getType()) {
                case VERTEX:
                    addVertex((Vertex) element);
                    break;
                case EDGE:
                    addEdge((DirectedEdge) element);
                    break;
                case ATTACHED_FEATURE:
                    addAttachedFeature((Feature) element);
                    break;
                case STANDALONE_FEATURE:
                    addStandaloneFeature((Feature) element);
                    break;
                case HYPEREDGE:
                    addHyperEdge((HyperEdge) element);
                    break;
                default:
                    throw new IllegalStateException("Could not save this element type");
            }
        }

        /**
         * Delete {@link GraphElement}
         */
        public void deleteElement(GraphElement element) {
            switch (element.getType()) {
                case VERTEX:
                    deleteVertex((Vertex) element);
                    break;
                case EDGE:
                    deleteEdge((DirectedEdge) element);
                    break;
                case ATTACHED_FEATURE:
                    deleteAttachedFeature((Feature) element);
                    break;
                case STANDALONE_FEATURE:
                    deleteStandaloneFeature((Feature) element);
                    break;
                case HYPEREDGE:
                    deleteHyperEdge((HyperEdge) element);
                    break;
                default:
                    throw new IllegalStateException("Could not delete this element");
            }
        }

        /**
         * Update {@link GraphElement}
         */
        public void updateElement(GraphElement element, GraphElement memento) {
            switch (element.getType()) {
                case VERTEX:
                    updateVertex((Vertex) element, (Vertex) memento);
                    break;
                case EDGE:
                    updateEdge((DirectedEdge) element, (DirectedEdge) element);
                    break;
                case ATTACHED_FEATURE:
                    updateAttachedFeature((Feature) element, (Feature) memento);
                    break;
                case STANDALONE_FEATURE:
                    updateStandaloneFeature((Feature) element, (Feature) memento);
                    break;
                case HYPEREDGE:
                    updateHyperEdge((HyperEdge) element, (HyperEdge) memento);
                    break;
                default:
                    throw new IllegalStateException("Could not update this element");
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
            return runtimeContext;
        }
    }

    /**
     * Provider pattern for {@link BaseStorage}
     */
    public interface GraphStorageProvider extends Supplier<BaseStorage>, Serializable {}

    /**
     * Default provider using {@link ListObjectPoolGraphStorage}
     */
    public static class DefaultGraphStorageProvider implements GraphStorageProvider {
        @Override
        public BaseStorage get() {
            return new ListObjectPoolGraphStorage();
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
    public static class ObjectPoolScope implements AutoCloseable {

        protected byte openCount;

        protected ObjectPoolScope open(){
            openCount++;
            return this;
        }

        @Override
        public void close() {
            openCount--;
        }

        public boolean isOpen(){return openCount > 0;}

    }

}
