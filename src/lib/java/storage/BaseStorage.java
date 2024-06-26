package storage;

import com.esotericsoftware.reflectasm.ConstructorAccess;
import elements.*;
import elements.enums.CacheFeatureContext;
import elements.enums.EdgeType;
import elements.enums.ElementType;
import it.unimi.dsi.fastutil.bytes.Byte2ObjectMap;
import it.unimi.dsi.fastutil.bytes.Byte2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.PartNumber;
import org.apache.flink.runtime.state.tmshared.TMSharedKeyedStateBackend;
import org.apache.flink.runtime.state.tmshared.TMSharedState;
import org.apache.flink.streaming.api.operators.graph.interfaces.GraphRuntimeContext;
import org.apache.flink.util.Preconditions;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/**
 * Base Class for all Graph Storage States
 * <p>
 * This state is part of the TaskSharedState logic, as implementations might have overlaps between the versions
 * To facilitate faster access across various shared operators a {@link GraphView} should be created holding the {@link GraphRuntimeContext}
 * </p>
 */
abstract public class BaseStorage extends TMSharedState {

    /**
     * Logger
     */
    protected static final Logger LOG = LoggerFactory.getLogger(BaseStorage.class);

    /**
     * {@inheritDoc}
     * Will fail if the storage object is created outside of {@link GraphRuntimeContext} and non-part-number part
     */
    @Override
    public void register(TMSharedKeyedStateBackend<?> TMSharedKeyedStateBackend) {
        Preconditions.checkNotNull(GraphRuntimeContext.CONTEXT_THREAD_LOCAL.get(), "Graph Storage can only be used in GraphStorage Operators. GraphRuntimeContext is not detected");
        Preconditions.checkState(TMSharedKeyedStateBackend.getKeySerializer().createInstance() instanceof PartNumber, "GraphStorage can only be used with partitioned keyed streams");
        super.register(TMSharedKeyedStateBackend);
    }

    /**
     * Create or get the {@link GraphView}
     */
    abstract public GraphView getGraphStorageView(GraphRuntimeContext runtimeContext);

    /**
     * Provider pattern for {@link BaseStorage}
     */
    public interface GraphStorageProvider extends Supplier<BaseStorage>, Serializable {
    }

    /**
     * A thread local view of the graph object
     *
     * @implNote All the get methods assume that contains is checked before
     */
    abstract public static class GraphView {

        protected final GraphRuntimeContext runtimeContext;


        public GraphView(GraphRuntimeContext runtimeContext) {
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
         * Delete {@link Feature} that is attached to element
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
        public abstract DirectedEdge getEdge(String srcId, String destId, @Nullable String attributeId);

        /**
         * Get incided edges of {@link Vertex}.
         */
        public abstract Iterable<DirectedEdge> getIncidentEdges(Vertex vertex, EdgeType edgeType);

        /**
         * Get vertex degree
         */
        public abstract int getIncidentEdgeCount(Vertex vertex, EdgeType edgeType);

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
        public abstract @Nullable Feature getAttachedFeature(ElementType elementType, Object elementId, String featureName);

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
        public abstract boolean containsAttachedFeature(ElementType elementType, Object elementId, String featureName);

        /**
         * Does this storage contain {@link Feature} by its id
         */
        public abstract boolean containsStandaloneFeature(String featureName);

        /**
         * Does this storage contain {@link DirectedEdge} by its id
         */
        public abstract boolean containsEdge(String srcId, String destId, @Nullable String attributeId);

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
         * Retrieve the tensor-related memory usage in megabytes
         */
        public long tensorMemoryUsageInMb(){
            return 0;
        };

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
                    Tuple3<String, String, String> realEdgeId = (Tuple3<String, String, String>) id;
                    return containsEdge(realEdgeId.f0, realEdgeId.f1, realEdgeId.f2);
                case ATTACHED_FEATURE:
                    Tuple3<ElementType, Object, String> realFeatureId = (Tuple3<ElementType, Object, String>) id;
                    return containsAttachedFeature(realFeatureId.f0, realFeatureId.f1, realFeatureId.f2);
                case STANDALONE_FEATURE:
                    return containsStandaloneFeature(((Tuple3<ElementType, Object, String>) id).f2);
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
        public GraphElement getElement(Object id, ElementType type) {
            switch (type) {
                case VERTEX:
                    return getVertex((String) id);
                case EDGE:
                    Tuple3<String, String, String> realEdgeId = (Tuple3<String, String, String>) id;
                    return getEdge(realEdgeId.f0, realEdgeId.f1, realEdgeId.f2);
                case ATTACHED_FEATURE:
                    Tuple3<ElementType, Object, String> realFeatureId = (Tuple3<ElementType, Object, String>) id;
                    return getAttachedFeature(realFeatureId.f0, realFeatureId.f1, realFeatureId.f2);
                case STANDALONE_FEATURE:
                    return getStandaloneFeature(((Tuple3<ElementType, Object, String>) id).f2);
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
                case EDGE:
                    DirectedEdge directedEdge = (DirectedEdge) element;
                    return containsEdge(directedEdge.getSrcId(), directedEdge.getDestId(), directedEdge.getAttribute());
                case ATTACHED_FEATURE:
                    Feature attachedFeature = (Feature) element;
                    return containsAttachedFeature(attachedFeature.getAttachedElementType(), attachedFeature.getAttachedElementId(), attachedFeature.getName());
                case STANDALONE_FEATURE:
                    Feature standaloneFeature = (Feature) element;
                    return containsStandaloneFeature(standaloneFeature.getName());
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
                case EDGE:
                    DirectedEdge directedEdge = (DirectedEdge) element;
                    return getEdge(directedEdge.getSrcId(), directedEdge.getDestId(), directedEdge.getAttribute());
                case ATTACHED_FEATURE:
                    Feature attachedFeature = (Feature) element;
                    return getAttachedFeature(attachedFeature.getAttachedElementType(), attachedFeature.getAttachedElementId(), attachedFeature.getName());
                case STANDALONE_FEATURE:
                    Feature standaloneFeature = (Feature) element;
                    return getStandaloneFeature(standaloneFeature.getName());
                case HYPEREDGE:
                    return getHyperEdge((String) element.getId());
                default:
                    return null;
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
        final public GraphRuntimeContext getRuntimeContext() {
            return runtimeContext;
        }
    }

    public static class StorageException extends IllegalStateException {

    }

    /**
     * Default provider using {@link ListObjectPoolGraphStorage}
     */
    public static class DefaultGraphStorageProvider implements GraphStorageProvider {
        @Override
        public BaseStorage get() {
            return new ListBasedCompressedObjectPoolStorage();
        }
    }

    /**
     * Information about the Feature storing halo state constructor and etc.
     */
    protected static class AttachedFeatureInfo {

        boolean halo;

        byte position;

        Class<? extends Feature> clazz;

        ConstructorAccess<? extends Feature> constructorAccess;

        protected AttachedFeatureInfo(Feature<?, ?> feature, byte position) {
            this.position = position;
            this.halo = feature.isHalo();
            this.clazz = feature.getClass();
            this.constructorAccess = ConstructorAccess.get(this.clazz);
        }
    }

    /**
     * <p>
     * A special {@link AutoCloseable} object that should be opened when you want to access storage with reuse semantics
     * reuse semantics depends to the implementation of storage (Some of which might not have any effect whatsoever)
     * However, generally reuse semantics makes use of shared objects to reduce allocation costs
     * In such mode, UDF should not depend on storing the returned objects as they might change value later
     * </p>
     */
    public static abstract class ObjectPoolScope implements AutoCloseable {
        protected byte openCount;

        abstract public Vertex getVertex(String id, short masterPart);

        abstract public DirectedEdge getEdge(String srcId, String destId, @Nullable String attributeId);


        protected ObjectPoolScope open() {
            openCount++;
            return this;
        }

        @Override
        public void close() {
            openCount--;
        }

        /**
         * Refresh the pool scope by closing and opening the scope thus resetting all the elements
         */
        public final void refresh() {
            close();
            open();
        }

        public boolean isOpen() {
            return openCount > 0;
        }

    }

    /**
     * Reuse scope with elements cache per block
     * Caches {@link Vertex} {@link DirectedEdge} and attached-{@link Feature} objects
     */
    protected static class ObjectPool extends ObjectPoolScope {

        List<Vertex> vertices = new ObjectArrayList<>(10);

        IntList usingVerticesUpTo = new IntArrayList(List.of(0));

        List<DirectedEdge> edges = new ObjectArrayList<>(10);

        IntList usingEdgesUpTo = new IntArrayList(List.of(0));

        Byte2ObjectMap<Tuple2<List<Feature>, IntList>> vertexFeaturesMap = new Byte2ObjectOpenHashMap<>();

        public Vertex getVertex(String id, short masterPart) {
            if (vertices.size() <= usingVerticesUpTo.getInt(openCount)) vertices.add(new Vertex());
            Vertex v = vertices.get(usingVerticesUpTo.getInt(openCount));
            usingVerticesUpTo.set(openCount, usingVerticesUpTo.getInt(openCount) + 1);
            v.id = id;
            v.masterPart = masterPart;
            if (v.features != null) v.features.clear();
            return v;
        }

        public DirectedEdge getEdge(String srcId, String destId, @Nullable String attributeId) {
            if (edges.size() <= usingEdgesUpTo.getInt(openCount)) edges.add(new DirectedEdge());
            DirectedEdge edge = edges.get(usingEdgesUpTo.getInt(openCount));
            usingEdgesUpTo.set(openCount, usingEdgesUpTo.getInt(openCount) + 1);
            edge.src = null;
            edge.dest = null;
            edge.id.f0 = srcId;
            edge.id.f1 = destId;
            edge.id.f2 = attributeId;
            if (edge.features != null) edge.features.clear();
            return edge;
        }

        public Feature getVertexFeature(Object vertexId, String featureName, Object value, AttachedFeatureInfo attachedFeatureInfo) {
            Tuple2<List<Feature>, IntList> vertexFeatureTuple = vertexFeaturesMap.computeIfAbsent(attachedFeatureInfo.position, (position) -> Tuple2.of(new ObjectArrayList<>(), new IntArrayList(Collections.nCopies(openCount + 1, 0))));
            if (vertexFeatureTuple.f0.size() <= vertexFeatureTuple.f1.getInt(openCount))
                vertexFeatureTuple.f0.add(attachedFeatureInfo.constructorAccess.newInstance());
            Feature feature = vertexFeatureTuple.f0.get(vertexFeatureTuple.f1.getInt(openCount));
            vertexFeatureTuple.f1.set(openCount, vertexFeatureTuple.f1.getInt(openCount) + 1);
            feature.element = null;
            feature.halo = attachedFeatureInfo.halo;
            feature.value = value;
            feature.id.f0 = ElementType.VERTEX;
            feature.id.f1 = vertexId;
            feature.id.f2 = featureName;
            if (feature.features != null) feature.features.clear();
            return feature;
        }

        @Override
        protected ObjectPoolScope open() {
            usingVerticesUpTo.add(usingVerticesUpTo.getInt(openCount));
            usingEdgesUpTo.add(usingEdgesUpTo.getInt(openCount));
            vertexFeaturesMap.forEach((key, val) -> val.f1.add(val.f1.getInt(openCount)));
            return super.open();
        }

        @Override
        public void close() {
            usingVerticesUpTo.removeInt(openCount);
            usingEdgesUpTo.removeInt(openCount);
            vertexFeaturesMap.forEach((key, val) -> val.f1.removeInt(openCount));
            super.close();
        }
    }


}
