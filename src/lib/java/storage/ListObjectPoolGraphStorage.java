package storage;

import com.esotericsoftware.reflectasm.ConstructorAccess;
import elements.*;
import elements.enums.CacheFeatureContext;
import elements.enums.EdgeType;
import elements.enums.ElementType;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.graph.interfaces.GraphRuntimeContext;
import org.jctools.maps.NonBlockingHashMap;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Simple storage implementation with small data structures, base on list edges
 * <p>
 * Only support {@link Vertex} {@link Feature} for now
 * </p>
 */
public class ListObjectPoolGraphStorage extends BaseStorage {

    /**
     * Master Part table for vertices. This table is shared across tasks as vertices unique
     */
    private final Map<String, Short> vertexMasterTable = new NonBlockingHashMap<>(1000);

    /**
     * Vertex Feature Info
     */
    private final Map<String, AttachedFeatureInfo> vertexFeatureInfoTable = new ConcurrentHashMap<>();

    /**
     * Unique Vertex Feature Counter
     */
    private final AtomicInteger uniqueVertexFeatureCounter = new AtomicInteger(0);

    /**
     * Vertex Map
     */
    private final Map<Short, Map<String, VertexData>> vertexMap = new NonBlockingHashMap<>();

    @Override
    public GraphView getGraphStorageView(GraphRuntimeContext runtimeContext) {
        return new ListGraphView(runtimeContext);
    }

    @Override
    public void clear() {
        Map<Integer, Feature> featureTmpMap = vertexFeatureInfoTable.entrySet().stream().collect(Collectors.toMap(item -> item.getValue().position, item -> item.getValue().constructorAccess.newInstance()));
        vertexMap.forEach((part, vertexMapInternal) -> {
            vertexMapInternal.forEach((vertexId, vertexData) -> {
                if (vertexData.featureValues != null) {
                    for (int i = 0; i < vertexData.featureValues.length; i++) {
                        if (vertexData.featureValues[i] != null) {
                            Feature tmp = featureTmpMap.get(i);
                            tmp.value = vertexData.featureValues[i];
                            tmp.destroy();
                        }
                    }
                }
            });
            vertexMapInternal.clear();
        });
        vertexMap.clear();
        vertexFeatureInfoTable.clear();
        vertexMasterTable.clear();
        LOG.info("CLEARED Storage");
    }

    /**
     * Information stored per Vertex
     */
    protected static class VertexData {

        protected Object[] featureValues;

        protected List<String> outEdges;

        protected List<String> inEdges;

        protected void addOrUpdateFeature(Feature feature, AttachedFeatureInfo featureInfo) {
            if (featureValues == null) featureValues = new Object[featureInfo.position + 1];
            if (featureInfo.position >= featureValues.length) {
                Object[] tmp = new Object[featureInfo.position + 1];
                System.arraycopy(featureValues, 0, tmp, 0, featureValues.length);
                featureValues = tmp;
            }
            List<Feature<?, ?>> subFeatures = feature.features;
            feature.features = null;
            if (featureValues[featureInfo.position] != null) {
                // Is update
                Object oldFeatureValue = featureValues[featureInfo.position];
                if (oldFeatureValue != feature.value) {
                    // Values are different (non-in-place). Delay resume add and return
                    feature.delay();
                    featureValues[featureInfo.position] = feature.value;
                    feature.value = oldFeatureValue;
                    feature.resume();
                    feature.value = featureValues[featureInfo.position];
                }
            } else {
                // Is create: Delay and add
                feature.delay();
                featureValues[featureInfo.position] = feature.value;
            }
            feature.features = subFeatures;
        }

        protected boolean hasFeatureInPosition(int position) {
            return featureValues != null && featureValues.length > position && featureValues[position] != null;
        }

        protected void addOutEdge(DirectedEdge edge) {
            if (outEdges == null) outEdges = new ObjectArrayList<>(4);
            outEdges.add(edge.getDestId());
        }

        protected void addInEdge(DirectedEdge edge) {
            if (inEdges == null) inEdges = new ObjectArrayList<>(4);
            inEdges.add(edge.getSrcId());
        }

    }

    /**
     * Information about the Feature storing halo state constructor and etc.
     */
    protected static class AttachedFeatureInfo {

        boolean halo;

        int position;

        Class<? extends Feature> clazz;

        ConstructorAccess<? extends Feature> constructorAccess;

        protected AttachedFeatureInfo(Feature<?, ?> feature, int position) {
            this.position = position;
            this.halo = feature.isHalo();
            this.clazz = feature.getClass();
            this.constructorAccess = ConstructorAccess.get(this.clazz);
        }
    }

    /**
     * Reuse scope with elements cache per block
     * Caches {@link Vertex} {@link DirectedEdge} and attached-{@link Feature} objects
     */
    public static class ObjectPool extends ObjectPoolScope {

        List<Vertex> vertices = new ObjectArrayList<>(10);

        IntList usingVerticesUpTo = new IntArrayList(List.of(0));

        List<DirectedEdge> edges = new ObjectArrayList<>(10);

        IntList usingEdgesUpTo = new IntArrayList(List.of(0));

        Int2ObjectMap<Tuple2<List<Feature>, IntList>> vertexFeaturesMap = new Int2ObjectOpenHashMap<>();

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

    public class ListGraphView extends GraphView {

        /**
         * Scope pool object
         */
        protected final ObjectPool scopePool = new ObjectPool();

        public ListGraphView(GraphRuntimeContext runtimeContext) {
            super(runtimeContext);
            runtimeContext.getThisOperatorParts().forEach(part -> {
                vertexMap.putIfAbsent(part, new HashMap<>());
            });
        }

        @Override
        public void addAttachedFeature(Feature feature) {
            if (feature.getAttachedElementType() == ElementType.VERTEX) {
                AttachedFeatureInfo attachedFeatureInfo = vertexFeatureInfoTable.computeIfAbsent(feature.getName(), (key) -> new AttachedFeatureInfo(feature, uniqueVertexFeatureCounter.getAndIncrement()));
                vertexMap.get(getRuntimeContext().getCurrentPart()).get(feature.getAttachedElementId()).addOrUpdateFeature(feature, attachedFeatureInfo);
                return;
            }
            throw new IllegalStateException("NOT IMPLEMENTED");
        }

        @Override
        public void addStandaloneFeature(Feature feature) {
            throw new IllegalStateException("NOT IMPLEMENTED");
        }

        @Override
        public void addVertex(Vertex vertex) {
            vertexMasterTable.putIfAbsent(vertex.getId(), vertex.getMasterPart());
            vertexMap.get(getRuntimeContext().getCurrentPart()).putIfAbsent(vertex.getId(), new VertexData());
        }

        @Override
        public void addEdge(DirectedEdge directedEdge) {
            vertexMap.get(getRuntimeContext().getCurrentPart()).get(directedEdge.getSrcId()).addOutEdge(directedEdge);
            vertexMap.get(getRuntimeContext().getCurrentPart()).get(directedEdge.getDestId()).addInEdge(directedEdge);
        }

        @Override
        public void addHyperEdge(HyperEdge hyperEdge) {
            throw new IllegalStateException("NOT IMPLEMENTED");
        }

        @Override
        public void updateAttachedFeature(Feature feature, Feature memento) {
            if (feature.getAttachedElementType() == ElementType.VERTEX) {
                vertexMap.get(getRuntimeContext().getCurrentPart()).get(feature.getAttachedElementId()).addOrUpdateFeature(feature, vertexFeatureInfoTable.get(feature.getName()));
                return;
            }
            throw new NotImplementedException("NOT IMPLEMENTED");
        }

        @Override
        public void updateStandaloneFeature(Feature feature, Feature memento) {
            throw new IllegalStateException("NOT IMPLEMENTED");
        }

        @Override
        public void updateVertex(Vertex vertex, Vertex memento) {
            // No need to do anything
        }

        @Override
        public void updateEdge(DirectedEdge directedEdge, DirectedEdge memento) {
            // No need to do anything
        }

        @Override
        public void updateHyperEdge(HyperEdge hyperEdge, HyperEdge memento) {
            throw new IllegalStateException("NOT IMPLEMENTED");
        }

        @Override
        public void deleteAttachedFeature(Feature feature) {
            throw new IllegalStateException("NOT IMPLEMENTED");
        }

        @Override
        public void deleteStandaloneFeature(Feature feature) {
            throw new IllegalStateException("NOT IMPLEMENTED");
        }

        @Override
        public void deleteVertex(Vertex vertex) {
            throw new IllegalStateException("NOT IMPLEMENTED");
        }

        @Override
        public void deleteEdge(DirectedEdge directedEdge) {
            List<String> outEdges = vertexMap.get(getRuntimeContext().getCurrentPart()).get(directedEdge.getSrcId()).outEdges;
            List<String> inEdges = vertexMap.get(getRuntimeContext().getCurrentPart()).get(directedEdge.getDestId()).inEdges;
            outEdges.remove(outEdges.lastIndexOf(directedEdge.getDestId()));
            inEdges.remove(inEdges.lastIndexOf(directedEdge.getSrcId()));
        }

        @Override
        public void deleteHyperEdge(HyperEdge hyperEdge) {
            throw new IllegalStateException("NOT IMPLEMENTED");
        }

        @Override
        public @Nullable Vertex getVertex(String vertexId) {
            if (scopePool.isOpen()) {
                return scopePool.getVertex(vertexId, vertexMasterTable.get(vertexId));
            }
            return new Vertex(vertexId, vertexMasterTable.get(vertexId));
        }

        @Override
        public Iterable<Vertex> getVertices() {
            return () -> vertexMap.get(getRuntimeContext().getCurrentPart()).keySet().stream().map(this::getVertex).iterator();
        }

        @Override
        public @Nullable DirectedEdge getEdge(String srcId, String destId, @Nullable String attributeId) {
            if (scopePool.isOpen()) {
                return scopePool.getEdge(srcId, destId, attributeId);
            }
            return new DirectedEdge(srcId, destId, attributeId);
        }

        @Override
        public Iterable<DirectedEdge> getIncidentEdges(Vertex vertex, EdgeType edgeType) {
            VertexData vertexData = vertexMap.get(getRuntimeContext().getCurrentPart()).get(vertex.getId());
            Iterator<DirectedEdge> inEdgeIterable = IteratorUtils.emptyIterator();
            Iterator<DirectedEdge> outEdgeIterable = IteratorUtils.emptyIterator();
            if (vertexData.outEdges != null && (edgeType == EdgeType.OUT || edgeType == EdgeType.BOTH)) {
                outEdgeIterable = vertexData.outEdges.stream().map(outVertexId -> {
                    DirectedEdge e = getEdge(vertex.getId(), outVertexId, null);
                    e.src = vertex;
                    return e;
                }).iterator();
            }
            if (vertexData.inEdges != null && (edgeType == EdgeType.IN || edgeType == EdgeType.BOTH)) {
                inEdgeIterable = vertexData.inEdges.stream().map(inVertexId -> {
                    DirectedEdge e = getEdge(inVertexId, vertex.getId(), null);
                    e.dest = vertex;
                    return e;
                }).iterator();
            }
            Iterator<DirectedEdge> res = IteratorUtils.chainedIterator(inEdgeIterable, outEdgeIterable);
            return () -> res;
        }

        @Override
        public int getIncidentEdgeCount(Vertex vertex, EdgeType edgeType) {
            int res = 0;
            VertexData vertexData = vertexMap.get(getRuntimeContext().getCurrentPart()).get(vertex.getId());
            if (vertexData.outEdges != null && edgeType == EdgeType.OUT || edgeType == EdgeType.BOTH)
                res += vertexData.outEdges.size();
            if (vertexData.inEdges != null && edgeType == EdgeType.IN || edgeType == EdgeType.BOTH)
                res += vertexData.inEdges.size();
            return res;
        }

        @Override
        public @Nullable HyperEdge getHyperEdge(String hyperEdgeId) {
            throw new IllegalStateException("NOT IMPLEMENTED");
        }

        @Override
        public Iterable<HyperEdge> getIncidentHyperEdges(Vertex vertex) {
            throw new IllegalStateException("NOT IMPLEMENTED");
        }

        protected @Nullable Feature getAttachedVertexFeature(Object elementId, String featureName, @Nullable Object value, @Nullable ListObjectPoolGraphStorage.AttachedFeatureInfo featureInfo) {
            if (featureInfo == null) featureInfo = vertexFeatureInfoTable.get(featureName);
            if (value == null)
                value = vertexMap.get(getRuntimeContext().getCurrentPart()).get(elementId).featureValues[featureInfo.position];
            if (scopePool.isOpen()) {
                return scopePool.getVertexFeature(elementId, featureName, value, featureInfo);
            } else {
                Feature feature = featureInfo.constructorAccess.newInstance();
                feature.value = value;
                feature.id.f0 = ElementType.VERTEX;
                feature.id.f1 = elementId;
                feature.id.f2 = featureName;
                feature.halo = featureInfo.halo;
                return feature;
            }
        }

        @Override
        public @Nullable Feature getAttachedFeature(ElementType elementType, Object elementId, String featureName) {
            if (elementType == ElementType.VERTEX) {
                return getAttachedVertexFeature(elementId, featureName, null, null);
            }
            throw new NotImplementedException("Not Implemented");
        }

        @Override
        public Iterable<Feature> getAttachedFeatures(ElementType elementType, String featureName) {
            if (elementType == ElementType.VERTEX) {
                final AttachedFeatureInfo attachedFeatureInfo = vertexFeatureInfoTable.get(featureName);
                return () -> vertexMap.get(getRuntimeContext().getCurrentPart()).entrySet()
                        .stream()
                        .filter(entrySet -> entrySet.getValue().hasFeatureInPosition(attachedFeatureInfo.position))
                        .map(entrySet -> {
                            Object value = entrySet.getValue().featureValues[attachedFeatureInfo.position];
                            return getAttachedVertexFeature(entrySet.getKey(), featureName, value, attachedFeatureInfo);
                        }).iterator();

            }
            throw new IllegalStateException("NOT IMPLEMENTED");
        }

        @Override
        public @Nullable Feature getStandaloneFeature(String featureName) {
            throw new IllegalStateException("NOT IMPLEMENTED");
        }

        @Override
        public Iterable<Feature> getStandaloneFeatures() {
            throw new IllegalStateException("NOT IMPLEMENTED");
        }

        @Override
        public boolean containsVertex(String vertexId) {
            return vertexMap.get(getRuntimeContext().getCurrentPart()).containsKey(vertexId);
        }

        @Override
        public boolean containsAttachedFeature(ElementType elementType, Object elementId, String featureName) {
            if (elementType == ElementType.VERTEX) {
                if (!vertexFeatureInfoTable.containsKey(featureName) || !vertexMap.get(getRuntimeContext().getCurrentPart()).containsKey(elementId))
                    return false;
                return vertexMap.get(getRuntimeContext().getCurrentPart()).get(elementId).hasFeatureInPosition(vertexFeatureInfoTable.get(featureName).position);
            }
            throw new IllegalStateException("NOT IMPLEMENTED");
        }

        @Override
        public boolean containsStandaloneFeature(String featureName) {
            throw new IllegalStateException("NOT IMPLEMENTED");
        }

        @Override
        public boolean containsEdge(String srcId, String destId, @Nullable String attributeId) {
            if (!vertexMap.get(getRuntimeContext().getCurrentPart()).containsKey(srcId)) return false;
            List<String> outEdges = vertexMap.get(getRuntimeContext().getCurrentPart()).get(srcId).outEdges;
            return outEdges != null && outEdges.contains(destId);
        }

        @Override
        public boolean containsHyperEdge(String hyperEdgeId) {
            throw new IllegalStateException("NOT IMPLEMENTED");
        }

        @Override
        public void cacheAttachedFeatures(GraphElement element, CacheFeatureContext context) {
            if (element.getType() == ElementType.VERTEX) {
                VertexData vertexData = vertexMap.get(getRuntimeContext().getCurrentPart()).get(element.getId());
                vertexFeatureInfoTable.forEach((featureName, featureInfo) -> {
                    if (featureInfo.halo ^ context == CacheFeatureContext.HALO || !vertexData.hasFeatureInPosition(featureInfo.position))
                        return;
                    element.getFeature(featureName); // Try to get it so element will cache
                });
                return;
            }
            throw new IllegalStateException("NOT IMPLEMENTED");
        }

        @Override
        public ObjectPoolScope openObjectPoolScope() {
            return scopePool.open();
        }

    }

}
