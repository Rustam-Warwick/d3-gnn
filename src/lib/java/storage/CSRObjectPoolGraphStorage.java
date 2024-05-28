package storage;

import elements.*;
import elements.enums.CacheFeatureContext;
import elements.enums.EdgeType;
import elements.enums.ElementType;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import it.unimi.dsi.fastutil.shorts.Short2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.shorts.ShortArrayList;
import it.unimi.dsi.fastutil.shorts.ShortList;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.streaming.api.operators.graph.interfaces.GraphRuntimeContext;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 *
 */
@Deprecated
public class CSRObjectPoolGraphStorage extends BaseStorage {

    protected final Map<String, AttachedFeatureInfo> vertexFeatureInfoTable = new ConcurrentHashMap<>();

    protected final Short2ObjectOpenHashMap<Int2ObjectMap<VertexData>> part2VertexDataMap = new Short2ObjectOpenHashMap<>();

    protected volatile byte uniqueVertexFeatureCounter = 0;

    @Override
    public GraphView getGraphStorageView(GraphRuntimeContext runtimeContext) {
        synchronized (this) {
            runtimeContext.getThisOperatorParts().forEach(part -> part2VertexDataMap.put(part, new Int2ObjectOpenHashMap<>()));
        }
        return new GraphViewImpl(runtimeContext);
    }

    @Override
    public void clear() {
        Map<Byte, Feature> featureTmpMap = vertexFeatureInfoTable.entrySet().stream().collect(Collectors.toMap(item -> item.getValue().position, item -> item.getValue().constructorAccess.newInstance()));
        part2VertexDataMap.forEach((part, vertexMapInternal) -> {
            vertexMapInternal.forEach((vertexId, vertexData) -> {
                if (vertexData.featureValues != null) {
                    for (byte i = 0; i < vertexData.featureValues.length; i++) {
                        if (vertexData.featureValues[i] != null) {
                            Feature tmp = featureTmpMap.get(i);
                            tmp.value = vertexData.featureValues[i];
                            tmp.destroy();
                        }
                    }
                }
            });
        });
        part2VertexDataMap.clear();
        vertexFeatureInfoTable.clear();
        LOG.info("CLEARED Storage");
    }

    /**
     * Information stored per Vertex
     */
    protected static class VertexData {

        protected Object[] featureValues;

        protected IntArrayList outEdges;

        protected IntArrayList inEdges;

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

        protected void addOutEdge(int id) {
            if (outEdges == null) outEdges = new IntArrayList(4);
            outEdges.add(id);
        }

        protected void addInEdge(int id) {
            if (inEdges == null) inEdges = new IntArrayList(4);
            inEdges.add(id);
        }

    }

    protected class GraphViewImpl extends GraphView {
        protected final ObjectPool scopePool = new ObjectPool();
        protected final Object2IntMap<String> vertexIdTranslation = new Object2IntOpenHashMap<>();
        protected final ShortList vertexMasterParts = new ShortArrayList();
        protected final ObjectList<String> vertexIdInverseTranslation = new ObjectArrayList<>();
        protected int internalVertexIdCounter = 0;

        public GraphViewImpl(GraphRuntimeContext runtimeContext) {
            super(runtimeContext);
        }

        @Override
        public void addAttachedFeature(Feature feature) {
            if (feature.getAttachedElementType() == ElementType.VERTEX) {
                AttachedFeatureInfo attachedFeatureInfo = vertexFeatureInfoTable.computeIfAbsent(feature.getName(), (key) -> new AttachedFeatureInfo(feature, uniqueVertexFeatureCounter++));

                int internalVertexId = vertexIdTranslation.getInt(feature.getAttachedElementId());

                part2VertexDataMap.get(getRuntimeContext().getCurrentPart()).get(internalVertexId).addOrUpdateFeature(feature, attachedFeatureInfo);
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
            vertexIdTranslation.computeIfAbsent(vertex.getId(), (s) -> {
                vertexMasterParts.size(Math.max(internalVertexIdCounter + 1, vertexMasterParts.size()));
                vertexMasterParts.set(internalVertexIdCounter, vertex.getMasterPart());
                vertexIdInverseTranslation.size(Math.max(internalVertexIdCounter + 1, vertexIdInverseTranslation.size()));
                vertexIdInverseTranslation.set(internalVertexIdCounter, vertex.getId());
                return internalVertexIdCounter++;
            });

            int internalVertexId = vertexIdTranslation.getInt(vertex.getId());
            part2VertexDataMap.get(getRuntimeContext().getCurrentPart()).computeIfAbsent(internalVertexId, (ignored) -> new VertexData());
        }

        @Override
        public void addEdge(DirectedEdge directedEdge) {

            int internalSrcId = vertexIdTranslation.getInt(directedEdge.getSrcId());
            int internalDestId = vertexIdTranslation.getInt(directedEdge.getDestId());

            part2VertexDataMap.get(getRuntimeContext().getCurrentPart()).get(internalSrcId).addOutEdge(internalDestId);
            part2VertexDataMap.get(getRuntimeContext().getCurrentPart()).get(internalDestId).addInEdge(internalSrcId);
        }

        @Override
        public void addHyperEdge(HyperEdge hyperEdge) {
            throw new IllegalStateException("NOT IMPLEMENTED");
        }

        @Override
        public void updateAttachedFeature(Feature feature, Feature memento) {
            if (feature.getAttachedElementType() == ElementType.VERTEX) {

                int internalVertexId = vertexIdTranslation.getInt(feature.getAttachedElementId());

                part2VertexDataMap.get(getRuntimeContext().getCurrentPart()).get(internalVertexId).addOrUpdateFeature(feature, vertexFeatureInfoTable.get(feature.getName()));
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

            int srcIndex = vertexIdTranslation.getInt(directedEdge.getSrcId());
            int destIndex = vertexIdTranslation.getInt(directedEdge.getDestId());

            IntArrayList outEdges = part2VertexDataMap.get(getRuntimeContext().getCurrentPart()).get(srcIndex).outEdges;
            IntArrayList inEdges = part2VertexDataMap.get(getRuntimeContext().getCurrentPart()).get(destIndex).inEdges;
            outEdges.removeInt(outEdges.lastIndexOf(destIndex));
            inEdges.removeInt(inEdges.lastIndexOf(srcIndex));
        }

        @Override
        public void deleteHyperEdge(HyperEdge hyperEdge) {
            throw new IllegalStateException("NOT IMPLEMENTED");
        }

        @Override
        public @Nullable Vertex getVertex(String vertexId) {

            short masterPart = vertexMasterParts.getShort(vertexIdTranslation.getInt(vertexId));

            if (scopePool.isOpen()) {
                return scopePool.getVertex(vertexId, masterPart);
            }
            return new Vertex(vertexId, masterPart);
        }

        protected @Nullable Vertex getVertex(int internalId) {

            String vertexId = vertexIdInverseTranslation.get(internalId);
            short masterPart = vertexMasterParts.getShort(internalId);

            if (scopePool.isOpen()) {
                return scopePool.getVertex(vertexId, masterPart);
            }
            return new Vertex(vertexId, masterPart);
        }

        @Override
        public Iterable<Vertex> getVertices() {
            return () -> part2VertexDataMap.get(getRuntimeContext().getCurrentPart()).keySet().intStream().mapToObj(this::getVertex).iterator();
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

            VertexData vertexData = part2VertexDataMap.get(getRuntimeContext().getCurrentPart()).get(vertexIdTranslation.getInt(vertex.getId()));

            Iterator<DirectedEdge> inEdgeIterable = IteratorUtils.emptyIterator();
            Iterator<DirectedEdge> outEdgeIterable = IteratorUtils.emptyIterator();
            if (vertexData.outEdges != null && (edgeType == EdgeType.OUT || edgeType == EdgeType.BOTH)) {
                outEdgeIterable = vertexData.outEdges.intStream().mapToObj(outVertexId -> {

                    String inverseId = vertexIdInverseTranslation.get(outVertexId);

                    DirectedEdge e = getEdge(vertex.getId(), inverseId, null);
                    e.src = vertex;
                    return e;
                }).iterator();
            }
            if (vertexData.inEdges != null && (edgeType == EdgeType.IN || edgeType == EdgeType.BOTH)) {
                inEdgeIterable = vertexData.inEdges.intStream().mapToObj(inVertexId -> {

                    String inverseId = vertexIdInverseTranslation.get(inVertexId);

                    DirectedEdge e = getEdge(inverseId, vertex.getId(), null);
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

            VertexData vertexData = part2VertexDataMap.get(getRuntimeContext().getCurrentPart()).get(vertexIdTranslation.getInt(vertex.getId()));

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

        protected @Nullable Feature getAttachedVertexFeature(String elementId, String featureName, @Nullable Object value, @Nullable AttachedFeatureInfo featureInfo) {
            if (featureInfo == null) featureInfo = vertexFeatureInfoTable.get(featureName);
            if (value == null) {

                value = part2VertexDataMap.get(getRuntimeContext().getCurrentPart()).get(vertexIdTranslation.getInt(elementId)).featureValues[featureInfo.position];

            }

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
                return getAttachedVertexFeature((String) elementId, featureName, null, null);
            }
            throw new NotImplementedException("Not Implemented");
        }

        @Override
        public Iterable<Feature> getAttachedFeatures(ElementType elementType, String featureName) {
            if (elementType == ElementType.VERTEX) {
                final AttachedFeatureInfo attachedFeatureInfo = vertexFeatureInfoTable.get(featureName);
                return () -> part2VertexDataMap.get(getRuntimeContext().getCurrentPart()).int2ObjectEntrySet()
                        .stream()
                        .filter(entrySet -> entrySet.getValue().hasFeatureInPosition(attachedFeatureInfo.position))
                        .map(entrySet -> {

                            String vertexId = vertexIdInverseTranslation.get(entrySet.getIntKey());

                            Object value = entrySet.getValue().featureValues[attachedFeatureInfo.position];
                            return getAttachedVertexFeature(vertexId, featureName, value, attachedFeatureInfo);
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

            int internalVertexId = vertexIdTranslation.getOrDefault(vertexId, -1);

            return part2VertexDataMap.get(getRuntimeContext().getCurrentPart()).containsKey(internalVertexId);
        }

        @Override
        public boolean containsAttachedFeature(ElementType elementType, Object elementId, String featureName) {
            if (elementType == ElementType.VERTEX) {
                if (!vertexFeatureInfoTable.containsKey(featureName) || !containsVertex((String) elementId))
                    return false;

                int internalVertexId = vertexIdTranslation.getInt(elementId);

                return part2VertexDataMap.get(getRuntimeContext().getCurrentPart()).get(internalVertexId).hasFeatureInPosition(vertexFeatureInfoTable.get(featureName).position);
            }
            throw new IllegalStateException("NOT IMPLEMENTED");
        }

        @Override
        public boolean containsStandaloneFeature(String featureName) {
            throw new IllegalStateException("NOT IMPLEMENTED");
        }

        @Override
        public boolean containsEdge(String srcId, String destId, @Nullable String attributeId) {
            throw new IllegalStateException("NOT IMPLEMENTED");
        }

        @Override
        public boolean containsHyperEdge(String hyperEdgeId) {
            throw new IllegalStateException("NOT IMPLEMENTED");
        }

        @Override
        public void cacheAttachedFeatures(GraphElement element, CacheFeatureContext context) {
            if (element.getType() == ElementType.VERTEX) {

                int vertexId = vertexIdTranslation.getInt(element.getId());

                VertexData vertexData = part2VertexDataMap.get(getRuntimeContext().getCurrentPart()).get(vertexId);
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
