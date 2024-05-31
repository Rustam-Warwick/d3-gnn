package storage;

import ai.djl.ndarray.NDArray;
import elements.*;
import elements.enums.CacheFeatureContext;
import elements.enums.EdgeType;
import elements.enums.ElementType;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.shorts.Short2ObjectOpenHashMap;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.streaming.api.operators.graph.interfaces.GraphRuntimeContext;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public class ListBasedCompressedObjectPoolStorage extends BaseStorage {

    public static int MERGE_FILLED_CAPACITY = 2000;

    public static int EDGE_LIST_CAPACITY_PER_PART = 300;

    protected final Map<String, AttachedFeatureInfo> vertexFeatureInfoTable = new ConcurrentHashMap<>();

    protected final Short2ObjectOpenHashMap<PerPartVertexStorage> part2VertexDataMap = new Short2ObjectOpenHashMap<>();

    protected volatile byte uniqueVertexFeatureCounter = 0;

    @Override
    public GraphView getGraphStorageView(GraphRuntimeContext runtimeContext) {
        return new GraphViewImpl(runtimeContext);
    }

    @Override
    public void clear() {
        Map<Byte, Feature> featureTmpMap = vertexFeatureInfoTable.entrySet().stream().collect(Collectors.toMap(item -> item.getValue().position, item -> item.getValue().constructorAccess.newInstance()));
        part2VertexDataMap.values().forEach(perPartVertexStorage -> {
            perPartVertexStorage.iterator().forEachRemaining(vertexData -> {
                if(vertexData.featureValues == null) return;
                for (byte i = 0; i < vertexData.featureValues.length; i++) {
                    if (vertexData.featureValues[i] != null) {
                        Feature tmp = featureTmpMap.get(i);
                        tmp.value = vertexData.featureValues[i];
                        tmp.destroy();
                    }
                }
            });
        });
        part2VertexDataMap.clear();
    }

    protected static class PerPartVertexStorage {

        ObjectArrayList<VertexData> sortedVertexData = new ObjectArrayList<>(0);

        ObjectArrayList<VertexData> unsortedVertexData = new ObjectArrayList<>(MERGE_FILLED_CAPACITY);

        public static int linearSearch(ObjectArrayList<VertexData> list, String key) {
            for (int i = 0; i < list.size(); i++) {
                if (list.get(i).id.equals(key)) {
                    return i; // key found
                }
            }
            return -1; // key not found
        }

        public static int binarySearch(ObjectArrayList<VertexData> list, String key) {
            int low = 0;
            int high = list.size() - 1;

            while (low <= high) {
                int mid = (low + high) >>> 1;
                VertexData midVal = list.get(mid);
                int cmp = midVal.id.compareTo(key);

                if (cmp < 0) {
                    low = mid + 1;
                } else if (cmp > 0) {
                    high = mid - 1;
                } else {
                    return mid; // key found
                }
            }
            return -(low + 1);  // key not found
        }

        protected void addVertex(Vertex v) {
            unsortedVertexData.add(new VertexData(v.getMasterPart(), v.getId()));
            if (unsortedVertexData.size() == MERGE_FILLED_CAPACITY) {
                // Move over and sort
                int oldSizeOfSorted = sortedVertexData.size();
                int newSizeOfSorted = oldSizeOfSorted + MERGE_FILLED_CAPACITY;
                sortedVertexData.size(newSizeOfSorted);
                Object[] backingArrayOfSorted = sortedVertexData.elements();
                System.arraycopy(unsortedVertexData.elements(), 0, backingArrayOfSorted, oldSizeOfSorted, MERGE_FILLED_CAPACITY);
                Arrays.sort(backingArrayOfSorted);
                unsortedVertexData.clear();
            }
        }

        protected boolean hasVertex(String id) {
            return binarySearch(sortedVertexData, id) >= 0 || linearSearch(unsortedVertexData, id) >= 0;
        }

        protected VertexData getVertex(String id) {
            int index = binarySearch(sortedVertexData, id);
            if (index >= 0) return sortedVertexData.get(index);
            else {
                index = linearSearch(unsortedVertexData, id);
            }
            if (index >= 0) return unsortedVertexData.get(index);
            return null;
        }

        protected Iterator<VertexData> iterator() {
            return IteratorUtils.chainedIterator(sortedVertexData.iterator(), unsortedVertexData.iterator());
        }

    }

    protected static class VertexData implements Comparable<VertexData> {

        protected short masterPart;

        protected String id;
        protected Object[] featureValues;
        protected List<VertexData> outEdges;
        protected List<VertexData> inEdges;

        public VertexData(short masterPart, String id) {
            this.masterPart = masterPart;
            this.id = id;
        }

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

        protected void addOutEdge(VertexData out) {
            if (outEdges == null) outEdges = new ObjectArrayList<>(4);
            if (outEdges.size() == EDGE_LIST_CAPACITY_PER_PART) {
                int randomIndexRemoval = ThreadLocalRandom.current().nextInt(0, EDGE_LIST_CAPACITY_PER_PART);
                VertexData prev = outEdges.set(randomIndexRemoval, out);
                prev.inEdges.remove(prev.inEdges.lastIndexOf(this));
            } else outEdges.add(out);
        }

        protected void addInEdge(VertexData in) {
            if (inEdges == null) inEdges = new ObjectArrayList<>(4);
            if (inEdges.size() == EDGE_LIST_CAPACITY_PER_PART) {
                int randomIndexRemoval = ThreadLocalRandom.current().nextInt(0, EDGE_LIST_CAPACITY_PER_PART);
                VertexData prev = inEdges.set(randomIndexRemoval, in);
                prev.outEdges.remove(prev.outEdges.lastIndexOf(this));
            } else inEdges.add(in);
        }

        @Override
        public int compareTo(VertexData o) {
            return id.compareTo(o.id);
        }
    }

    protected class GraphViewImpl extends GraphView {

        protected final ObjectPool scopePool = new ObjectPool();

        protected long tensorMemoryInBytes = 0;

        public GraphViewImpl(GraphRuntimeContext runtimeContext) {
            super(runtimeContext);
            synchronized (part2VertexDataMap) {
                runtimeContext.getThisOperatorParts().forEach(part -> part2VertexDataMap.put(part, new PerPartVertexStorage()));
            }
        }

        private void updateMemory(NDArray ndArray){
            tensorMemoryInBytes+= ndArray.size() * 4;
        }

        @Override
        public long tensorMemoryUsageInMb() {
            return (long) (tensorMemoryInBytes / (1024.0 * 1024.0));
        }

        @Override
        public void addAttachedFeature(Feature feature) {
            if (feature.getAttachedElementType() == ElementType.VERTEX) {
                AttachedFeatureInfo attachedFeatureInfo = vertexFeatureInfoTable.computeIfAbsent(feature.getName(), (key) -> new AttachedFeatureInfo(feature, uniqueVertexFeatureCounter++));
                part2VertexDataMap.get(getRuntimeContext().getCurrentPart()).getVertex((String) feature.getAttachedElementId()).addOrUpdateFeature(feature, attachedFeatureInfo);
                if(feature.getValue() instanceof NDArray) updateMemory((NDArray) feature.getValue());
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
            part2VertexDataMap.get(getRuntimeContext().getCurrentPart()).addVertex(vertex);
        }

        @Override
        public void addEdge(DirectedEdge directedEdge) {
            PerPartVertexStorage storage = part2VertexDataMap.get(getRuntimeContext().getCurrentPart());
            VertexData srcData = storage.getVertex(directedEdge.getSrcId());
            VertexData destData = storage.getVertex(directedEdge.getDestId());
            srcData.addOutEdge(destData);
            destData.addInEdge(srcData);
        }

        @Override
        public void addHyperEdge(HyperEdge hyperEdge) {
            throw new IllegalStateException("NOT IMPLEMENTED");
        }

        @Override
        public void updateAttachedFeature(Feature feature, Feature memento) {
            if (feature.getAttachedElementType() == ElementType.VERTEX) {
                part2VertexDataMap.get(getRuntimeContext().getCurrentPart()).getVertex((String) feature.getAttachedElementId()).addOrUpdateFeature(feature, vertexFeatureInfoTable.get(feature.getName()));
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
            // Do nothing
        }

        @Override
        public void updateEdge(DirectedEdge directedEdge, DirectedEdge memento) {
            // Do nothing
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
            PerPartVertexStorage storage = part2VertexDataMap.get(getRuntimeContext().getCurrentPart());
            VertexData srcData = storage.getVertex(directedEdge.getSrcId());
            VertexData destData = storage.getVertex(directedEdge.getDestId());
            srcData.outEdges.remove(srcData.outEdges.lastIndexOf(destData));
            destData.inEdges.remove(destData.inEdges.lastIndexOf(srcData));

        }

        @Override
        public void deleteHyperEdge(HyperEdge hyperEdge) {
            throw new IllegalStateException("NOT IMPLEMENTED");
        }

        @Override
        public @Nullable Vertex getVertex(String vertexId) {
            short masterPart = part2VertexDataMap.get(getRuntimeContext().getCurrentPart()).getVertex(vertexId).masterPart;
            if (scopePool.isOpen()) {
                return scopePool.getVertex(vertexId, masterPart);
            }
            return new Vertex(vertexId, masterPart);
        }

        @Override
        public Iterable<Vertex> getVertices() {
            return () -> IteratorUtils.transformedIterator(
                    part2VertexDataMap.get(getRuntimeContext().getCurrentPart()).iterator(),
                    (data) -> {
                        VertexData vertexData = (VertexData) data;
                        if (scopePool.isOpen()) {
                            return scopePool.getVertex(vertexData.id, vertexData.masterPart);
                        }
                        return new Vertex(vertexData.id, vertexData.masterPart);
                    });
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
            VertexData vertexData = part2VertexDataMap.get(getRuntimeContext().getCurrentPart()).getVertex(vertex.getId());
            Iterator<DirectedEdge> inEdgeIterable = IteratorUtils.emptyIterator();
            Iterator<DirectedEdge> outEdgeIterable = IteratorUtils.emptyIterator();
            if (vertexData.outEdges != null && (edgeType == EdgeType.OUT || edgeType == EdgeType.BOTH)) {
                outEdgeIterable = IteratorUtils.transformedIterator(vertexData.outEdges.iterator(), (data) -> {
                    VertexData destData = (VertexData) data;
                    DirectedEdge e = getEdge(vertex.getId(), destData.id, null);
                    e.src = vertex;
                    return e;
                });
            }
            if (vertexData.inEdges != null && (edgeType == EdgeType.IN || edgeType == EdgeType.BOTH)) {
                outEdgeIterable = IteratorUtils.transformedIterator(vertexData.inEdges.iterator(), (data) -> {
                    VertexData srcData = (VertexData) data;
                    DirectedEdge e = getEdge(srcData.id, vertex.getId(), null);
                    e.dest = vertex;
                    return e;
                });
            }
            Iterator<DirectedEdge> res = IteratorUtils.chainedIterator(inEdgeIterable, outEdgeIterable);
            return () -> res;
        }

        @Override
        public int getIncidentEdgeCount(Vertex vertex, EdgeType edgeType) {
            int res = 0;
            VertexData vertexData = part2VertexDataMap.get(getRuntimeContext().getCurrentPart()).getVertex(vertex.getId());
            if (vertexData.outEdges != null && (edgeType == EdgeType.OUT || edgeType == EdgeType.BOTH))
                res += vertexData.outEdges.size();
            if (vertexData.inEdges != null && (edgeType == EdgeType.IN || edgeType == EdgeType.BOTH))
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
                value = part2VertexDataMap.get(getRuntimeContext().getCurrentPart()).getVertex(elementId).featureValues[featureInfo.position];
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
                return () -> IteratorUtils.transformedIterator(
                        IteratorUtils.filteredIterator(
                                part2VertexDataMap.get(getRuntimeContext().getCurrentPart()).iterator(),
                                data -> {
                                    VertexData vertexData = (VertexData) data;
                                    return vertexData.hasFeatureInPosition(attachedFeatureInfo.position);
                                }),
                        data -> {
                            VertexData vertexData = (VertexData) data;
                            return getAttachedVertexFeature(vertexData.id, featureName, vertexData.featureValues[attachedFeatureInfo.position], attachedFeatureInfo);
                        });
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
            return part2VertexDataMap.get(getRuntimeContext().getCurrentPart()).hasVertex(vertexId);
        }

        @Override
        public boolean containsAttachedFeature(ElementType elementType, Object elementId, String featureName) {
            if (elementType == ElementType.VERTEX) {
                if (!vertexFeatureInfoTable.containsKey(featureName) || !containsVertex((String) elementId))
                    return false;

                return part2VertexDataMap.get(getRuntimeContext().getCurrentPart()).getVertex((String) elementId).hasFeatureInPosition(vertexFeatureInfoTable.get(featureName).position);
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
                VertexData vertexData = part2VertexDataMap.get(getRuntimeContext().getCurrentPart()).getVertex((String) element.getId());
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
