//package storage;
//
//import ai.djl.ndarray.LifeCycleControl;
//import com.esotericsoftware.reflectasm.ConstructorAccess;
//import elements.*;
//import elements.enums.CacheFeatureContext;
//import elements.enums.EdgeType;
//import elements.enums.ElementType;
//import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
//import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
//import it.unimi.dsi.fastutil.shorts.Short2ObjectOpenHashMap;
//import org.apache.commons.collections.IteratorUtils;
//import org.apache.commons.lang3.NotImplementedException;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.api.java.tuple.Tuple3;
//import org.apache.flink.api.java.tuple.Tuple4;
//import org.apache.flink.runtime.state.taskshared.TaskSharedKeyedStateBackend;
//import org.apache.flink.streaming.api.operators.graph.interfaces.GraphRuntimeContext;
//import org.cliffc.high_scale_lib.NonBlockingHashMap;
//import org.jetbrains.annotations.Nullable;
//
//import java.util.Collections;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.Map;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.atomic.AtomicInteger;
//
//public final class DefaultStorage extends BaseStorage {
//
//    /**
//     * Master Part table for vertices. This table is shared across tasks as vertices unique
//     */
//    private final Map<String, Short> vertexMasterTable = new NonBlockingHashMap<>(1000);
//
//    /**
//     * Vertex feature -> [halo, constructor, index in the vertex table, LifeCycleManager]
//     */
//    private final Map<String, Tuple4<Boolean, ConstructorAccess<? extends Feature>, Integer, Boolean>> vertexFeatureInfo = new ConcurrentHashMap<>(10);
//
//
//    /**
//     * Counter for vertex features, the localVertex table will hold feature in a list according to this index
//     */
//    private final AtomicInteger vertexFeatureIndex = new AtomicInteger(0);
//
//    /**
//     * Vertex table described by vertex id and part so it is unique for tasks
//     * * (PartID, VertexId) -> [In-Edge-Table, Out-Edge-Table, [feature values]]
//     */
//    private final Map<Short, Map<String, Tuple3<ObjectOpenHashSet<Tuple2<String, String>>, ObjectOpenHashSet<Tuple2<String, String>>, Object[]>>> localVertexTable = new Short2ObjectOpenHashMap<>();
//
//
//    @Override
//    public synchronized void register(TaskSharedKeyedStateBackend<?> taskSharedStateBackend) {
//        super.register(taskSharedStateBackend);
//        for (Short thisOperatorPart : GraphRuntimeContext.CONTEXT_THREAD_LOCAL.get().getThisOperatorParts()) {
//            localVertexTable.put(thisOperatorPart, new HashMap<>());
//        }
//    }
//
//    @Override
//    public GraphView createGraphStorageView(GraphRuntimeContext runtimeContext) {
//        return new DefaultGraphView(runtimeContext);
//    }
//
//    @Override
//    public void clear() {}
//
//    /**
//     * Graph Object for each
//     */
//    public class DefaultGraphView extends GraphView {
//
//        private final Tuple2<String, String> reuse = new Tuple2<>();
//
//        private final ScopeWithElements scope = new ScopeWithElements();
//
//        public DefaultGraphView(GraphRuntimeContext runtimeContext) {
//            super(runtimeContext);
//        }
//
//        @Override
//        public void addAttachedFeature(Feature feature) {
//            if (feature.getAttachedElementType() == ElementType.VERTEX) {
//                vertexFeatureInfo.computeIfAbsent(feature.getName(), (key) -> Tuple4.of(feature.isHalo(), ConstructorAccess.get(feature.getClass()), vertexFeatureIndex.getAndIncrement(), (feature.value instanceof LifeCycleControl)));
//                localVertexTable.get(getRuntimeContext().getCurrentPart()).compute((String) feature.getAttachedElementId(), (key, val) -> {
//                    if (val.f2 == null) val.f2 = new Object[0];
//                    Tuple4<?, ?, Integer, Boolean> featureInfo = vertexFeatureInfo.get(feature.getName());
//                    if (featureInfo.f2 >= val.f2.length) {
//                        Object[] tmp = new Object[featureInfo.f2 + 1];
//                        System.arraycopy(val.f2, 0, tmp, 0, val.f2.length);
//                        val.f2 = tmp;
//                    }
//                    val.f2[featureInfo.f2] = feature.value;
//                    if (featureInfo.f3) ((LifeCycleControl) feature.value).delay();
//                    return val;
//                });
//                return true;
//            }
//            throw new NotImplementedException("Attached Features only implemented for Vertices");
//        }
//
//        @Override
//        public void addStandaloneFeature(Feature feature) {
//            throw new NotImplementedException("Delete not implemented yet");
//        }
//
//        @Override
//        public void addVertex(Vertex vertex) {
//            vertexMasterTable.putIfAbsent(vertex.getId(), vertex.getMasterPart());
//            localVertexTable.get(getRuntimeContext().getCurrentPart()).put(vertex.getId(), Tuple3.of(null, null, null));
//            return true;
//        }
//
//        @Override
//        public void addEdge(DirectedEdge directedEdge) {
//            localVertexTable.get(getRuntimeContext().getCurrentPart()).compute(directedEdge.getSrcId(), (key, val) -> {
//                if (val.f1 == null) val.f1 = new ObjectOpenHashSet<>();
//                val.f1.add(Tuple2.of(directedEdge.getDestId(), directedEdge.getAttribute()));
//                return val;
//            });
//            localVertexTable.get(getRuntimeContext().getCurrentPart()).compute(directedEdge.getDestId(), (key, val) -> {
//                if (val.f0 == null) val.f0 = new ObjectOpenHashSet<>();
//                val.f0.add(Tuple2.of(directedEdge.getSrcId(), directedEdge.getAttribute()));
//                return val;
//            });
//            return true;
//        }
//
//        @Override
//        public void addHyperEdge(HyperEdge hyperEdge) {
//            throw new NotImplementedException("Delete not implemented yet");
//        }
//
//        @Override
//        public void updateAttachedFeature(Feature feature, Feature memento) {
//            if (feature.getAttachedElementType() == ElementType.VERTEX) {
//                Tuple4<?, ?, Integer, Boolean> featureInfo = vertexFeatureInfo.get(feature.getName());
//                Object[] features = localVertexTable.get(getRuntimeContext().getCurrentPart()).get((String) feature.getAttachedElementId()).f2;
//                if (featureInfo.f3) {
//                    ((LifeCycleControl) features[featureInfo.f2]).resume();
//                    ((LifeCycleControl) feature.value).delay();
//                }
//                features[featureInfo.f2] = feature.value;
//                return true;
//            }
//            throw new NotImplementedException("Attached Features only implemented for Vertices");
//        }
//
//        @Override
//        public void updateStandaloneFeature(Feature feature, Feature memento) {
//            throw new NotImplementedException("Delete not implemented yet");
//        }
//
//        @Override
//        public void updateVertex(Vertex vertex, Vertex memento) {
//            return true;
//        }
//
//        @Override
//        public void updateEdge(DirectedEdge directedEdge, DirectedEdge memento) {
//            return true;
//        }
//
//        @Override
//        public void updateHyperEdge(HyperEdge hyperEdge, HyperEdge memento) {
//            throw new NotImplementedException("Delete not implemented yet");
//        }
//
//        @Override
//        public void deleteAttachedFeature(Feature feature) {
//            throw new NotImplementedException("Delete not implemented yet");
//        }
//
//        @Override
//        public void deleteStandaloneFeature(Feature feature) {
//            throw new NotImplementedException("Delete not implemented yet");
//        }
//
//        @Override
//        public void deleteVertex(Vertex vertex) {
//            throw new NotImplementedException("Delete not implemented yet");
//        }
//
//        @Override
//        public void deleteEdge(DirectedEdge directedEdge) {
//            throw new NotImplementedException("Delete not implemented yet");
//        }
//
//        @Override
//        public void deleteHyperEdge(HyperEdge hyperEdge) {
//            throw new NotImplementedException("Delete not implemented yet");
//        }
//
//        @Override
//        public @Nullable Vertex getVertex(String vertexId) {
//            short masterPart = vertexMasterTable.get(vertexId);
//            if (scope.isOpen()) {
//                Vertex v = scope.getCache().getVertex();
//                v.id = vertexId;
//                v.masterPart = masterPart;
//                return v;
//            }
//            return new Vertex(vertexId, masterPart);
//        }
//
//        @Override
//        public Iterable<Vertex> getVertices() {
//            try {
//                if (scope.isOpen()) {
//                    Vertex reusable = scope.getCache().getVertex();
//                    return () -> localVertexTable.get(getRuntimeContext().getCurrentPart()).keySet().stream().map(item -> {
//                        if (reusable.features != null) reusable.features.clear();
//                        reusable.masterPart = vertexMasterTable.get(item);
//                        reusable.id = item;
//                        return reusable;
//                    }).iterator();
//                }
//                return () -> localVertexTable.get(getRuntimeContext().getCurrentPart()).keySet().stream().map(item -> new Vertex(item, vertexMasterTable.get(item))).iterator();
//            } catch (NullPointerException e) {
//                return Collections.emptyList();
//            }
//        }
//
//        @Override
//        public @Nullable DirectedEdge getEdge(Tuple3<String, String, String> id) {
//            if (scope.isOpen()) {
//                DirectedEdge edge = scope.getCache().getDirectedEdge();
//                edge.id.f0 = id.f0;
//                edge.id.f1 = id.f1;
//                edge.id.f2 = id.f2;
//                return edge;
//            }
//            return new DirectedEdge(id.f0, id.f1, id.f2);
//        }
//
//        @Override
//        public Iterable<DirectedEdge> getIncidentEdges(Vertex vertex, EdgeType edge_type) {
//            try {
//                Tuple3<ObjectOpenHashSet<Tuple2<String, String>>, ObjectOpenHashSet<Tuple2<String, String>>, ?> vertexTable = localVertexTable.get(getRuntimeContext().getCurrentPart()).get(vertex.getId());
//                Iterator<DirectedEdge> srcEdgeIterable = Collections.emptyIterator();
//                Iterator<DirectedEdge> destEdgeIterable = Collections.emptyIterator();
//                if (edge_type == EdgeType.OUT || edge_type == EdgeType.BOTH) {
//                    if (vertexTable.f1 != null) {
//                        if (!scope.isOpen())
//                            destEdgeIterable = vertexTable.f1.stream().map(dstatt -> (new DirectedEdge(vertex.getId(), dstatt.f0, dstatt.f1))).iterator();
//                        else {
//                            DirectedEdge reusable = scope.getCache().getDirectedEdge();
//                            destEdgeIterable = vertexTable.f1.stream().map(dstatt -> {
//                                if (reusable.features != null) reusable.features.clear();
//                                reusable.src = null;
//                                reusable.dest = null;
//                                reusable.id.f0 = vertex.getId();
//                                reusable.id.f1 = dstatt.f0;
//                                reusable.id.f2 = dstatt.f1;
//                                return reusable;
//                            }).iterator();
//                        }
//                    }
//                }
//                if (edge_type == EdgeType.IN || edge_type == EdgeType.BOTH) {
//                    if (vertexTable.f0 != null) {
//                        if (!scope.isOpen())
//                            srcEdgeIterable = vertexTable.f0.stream().map(srcatt -> (new DirectedEdge(srcatt.f0, vertex.getId(), srcatt.f1))).iterator();
//                        else {
//                            DirectedEdge reusable = scope.getCache().getDirectedEdge();
//                            destEdgeIterable = vertexTable.f1.stream().map(srcAtt -> {
//                                if (reusable.features != null) reusable.features.clear();
//                                reusable.src = null;
//                                reusable.dest = null;
//                                reusable.id.f0 = srcAtt.f0;
//                                reusable.id.f1 = vertex.getId();
//                                reusable.id.f2 = srcAtt.f1;
//                                return reusable;
//                            }).iterator();
//                        }
//                    }
//                }
//                final Iterator<DirectedEdge> srcIteratorFinal = srcEdgeIterable;
//                final Iterator<DirectedEdge> destIteratorFinal = destEdgeIterable;
//                return () -> IteratorUtils.chainedIterator(srcIteratorFinal, destIteratorFinal);
//            } catch (Exception ignored) {
//                return Collections.emptyList();
//            }
//        }
//
//        @Override
//        public @Nullable HyperEdge getHyperEdge(String hyperEdgeId) {
//            throw new NotImplementedException("Delete not implemented yet");
//        }
//
//        @Override
//        public Iterable<HyperEdge> getIncidentHyperEdges(Vertex vertex) {
//            throw new NotImplementedException("Delete not implemented yet");
//        }
//
//        @Override
//        public @Nullable Feature getAttachedFeature(Tuple3<ElementType, Object, String> id) {
//            if (id.f0 == ElementType.VERTEX) {
//                Tuple4<Boolean, ConstructorAccess<? extends Feature>, Integer, ?> featureInfo = vertexFeatureInfo.get(id.f2);
//                Object value = localVertexTable.get(getRuntimeContext().getCurrentPart()).get((String) id.f1).f2[featureInfo.f2];
//                Feature feature = scope.isOpen() ? scope.getCache().getVertexFeature(id.f2) : featureInfo.f1.newInstance();
//                feature.value = value;
//                feature.id.f0 = id.f0;
//                feature.id.f1 = id.f1;
//                feature.id.f2 = id.f2;
//                feature.halo = featureInfo.f0;
//                return feature;
//            }
//            throw new NotImplementedException("Attached Features only implemented for Vertices");
//        }
//
//        @Override
//        public Iterable<Feature> getAttachedFeatures(ElementType elementType, String featureName) {
//            if (elementType == ElementType.VERTEX) {
//                Tuple4<Boolean, ConstructorAccess<? extends Feature>, Integer, ?> featureInfo = vertexFeatureInfo.get(featureName);
//                if (!scope.isOpen()) {
//                    return () -> localVertexTable.get(getRuntimeContext().getCurrentPart()).entrySet()
//                            .stream()
//                            .filter(entry -> entry.getValue().f2 != null && entry.getValue().f2.length > featureInfo.f2 && entry.getValue().f2[featureInfo.f2] != null)
//                            .map(entry -> {
//                                Feature feature = featureInfo.f1.newInstance();
//                                feature.value = entry.getValue().f2[featureInfo.f2];
//                                feature.id.f0 = elementType;
//                                feature.id.f1 = entry.getKey();
//                                feature.id.f2 = featureName;
//                                feature.halo = featureInfo.f0;
//                                return feature;
//                            }).iterator();
//                } else {
//                    Feature reusable = scope.getCache().getVertexFeature(featureName);
//                    return () -> localVertexTable.get(getRuntimeContext().getCurrentPart()).entrySet()
//                            .stream()
//                            .filter(entry -> entry.getValue().f2 != null && entry.getValue().f2.length > featureInfo.f2 && entry.getValue().f2[featureInfo.f2] != null)
//                            .map(entry -> {
//                                if (reusable.features != null) reusable.features.clear();
//                                reusable.element = null;
//                                reusable.value = entry.getValue().f2[featureInfo.f2];
//                                reusable.id.f0 = elementType;
//                                reusable.id.f1 = entry.getKey();
//                                reusable.id.f2 = featureName;
//                                reusable.halo = featureInfo.f0;
//                                return reusable;
//                            }).iterator();
//                }
//
//            }
//
//            throw new NotImplementedException("Attached Features only implemented for Vertices");
//
//        }
//
//        @Override
//        public @Nullable Feature getStandaloneFeature(String featureName) {
//            throw new NotImplementedException("Delete not implemented yet");
//        }
//
//        @Override
//        public Iterable<Feature> getStandaloneFeatures() {
//            throw new NotImplementedException("Not implemented yet");
//        }
//
//        @Override
//        public boolean containsVertex(String vertexId) {
//            try {
//                return localVertexTable.get(getRuntimeContext().getCurrentPart()).containsKey(vertexId);
//            } catch (NullPointerException e) {
//                return false;
//            }
//        }
//
//        @Override
//        public boolean containsAttachedFeature(Tuple3<ElementType, Object, String> id) {
//            try {
//                if (id.f0 == ElementType.VERTEX) {
//                    int index = vertexFeatureInfo.get(id.f2).f2;
//                    Object[] features = localVertexTable.get(getRuntimeContext().getCurrentPart()).get((String) id.f1).f2;
//                    return features != null && features.length > index && features[index] != null;
//                }
//                throw new NotImplementedException("Attached Features only implemented for Vertices");
//            } catch (NullPointerException ignored) {
//                return false;
//            }
//        }
//
//        @Override
//        public boolean containsStandaloneFeature(String featureName) {
//            throw new NotImplementedException("Delete not implemented yet");
//        }
//
//        @Override
//        public boolean containsEdge(Tuple3<String, String, String> id) {
//            try {
//                reuse.f0 = id.f1;
//                reuse.f1 = id.f2;
//                ObjectOpenHashSet<Tuple2<String, String>> outEdges = localVertexTable.get(getRuntimeContext().getCurrentPart()).get(id.f0).f1;
//                return outEdges != null && outEdges.contains(reuse);
//            } catch (NullPointerException e) {
//                return false;
//            }
//        }
//
//        @Override
//        public boolean containsHyperEdge(String hyperEdgeId) {
//            throw new NotImplementedException("Delete not implemented yet");
//        }
//
//        @Override
//        public ReuseScope openReuseScope() {
//            return scope.open();
//        }
//
//        @Override
//        public byte getOpenedScopeCount() {
//            return scope.openCount;
//        }
//
//        @Override
//        public void cacheAttachedFeatures(GraphElement element, CacheFeatureContext context) {
//            try {
//                if (element.getType() == ElementType.VERTEX) {
//                    Object[] features = localVertexTable.get(getRuntimeContext().getCurrentPart()).get((String) element.getId()).f2;
//                    for (Map.Entry<String, Tuple4<Boolean, ConstructorAccess<? extends Feature>, Integer, Boolean>> stringTuple3Entry : vertexFeatureInfo.entrySet()) {
//                        if ((!stringTuple3Entry.getValue().f0 && context == CacheFeatureContext.HALO) || (stringTuple3Entry.getValue().f0 && context == CacheFeatureContext.NON_HALO) || features.length <= stringTuple3Entry.getValue().f2 || features[stringTuple3Entry.getValue().f2] == null)
//                            continue; // This feature not needed to cache
//                        if (element.features != null && element.features.stream().anyMatch(item -> item.getName().equals(stringTuple3Entry.getKey())))
//                            continue; // Already cached check
//                        Feature feature = stringTuple3Entry.getValue().f1.newInstance();
//                        feature.id.f2 = stringTuple3Entry.getKey();
//                        feature.halo = stringTuple3Entry.getValue().f0;
//                        feature.value = features[stringTuple3Entry.getValue().f2];
//                        feature.setElement(element, false);
//                    }
//                }
//            } catch (Exception ignored) {
//                // pass
//            }
//        }
//
//        /**
//         * Reuse Scope with elements cache
//         */
//        public class ScopeWithElements extends ReuseScope {
//
//            private SmallElementsCache[] caches = new SmallElementsCache[0];
//
//            public SmallElementsCache getCache(){
//                throw new IllegalStateException("NOT USED NOW");
////                if(caches.length < openCount){
////                    SmallElementsCache[] tmpNew = new SmallElementsCache[openCount];
////                    System.arraycopy(caches,0, tmpNew,0, caches.length);
////                    caches = tmpNew;
////
////                }
////                if(caches[openCount - 1]==null) caches[openCount - 1] = new SmallElementsCache();
////                return caches[openCount - 1];
//            }
//        }
//
//        /**
//         * Cache with single element per {@link GraphElement}
//         */
//        public class SmallElementsCache {
//
//            private final Vertex vertex = new Vertex();
//
//            private final DirectedEdge directedEdge = new DirectedEdge();
//
//            private final Map<String, Feature> vertexFeatures = new Object2ObjectOpenHashMap<>(5);
//
//            public Vertex getVertex() {
//                if(vertex.features != null){
//                    vertex.features.forEach(feature -> feature.element = null);
//                    vertex.features.clear();
//                }
//                vertex.id = null;
//                vertex.masterPart = -1;
//                return vertex;
//            }
//
//            public DirectedEdge getDirectedEdge() {
//                if(directedEdge.features != null){
//                    directedEdge.features.forEach(feature -> feature.element = null);
//                    directedEdge.features.clear();
//                }
//                directedEdge.src = null;
//                directedEdge.dest = null;
//                directedEdge.id.f0 = null;
//                directedEdge.id.f1 = null;
//                directedEdge.id.f2 = null;
//                return directedEdge;
//            }
//
//            public Feature getVertexFeature(String featureName) {
//                final Feature vertexFeature = vertexFeatures.get(featureName);
//                if(vertexFeature != null){
//                    if(vertexFeature.features != null){
//                        vertexFeature.features.forEach(attached -> attached.element = null);
//                        vertexFeature.features.clear();
//                    }
//                    if(vertexFeature.element != null && vertexFeature.element.features != null){
//                        vertexFeature.element.features.removeIf(attached -> attached == vertexFeature);
//                    }
//                    vertexFeature.element = null;
//                    return vertexFeature;
//                }
//                Tuple4<Boolean, ConstructorAccess<? extends Feature>, Integer, Boolean> info = vertexFeatureInfo.get(featureName);
//                final Feature vertexFeatureNew = info.f1.newInstance();
//                vertexFeatureNew.id.f2 = featureName;
//                vertexFeatureNew.halo = info.f0;
//                vertexFeatures.put(featureName, vertexFeatureNew);
//                return vertexFeatureNew;
//            }
//        }
//    }
//
//
//}
