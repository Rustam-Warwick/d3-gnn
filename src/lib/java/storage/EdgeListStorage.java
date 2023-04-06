//package storage;
//
//import com.esotericsoftware.reflectasm.ConstructorAccess;
//import elements.*;
//import elements.enums.CacheFeatureContext;
//import elements.enums.EdgeType;
//import elements.enums.ElementType;
//import it.unimi.dsi.fastutil.ints.IntArrayList;
//import org.apache.commons.collections.IteratorUtils;
//import org.apache.commons.lang3.NotImplementedException;
//import org.apache.flink.api.common.state.ListState;
//import org.apache.flink.api.common.state.ListStateDescriptor;
//import org.apache.flink.api.common.state.MapState;
//import org.apache.flink.api.common.state.MapStateDescriptor;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.api.common.typeinfo.Types;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.api.java.tuple.Tuple3;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.runtime.state.graph.BaseStorage;
//import org.jetbrains.annotations.Nullable;
//
//import java.util.*;
//
///**
// * Optimizations for RocksDB
// * <p>
// *      vertex id translation added to vertexTable
// *      vertexTable, edgeList in state backend
// *      v2e should be in-memory and recovered on restart. We can implement a custom HashMap that takes long keys [src_id, dest_id] resulting with a string list of attributes
// *      features can remain like this or better to aggregate a caching layer moving elements to backend on TTL
// *      More useful if feature tables are separated for vertices, edges and etc...
// * </p>
// */
//public class EdgeListStorage extends BaseStorage {
//
//    public transient MapState<String, Short> vertexTable;
//
//    public transient ListState<Tuple3<String, String, String>> edgeList;
//
//    public transient MapState<String, Tuple2<IntArrayList, IntArrayList>> v2e; // Src, destination lists with indices to edgeList, this should be in-memory
//
//    public transient Map<Tuple2<String, ElementType>, Tuple3<MapState<Object, Object>, Boolean, ConstructorAccess<? extends Feature>>> attFeatureTable;
//
//    public transient Tuple2<String, ElementType> reuse;
//
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        super.open(parameters);
//        MapStateDescriptor<String, Short> vertexTableDescriptor = new MapStateDescriptor<>("vertexTable", Types.STRING, Types.SHORT);
//        ListStateDescriptor<Tuple3<String, String, String>> edgeListTableDesc = new ListStateDescriptor<>("edgeList", Types.TUPLE(Types.STRING, Types.STRING, Types.STRING));
//        MapStateDescriptor<String, Tuple2<IntArrayList, IntArrayList>> v2eTableDescriptor = new MapStateDescriptor<>("v2e", Types.STRING, Types.TUPLE(TypeInformation.of(IntArrayList.class), TypeInformation.of(IntArrayList.class)));
//        vertexTable = getRuntimeContext().getMapState(vertexTableDescriptor);
//        edgeList = getRuntimeContext().getListState(edgeListTableDesc);
//        v2e = getRuntimeContext().getMapState(v2eTableDescriptor);
//        attFeatureTable = new HashMap<>();
//        reuse = new Tuple2<>();
//    }
//
//    @Override
//    public boolean addAttachedFeature(Feature<?, ?> feature) {
//        try {
//            reuse.f0 = feature.getName();
//            reuse.f1 = feature.getAttachedElementType();
//            if (!attFeatureTable.containsKey(reuse)) {
//                ConstructorAccess<? extends Feature> tmpConstructor = ConstructorAccess.get(feature.getClass());
//                MapState<Object, Object> tmpFt = getRuntimeContext().getMapState(new MapStateDescriptor<Object, Object>(reuse.f0 + "att" + reuse.f1.ordinal(), (TypeInformation<Object>) Types.GENERIC(feature.getAttachedElementId().getClass()), (TypeInformation<Object>) feature.getValueTypeInfo()));
//                attFeatureTable.put(reuse.copy(), new Tuple3<>(tmpFt, feature.isHalo(), tmpConstructor));
//            }
//            attFeatureTable.get(reuse).f0.put(feature.id.f1, feature.value);
//            return true;
//        } catch (Exception e) {
//            e.printStackTrace();
//            return false;
//        }
//    }
//
//    @Override
//    public boolean addStandaloneFeature(Feature<?, ?> feature) {
//        throw new NotImplementedException("Not implemented");
//    }
//
//    @Override
//    public boolean addVertex(Vertex vertex) {
//        try {
//            vertexTable.put(vertex.getId(), vertex.getMasterPart());
//            return true;
//        } catch (Exception e) {
//            LOG.error(e.getMessage());
//            return false;
//        }
//    }
//
//    @Override
//    public boolean addEdge(DirectedEdge directedEdge) {
//        try {
//            int index = ((List<?>) edgeList.get()).size();
//            edgeList.aggregate(directedEdge.getId());
//            Tuple2<IntArrayList, IntArrayList> srcTuple = v2e.get(directedEdge.getSrcId());
//            Tuple2<IntArrayList, IntArrayList> destTuple = v2e.get(directedEdge.getDestId());
//            if (srcTuple == null) srcTuple = Tuple2.of(new IntArrayList(2), new IntArrayList());
//            if (destTuple == null) destTuple = Tuple2.of(new IntArrayList(2), new IntArrayList());
//            srcTuple.f1.aggregate(index);
//            destTuple.f0.aggregate(index);
//            v2e.put(directedEdge.getSrcId(), srcTuple);
//            v2e.put(directedEdge.getDestId(), destTuple);
//            return true;
//        } catch (Exception e) {
//            LOG.error(e.getMessage());
//            return false;
//        }
//    }
//
//    @Override
//    public boolean addHyperEdge(HyperEdge hyperEdge) {
//        throw new NotImplementedException("Not implemented");
//    }
//
//    @Override
//    public boolean updateAttachedFeature(Feature<?, ?> feature, Feature<?, ?> memento) {
//        try {
//            reuse.f0 = feature.getName();
//            reuse.f1 = feature.getAttachedElementType();
//            attFeatureTable.get(reuse).f0.put(feature.getAttachedElementId(), feature.value);
//            return true;
//        } catch (Exception e) {
//            e.printStackTrace();
//            return false;
//        }
//    }
//
//    @Override
//    public boolean updateStandaloneFeature(Feature<?, ?> feature, Feature<?, ?> memento) {
//        throw new NotImplementedException("Not implemented");
//    }
//
//    @Override
//    public boolean updateVertex(Vertex vertex, Vertex memento) {
//        return true;
//    }
//
//    @Override
//    public boolean updateEdge(DirectedEdge directedEdge, DirectedEdge memento) {
//        return true;
//    }
//
//    @Override
//    public boolean updateHyperEdge(HyperEdge hyperEdge, HyperEdge memento) {
//        throw new NotImplementedException("Not implemented");
//    }
//
//    @Override
//    public boolean deleteAttachedFeature(Feature<?, ?> feature) {
//        throw new NotImplementedException("Not implemented");
//    }
//
//    @Override
//    public boolean deleteStandaloneFeature(Feature<?, ?> feature) {
//        throw new NotImplementedException("Not implemented");
//    }
//
//    @Override
//    public boolean deleteVertex(Vertex vertex) {
//        throw new NotImplementedException("Not implemented");
//    }
//
//    @Override
//    public boolean deleteEdge(DirectedEdge directedEdge) {
//        throw new NotImplementedException("Not implemented");
//    }
//
//    @Override
//    public boolean deleteHyperEdge(HyperEdge hyperEdge) {
//        throw new NotImplementedException("Not implemented");
//    }
//
//    @Override
//    public @Nullable Vertex getVertex(String vertexId) {
//        try {
//            short masterPart = vertexTable.get(vertexId);
//            return new Vertex(vertexId, masterPart);
//        } catch (Exception e) {
//            LOG.error(e.getMessage());
//            return null;
//        }
//    }
//
//    @Override
//    public Iterable<Vertex> getVertices() {
//        try {
//            Iterator<Map.Entry<String, Short>> entries = vertexTable.entries().iterator();
//            return () -> IteratorUtils.transformedIterator(entries, (entry) -> {
//                Map.Entry<String, Short> tmp = (Map.Entry<String, Short>) entry;
//                return new Vertex(tmp.getKey(), tmp.getValue());
//            });
//        } catch (Exception e) {
//            return Collections.emptyList();
//        }
//    }
//
//    @Override
//    public @Nullable DirectedEdge getEdge(Tuple3<String, String, String> ids) {
//        DirectedEdge edge = new DirectedEdge();
//        edge.id.f0 = ids.f0;
//        edge.id.f1 = ids.f1;
//        edge.id.f2 = ids.f2;
//        return edge;
//    }
//
//    @Override
//    public Iterable<DirectedEdge> getIncidentEdges(Vertex vertex, EdgeType edgeType) {
//        try {
//            if (!v2e.contains(vertex.getId())) return Collections.emptyList();
//            Iterator<DirectedEdge> outEdgesIter = Collections.emptyIterator();
//            Iterator<DirectedEdge> inEdgesIter = Collections.emptyIterator();
//            List<Tuple3<String, String, String>> edges = (List<Tuple3<String, String, String>>) edgeList.get();
//            Tuple2<IntArrayList, IntArrayList> vTuple = v2e.get(vertex.getId());
//            if (edgeType == EdgeType.OUT || edgeType == EdgeType.BOTH) {
//                outEdgesIter = IteratorUtils.transformedIterator(vTuple.f1.iterator(), (v) -> {
//                    int index = (int) v;
//                    Tuple3<String, String, String> eId = edges.get(index);
//                    DirectedEdge edge = new DirectedEdge();
//                    edge.id.f0 = eId.f0;
//                    edge.id.f1 = eId.f1;
//                    edge.id.f2 = eId.f2;
//                    return edge;
//                });
//            }
//            if (edgeType == EdgeType.IN || edgeType == EdgeType.BOTH) {
//                inEdgesIter = IteratorUtils.transformedIterator(vTuple.f0.iterator(), (v) -> {
//                    int index = (int) v;
//                    Tuple3<String, String, String> eId = edges.get(index);
//                    DirectedEdge edge = new DirectedEdge();
//                    edge.id.f0 = eId.f0;
//                    edge.id.f1 = eId.f1;
//                    edge.id.f2 = eId.f2;
//                    return edge;
//                });
//            }
//            final Iterator<DirectedEdge> finalOutEdgesIter = outEdgesIter;
//            final Iterator<DirectedEdge> finalInEdgesIter = inEdgesIter;
//            return () -> IteratorUtils.chainedIterator(finalOutEdgesIter, finalInEdgesIter);
//        } catch (Exception e) {
//            LOG.error(e.getMessage());
//            return Collections.emptyList();
//        }
//    }
//
//    @Override
//    public @Nullable HyperEdge getHyperEdge(String hyperEdgeId) {
//        throw new NotImplementedException("Not implemented");
//    }
//
//    @Override
//    public Iterable<HyperEdge> getIncidentHyperEdges(Vertex vertex) {
//        throw new NotImplementedException("Not implemented");
//    }
//
//    @Override
//    public @Nullable Feature<?, ?> getAttachedVertexFeature(Tuple3<ElementType, Object, String> ids) {
//        try {
//            reuse.f0 = ids.f2;
//            reuse.f1 = ids.f0;
//            Tuple3<MapState<Object, Object>, Boolean, ConstructorAccess<? extends Feature>> featureProps = attFeatureTable.get(reuse);
//            Feature feature = featureProps.f2.newInstance();
//            feature.id.f0 = ids.f0;
//            feature.id.f1 = ids.f1;
//            feature.id.f2 = ids.f2;
//            feature.halo = featureProps.f1;
//            feature.value = featureProps.f0.get(ids.f1);
//            return feature;
//        } catch (Exception e) {
//            LOG.error(e.getMessage());
//            return null;
//        }
//    }
//
//    @Override
//    public @Nullable Feature<?, ?> getStandaloneFeature(Tuple3<ElementType, Object, String> ids) {
//        throw new NotImplementedException("Not implemented");
//    }
//
//    @Override
//    public boolean containsVertex(String vertexId) {
//        try {
//            return vertexTable.contains(vertexId);
//        } catch (Exception e) {
//            LOG.error(e.getMessage());
//            return false;
//        }
//    }
//
//    @Override
//    public boolean containsAttachedFeature(Tuple3<ElementType, Object, String> ids) {
//        try {
//            reuse.f0 = ids.f2;
//            reuse.f1 = ids.f0;
//            return attFeatureTable.containsKey(reuse) && attFeatureTable.get(reuse).f0.contains(ids.f1);
//        } catch (Exception e) {
//            LOG.error(e.getMessage());
//            return false;
//        }
//    }
//
//    @Override
//    public boolean containsStandaloneFeature(Tuple3<ElementType, Object, String> ids) {
//        throw new NotImplementedException("Not implemented");
//    }
//
//    @Override
//    public boolean containsEdge(Tuple3<String, String, String> ids) {
//        try {
//            return ((List<Tuple3<String, String, String>>) edgeList.get()).contains(ids);
//        } catch (Exception e) {
//            LOG.error(e.getMessage());
//            return false;
//        }
//    }
//
//    @Override
//    public boolean containsHyperEdge(String hyperEdgeId) {
//        throw new NotImplementedException("Not implemented");
//    }
//
//    @Override
//    public void cacheAttachedFeatures(GraphElement element, CacheFeatureContext context) {
//        try {
//            for (Map.Entry<Tuple2<String, ElementType>, Tuple3<MapState<Object, Object>, Boolean, ConstructorAccess<? extends Feature>>> tuple2Tuple3Entry : attFeatureTable.entrySet()) {
//                if ((!tuple2Tuple3Entry.getValue().f1 && context == CacheFeatureContext.HALO) || (tuple2Tuple3Entry.getValue().f1 && context == CacheFeatureContext.NON_HALO) || tuple2Tuple3Entry.getKey().f1 != element.getType() || !tuple2Tuple3Entry.getValue().f0.contains(element.getId()))
//                    continue; // This feature not needed to cache
//                if (element.features != null && element.features.stream().anyMatch(item -> item.getName().equals(tuple2Tuple3Entry.getKey().f0)))
//                    return; // Already cached check
//                Feature feature = tuple2Tuple3Entry.getValue().f2.newInstance();
//                feature.id.f2 = tuple2Tuple3Entry.getKey().f0;
//                feature.halo = tuple2Tuple3Entry.getValue().f1;
//                feature.value = tuple2Tuple3Entry.getValue().f0.get(element.getId());
//                feature.setElement(element, false);
//            }
//        } catch (Exception e) {
//            LOG.error(e.getMessage());
//        }
//    }
//
//}
