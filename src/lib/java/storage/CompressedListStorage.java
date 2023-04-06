//package storage;
//
//import com.esotericsoftware.reflectasm.ConstructorAccess;
//import elements.*;
//import elements.enums.CacheFeatureContext;
//import elements.enums.EdgeType;
//import elements.enums.ElementType;
//import org.apache.commons.collections.IteratorUtils;
//import org.apache.commons.lang3.NotImplementedException;
//import org.apache.flink.api.common.state.MapState;
//import org.apache.flink.api.common.state.MapStateDescriptor;
//import org.apache.flink.api.common.typeinfo.TypeHint;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.api.common.typeinfo.Types;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.api.java.tuple.Tuple3;
//import org.apache.flink.configuration.Configuration;
//import org.jetbrains.annotations.Nullable;
//
//import java.util.*;
//
///**
// * Storage for a more compressed representation of data, however fetching data has to create new classes which can have some overheads.
// */
//@Deprecated
//public class CompressedListStorage extends BaseStorage {
//
//    public transient MapState<String, Short> vertexTable;
//
//    public transient MapState<String, Set<Tuple2<String, String>>> eOutTable;
//
//    public transient MapState<String, Set<Tuple2<String, String>>> eInTable;
//
//    public transient MapState<String, List<String>> v2HEdge;
//
//    public transient MapState<String, Tuple2<Short, List<String>>> hyperEdges;
//
//    public transient Map<Tuple2<String, ElementType>, Tuple3<MapState<String, Object>, Boolean, ConstructorAccess<? extends Feature>>> attFeatureTable;
//
//    public transient Tuple2<String, ElementType> reuse;
//
//    public transient Tuple2<String, String> reuse2;
//
//
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        vertexTable = getRuntimeContext().getMapState(new MapStateDescriptor<>("vertexTable", String.class, Short.class));
//        eOutTable = getRuntimeContext().getMapState(new MapStateDescriptor<>("eOutTable", Types.STRING, TypeInformation.of(new TypeHint<Set<Tuple2<String, String>>>() {
//        })));
//        eInTable = getRuntimeContext().getMapState(new MapStateDescriptor<>("eInTable", Types.STRING, TypeInformation.of(new TypeHint<Set<Tuple2<String, String>>>() {
//        })));
//        v2HEdge = getRuntimeContext().getMapState(new MapStateDescriptor<String, List<String>>("v2HEdge", Types.STRING, Types.LIST(Types.STRING)));
//        hyperEdges = getRuntimeContext().getMapState(new MapStateDescriptor<String, Tuple2<Short, List<String>>>("hyperEdges", Types.STRING, Types.TUPLE(Types.SHORT, Types.LIST(Types.STRING))));
//        attFeatureTable = new HashMap<>(1 << 3);
//        reuse = new Tuple2<>();
//        reuse2 = new Tuple2<>();
//        super.open(parameters);
//    }
//
//    @Override
//    public void close() throws Exception {
//        super.close();
//        for (Map.Entry<Tuple2<String, ElementType>, Tuple3<MapState<String, Object>, Boolean, ConstructorAccess<? extends Feature>>> tuple2Tuple3Entry : attFeatureTable.entrySet()) {
//            Feature tmpF = tuple2Tuple3Entry.getValue().f2.newInstance();
//            tuple2Tuple3Entry.getValue().f0.values().forEach(item -> {
//                tmpF.value = item;
//                tmpF.resume();
//            });
//        }
//    }
//
//    @Override
//    public boolean addAttachedFeature(Feature<?, ?> feature) {
//        try {
//            reuse.f0 = feature.getName();
//            reuse.f1 = feature.id.f0;
//            if (!attFeatureTable.containsKey(reuse)) {
//                ConstructorAccess<? extends Feature> tmpConstructor = ConstructorAccess.get(feature.getClass());
//                MapState<String, Object> tmpFt = getRuntimeContext().getMapState(new MapStateDescriptor<String, Object>(reuse.f0 + "att" + reuse.f1.ordinal(), Types.STRING, (TypeInformation<Object>) feature.getValueTypeInfo()));
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
//            Set<Tuple2<String, String>> eOut = eOutTable.get(directedEdge.getSrcId());
//            if (eOut == null) eOut = new HashSet<>(10);
//            Set<Tuple2<String, String>> eIn = eInTable.get(directedEdge.getDestId());
//            if (eIn == null) eIn = new HashSet<>(10);
//            eOut.aggregate(Tuple2.of(directedEdge.getDestId(), directedEdge.getAttribute()));
//            eIn.aggregate(Tuple2.of(directedEdge.getSrcId(), directedEdge.getAttribute()));
//            eOutTable.put(directedEdge.getSrcId(), eOut);
//            eInTable.put(directedEdge.getDestId(), eIn);
//            return true;
//        } catch (Exception e) {
//            LOG.error(e.getMessage());
//            return false;
//        }
//    }
//
//    @Override
//    public boolean addHyperEdge(HyperEdge hyperEdge) {
//        try {
//            hyperEdges.put(hyperEdge.getId(), Tuple2.of(hyperEdge.getMasterPart(), hyperEdge.getVertexIds()));
//            for (String vertexId : hyperEdge.getVertexIds()) {
//                List<String> tmp = v2HEdge.get(vertexId);
//                if (tmp == null) tmp = new ArrayList<>(10);
//                tmp.aggregate(hyperEdge.getId());
//                v2HEdge.put(vertexId, tmp);
//            }
//            return true;
//        } catch (Exception e) {
//            e.printStackTrace();
//            return false;
//        }
//    }
//
//    @Override
//    public boolean updateAttachedFeature(Feature<?, ?> feature, Feature<?, ?> memento) {
//        try {
//            reuse.f0 = feature.getName();
//            reuse.f1 = feature.id.f0;
//            attFeatureTable.get(reuse).f0.put(feature.id.f1, feature.value);
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
//        try {
//            hyperEdges.put(hyperEdge.getId(), Tuple2.of(hyperEdge.getMasterPart(), hyperEdge.getVertexIds()));
//            for (String vertexId : memento.getVertexIds()) {
//                List<String> tmp = v2HEdge.get(vertexId);
//                if (tmp == null) tmp = new ArrayList<>(10);
//                tmp.aggregate(hyperEdge.getId());
//                v2HEdge.put(vertexId, tmp);
//            }
//            return true;
//        } catch (Exception e) {
//            e.printStackTrace();
//            return false;
//        }
//    }
//
//    @Override
//    public boolean deleteAttachedFeature(Feature<?, ?> feature) {
//        throw new NotImplementedException("Not implemneted");
//    }
//
//    @Override
//    public boolean deleteStandaloneFeature(Feature<?, ?> feature) {
//        throw new NotImplementedException("Not implemented");
//    }
//
//    @Override
//    public boolean deleteVertex(Vertex vertex) {
//        throw new NotImplementedException("Not implemneted");
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
//    @Nullable
//    @Override
//    public Vertex getVertex(String vertexId) {
//        try {
//            short masterPart = vertexTable.get(vertexId);
//            return new Vertex(vertexId, masterPart);
//        } catch (Exception e) {
//            e.printStackTrace();
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
//    @Nullable
//    @Override
//    public DirectedEdge getEdge(String srcId, String destId, @Nullable String attributeId, @Nullable String edgeId) {
//        try {
//            return new DirectedEdge(srcId, destId, attributeId);
//        } catch (Exception e) {
//            e.printStackTrace();
//            return null;
//        }
//    }
//
//    @Override
//    public Iterable<DirectedEdge> getEdges(String srcId, String destId) {
//        throw new NotImplementedException("Not Implemments");
//    }
//
//    @Override
//    public Iterable<DirectedEdge> getIncidentEdges(Vertex vertex, EdgeType edge_type) {
//        try {
//            Iterator<DirectedEdge> outEdgesIter = Collections.emptyIterator();
//            Iterator<DirectedEdge> inEdgesIter = Collections.emptyIterator();
//            if (edge_type == EdgeType.OUT || edge_type == EdgeType.BOTH) {
//                if (eOutTable.contains(vertex.getId())) {
//                    outEdgesIter = IteratorUtils.transformedIterator(eOutTable.get(vertex.getId()).iterator(), (v) -> {
//                        Tuple2<String, String> destIdAndAttribute = (Tuple2<String, String>) v;
//                        DirectedEdge edge = new DirectedEdge(vertex.getId(), destIdAndAttribute.f0, destIdAndAttribute.f1);
//                        edge.src = vertex;
//                        return edge;
//                    });
//                }
//            }
//            if (edge_type == EdgeType.IN || edge_type == EdgeType.BOTH) {
//                if (eInTable.contains(vertex.getId())) {
//                    inEdgesIter = IteratorUtils.transformedIterator(eInTable.get(vertex.getId()).iterator(), (v) -> {
//                        Tuple2<String, String> srcIdAndAttribute = (Tuple2<String, String>) v;
//                        DirectedEdge edge = new DirectedEdge(srcIdAndAttribute.f0, vertex.getId(), srcIdAndAttribute.f1);
//                        edge.dest = vertex;
//                        return edge;
//                    });
//                }
//            }
//
//            Iterator<DirectedEdge> finalOutEdgesIter = outEdgesIter;
//            Iterator<DirectedEdge> finalInEdgesIter = inEdgesIter;
//            return () -> IteratorUtils.chainedIterator(finalOutEdgesIter, finalInEdgesIter);
//
//        } catch (Exception e) {
//            e.printStackTrace();
//            return Collections.emptyList();
//        }
//    }
//
//    @Override
//    public HyperEdge getHyperEdge(String hyperEdgeId) {
//        try {
//            Tuple2<Short, List<String>> tmp = hyperEdges.get(hyperEdgeId);
//            return new HyperEdge(hyperEdgeId, tmp.f1, tmp.f0);
//        } catch (Exception e) {
//            e.printStackTrace();
//            return null;
//        }
//    }
//
//    @Override
//    public Iterable<HyperEdge> getIncidentHyperEdges(Vertex vertex) {
//        try {
//            List<String> vertices = v2HEdge.get(vertex.getId());
//            if (vertices == null) return Collections.emptyList();
//            return () -> IteratorUtils.transformedIterator(vertices.listIterator(), hyperEdgeId -> getHyperEdge((String) hyperEdgeId));
//        } catch (Exception e) {
//            e.printStackTrace();
//            return null;
//        }
//    }
//
//    @Nullable
//    @Override
//    public Feature<?, ?> getAttachedVertexFeature(ElementType attachedType, String attachedId, String featureName, @Nullable String featureId) {
//        try {
//            reuse.f0 = featureName;
//            reuse.f1 = attachedType;
//            Tuple3<MapState<String, Object>, Boolean, ConstructorAccess<? extends Feature>> tmp = attFeatureTable.get(reuse);
//            Feature feature = tmp.f2.newInstance();
//            feature.id.f0 = attachedType;
//            feature.id.f1 = attachedId;
//            feature.id.f2 = featureName;
//            feature.halo = tmp.f1;
//            feature.value = tmp.f0.get(attachedId);
//            return feature;
//        } catch (Exception e) {
//            e.printStackTrace();
//            return null;
//        }
//    }
//
//    @Nullable
//    @Override
//    public Feature<?, ?> getStandaloneFeature(String featureName) {
//        throw new NotImplementedException("Not Implemented");
//    }
//
//    @Override
//    public boolean containsVertex(String vertexId) {
//        try {
//            return vertexTable.contains(vertexId);
//        } catch (Exception e) {
//            e.printStackTrace();
//            return false;
//        }
//    }
//
//    @Override
//    public boolean containsAttachedFeature(ElementType attachedType, String attachedId, String featureName, @Nullable String featureId) {
//        try {
//            reuse.f0 = featureName;
//            reuse.f1 = attachedType;
//            if (!attFeatureTable.containsKey(reuse)) return false;
//            return attFeatureTable.get(reuse).f0.contains(attachedId);
//        } catch (Exception e) {
//            e.printStackTrace();
//            return false;
//        }
//    }
//
//    @Override
//    public boolean containsStandaloneFeature(String featureName) {
//        throw new NotImplementedException("Not Implemented");
//    }
//
//    @Override
//    public boolean containsEdge(String srcId, String destId, @Nullable String attributeId, @Nullable String edgeId) {
//        try {
//            reuse2.f0 = destId;
//            reuse2.f1 = attributeId;
//            return eOutTable.contains(srcId) && eOutTable.get(srcId).contains(reuse2);
//        } catch (Exception e) {
//            e.printStackTrace();
//            return false;
//        }
//    }
//
//    @Override
//    public boolean containsHyperEdge(String hyperEdgeId) {
//        try {
//            return hyperEdges.contains(hyperEdgeId);
//        } catch (Exception e) {
//            e.printStackTrace();
//            return false;
//        }
//    }
//
//    @Override
//    public void cacheAttachedFeatures(GraphElement element, CacheFeatureContext context) {
//        try {
//            String elId = element.getId();
//            for (Map.Entry<Tuple2<String, ElementType>, Tuple3<MapState<String, Object>, Boolean, ConstructorAccess<? extends Feature>>> tuple2Tuple3Entry : attFeatureTable.entrySet()) {
//                if ((!tuple2Tuple3Entry.getValue().f1 && context == CacheFeatureContext.HALO) || (tuple2Tuple3Entry.getValue().f1 && context == CacheFeatureContext.NON_HALO) || tuple2Tuple3Entry.getKey().f1 != element.getType() || !tuple2Tuple3Entry.getValue().f0.contains(elId))
//                    continue;
//                if (element.features != null && element.features.stream().anyMatch(item -> item.getName().equals(tuple2Tuple3Entry.getKey().f0)))
//                    return;
//                Feature<?, ?> feature = getAttachedVertexFeature(element.getType(), elId, tuple2Tuple3Entry.getKey().f0, null);
//                feature.setElement(element, false);
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//}
