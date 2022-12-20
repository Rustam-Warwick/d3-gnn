//package storage;
//
//import ai.djl.ndarray.LifeCycleControl;
//import elements.*;
//import elements.enums.CacheFeatureContext;
//import elements.enums.EdgeType;
//import elements.enums.ElementType;
//import org.apache.commons.collections.IteratorUtils;
//import org.apache.flink.api.common.state.MapState;
//import org.apache.flink.api.common.state.MapStateDescriptor;
//import org.apache.flink.api.common.typeinfo.Types;
//import org.apache.flink.configuration.Configuration;
//import org.jetbrains.annotations.Nullable;
//import typeinfo.recursivepojoinfo.RecursivePojoTypeInfoFactory;
//
//import java.util.*;
//
///**
// * @implNote Only use if using InMemoryStateBackend backend
// */
//public class FlatObjectStorage extends BaseStorage {
//    protected MapState<String, Vertex> vertexTable;
//    protected MapState<String, Feature<?, ?>> attachedFeatureTable;
//    protected MapState<String, Feature<?, ?>> independentFeatureTable;
//    protected MapState<String, Map<String, List<DirectedEdge>>> outEdgeTable;
//    protected MapState<String, Map<String, List<DirectedEdge>>> inEdgeTable;
//    protected MapState<String, DirectedEdge> edgeTable;
//    protected MapState<String, HyperEdge> hyperEdgeTable;
//    protected MapState<String, List<HyperEdge>> vertex2HyperEdge;
//
//    public FlatObjectStorage() {
//
//    }
//
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        MapStateDescriptor<String, Vertex> vertexTableDesc = new MapStateDescriptor<>("vertexTable", Types.STRING, new RecursivePojoTypeInfoFactory<Vertex>().createTypeInfo(Vertex.class, null));
//        MapStateDescriptor<String, Feature<?, ?>> featureTableDesc = new MapStateDescriptor<>("attachedFeatureTable", Types.STRING, new RecursivePojoTypeInfoFactory<Feature<?, ?>>().createTypeInfo(Feature.class, null));
//        MapStateDescriptor<String, Feature<?, ?>> independentFeatureTableDesc = new MapStateDescriptor<>("independentFeatureTable", Types.STRING, new RecursivePojoTypeInfoFactory<Feature<?, ?>>().createTypeInfo(Feature.class, null));
//        MapStateDescriptor<String, DirectedEdge> edgeTableDesc = new MapStateDescriptor<>("edgeTable", Types.STRING, new RecursivePojoTypeInfoFactory<DirectedEdge>().createTypeInfo(DirectedEdge.class, null));
//        MapStateDescriptor<String, HyperEdge> hyperEdgeTableDesc = new MapStateDescriptor<>("hyperEdgeTable", Types.STRING, new RecursivePojoTypeInfoFactory<HyperEdge>().createTypeInfo(HyperEdge.class, null));
//        MapStateDescriptor<String, Map<String, List<DirectedEdge>>> outEdgeTableDesc = new MapStateDescriptor<>("outEdgeTable", Types.STRING, Types.MAP(Types.STRING, Types.LIST(new RecursivePojoTypeInfoFactory<DirectedEdge>().createTypeInfo(DirectedEdge.class, null))));
//        MapStateDescriptor<String, Map<String, List<DirectedEdge>>> inEdgeTableDesc = new MapStateDescriptor<>("inEdgeTable", Types.STRING, Types.MAP(Types.STRING, Types.LIST(new RecursivePojoTypeInfoFactory<DirectedEdge>().createTypeInfo(DirectedEdge.class, null))));
//        MapStateDescriptor<String, List<HyperEdge>> vertex2HyperEdgeDesc = new MapStateDescriptor<>("vertex2HyperEdge", Types.STRING, Types.LIST(new RecursivePojoTypeInfoFactory<HyperEdge>().createTypeInfo(HyperEdge.class, null)));
//
//        edgeTable = getRuntimeContext().getMapState(edgeTableDesc);
//        hyperEdgeTable = getRuntimeContext().getMapState(hyperEdgeTableDesc);
//        outEdgeTable = getRuntimeContext().getMapState(outEdgeTableDesc);
//        inEdgeTable = getRuntimeContext().getMapState(inEdgeTableDesc);
//        vertexTable = getRuntimeContext().getMapState(vertexTableDesc);
//        attachedFeatureTable = getRuntimeContext().getMapState(featureTableDesc);
//        independentFeatureTable = getRuntimeContext().getMapState(independentFeatureTableDesc);
//        vertex2HyperEdge = getRuntimeContext().getMapState(vertex2HyperEdgeDesc);
//        super.open(parameters);
//    }
//
//    @Override
//    public void close() throws Exception {
//        attachedFeatureTable.values().forEach(LifeCycleControl::resume);
//        independentFeatureTable.values().forEach(LifeCycleControl::resume);
//        super.close();
//    }
//
//    @Override
//    public boolean addStandaloneFeature(Feature<?, ?> feature) {
//        try {
//            independentFeatureTable.put(feature.getId(), feature);
//            return true;
//        } catch (Exception e) {
//            e.printStackTrace();
//            return false;
//        }
//    }
//
//    @Override
//    public boolean addAttachedFeature(Feature<?, ?> feature) {
//        try {
//            attachedFeatureTable.put(feature.getId(), feature);
//            return true;
//        } catch (Exception e) {
//            e.printStackTrace();
//            return false;
//        }
//    }
//
//    @Override
//    public boolean addVertex(Vertex vertex) {
//        try {
//            vertexTable.put(vertex.getId(), vertex);
//            return true;
//        } catch (Exception e) {
//            e.printStackTrace();
//            return false;
//        }
//    }
//
//    @Override
//    public boolean addEdge(DirectedEdge directedEdge) {
//        try {
//            directedEdge.src = getVertex(directedEdge.getSrcId());
//            directedEdge.dest = getVertex(directedEdge.getDestId());
//            edgeTable.put(directedEdge.getId(), directedEdge);
//            Map<String, List<DirectedEdge>> outEdges = outEdgeTable.contains(directedEdge.getSrcId()) ? outEdgeTable.get(directedEdge.getSrcId()) : new HashMap<>();
//            Map<String, List<DirectedEdge>> inEdges = inEdgeTable.contains(directedEdge.getDestId()) ? inEdgeTable.get(directedEdge.getDestId()) : new HashMap<>();
//            outEdges.computeIfAbsent(directedEdge.getDestId(), (key) -> new ArrayList<>());
//            inEdges.computeIfAbsent(directedEdge.getSrcId(), (key) -> new ArrayList<>());
//            outEdges.get(directedEdge.getDestId()).add(directedEdge);
//            inEdges.get(directedEdge.getSrcId()).add(directedEdge);
//            outEdgeTable.put(directedEdge.getSrcId(), outEdges);
//            inEdgeTable.put(directedEdge.getDestId(), inEdges);
//            return true;
//        } catch (Exception e) {
//            e.printStackTrace();
//            return false;
//        }
//    }
//
//    @Override
//    public boolean addHyperEdge(HyperEdge hyperEdge) {
//        try {
//            hyperEdgeTable.put(hyperEdge.getId(), hyperEdge);
//            for (String vertexId : hyperEdge.getVertexIds()) {
//                List<HyperEdge> backwardList = vertex2HyperEdge.contains(vertexId) ? vertex2HyperEdge.get(vertexId) : new ArrayList<>(50);
//                backwardList.add(hyperEdge);
//                vertex2HyperEdge.put(vertexId, backwardList);
//            }
//            if (hyperEdge.vertices != null) {
//                for (int i = 0; i < hyperEdge.vertices.size(); i++) {
//                    hyperEdge.vertices.set(i, getVertex(hyperEdge.vertexIds.get(i)));
//                }
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
//            attachedFeatureTable.put(feature.getId(), feature);
//            return true;
//        } catch (Exception e) {
//            e.printStackTrace();
//            return false;
//        }
//    }
//
//    @Override
//    public boolean updateStandaloneFeature(Feature<?, ?> feature, Feature<?, ?> memento) {
//        try {
//            independentFeatureTable.put(feature.getId(), feature);
//            return true;
//        } catch (Exception e) {
//            e.printStackTrace();
//            return false;
//        }
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
//            for (String vertexId : memento.getVertexIds()) {
//                List<HyperEdge> backwardList = vertex2HyperEdge.contains(vertexId) ? vertex2HyperEdge.get(vertexId) : new ArrayList<>(50);
//                backwardList.add(hyperEdge);
//                vertex2HyperEdge.put(vertexId, backwardList);
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
//        return false;
//    }
//
//    @Override
//    public boolean deleteStandaloneFeature(Feature<?, ?> feature) {
//        return false;
//    }
//
//    @Override
//    public boolean deleteVertex(Vertex vertex) {
//        return false;
//    }
//
//    @Override
//    public boolean deleteEdge(DirectedEdge directedEdge) {
//        return false;
//    }
//
//    @Override
//    public boolean deleteHyperEdge(HyperEdge hyperEdge) {
//        return false;
//    }
//
//    @Nullable
//    @Override
//    public Vertex getVertex(String vertexId) {
//        try {
//            return vertexTable.get(vertexId);
//        } catch (Exception e) {
//            e.printStackTrace();
//            return null;
//        }
//    }
//
//    @Override
//    public Iterable<Vertex> getVertices() {
//        try {
//            Iterator<Vertex> iterator = vertexTable.values().iterator();
//            return () -> iterator;
//        } catch (Exception e) {
//            e.printStackTrace();
//            return Collections.emptyList();
//        }
//    }
//
//    @Nullable
//    @Override
//    public DirectedEdge getEdge(String srcId, String destId, @Nullable String attributeId, @Nullable String edgeId) {
//        try {
//            if (edgeId == null)
//                edgeId = DirectedEdge.encodeEdgeId(srcId, destId, attributeId);
//            return edgeTable.get(edgeId);
//        } catch (Exception e) {
//            return null;
//        }
//    }
//
//    @Override
//    public Iterable<DirectedEdge> getEdges(String srcId, String destId) {
//        try {
//            if (outEdgeTable.contains(srcId)) {
//                Map<String, List<DirectedEdge>> outEdges = outEdgeTable.get(srcId);
//                if (outEdges.containsKey(destId)) {
//                    return () -> IteratorUtils.transformedIterator(
//                            outEdges.get(destId).iterator(), edge -> edge
//                    );
//                }
//            }
//            return Collections.emptyList();
//        } catch (Exception e) {
//            return Collections.emptyList();
//        }
//    }
//
//    @Override
//    public Iterable<DirectedEdge> getIncidentEdges(Vertex vertex, EdgeType edge_type) {
//        try {
//            Iterator<DirectedEdge> outEdgesIterator = IteratorUtils.emptyIterator();
//            Iterator<DirectedEdge> inEdgesIterator = IteratorUtils.emptyIterator();
//            if (edge_type == EdgeType.OUT || edge_type == EdgeType.BOTH) {
//                if (outEdgeTable.contains(vertex.getId())) {
//                    Map<String, List<DirectedEdge>> outEdges = outEdgeTable.get(vertex.getId());
//                    outEdgesIterator = outEdges.values().stream().flatMap(Collection::stream).iterator();
//                }
//
//            } else if (edge_type == EdgeType.IN || edge_type == EdgeType.BOTH) {
//                if (inEdgeTable.contains(vertex.getId())) {
//                    Map<String, List<DirectedEdge>> inEdges = inEdgeTable.get(vertex.getId());
//                    inEdgesIterator = inEdges.values().stream().flatMap(Collection::stream).iterator();
//                }
//            }
//            Iterator<DirectedEdge> finalOutEdgesIterator = outEdgesIterator;
//            Iterator<DirectedEdge> finalInEdgesIterator = inEdgesIterator;
//            return () -> IteratorUtils.chainedIterator(finalOutEdgesIterator, finalInEdgesIterator);
//        } catch (Exception e) {
//            e.printStackTrace();
//            return Collections.emptyList();
//        }
//    }
//
//
//    @Override
//    public Iterable<HyperEdge> getIncidentHyperEdges(Vertex vertex) {
//        try {
//            if (vertex2HyperEdge.contains(vertex.getId())) {
//                final Iterator<HyperEdge> tmpIterator = vertex2HyperEdge.get(vertex.getId()).iterator();
//                return () -> tmpIterator;
//            } else return Collections.emptyList();
//        } catch (Exception e) {
//            e.printStackTrace();
//            return Collections.emptyList();
//        }
//    }
//
//    @Override
//    public HyperEdge getHyperEdge(String hyperEdgeId) {
//        try {
//            return hyperEdgeTable.get(hyperEdgeId);
//        } catch (Exception e) {
//            e.printStackTrace();
//            return null;
//        }
//    }
//
//    @Nullable
//    @Override
//    public Feature<?, ?> getStandaloneFeature(String featureName) {
//        try {
//            return independentFeatureTable.get(featureName);
//        } catch (Exception e) {
//            e.printStackTrace();
//            return null;
//        }
//    }
//
//    @Nullable
//    @Override
//    public Feature<?, ?> getAttachedFeature(ElementType attachedType, String attachedId, String featureName, @Nullable String featureId) {
//        try {
//            if (featureId == null) featureId = Feature.encodeAttachedFeatureId(attachedType, attachedId, featureName);
//            return attachedFeatureTable.get(featureId);
//        } catch (Exception e) {
//            e.printStackTrace();
//            return null;
//        }
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
//            if (featureId == null) featureId = Feature.encodeAttachedFeatureId(attachedType, attachedId, featureName);
//            return attachedFeatureTable.contains(featureId);
//        } catch (Exception e) {
//            e.printStackTrace();
//            return false;
//        }
//    }
//
//    @Override
//    public boolean containsStandaloneFeature(String featureName) {
//        try {
//            return independentFeatureTable.contains(featureName);
//        } catch (Exception e) {
//            e.printStackTrace();
//            return false;
//        }
//    }
//
//    @Override
//    public boolean containsEdge(String srcId, String destId, @Nullable String attributeId, @Nullable String edgeId) {
//        try {
//            if (edgeId == null)
//                edgeId = DirectedEdge.encodeEdgeId(srcId, destId, attributeId);
//            return edgeTable.contains(edgeId);
//        } catch (Exception e) {
//            e.printStackTrace();
//            return false;
//        }
//    }
//
//    @Override
//    public boolean containsHyperEdge(String hyperEdgeId) {
//        try {
//            return hyperEdgeTable.contains(hyperEdgeId);
//        } catch (Exception e) {
//            e.printStackTrace();
//            return false;
//        }
//    }
//
//    @Override
//    public void cacheFeatures(GraphElement element, CacheFeatureContext context) {
//        // Pass
//    }
//}
