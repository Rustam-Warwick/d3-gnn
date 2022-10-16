package storage;

import aggregators.MeanAggregator;
import ai.djl.ndarray.NDArray;
import elements.*;
import features.Set;
import features.Tensor;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.collections.Transformer;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.*;

/**
 * Storage for a more compressed representation of data, however fetching data has to create new classes which can have some overheads.
 */
public class CompressedListStorage extends BaseStorage {

    // TOPOLOGY RELATED STATES
    protected ListState<Short> vertexMasters; // Master parts of vertices

    protected ListState<List<Tuple2<Integer, String>>> outEdges; // Out edges of vertices

    protected ListState<List<Tuple2<Integer, String>>> inEdges; // In edges of vertices

    protected MapState<String, Integer> vId2Int; // Vertex id -> Index in the list

    protected MapState<Integer, String> vInt2Id; // Index in the list -> Vertex id

    protected ValueState<Integer> counter; // Counter for incrementing Vertex ids

    protected Map<String, Tuple3<MapState<Integer, ? extends Object>, Boolean, Constructor<? extends Feature>>> vFeature; // Feature name -> <State, Halo, Constructor>

    protected Map<String, Tuple3<MapState<Integer, ? extends Object>, Boolean, Constructor<? extends Feature>>> pluginFeature; // Feature name -> <State, Halo, Constructor>

    @Override
    public void open() throws Exception {
        ListStateDescriptor<Short> vertexMastersDesc = new ListStateDescriptor<>("vertexMasters", Short.class);
        ValueStateDescriptor<Integer> counterDesc = new ValueStateDescriptor<>("counter", Types.INT);
        ListStateDescriptor<List<Tuple2<Integer, String>>> outEdgesDesc = new ListStateDescriptor<>("outEdges", Types.LIST(Types.TUPLE(Types.INT, Types.STRING)));
        ListStateDescriptor<List<Tuple2<Integer, String>>> intEdgesDesc = new ListStateDescriptor<>("inEdges", Types.LIST(Types.TUPLE(Types.INT, Types.STRING)));
        MapStateDescriptor<String, Integer> vertexIdTranslationDesc = new MapStateDescriptor<String, Integer>("vertexIdTranslation", Types.STRING, Types.INT);
        MapStateDescriptor<Integer, String> inverseIdTranslationDesc = new MapStateDescriptor<Integer, String>("inverseVertexIdTranslation", Types.INT, Types.STRING);
        this.vertexMasters = layerFunction.getRuntimeContext().getListState(vertexMastersDesc);
        this.vId2Int = layerFunction.getRuntimeContext().getMapState(vertexIdTranslationDesc);
        this.vInt2Id = layerFunction.getRuntimeContext().getMapState(inverseIdTranslationDesc);
        this.counter = layerFunction.getRuntimeContext().getState(counterDesc);
        this.outEdges = layerFunction.getRuntimeContext().getListState(outEdgesDesc);
        this.inEdges = layerFunction.getRuntimeContext().getListState(intEdgesDesc);
        this.vFeature = new HashMap<>(); // Single Hashmap holding Feature MapStates for Vertex

        // Populate the needed Features for vertex
        this.vFeature.put("f", Tuple3.of(
                layerFunction.getRuntimeContext().getMapState(new MapStateDescriptor<>("vFeature_feature", Types.INT, TypeInformation.of(NDArray.class))),
                false,
                Tensor.class.getConstructor()
        ));
        this.vFeature.put("agg", Tuple3.of(
                layerFunction.getRuntimeContext().getMapState(new MapStateDescriptor<>("vFeature_agg", Types.INT, Types.TUPLE(TypeInformation.of(NDArray.class), Types.INT))),
                true,
                MeanAggregator.class.getConstructor()
        ));
        this.vFeature.put("p", Tuple3.of(
                layerFunction.getRuntimeContext().getMapState(new MapStateDescriptor<>("vFeature_parts", Types.INT, Types.LIST(Types.SHORT))),
                true,
                Set.class.getConstructor()
        ));

        this.vFeature.put("train_l", Tuple3.of(
                layerFunction.getRuntimeContext().getMapState(new MapStateDescriptor<>("vFeature_parts", Types.INT, Types.LIST(Types.SHORT))),
                true,
                Set.class.getConstructor()
        ));
        super.open();
        layerFunction.getWrapperContext().runForAllKeys(() -> {
            try {
                counter.update(0);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public boolean addFeature(Feature<?, ?> feature) {
        try {
            if (feature.attachedTo != null) {
                // Independent Feature
                if (feature.attachedTo.f0 == ElementType.VERTEX) {
                    int vId = vId2Int.get(feature.attachedTo.f1);
                    MapState<Integer, Object> tmp = (MapState<Integer, Object>) vFeature.get(feature.getName()).f0;
                    tmp.put(vId, feature.value);
                } else {
                    throw new NotImplementedException("Other Element Features are not implemented yet!");
                }
            } else {
                throw new NotImplementedException("Non-Attached Features do not exist yet");
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean addVertex(Vertex vertex) {
        try {
            vId2Int.put(vertex.getId(), counter.value());
            vInt2Id.put(counter.value(), vertex.getId());
            vertexMasters.add(vertex.masterPart());
            inEdges.add(Collections.emptyList());
            outEdges.add(Collections.emptyList());
            counter.update(counter.value() + 1);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean addEdge(Edge edge) {
        try {
            int srcId = vId2Int.get(edge.getSrc().getId());
            int destId = vId2Int.get(edge.getDest().getId());
            List<Tuple2<Integer, String>> outEdgeList = (List<Tuple2<Integer, String>>) ((List<?>) outEdges.get()).get(srcId);
            List<Tuple2<Integer, String>> inEdgeList = (List<Tuple2<Integer, String>>) ((List<?>) inEdges.get()).get(destId);
            if (outEdgeList.size() == 0)
                outEdgeList = new ArrayList<>(List.of(Tuple2.of(destId, Edge.isAttributed(edge.getId()) ? Edge.decodeVertexIdsAndAttribute(edge.getId())[2] : null)));
            else
                outEdgeList.add(Tuple2.of(destId, Edge.isAttributed(edge.getId()) ? Edge.decodeVertexIdsAndAttribute(edge.getId())[2] : null));
            if (inEdgeList.size() == 0)
                inEdgeList = new ArrayList<>(List.of(Tuple2.of(srcId, Edge.isAttributed(edge.getId()) ? Edge.decodeVertexIdsAndAttribute(edge.getId())[2] : null)));
            else
                inEdgeList.add(Tuple2.of(srcId, Edge.isAttributed(edge.getId()) ? Edge.decodeVertexIdsAndAttribute(edge.getId())[2] : null));
            ((List<List<Tuple2<Integer, String>>>) outEdges.get()).set(srcId, outEdgeList);
            ((List<List<Tuple2<Integer, String>>>) inEdges.get()).set(destId, inEdgeList);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean addHyperEdge(HEdge hEdge) {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public boolean updateFeature(Feature<?, ?> feature) {
        return addFeature(feature);
    }

    @Override
    public boolean updateVertex(Vertex vertex) {
        return true;
    }

    @Override
    public boolean updateEdge(Edge edge) {
        return true;
    }

    @Override
    public boolean updateHyperEdge(HEdge hEdge) {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public boolean deleteFeature(Feature<?, ?> feature) {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public boolean deleteVertex(Vertex vertex) {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public boolean deleteEdge(Edge edge) {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public boolean deleteHyperEdge(HEdge hEdge) {
        throw new NotImplementedException("Not implemented");
    }

    @Nullable
    @Override
    public Vertex getVertex(String id) {
        try {
            int vId = vId2Int.get(id);
            Vertex v = new Vertex(id, false, ((List<Short>) vertexMasters.get()).get(vId));
            v.setStorage(this);
            return v;
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public Iterable<Vertex> getVertices() {
        try {
            Iterator<String> keys = vId2Int.keys().iterator();
            return () -> IteratorUtils.transformedIterator(keys, new Transformer() {
                @Override
                public Vertex transform(Object vertexId) {
                    return getVertex((String) vertexId);
                }
            });
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }

    @Nullable
    @Override
    public Edge getEdge(String id) {
        try {
            Edge e = new Edge(id);
            e.setStorage(this);
            return e;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public Iterable<Edge> getEdges(String src, String dest) {
        try {
            int srcId = vId2Int.get(src);
            int destId = vId2Int.get(dest);
            List<Tuple2<Integer, String>> srcOutVertices = (List<Tuple2<Integer, String>>) ((List<?>) outEdges.get()).get(srcId);
            return () -> srcOutVertices.stream().filter(item -> item.f0 == destId).map(item -> {
                if (item.f1 == null) {
                    return getEdge(Edge.encodeEdgeId(src, dest));
                } else {
                    return getEdge(Edge.encodeEdgeId(src, dest, item.f1));
                }
            }).iterator();
        } catch (Exception e) {
            e.printStackTrace();
            return Collections.emptyList();
        }
    }

    @Override
    public Iterable<Edge> getIncidentEdges(Vertex vertex, EdgeType edge_type) {
        try {
            int vId = vId2Int.get(vertex.getId());
            Iterator<Edge> outEdgesIterator = null;
            Iterator<Edge> inEdgesIterator = null;
            if (edge_type == EdgeType.OUT || edge_type == EdgeType.BOTH) {
                List<Tuple2<Integer, String>> outEdgesList = (List<Tuple2<Integer, String>>) ((List<?>) outEdges.get()).get(vId);
                outEdgesIterator = IteratorUtils.transformedIterator(outEdgesList.iterator(), val -> {
                    Tuple2<Integer, String> valT = (Tuple2<Integer, String>) val;
                    try {
                        if (valT.f1 == null) {
                            return getEdge(Edge.encodeEdgeId(vertex.getId(), vInt2Id.get(valT.f0)));
                        } else {
                            return getEdge(Edge.encodeEdgeId(vertex.getId(), vInt2Id.get(valT.f0), valT.f1));
                        }
                    } catch (Exception ignored) {
                        ignored.printStackTrace();
                        return null;
                    }
                });
            } else if (edge_type == EdgeType.IN || edge_type == EdgeType.BOTH) {
                List<Tuple2<Integer, String>> inEdgeList = (List<Tuple2<Integer, String>>) ((List<?>) inEdges.get()).get(vId);
                inEdgesIterator = IteratorUtils.transformedIterator(inEdgeList.iterator(), val -> {
                    Tuple2<Integer, String> valT = (Tuple2<Integer, String>) val;
                    try {
                        if (valT.f1 == null) {
                            return getEdge(Edge.encodeEdgeId(vInt2Id.get(valT.f0), vertex.getId()));
                        } else {
                            return getEdge(Edge.encodeEdgeId(vInt2Id.get(valT.f0), vertex.getId(), valT.f1));
                        }
                    } catch (Exception ignored) {
                        ignored.printStackTrace();
                        return null;
                    }
                });
            }
            if (outEdgesIterator != null && inEdgesIterator != null) {
                Iterator<Edge> finalOutEdgesIterator = outEdgesIterator;
                Iterator<Edge> finalInEdgesIterator = inEdgesIterator;
                return () -> IteratorUtils.chainedIterator(finalOutEdgesIterator, finalInEdgesIterator);
            } else if (outEdgesIterator != null) {
                Iterator<Edge> finalOutEdgesIterator1 = outEdgesIterator;
                return () -> finalOutEdgesIterator1;
            } else if (inEdgesIterator != null) {
                Iterator<Edge> finalInEdgesIterator1 = inEdgesIterator;
                return () -> finalInEdgesIterator1;
            } else return Collections.emptyList();

        } catch (Exception e) {
            e.printStackTrace();
            return Collections.emptyList();
        }
    }

    @Override
    public HEdge getHyperEdge(String id) {
        throw new IllegalStateException("Not implemented");
    }

    @Override
    public Iterable<HEdge> getHyperEdges(Vertex id) {
        throw new IllegalStateException("Not implemented");
    }

    @Nullable
    @Override
    public Feature<?, ?> getAttachedFeature(String elementId, String featureName, ElementType elementType, @Nullable String id) {
        try {
            if (elementType == ElementType.VERTEX) {
                int vId = vId2Int.get(elementId);
                Object featureValue = vFeature.get(featureName).f0.get(vId);
                Boolean isHalo = vFeature.get(featureName).f1;
                Feature<Object, Object> tmpFeature = vFeature.get(featureName).f2.newInstance();
                tmpFeature.value = featureValue;
                tmpFeature.halo = isHalo;
                tmpFeature.attachedTo = Tuple2.of(ElementType.VERTEX, elementId);
                tmpFeature.id = featureName;
                tmpFeature.setStorage(this);
                return tmpFeature;
            }
            return null;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Nullable
    @Override
    public Feature<?, ?> getStandaloneFeature(String id) {
        throw new NotImplementedException("Standalone Features not yet available");
    }

    @Override
    public boolean containsVertex(String id) {
        try {
            return vId2Int.contains(id);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean containsStandaloneFeature(String id) {
        throw new NotImplementedException("Standalone Features not yet available");
    }

    @Override
    public boolean containsAttachedFeature(String elementId, String featureName, ElementType elementType, @Nullable String id) {
        try {
            if (elementType == ElementType.VERTEX) {
                int vId = vId2Int.get(elementId);
                return vFeature.get(featureName).f0.contains(vId);
            }
            return false;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean containsEdge(String id) {
        try {
            boolean isAttr = Edge.isAttributed(id);
            String[] srcDestIds = Edge.decodeVertexIdsAndAttribute(id);
            int srcId = vId2Int.get(srcDestIds[0]);
            int destId = vId2Int.get(srcDestIds[1]);
            List<Tuple2<Integer, String>> destList = (List<Tuple2<Integer, String>>) ((List<?>) outEdges.get()).get(srcId);
            for (Tuple2<Integer, String> integerStringTuple2 : destList) {
                if (integerStringTuple2.f0.equals(destId) && (!isAttr || integerStringTuple2.f1.equals(srcDestIds[2])))
                    return true;
            }
            return false;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public boolean containsHyperEdge(String id) {
        throw new IllegalStateException("Not implemented");
    }

    @Override
    public void cacheFeaturesOf(GraphElement e) {
        if (e.elementType() == ElementType.VERTEX) {
            e.getFeature("f");
        }
    }
}
