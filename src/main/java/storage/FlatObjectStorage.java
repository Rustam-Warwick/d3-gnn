package storage;

import ai.djl.ndarray.LifeCycleControl;
import elements.*;
import elements.enums.CacheFeatureContext;
import elements.enums.EdgeType;
import elements.enums.ElementType;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.jetbrains.annotations.Nullable;
import typeinfo.recursivepojoinfo.RecursivePojoTypeInfoFactory;

import java.util.*;

/**
 * @implNote Only use if using InMemoryState backend
 */
public class FlatObjectStorage extends BaseStorage {
    protected MapState<String, Vertex> vertexTable;
    protected MapState<String, Feature<?, ?>> attachedFeatureTable;
    protected MapState<String, Feature<?, ?>> independentFeatureTable;
    protected MapState<String, Map<String, List<String>>> outEdgeTable;
    protected MapState<String, Map<String, List<String>>> inEdgeTable;
    protected MapState<String, DEdge> edgeTable;
    protected MapState<String, HEdge> hyperEdgeTable;
    protected MapState<String, List<String>> vertex2HyperEdge;

    public FlatObjectStorage() {

    }

    @Override
    public void open() throws Exception {
        MapStateDescriptor<String, Vertex> vertexTableDesc = new MapStateDescriptor<>("vertexTable", Types.STRING, new RecursivePojoTypeInfoFactory<Vertex>().createTypeInfo(Vertex.class, null, true));
        MapStateDescriptor<String, Feature<?, ?>> featureTableDesc = new MapStateDescriptor<>("attachedFeatureTable", Types.STRING, new RecursivePojoTypeInfoFactory<Feature<?, ?>>().createTypeInfo(Feature.class, null, true));
        MapStateDescriptor<String, Feature<?, ?>> independentFeatureTableDesc = new MapStateDescriptor<>("independentFeatureTable", Types.STRING, new RecursivePojoTypeInfoFactory<Feature<?, ?>>().createTypeInfo(Feature.class, null, true));
        MapStateDescriptor<String, DEdge> edgeTableDesc = new MapStateDescriptor<>("edgeTable", Types.STRING, new RecursivePojoTypeInfoFactory<DEdge>().createTypeInfo(DEdge.class, null, true));
        MapStateDescriptor<String, HEdge> hyperEdgeTableDesc = new MapStateDescriptor<>("hyperEdgeTable", Types.STRING, new RecursivePojoTypeInfoFactory<HEdge>().createTypeInfo(HEdge.class, null, true));
        MapStateDescriptor<String, Map<String, List<String>>> outEdgeTableDesc = new MapStateDescriptor<>("outEdgeTable", Types.STRING, Types.MAP(Types.STRING, Types.LIST(Types.STRING)));
        MapStateDescriptor<String, Map<String, List<String>>> inEdgeTableDesc = new MapStateDescriptor<>("inEdgeTable", Types.STRING, Types.MAP(Types.STRING, Types.LIST(Types.STRING)));
        MapStateDescriptor<String, List<String>> vertex2HyperEdgeDesc = new MapStateDescriptor<>("vertex2HyperEdge", Types.STRING, Types.LIST(Types.STRING));

        edgeTable = layerFunction.getRuntimeContext().getMapState(edgeTableDesc);
        hyperEdgeTable = layerFunction.getRuntimeContext().getMapState(hyperEdgeTableDesc);
        outEdgeTable = layerFunction.getRuntimeContext().getMapState(outEdgeTableDesc);
        inEdgeTable = layerFunction.getRuntimeContext().getMapState(inEdgeTableDesc);
        vertexTable = layerFunction.getRuntimeContext().getMapState(vertexTableDesc);
        attachedFeatureTable = layerFunction.getRuntimeContext().getMapState(featureTableDesc);
        independentFeatureTable = layerFunction.getRuntimeContext().getMapState(independentFeatureTableDesc);
        vertex2HyperEdge = layerFunction.getRuntimeContext().getMapState(vertex2HyperEdgeDesc);
        super.open();
    }

    @Override
    public void close() throws Exception {
        attachedFeatureTable.values().forEach(LifeCycleControl::resume);
        independentFeatureTable.values().forEach(LifeCycleControl::resume);
        super.close();
    }

    @Override
    public boolean addStandaloneFeature(Feature<?, ?> feature) {
        try {
            independentFeatureTable.put(feature.getId(), feature);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean addAttachedFeature(Feature<?, ?> feature) {
        try {
            attachedFeatureTable.put(feature.getId(), feature);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean addVertex(Vertex vertex) {
        try {
            vertexTable.put(vertex.getId(), vertex);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean addEdge(DEdge dEdge) {
        try {
            edgeTable.put(dEdge.getId(), dEdge);
            if (!outEdgeTable.contains(dEdge.getSrcId()))
                outEdgeTable.put(dEdge.getSrcId(), new HashMap<>());
            if (!inEdgeTable.contains(dEdge.getDestId()))
                inEdgeTable.put(dEdge.getDestId(), new HashMap<>());
            Map<String, List<String>> outEdges = outEdgeTable.get(dEdge.getSrcId());
            Map<String, List<String>> inEdges = inEdgeTable.get(dEdge.getDestId());
            if (!outEdges.containsKey(dEdge.getDestId()))
                outEdges.put(dEdge.getDestId(), new ArrayList<>());
            if (!inEdges.containsKey(dEdge.getSrcId()))
                inEdges.put(dEdge.getSrcId(), new ArrayList<>());
            outEdges.get(dEdge.getDestId()).add(dEdge.getId());
            inEdges.get(dEdge.getSrcId()).add(dEdge.getId());
            outEdgeTable.put(dEdge.getSrcId(), outEdges);
            inEdgeTable.put(dEdge.getDestId(), inEdges);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean addHyperEdge(HEdge hEdge) {
        try {
            hyperEdgeTable.put(hEdge.getId(), hEdge);
            for (String vertexId : hEdge.vertexIds) {
                if (!vertex2HyperEdge.contains(vertexId)) vertex2HyperEdge.put(vertexId, new ArrayList<>());
                List<String> tmp = vertex2HyperEdge.get(vertexId);
                if (!tmp.contains(hEdge.getId())) {
                    tmp.add(hEdge.getId());
                    vertex2HyperEdge.put(vertexId, tmp);
                }
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean updateAttachedFeature(Feature<?, ?> feature, Feature<?, ?> memento) {
        try {
            attachedFeatureTable.put(feature.getId(), feature);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean updateStandaloneFeature(Feature<?, ?> feature, Feature<?, ?> memento) {
        try {
            independentFeatureTable.put(feature.getId(), feature);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean updateVertex(Vertex vertex, Vertex memento) {
        return true;
    }

    @Override
    public boolean updateEdge(DEdge dEdge, DEdge memento) {
        return true;
    }

    @Override
    public boolean updateHyperEdge(HEdge hEdge, HEdge memento) {
        return true;
    }

    @Override
    public boolean deleteAttachedFeature(Feature<?, ?> feature) {
        return false;
    }

    @Override
    public boolean deleteStandaloneFeature(Feature<?, ?> feature) {
        return false;
    }

    @Override
    public boolean deleteVertex(Vertex vertex) {
        return false;
    }

    @Override
    public boolean deleteEdge(DEdge dEdge) {
        return false;
    }

    @Override
    public boolean deleteHyperEdge(HEdge hEdge) {
        return false;
    }

    @Nullable
    @Override
    public Vertex getVertex(String id) {
        try {
            Vertex v = vertexTable.get(id);
            if (v == null) return null;
            if (v.storage == null) {
                v.setStorage(this);
            }
            return v;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public Iterable<Vertex> getVertices() {
        try {
            Iterator<Vertex> iterator = vertexTable.values().iterator();
            return () -> IteratorUtils.transformedIterator(iterator, item -> {
                ((Vertex) item).setStorage(this);
                return item;
            });
        } catch (Exception e) {
            e.printStackTrace();
            return Collections.emptyList();
        }
    }

    @Nullable
    @Override
    public DEdge getEdge(String srcId, String destId, @Nullable String attributeId, @Nullable String id) {
        try {
            if (id == null)
                id = DEdge.encodeEdgeId(srcId, destId, attributeId);
            DEdge e = edgeTable.get(id);
            if (e.storage == null) {
                e.setStorage(this);
            }
            return e;
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public Iterable<DEdge> getEdges(String src, String dest) {
        try {
            if (outEdgeTable.contains(src)) {
                Map<String, List<String>> outEdges = outEdgeTable.get(src);
                if (outEdges.containsKey(dest)) {
                    return () -> IteratorUtils.transformedIterator(
                            outEdges.get(dest).iterator(), str -> getEdge(src, dest, null, (String) str)
                    );
                } else {
                    return Collections.emptyList();

                }
            } else {
                return Collections.emptyList();
            }
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }

    @Override
    public Iterable<DEdge> getIncidentEdges(Vertex vertex, EdgeType edge_type) {
        try {
            Iterator<DEdge> outEdgesIterator = null;
            Iterator<DEdge> inEdgesIterator = null;
            if (edge_type == EdgeType.OUT || edge_type == EdgeType.BOTH) {
                if (outEdgeTable.contains(vertex.getId())) {
                    Map<String, List<String>> outEdges = outEdgeTable.get(vertex.getId());
                    outEdgesIterator = IteratorUtils.transformedIterator(outEdges.values().stream().flatMap(edgesList -> edgesList.stream()).iterator(), str -> getEdge(null, null, null, (String) str));
                }

            } else if (edge_type == EdgeType.IN || edge_type == EdgeType.BOTH) {
                if (inEdgeTable.contains(vertex.getId())) {
                    Map<String, List<String>> inEdges = inEdgeTable.get(vertex.getId());
                    inEdgesIterator = IteratorUtils.transformedIterator(inEdges.values().stream().flatMap(edgesList -> edgesList.stream()).iterator(), str -> getEdge(null, null, null, (String) str));
                }
            }
            if (outEdgesIterator != null && inEdgesIterator != null) {
                Iterator<DEdge> finalOutEdgesIterator = outEdgesIterator;
                Iterator<DEdge> finalInEdgesIterator = inEdgesIterator;
                return () -> IteratorUtils.chainedIterator(finalOutEdgesIterator, finalInEdgesIterator);
            } else if (outEdgesIterator != null) {
                Iterator<DEdge> finalOutEdgesIterator1 = outEdgesIterator;
                return () -> finalOutEdgesIterator1;
            } else if (inEdgesIterator != null) {
                Iterator<DEdge> finalInEdgesIterator1 = inEdgesIterator;
                return () -> finalInEdgesIterator1;
            } else return Collections.emptyList();

        } catch (Exception e) {
            e.printStackTrace();
            return Collections.emptyList();
        }
    }


    @Override
    public Iterable<HEdge> getIncidentHyperEdges(Vertex id) {
        try {
            if (vertex2HyperEdge.contains(id.getId())) {
                List<String> vEdges = vertex2HyperEdge.get(id.getId());
                return () -> IteratorUtils.transformedIterator(vEdges.iterator(), hEdgeId -> getHyperEdge((String) hEdgeId));
            } else return Collections.emptyList();
        } catch (Exception e) {
            e.printStackTrace();
            return Collections.emptyList();
        }
    }

    @Override
    public HEdge getHyperEdge(String id) {
        try {
            return hyperEdgeTable.get(id);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Nullable
    @Override
    public Feature<?, ?> getStandaloneFeature(String id) {
        try {
            Feature<?, ?> tmp = independentFeatureTable.get(id);
            if (tmp.storage == null) {
                tmp.setStorage(this);
            }
            return tmp;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Nullable
    @Override
    public Feature<?, ?> getAttachedFeature(ElementType elementType, String elementId, String featureName, @Nullable String id) {
        try {
            if (id == null) id = Feature.encodeFeatureId(elementType, elementId, featureName);
            Feature<?, ?> tmp = attachedFeatureTable.get(id);
            if (tmp.storage == null) {
                tmp.setStorage(this);
            }
            return tmp;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public boolean containsVertex(String id) {
        try {
            return vertexTable.contains(id);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean containsAttachedFeature(ElementType elementType, String elementId, String featureName, @Nullable String id) {
        try {
            if (id == null) id = Feature.encodeFeatureId(elementType, elementId, featureName);
            return attachedFeatureTable.contains(id);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean containsStandaloneFeature(String id) {
        try {
            return independentFeatureTable.contains(id);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean containsEdge(String srcId, String destId, @Nullable String attributeId, @Nullable String id) {
        try {
            if (id == null)
                id = DEdge.encodeEdgeId(srcId, destId, attributeId);
            return edgeTable.contains(id);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean containsHyperEdge(String id) {
        try {
            return hyperEdgeTable.contains(id);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public void cacheFeatures(GraphElement element, CacheFeatureContext context) {
        // Pass
    }
}
