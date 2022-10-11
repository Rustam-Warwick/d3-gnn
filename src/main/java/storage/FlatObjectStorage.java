package storage;

import elements.*;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import typeinfo.RecursiveListFieldsTypeInfoFactory;

import javax.annotation.Nullable;
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
    protected MapState<String, Edge> edgeTable;
    protected MapState<String, HEdge> hyperEdgeTable;
    protected MapState<String, List<String>> vertex2HyperEdge;

    public FlatObjectStorage() {

    }

    @Override
    public void open() throws Exception {
        MapStateDescriptor<String, Vertex> vertexTableDesc = new MapStateDescriptor<>("vertexTable", Types.STRING, new RecursiveListFieldsTypeInfoFactory<Vertex>().createTypeInfo(Vertex.class, null, true));
        MapStateDescriptor<String, Feature<?, ?>> featureTableDesc = new MapStateDescriptor<>("attachedFeatureTable", Types.STRING, new RecursiveListFieldsTypeInfoFactory<Feature<?, ?>>().createTypeInfo(Feature.class, null, true));
        MapStateDescriptor<String, Feature<?, ?>> independentFeatureTableDesc = new MapStateDescriptor<>("independentFeatureTable", Types.STRING, new RecursiveListFieldsTypeInfoFactory<Feature<?, ?>>().createTypeInfo(Feature.class, null, true));
        MapStateDescriptor<String, Edge> edgeTableDesc = new MapStateDescriptor<>("edgeTable", Types.STRING, new RecursiveListFieldsTypeInfoFactory<Edge>().createTypeInfo(Edge.class, null, true));
        MapStateDescriptor<String, HEdge> hyperEdgeTableDesc = new MapStateDescriptor<>("hyperEdgeTable", Types.STRING, new RecursiveListFieldsTypeInfoFactory<HEdge>().createTypeInfo(HEdge.class, null, true));
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
    public boolean addFeature(Feature<?, ?> feature) {
        try {
            if (feature.attachedTo == null) {
                independentFeatureTable.put(feature.getId(), feature);
            } else {
                attachedFeatureTable.put(feature.getId(), feature);
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
            vertexTable.put(vertex.getId(), vertex);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean addEdge(Edge edge) {
        try {
            edgeTable.put(edge.getId(), edge);
            if (!outEdgeTable.contains(edge.getSrc().getId())) outEdgeTable.put(edge.getSrc().getId(), new HashMap<>());
            if (!inEdgeTable.contains(edge.getDest().getId())) inEdgeTable.put(edge.getDest().getId(), new HashMap<>());
            Map<String, List<String>> outEdges = outEdgeTable.get(edge.getSrc().getId());
            Map<String, List<String>> inEdges = inEdgeTable.get(edge.getDest().getId());
            if (!outEdges.containsKey(edge.getDest().getId())) outEdges.put(edge.getDest().getId(), new ArrayList());
            if (!inEdges.containsKey(edge.getSrc().getId())) inEdges.put(edge.getSrc().getId(), new ArrayList());
            outEdges.get(edge.getDest().getId()).add(edge.getId());
            inEdges.get(edge.getSrc().getId()).add(edge.getId());
            outEdgeTable.put(edge.getSrc().getId(), outEdges);
            inEdgeTable.put(edge.getDest().getId(), inEdges);
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
    public boolean updateFeature(Feature<?, ?> feature) {
        try {
            if (feature.attachedTo == null) {
                independentFeatureTable.put(feature.getId(), feature);
            } else {
                attachedFeatureTable.put(feature.getId(), feature);
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
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
        return true;
    }

    @Override
    public boolean deleteFeature(Feature<?, ?> feature) {
        return false;
    }

    @Override
    public boolean deleteVertex(Vertex vertex) {
        return false;
    }

    @Override
    public boolean deleteEdge(Edge edge) {
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
                v.setId(id);
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
    public Edge getEdge(String id) {
        try {
            Edge e = edgeTable.get(id);
            if (e.storage == null) {
                e.setId(id);
                e.setStorage(this);
            }
            return e;
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public Iterable<Edge> getEdges(String src, String dest) {
        try {
            if (outEdgeTable.contains(src)) {
                Map<String, List<String>> outEdges = outEdgeTable.get(src);
                if (outEdges.containsKey(dest)) {
                    return () -> IteratorUtils.transformedIterator(
                            outEdges.get(dest).iterator(), str -> (getEdge((String) str))
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
    public Iterable<Edge> getIncidentEdges(Vertex vertex, EdgeType edge_type) {
        try {
            Iterator<Edge> outEdgesIterator = null;
            Iterator<Edge> inEdgesIterator = null;
            if (edge_type == EdgeType.OUT || edge_type == EdgeType.BOTH) {
                if (outEdgeTable.contains(vertex.getId())) {
                    Map<String, List<String>> outEdges = outEdgeTable.get(vertex.getId());
                    outEdgesIterator = IteratorUtils.transformedIterator(outEdges.values().stream().flatMap(edgesList -> edgesList.stream()).iterator(), str -> getEdge((String) str));
                }

            } else if (edge_type == EdgeType.IN || edge_type == EdgeType.BOTH) {
                if (inEdgeTable.contains(vertex.getId())) {
                    Map<String, List<String>> inEdges = inEdgeTable.get(vertex.getId());
                    inEdgesIterator = IteratorUtils.transformedIterator(inEdges.values().stream().flatMap(edgesList -> edgesList.stream()).iterator(), str -> getEdge((String) str));
                }
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
    public Iterable<HEdge> getHyperEdges(Vertex id) {
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
    public Feature<?, ?> getAttachedFeature(String elementId, String featureName, ElementType elementType, @Nullable String id) {
        try {
            if (id == null) id = Feature.encodeAttachedFeatureId(featureName, elementId, elementType);
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
    public boolean containsAttachedFeature(String elementId, String featureName, ElementType elementType, @Nullable String id) {
        try {
            if (id == null) id = Feature.encodeAttachedFeatureId(featureName, elementId, elementType);
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
    public boolean containsEdge(String id) {
        try {
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
    public void cacheFeaturesOf(GraphElement e) {
        // Pass
    }
}
