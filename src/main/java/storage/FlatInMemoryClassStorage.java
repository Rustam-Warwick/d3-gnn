package storage;

import elements.*;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import typeinfo.OmittingPojoTypeInfoFactory;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @implNote Only use if using InMemoryState backend
 */
public class FlatInMemoryClassStorage extends BaseStorage {
    protected MapState<String, Vertex> vertexTable;
    protected MapState<String, Feature<?, ?>> attachedFeatureTable;
    protected MapState<String, Feature<?, ?>> independentFeatureTable;
    protected MapState<String, Map<String, Edge>> edgeTable;

    public FlatInMemoryClassStorage() {

    }


    @Override
    public void open() throws Exception {
        MapStateDescriptor<String, Vertex> vertexTableDesc = new MapStateDescriptor<>("vertexTable", Types.STRING, new OmittingPojoTypeInfoFactory<Vertex>().createTypeInfo(Vertex.class, null));
        MapStateDescriptor<String, Map<String, Edge>> edgeTableDesc = new MapStateDescriptor<>("edgeTable", Types.STRING, Types.MAP(Types.STRING, new OmittingPojoTypeInfoFactory<Edge>().createTypeInfo(Edge.class, null)));
        MapStateDescriptor<String, Feature<?, ?>> featureTableDesc = new MapStateDescriptor<>("attachedFeatureTable", Types.STRING, new OmittingPojoTypeInfoFactory<Feature<?,?>>().createTypeInfo(Feature.class, null));
        MapStateDescriptor<String, Feature<?, ?>> independentFeatureTableDesc = new MapStateDescriptor<>("independentFeatureTable", Types.STRING, new OmittingPojoTypeInfoFactory<Feature<?,?>>().createTypeInfo(Feature.class, null));

        vertexTable = layerFunction.getRuntimeContext().getMapState(vertexTableDesc);
        edgeTable = layerFunction.getRuntimeContext().getMapState(edgeTableDesc);
        attachedFeatureTable = layerFunction.getRuntimeContext().getMapState(featureTableDesc);
        independentFeatureTable = layerFunction.getRuntimeContext().getMapState(independentFeatureTableDesc);
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
            if (!edgeTable.contains(edge.src.getId())) edgeTable.put(edge.src.getId(), new HashMap<>());
            edge.src = getVertex(edge.src.getId());
            edge.dest = getVertex(edge.dest.getId());
            edgeTable.get(edge.src.getId()).put(edge.dest.getId(), edge);
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

    @Nullable
    @Override
    public Vertex getVertex(String id) {
        try {
            return vertexTable.get(id);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public Iterable<Vertex> getVertices() {
        try {
            return vertexTable.values();
        } catch (Exception e) {
            e.printStackTrace();
            return Collections.emptyList();
        }
    }

    @Nullable
    @Override
    public Edge getEdge(String src, String dest) {
        try {
            return edgeTable.get(src).get(dest);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }


    @Override
    public Iterable<Edge> getIncidentEdges(Vertex vertex, EdgeType edge_type) {
        try {
            switch (edge_type) {
                case IN:
                    throw new IllegalStateException("In Edges not suported");
                case OUT:
                    return edgeTable.contains(vertex.getId()) ? edgeTable.get(vertex.getId()).values() : Collections.emptyList();
                case BOTH:
                    Iterator<Edge> finalIterator = IteratorUtils.chainedIterator(getIncidentEdges(vertex, EdgeType.IN).iterator(), getIncidentEdges(vertex, EdgeType.OUT).iterator());
                    return () -> finalIterator;
                default:
                    return Collections.emptyList();
            }
        } catch (Exception e) {
            e.printStackTrace();
            return Collections.emptyList();
        }
    }

    @Nullable
    @Override
    public Feature<?, ?> getFeature(String id) {
        try {
            if (id.contains(":")) {
                // This is attached feature
                return attachedFeatureTable.get(id);
            } else {
                // This is independent Feature
                return independentFeatureTable.get(id);
            }
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
    public boolean containsFeature(String id) {
        try {
            if (id.contains(":")) {
                // Attached Feature
                return attachedFeatureTable.contains(id);
            } else {
                return independentFeatureTable.contains(id);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean containsEdge(String id) {
        try {
            String[] ids = id.split(":");
            if (edgeTable.contains(ids[0])) {
                return edgeTable.get(ids[0]).containsKey(ids[1]);
            } else {
                return false;
            }
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
