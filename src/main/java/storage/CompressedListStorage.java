package storage;

import elements.*;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.jetbrains.annotations.Nullable;

import java.util.HashSet;
import java.util.Map;

/**
 * Storage for a more compressed representation of data, however fetching data has to create new classes which can have some overheads.
 */
public class CompressedListStorage extends BaseStorage {

    public transient MapState<String, Short> vertexTable;

    public transient MapState<String, HashSet<String>> eOutTable;

    public transient MapState<String, HashSet<String>> eInTable;

    public transient Map<Tuple2<String, ElementType>, MapState<String,Object>> attFeatureTable;

    @Override
    public void open() throws Exception {
        vertexTable = layerFunction.getRuntimeContext().getMapState(new MapStateDescriptor<>("vertexTable", String.class, Short.class));
        eOutTable = layerFunction.getRuntimeContext().getMapState(new MapStateDescriptor<>("eOutTable", Types.STRING, TypeInformation.of(new TypeHint<HashSet<String>>() {
        })));
        eInTable = layerFunction.getRuntimeContext().getMapState(new MapStateDescriptor<>("eInTable", Types.STRING, TypeInformation.of(new TypeHint<HashSet<String>>() {
        })));
        super.open();
    }

    @Override
    public boolean addAttachedFeature(Feature<?, ?> feature) {
        return false;
    }

    @Override
    public boolean addStandaloneFeature(Feature<?, ?> feature) {
        return false;
    }

    @Override
    public boolean addVertex(Vertex vertex) {
        try {
            vertexTable.put(vertex.getId(), vertex.masterPart());
            return true;
        } catch (Exception e) {
            LOG.error(e.getMessage());
            return false;
        }
    }

    @Override
    public boolean addEdge(UniEdge uniEdge) {
        try {
            if(!eOutTable.contains(uniEdge.getSrcId()))eOutTable.put(uniEdge.getSrcId(), new HashSet<>());
            if(!eInTable.contains(uniEdge.getDestId()))eInTable.put(uniEdge.getDestId(), new HashSet<>());
            eOutTable.get(uniEdge.getSrcId()).add(uniEdge.getDestId());
            eInTable.get(uniEdge.getDestId()).add(uniEdge.getSrcId());
            return true;
        } catch (Exception e) {
            LOG.error(e.getMessage());
            return false;
        }
    }

    @Override
    public boolean addHyperEdge(HEdge hEdge) {
        throw new NotImplementedException("");
    }

    @Override
    public boolean updateAttachedFeature(Feature<?, ?> feature) {
        return false;
    }

    @Override
    public boolean updateStandaloneFeature(Feature<?, ?> feature) {
        return false;
    }

    @Override
    public boolean updateVertex(Vertex vertex) {
        return false;
    }

    @Override
    public boolean updateEdge(UniEdge uniEdge) {
        return false;
    }

    @Override
    public boolean updateHyperEdge(HEdge hEdge) {
        return false;
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
    public boolean deleteEdge(UniEdge uniEdge) {
        return false;
    }

    @Override
    public boolean deleteHyperEdge(HEdge hEdge) {
        return false;
    }

    @Nullable
    @Override
    public Vertex getVertex(String id) {
        return null;
    }

    @Override
    public Iterable<Vertex> getVertices() {
        return null;
    }

    @Nullable
    @Override
    public UniEdge getEdge(String id) {
        return null;
    }

    @Override
    public Iterable<UniEdge> getEdges(String src, String dest) {
        return null;
    }

    @Override
    public Iterable<UniEdge> getIncidentEdges(Vertex vertex, EdgeType edge_type) {
        return null;
    }

    @Override
    public HEdge getHyperEdge(String id) {
        return null;
    }

    @Override
    public Iterable<HEdge> getHyperEdges(Vertex id) {
        return null;
    }

    @Nullable
    @Override
    public Feature<?, ?> getAttachedFeature(String elementId, String featureName, ElementType elementType, @Nullable String id) {
        return null;
    }

    @Nullable
    @Override
    public Feature<?, ?> getStandaloneFeature(String id) {
        return null;
    }

    @Override
    public boolean containsVertex(String id) {
        return false;
    }

    @Override
    public boolean containsAttachedFeature(String elementId, String featureName, ElementType elementType, @Nullable String id) {
        return false;
    }

    @Override
    public boolean containsStandaloneFeature(String id) {
        return false;
    }

    @Override
    public boolean containsEdge(String id) {
        return false;
    }

    @Override
    public boolean containsHyperEdge(String id) {
        return false;
    }

    @Override
    public void cacheFeaturesOf(GraphElement e) {

    }
}
