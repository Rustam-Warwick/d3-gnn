package storage;

import elements.*;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class HashMapStorage extends BaseStorage {
    public transient MapState<String, Integer> translationTable;
    public transient MapState<Integer, String> reverseTranslationTable;
    public transient MapState<Integer, Vertex> vertexTable;
    public transient MapState<Integer, Feature> featureTable;
    public transient MapState<Integer, List<Integer>> elementFeatures;
    public transient MapState<Integer, List<Integer>> vertexOutEdges;
    public transient MapState<Integer, List<Integer>> vertexInEdges;
    public transient ValueState<Integer> lastId;
    public transient ValueState<List<String>> featureFieldNames;

    @Override
    public boolean deleteFeature(Feature feature) {
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
    public Iterable<Edge> getEdges() {
        return null;
    }

    @Override
    public void open() throws Exception {
        super.open();
        MapStateDescriptor<String, Integer> translationTableDesc = new MapStateDescriptor<String, Integer>("translationTable", String.class, Integer.class);
        MapStateDescriptor<Integer, String> reverseTranslationTableDesc = new MapStateDescriptor("reverseTranslationTable", Integer.class, String.class);
        MapStateDescriptor<Integer, Vertex> vertexTableDesc = new MapStateDescriptor("vertexTable", Integer.class, Vertex.class);
        MapStateDescriptor<Integer, Feature> featureTableDesc = new MapStateDescriptor("featureTable", Integer.class, Feature.class);
        MapStateDescriptor<Integer, List<Integer>> elementFeaturesDesc = new MapStateDescriptor("elementFeatures", Integer.class, List.class);
        MapStateDescriptor<Integer, List<Integer>> vertexOutEdgesDesc = new MapStateDescriptor("vertexOutEdges", Integer.class, List.class);
        MapStateDescriptor<Integer, List<Integer>> vertexInEdgesDesc = new MapStateDescriptor("vertexInEdges", Integer.class, List.class);
        ValueStateDescriptor<Integer> lastIdDesc = new ValueStateDescriptor<Integer>("lastId", Integer.class);
        ValueStateDescriptor<List<String>> featureFieldNamesDescriptor = new ValueStateDescriptor("featureFieldNames", List.class);
        this.translationTable = layerFunction.getRuntimeContext().getMapState(translationTableDesc);
        this.reverseTranslationTable = layerFunction.getRuntimeContext().getMapState(reverseTranslationTableDesc);
        this.vertexTable = layerFunction.getRuntimeContext().getMapState(vertexTableDesc);
        this.featureTable = layerFunction.getRuntimeContext().getMapState(featureTableDesc);
        this.elementFeatures = layerFunction.getRuntimeContext().getMapState(elementFeaturesDesc);
        this.vertexOutEdges = layerFunction.getRuntimeContext().getMapState(vertexOutEdgesDesc);
        this.vertexInEdges = layerFunction.getRuntimeContext().getMapState(vertexInEdgesDesc);
        this.lastId = layerFunction.getRuntimeContext().getState(lastIdDesc);
        this.featureFieldNames = layerFunction.getRuntimeContext().getState(featureFieldNamesDescriptor);
    }

    private int getLastId() throws IOException {
        Integer last_id = this.lastId.value();
        if (last_id == null) {
            last_id = 0;
        }
        this.lastId.update(last_id + 1); // Increment, better sooner than later
        return last_id;
    }

    private void addFieldName(String fieldName) throws IOException {
        List<String> fieldNames = this.featureFieldNames.value();
        if (fieldNames == null) {
            fieldNames = new ArrayList<>();
        }
        if (!fieldNames.contains(fieldName)) {
            fieldNames.add(fieldName);
        }
        this.featureFieldNames.update(fieldNames);
    }

    @Override
    public boolean addFeature(Feature feature) {
        try {
            if (this.translationTable.contains(feature.getId())) return false;
            int last_id = this.getLastId();
            this.translationTable.put(feature.getId(), last_id);
            this.reverseTranslationTable.put(last_id, feature.getId());
            this.featureTable.put(last_id, feature);
            if (feature.attachedTo._1 != ElementType.NONE) {
                this.addFieldName(feature.getName());
            }
            return true;
        } catch (Exception e) {
            return false;
        }

    }


    @Override
    public boolean addVertex(Vertex vertex) {
        try {
            if (this.translationTable.contains(vertex.getId())) return false;
            int last_id = this.getLastId();
            this.translationTable.put(vertex.getId(), last_id);
            this.reverseTranslationTable.put(last_id, vertex.getId());
            this.vertexTable.put(last_id, vertex);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public boolean addEdge(Edge edge) {
        try {
            int srcIntId = this.translationTable.get(edge.src.getId());
            int destIntId = this.translationTable.get(edge.dest.getId());
            if (!this.vertexInEdges.contains(destIntId)) {
                this.vertexInEdges.put(destIntId, new ArrayList<Integer>());
            }
            if (!this.vertexOutEdges.contains(srcIntId)) {
                this.vertexOutEdges.put(srcIntId, new ArrayList<Integer>());
            }

            List<Integer> destInEdges = this.vertexInEdges.get(destIntId);
            destInEdges.add(srcIntId);
            this.vertexInEdges.put(destIntId, destInEdges);

            List<Integer> srcOutEdges = this.vertexOutEdges.get(srcIntId);
            srcOutEdges.add(destIntId);
            this.vertexOutEdges.put(srcIntId, srcOutEdges);
            return true;

        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public boolean updateFeature(Feature feature) {
        try {
            int featureId = this.translationTable.get(feature.getId());
            this.featureTable.put(featureId, feature);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public boolean updateVertex(Vertex vertex) {
        try {
            int vertexId = this.translationTable.get(vertex.getId());
            this.vertexTable.put(vertexId, vertex);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public boolean updateEdge(Edge edge) {
        return false;
    }

    @Override
    public Vertex getVertex(String id) {
        try {
            int vertexId = this.translationTable.get(id);
            Vertex res = this.vertexTable.get(vertexId);
            res.setStorage(this);
            return res;
        } catch (Exception e) {
            return null;
        }

    }

    @Override
    public Iterable<Vertex> getVertices() {
        try {
            return this.vertexTable.values();
        } catch (Exception e) {
            return null;
        }

    }

    @Override
    public Edge getEdge(String id) {
        try {
            String[] idArr = id.split(":");
            int internalSrcId = translationTable.get(idArr[0]);
            int internalDestId = translationTable.get(idArr[1]);
            List<Integer> destIds = vertexOutEdges.get(internalSrcId);
            if (!destIds.contains(internalDestId)) return null;
            Vertex src = getVertex(idArr[0]);
            Vertex dest = getVertex(idArr[1]);
            Edge edge = new Edge(src, dest);
            edge.setStorage(this);
            return edge;
        } catch (Exception e) {
            return null;
        }

    }

    @Override
    public Iterable<Edge> getIncidentEdges(Vertex vertex, EdgeType edge_type) {
        try {
            int vertex_id = this.translationTable.get(vertex.getId());
            switch (edge_type) {
                case IN:
                    return this.vertexInEdges.get(vertex_id).stream().map(srcId -> {
                        try {
                            Vertex src = this.vertexTable.get(srcId);
                            Edge tmp = new Edge(src, vertex);
                            tmp.setStorage(this);
                            return tmp;
                        } catch (Exception e) {
                            return null;
                        }
                    }).collect(Collectors.toList());
                case OUT:
                    return this.vertexOutEdges.get(vertex_id).stream().map(destId -> {
                        try {
                            Vertex dest = this.vertexTable.get(destId);
                            Edge tmp = new Edge(vertex, dest);
                            tmp.setStorage(this);
                            return tmp;
                        } catch (Exception e) {
                            return null;
                        }
                    }).collect(Collectors.toList());
                default:
                    return new ArrayList<>();
            }
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }

    @Override
    public Feature getFeature(String id) {
        try {
            int featureId = this.translationTable.get(id);
            Feature res = this.featureTable.get(featureId);
            res.setStorage(this);
            return res;
        } catch (Exception e) {
            return null;
        }
    }


    @Override
    public void cacheFeaturesOf(GraphElement e) {
        try {
            List<String> fieldNames = featureFieldNames.value();
            if (Objects.nonNull(fieldNames) && fieldNames.size() > 0) {
                fieldNames.forEach(e::getFeature);
            }
        } catch (Exception ignored) {

        }


    }


}