package storage;

import elements.*;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

import java.util.*;

public class TupleStorage extends BaseStorage {
    public transient MapState<String, Short> vertexTable;
    public transient MapState<String, Tuple4<Object, Boolean, ElementType, String>> attachedFeatureTable;
    public transient MapState<String, Tuple3<Object, Boolean, Short>> independentFeatureTable;
    public transient MapState<String, HashMap<String, Byte>> edges;
    public transient HashMap<String,TypeInformation<? extends Feature<?,?>>> fieldNames = new HashMap<>();


    @Override
    public void open() throws Exception {
        super.open();
        MapStateDescriptor<String, Short> vertexTableDesc = new MapStateDescriptor<String, Short>("vertexTable", String.class, Short.class);
        MapStateDescriptor<String, Tuple4<Object, Boolean, ElementType, String>> attachedFeatureTableDesc = new MapStateDescriptor<>("attachedFeatureTable", TypeInformation.of(String.class), TypeInformation.of(new TypeHint<Tuple4<Object, Boolean, ElementType, String>>() {
        }));
        MapStateDescriptor<String, Tuple3<Object, Boolean, Short>> independentFeatureTableDesc = new MapStateDescriptor<String, Tuple3<Object, Boolean, Short>>("independentFeatureTable", TypeInformation.of(String.class), TypeInformation.of(new TypeHint<Tuple3<Object, Boolean, Short>>(){}));
        MapStateDescriptor<String, HashMap<String, Byte>> edgesDesc = new MapStateDescriptor("edges", TypeInformation.of(String.class), TypeInformation.of(new TypeHint<HashMap<String, Byte>>(){}));

        this.vertexTable = layerFunction.getRuntimeContext().getMapState(vertexTableDesc);
        this.attachedFeatureTable= layerFunction.getRuntimeContext().getMapState(attachedFeatureTableDesc);
        this.independentFeatureTable= layerFunction.getRuntimeContext().getMapState(independentFeatureTableDesc);
        this.edges = layerFunction.getRuntimeContext().getMapState(edgesDesc);

    }

    @Override
    public boolean addFeature(Feature<?, ?> feature) {
        try{
            if(feature.attachedTo == null){
                // Not attached Feature
                independentFeatureTable.put(feature.getId(), Tuple3.of(feature.value, feature.halo, feature.master));
            }else{
                attachedFeatureTable.put(feature.getId(), Tuple4.of(feature.value, feature.halo, feature.attachedTo.f0, feature.attachedTo.f1));
            }
            return true;
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean addVertex(Vertex vertex) {
        try {
            vertexTable.put(vertex.getId(), vertex.masterPart());
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public boolean addEdge(Edge edge) {
        try {
            HashMap<String, Byte> entries;
            if(edges.contains(edge.src.getId()))entries = edges.get(edge.src.getId());
            else entries = new HashMap<>(3);
            entries.put(edge.dest.getId(), (byte)1);
            edges.put(edge.src.getId(), entries);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean updateFeature(Feature<?,?> feature) {
        try {
            if(feature.attachedTo == null){
                // Not attached Feature
                independentFeatureTable.put(feature.getId(), Tuple3.of(feature.value, feature.halo, feature.master));
            }else{
                attachedFeatureTable.put(feature.getId(), Tuple4.of(feature.value, feature.halo, feature.attachedTo.f0, feature.attachedTo.f1));
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean updateVertex(Vertex vertex) {
        try {
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean updateEdge(Edge edge) {
        try {
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public boolean deleteFeature(Feature<?,?> feature) {
        try {
            if(feature.attachedTo == null){
                independentFeatureTable.remove(feature.getId());
            }else{
                attachedFeatureTable.remove(feature.getId());
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public boolean deleteVertex(Vertex vertex) {
        try {
            vertexTable.remove(vertex.getId());
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public boolean deleteEdge(Edge edge) {
        try {
            HashMap<String, Byte> val = edges.get(edge.src.getId());
            val.remove(edge.dest.getId());
            if(val.isEmpty()) edges.remove(edge.src.getId());
            else edges.put(edge.src.getId(), val);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public Vertex getVertex(String id) {
        try {
            Short masterPart = vertexTable.get(id);
            Vertex res = new Vertex(id, false, masterPart);
            res.setStorage(this);
            return res;
        } catch (Exception e) {
            return null;
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
            return false;
        }
    }

    @Override
    public boolean containsEdge(String id) {
        return false;
    }

    @Override
    public Iterable<Vertex> getVertices() {
        try {
            BaseStorage _this = this;
            Iterator<Vertex> vertexIterator = IteratorUtils.transformedIterator(vertexTable.iterator(), input -> {
                Map.Entry<String, Short> value = (Map.Entry<String, Short>) input;
                Vertex res = new Vertex(value.getKey(), false, value.getValue());
                res.setStorage(_this);
                return res;
            });
            return () -> vertexIterator;
        } catch (Exception e) {
            return null;
        }

    }

    public Edge getEdge(String srcId, String destId) {
        try {
            Vertex src = getVertex(srcId);
            Vertex dest = getVertex(destId);
            Edge e = new Edge(src, dest);
            e.setStorage(this);
            return e;
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public Iterable<Edge> getIncidentEdges(Vertex vertex, EdgeType edge_type) {
        try {
            Iterator<Edge> inIterator = IteratorUtils.emptyIterator();
            Iterator<Edge> outIterator = IteratorUtils.emptyIterator();
            BaseStorage _this = this;

            if (edge_type == EdgeType.IN || edge_type == EdgeType.BOTH) {
                throw new IllegalStateException("IN Edges not implemented yet");
//                if (vertexInEdges.contains(vertex.getId())) {
//                    List<String> tmp = vertexInEdges.get(vertex.getId());
//                    inIterator = IteratorUtils.transformedIterator(tmp.iterator(), item -> {
//                        try {
//                            String srcId = (String) item;
//                            Vertex src = getVertex(srcId);
//                            Edge e = new Edge(src, vertex);
//                            e.setStorage(_this);
//                            return e;
//                        } catch (Exception e) {
//                            return null;
//                        }
//                    });
//                }
            }

            if (edge_type == EdgeType.OUT || edge_type == EdgeType.BOTH) {
                if (edges.contains(vertex.getId())) {
                    Set<String> tmp = edges.get(vertex.getId()).keySet();
                    outIterator = IteratorUtils.transformedIterator(tmp.iterator(), item -> {
                        try {
                            String destId = (String) item;
                            Vertex dest = getVertex(destId);
                            Edge e = new Edge(vertex, dest);
                            e.setStorage(_this);
                            return e;
                        } catch (Exception e) {
                            return null;
                        }
                    });
                }
            }

            if (edge_type == EdgeType.IN) {
                Iterator<Edge> finalIterator = inIterator;
                return () -> finalIterator;
            }
            if (edge_type == EdgeType.OUT) {
                Iterator<Edge> finalIterator = outIterator;
                return () -> finalIterator;
            }
            if (edge_type == EdgeType.BOTH) {
                Iterator<Edge> finalIterator = IteratorUtils.chainedIterator(inIterator, outIterator);
                return () -> finalIterator;
            }

            return Collections.emptyList();
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }

    @Override
    public Feature getFeature(String id) {
        try {
            if (id.contains(":")) {
                // This is attached feature
                Tuple4<Object, Boolean, ElementType, String> tmp = attachedFeatureTable.get(id);
            } else {
                // This is independent Feature
            }

            return null;
        } catch (Exception e) {
            return null;
        }
    }


    @Override
    public boolean containsVertex(String id) {
        try {
            return vertexTable.contains(id);
        } catch (Exception e) {
            return false;
        }
    }


    @Override
    public void cacheFeaturesOf(GraphElement e) {
        try {
            if (Objects.nonNull(fieldNames) && fieldNames.size() > 0) {
                fieldNames.keySet().forEach(e::getFeature);
            }
        } catch (Exception ignored) {

        }


    }


}