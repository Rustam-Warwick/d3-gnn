package storage;

import elements.*;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;

import java.util.*;

public class TupleStorage extends BaseStorage {
    public HashSet<String> fieldNames = new HashSet<>();
    public transient MapState<String, Short> vertexTable;
    public transient MapState<String, Feature<?, ?>> featureTable;
    public transient MapState<String, List<String>> vertexOutEdges;
    public transient MapState<String, List<String>> vertexInEdges;
    public transient MapState<String, Integer> edgeTimestamps;


    @Override
    public void open() throws Exception {
        super.open();
        MapStateDescriptor<String, Short> vertexTableDesc = new MapStateDescriptor("vertexTable", String.class, Short.class);
        MapStateDescriptor<String, Feature<?, ?>> featureTableDesc = new MapStateDescriptor("featureTable", String.class, Feature.class);
        MapStateDescriptor<String, List<String>> vertexOutEdgesDesc = new MapStateDescriptor("vertexOutEdges", String.class, List.class);
        MapStateDescriptor<String, List<String>> vertexInEdgesDesc = new MapStateDescriptor("vertexInEdges", String.class, List.class);
        MapStateDescriptor<String, Integer> edgeTimeStamps = new MapStateDescriptor("edgeTimestamps", String.class, Integer.class);
        this.vertexTable = layerFunction.getRuntimeContext().getMapState(vertexTableDesc);
        this.featureTable = layerFunction.getRuntimeContext().getMapState(featureTableDesc);
        this.vertexOutEdges = layerFunction.getRuntimeContext().getMapState(vertexOutEdgesDesc);
        this.vertexInEdges = layerFunction.getRuntimeContext().getMapState(vertexInEdgesDesc);
        this.edgeTimestamps = layerFunction.getRuntimeContext().getMapState(edgeTimeStamps);
    }

    private void registerFieldName(Feature feature) {
        fieldNames.add(feature.getName());
    }

    @Override
    public boolean addFeature(Feature feature) {
        try {
            if (featureTable.contains(feature.getId()))
                throw new Exception("Graph Element exists"); // if exists not create;
            if (feature.attachedTo._1 == ElementType.VERTEX) {
                if (!vertexTable.contains((String) feature.attachedTo._2())) throw new Exception("Vertex not here yet");
                registerFieldName(feature);
            }
            if (feature.attachedTo._1 == ElementType.EDGE) {
                if (Objects.isNull(getEdge((String) feature.attachedTo._2))) throw new Exception("Edge not here yet");
                registerFieldName(feature);
            }

            this.featureTable.put(feature.getId(), (Feature<?, ?>) feature.copy());
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

    }


    @Override
    public boolean addVertex(Vertex vertex) {
        try {
            if (vertexTable.contains(vertex.getId())) throw new Exception("Vertex exists");
            vertexTable.put(vertex.getId(), vertex.masterPart());
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public boolean addEdge(Edge edge) {
        try {
            if (!vertexTable.contains(edge.src.getId()) || !vertexTable.contains(edge.dest.getId()))
                throw new Exception("One vertex does not exists");
            if (!this.vertexOutEdges.contains(edge.src.getId())) {
                this.vertexOutEdges.put(edge.src.getId(), new ArrayList<String>());
            }
            if (!this.vertexInEdges.contains(edge.dest.getId())) {
                this.vertexInEdges.put(edge.dest.getId(), new ArrayList<String>());
            }

            List<String> srcOutEdges = this.vertexOutEdges.get(edge.src.getId());
            srcOutEdges.add(edge.dest.getId());
            this.vertexOutEdges.put(edge.src.getId(), srcOutEdges);

            List<String> destInEdges = this.vertexInEdges.get(edge.dest.getId());
            destInEdges.add(edge.src.getId());
            this.vertexInEdges.put(edge.dest.getId(), destInEdges);
            this.edgeTimestamps.put(edge.getId(), edge.getTimestamp());
            return true;

        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public boolean updateFeature(Feature feature) {
        try {
            if (!featureTable.contains(feature.getId())) throw new Exception("Feature not here");
            this.featureTable.put(feature.getId(), (Feature<?, ?>) feature.copy());
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public boolean updateVertex(Vertex vertex) {
        try {
            if (!vertexTable.contains(vertex.getId())) throw new Exception("Vertex not here");
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public boolean updateEdge(Edge edge) {
        try {
            edgeTimestamps.put(edge.getId(), edge.getTimestamp());
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public boolean deleteFeature(Feature feature) {
        try {
            featureTable.remove(feature.getId());
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public boolean deleteVertex(Vertex vertex) {
        try {
            vertexOutEdges.remove(vertex.getId());
            vertexInEdges.remove(vertex.getId());
            vertexTable.remove(vertex.getId());
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public boolean deleteEdge(Edge edge) {
        try {
            List<String> outEdges = vertexOutEdges.get(edge.src.getId());
            List<String> inEdges = vertexInEdges.get(edge.dest.getId());
            outEdges.remove(edge.dest.getId());
            inEdges.remove(edge.src.getId());
            vertexOutEdges.put(edge.src.getId(), outEdges);
            vertexInEdges.put(edge.dest.getId(), inEdges);
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
            if (!vertexOutEdges.contains(srcId) || !vertexInEdges.contains(destId))
                throw new Exception("Vertices not here"); // If not initialized no overlap
            if (!vertexOutEdges.get(srcId).contains(destId))
                throw new Exception("Edge does not exist"); // Really not in the list
            // Otherwise edge exists
            Vertex src = getVertex(srcId);
            Vertex dest = getVertex(destId);
            Edge e = new Edge(src, dest);
            int ts = edgeTimestamps.get(e.getId());
            e.setTimestamp(ts);
            e.setStorage(this);
            return e;
        } catch (Exception e) {
            return null;
        }
    }


    @Override
    public Edge getEdge(String id) {
        try {
            String[] idArr = id.split(":");
            return getEdge(idArr[0], idArr[1]);
        } catch (Exception e) {
            return null;
        }

    }

    @Override
    public Iterable<Edge> getEdges() {
        return null;
    }


    @Override
    public Iterable<Edge> getIncidentEdges(Vertex vertex, EdgeType edge_type) {
        try {
            Iterator<Edge> inIterator = IteratorUtils.emptyIterator();
            Iterator<Edge> outIterator = IteratorUtils.emptyIterator();
            BaseStorage _this = this;

            if (edge_type == EdgeType.IN || edge_type == EdgeType.BOTH) {
                if (vertexInEdges.contains(vertex.getId())) {
                    List<String> tmp = vertexInEdges.get(vertex.getId());
                    inIterator = IteratorUtils.transformedIterator(tmp.iterator(), item -> {
                        try {
                            String srcId = (String) item;
                            Vertex src = getVertex(srcId);
                            Edge e = new Edge(src, vertex);
                            int ts = edgeTimestamps.get(e.getId());
                            e.setTimestamp(ts);
                            e.setStorage(_this);
                            return e;
                        } catch (Exception e) {
                            return null;
                        }
                    });
                }
            }

            if (edge_type == EdgeType.OUT || edge_type == EdgeType.BOTH) {
                if (vertexOutEdges.contains(vertex.getId())) {
                    List<String> tmp = vertexOutEdges.get(vertex.getId());
                    outIterator = IteratorUtils.transformedIterator(tmp.iterator(), item -> {
                        try {
                            String destId = (String) item;
                            Vertex dest = getVertex(destId);
                            Edge e = new Edge(vertex, dest);
                            int ts = edgeTimestamps.get(e.getId());
                            e.setTimestamp(ts);
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
            Feature<?, ?> res = this.featureTable.get(id).copy(); // Copy so that in case if this is in-memory caches are not stored
            res.setStorage(this);
            return res;
        } catch (Exception e) {
            return null;
        }
    }


    @Override
    public void cacheFeaturesOf(GraphElement e) {
        try {
            if (Objects.nonNull(fieldNames) && fieldNames.size() > 0) {
                fieldNames.forEach(e::getFeature);
            }
        } catch (Exception ignored) {

        }


    }


}