package storage;

import elements.*;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;

public class FlatInMemoryClassStorage extends BaseStorage{
    protected MapState<String, Vertex> vertexTable;
    protected MapState<String, Feature<?,?>> attachedFeatureTable;
    protected MapState<String, HashMap<String, Edge>> edgeTable;

    public FlatInMemoryClassStorage() {

    }

    @Override
    public void open() throws Exception {
        super.open();
        MapStateDescriptor<String, Vertex> vertexTableDesc = new MapStateDescriptor<>("vertexTable", String.class, Vertex.class);
        MapStateDescriptor<String, HashMap<String, Edge>> edgeTableDesc= new MapStateDescriptor<>("edgeTable", TypeInformation.of(String.class), TypeInformation.of(new TypeHint<HashMap<String, Edge>>() {
        }));
        MapStateDescriptor<String, Feature<?,?>> featureTableDesc = new MapStateDescriptor<>("featureTable", TypeInformation.of(String.class), TypeInformation.of(new TypeHint<Feature<?, ?>>() {
        }));

        vertexTable = layerFunction.getRuntimeContext().getMapState(vertexTableDesc);
        edgeTable = layerFunction.getRuntimeContext().getMapState(edgeTableDesc);
        attachedFeatureTable = layerFunction.getRuntimeContext().getMapState(featureTableDesc);

    }

    @Override
    public boolean addFeature(Feature<?,?> feature) {
        try{
            if(feature.attachedTo == null) throw new IllegalStateException("Independent Features not supported here");
            else{
                GraphElement el = getElement(feature.attachedTo.f1, feature.attachedTo.f0);
                feature.setElement(el);
                if(feature.element == el){
                    attachedFeatureTable.put(feature.getId(), feature);
                    return true;
                }else{
                    return false;
                }
            }
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean addVertex(Vertex vertex) {
        try{
            vertexTable.put(vertex.getId(), vertex);
            return true;
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean addEdge(Edge edge) {
        try{
            if(!edgeTable.contains(edge.src.getId()))edgeTable.put(edge.src.getId(), new HashMap<>());
            edge.src = getVertex(edge.src.getId());
            edge.dest = getVertex(edge.dest.getId());
            edgeTable.get(edge.src.getId()).put(edge.dest.getId(), edge);
            return true;
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean updateFeature(Feature<?,?> feature) {
        try{
            if(feature.attachedTo == null) throw new IllegalStateException("Independent Features not supported yet");
            return true;
        }catch (Exception e){
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
    public boolean deleteFeature(Feature<?,?> feature) {
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
    public Edge getEdge(String id) {
        try{
            String[] ids = id.split(":");
            return edgeTable.get(ids[0]).get(ids[1]);
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }


    @Override
    public Iterable<Edge> getIncidentEdges(Vertex vertex, EdgeType edge_type) {
        try{
            switch (edge_type){
                case IN:
                    throw new IllegalStateException("In Edges not suported");
                case OUT:
                    return edgeTable.contains(vertex.getId())?edgeTable.get(vertex.getId()).values():Collections.emptyList();
                case BOTH:
                    Iterator<Edge> finalIterator = IteratorUtils.chainedIterator(getIncidentEdges(vertex, EdgeType.IN).iterator(), getIncidentEdges(vertex, EdgeType.OUT).iterator());
                    return ()->finalIterator;
                default:
                    return Collections.emptyList();
            }
        }catch (Exception e){
            e.printStackTrace();
            return Collections.emptyList();
        }
    }

    @Nullable
    @Override
    public Feature<?, ?> getFeature(String id) {
        try{
            if(id.contains(":")){
                // This is attached feature
               return attachedFeatureTable.get(id);
            }else{
                // This is independent Feature
                throw new IllegalStateException("Independed features not supported");
            }
        }catch (Exception e){
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
            if(id.contains(":")){
                // Attached Feature
                return attachedFeatureTable.contains(id);
            }else{
                throw new IllegalStateException("Independent Features not allowed");
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean containsEdge(String id) {
        try{
            String[] ids = id.split(":");
            if(edgeTable.contains(ids[0])){
                return edgeTable.get(ids[0]).containsKey(ids[1]);
            }else{
                return false;
            }
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public void cacheFeaturesOf(GraphElement e) {

    }
}
