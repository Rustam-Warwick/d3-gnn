package storage;

import elements.*;
import elements.Feature;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ValueState;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class HashMapStorage extends BaseStorage{
    public MapState<String, Integer> translationTable;
    public MapState<Integer, String> reverseTranslationTable;
    public MapState<Integer, Vertex> vertexTable;
    public MapState<Integer, Feature> featureTable;
    public MapState<Integer, List<Integer>> elementFeatures;
    public MapState<Integer, List<Integer>> vertexOutEdges;
    public MapState<Integer, List<Integer>> vertexInEdges;
    public ValueState<Integer> lastId;



    @Override
    public boolean addFeature(Feature feature) {
        try{
            if(this.translationTable.contains(feature.getId()))return false;
            int last_id = this.lastId.value();
            this.translationTable.put(feature.getId(),last_id);
            this.reverseTranslationTable.put(last_id, feature.getId());
            this.featureTable.put(last_id, feature);
            if(feature.attachedTo._1 != ElementType.NONE){
                // Feature belongs to some other element
                int elementId = this.translationTable.get((String) feature.attachedTo._2());
                if(!this.elementFeatures.contains(elementId)){
                    this.elementFeatures.put(elementId, new ArrayList<>());
                }
                List<Integer> featureIds = this.elementFeatures.get(elementId);
                featureIds.add(last_id);
                this.elementFeatures.put(elementId, featureIds);
            }
            this.lastId.update(last_id + 1);
            return true;
        }catch (Exception e){
            return false;
        }

    }

    @Override
    public boolean addVertex(Vertex vertex){
        try{
            if(this.translationTable.contains(vertex.getId()))return false;
            int last_id = this.lastId.value();
            this.translationTable.put(vertex.getId(),last_id);
            this.reverseTranslationTable.put(last_id, vertex.getId());
            this.vertexTable.put(last_id, vertex);
            this.lastId.update(last_id + 1);
            return true;
        }
        catch (Exception e){
            return false;
        }
    }

    @Override
    public boolean addEdge(Edge edge) {
       try{
           int srcIntId = this.translationTable.get(edge.src.getId());
           int destIntId = this.translationTable.get(edge.dest.getId());
           if(!this.vertexInEdges.contains(destIntId)){
               this.vertexInEdges.put(destIntId, new ArrayList<Integer>());
           }
           if(!this.vertexOutEdges.contains(srcIntId)){
               this.vertexOutEdges.put(srcIntId, new ArrayList<Integer>());
           }

           List<Integer> destInEdges = this.vertexInEdges.get(destIntId);
           destInEdges.add(srcIntId);
           this.vertexInEdges.put(destIntId, destInEdges);

           List<Integer> srcOutEdges = this.vertexOutEdges.get(srcIntId);
           srcOutEdges.add(destIntId);
           this.vertexOutEdges.put(srcIntId, srcOutEdges);
           return true;

       }catch (Exception e){
           return false;
       }
    }

    @Override
    public boolean addAggregator(Aggregator agg) {
        return false;
    }

    @Override
    public boolean updateFeature(Feature feature) {
        return false;
    }

    @Override
    public boolean updateVertex(Vertex vertex) {
        return false;
    }

    @Override
    public boolean updateEdge(Edge edge) {
        return false;
    }

    @Override
    public Vertex getVertex(String id) {
        return null;
    }

    @Override
    public Stream<Vertex> getVertices() {
        return null;
    }

    @Override
    public Edge getEdge(String id) {
        return null;
    }

    @Override
    public Stream<Edge> getIncidentEdges(Vertex vertex, String edge_type) {
        return null;
    }

    @Override
    public Feature getFeature(String id) {
        return null;
    }

    @Override
    public Map<String, Feature> getFeatures(GraphElement e) {
        return null;
    }

    @Override
    public Aggregator getAggregator(String id) {
        return null;
    }

    @Override
    public Stream<Aggregator> getAggregators() {
        return null;
    }

    @Override
    public void message(GraphOp op) {

    }
}
