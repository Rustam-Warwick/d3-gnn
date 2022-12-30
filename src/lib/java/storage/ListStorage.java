package storage;

import com.esotericsoftware.reflectasm.ConstructorAccess;
import elements.*;
import elements.enums.CacheFeatureContext;
import elements.enums.EdgeType;
import elements.enums.ElementType;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

public class ListStorage extends BaseStorage {

    /**
     * Master Part table for vertices. This table is shared across tasks as vertices unique
     */
    NonBlockingHashMap<String, Short> vertexMasterTable = new NonBlockingHashMap<>(1000);

    /**
     * Vertex feature -> [halo, constructor, index in the vertex table]
     */
    NonBlockingHashMap<String, Tuple3<Boolean, ConstructorAccess<? extends Feature>, Integer>> vertexFeatureInfo = new NonBlockingHashMap<>();

    /**
     * Counter for vertex features, the localVertex table will hold feature in a list according to this index
     */
    AtomicInteger vertexFeatureIndex = new AtomicInteger(0);

    /**
     * Vertex table described by vertex id and part so it is unique for tasks
     * * (PartID, VertexId) -> [In-Edge-Table, Out-Edge-Table, [feature values]]
     */
    NonBlockingHashMap<Tuple2<Short, String>, Tuple3<ObjectOpenHashSet<Tuple2<String,String>>, ObjectOpenHashSet<Tuple2<String,String>>, ObjectArrayList<Object>>> localVertexTable = new NonBlockingHashMap<>();


    @Override
    public boolean addAttachedFeature(Feature<?, ?> feature) {
        if(feature.getAttachedElementType() == ElementType.VERTEX){
            vertexFeatureInfo.computeIfAbsent(feature.getName(), (key)-> Tuple3.of(feature.isHalo(), ConstructorAccess.get(feature.getClass()), vertexFeatureIndex.getAndIncrement()));
            localVertexTable.compute(Tuple2.of(getRuntimeContext().getCurrentPart(), (String) feature.getAttachedElementId()), (key, val)->{
                if(val.f2 == null) val.f2 = new ObjectArrayList<>();
                int indexOfFeature = vertexFeatureInfo.get(feature.getName()).f2;
                for (int i = val.f2.size(); i <= indexOfFeature; i++) {
                    val.f2.add(null);
                }
                val.f2.add(indexOfFeature, feature.value);
                return val;
            });
            return true;
        }
        return false;
    }

    @Override
    public boolean addStandaloneFeature(Feature<?, ?> feature) {
        throw new NotImplementedException("Delete not implemented yet");
    }

    @Override
    public boolean addVertex(Vertex vertex) {
        vertexMasterTable.putIfAbsent(vertex.getId(), vertex.getMasterPart());
        localVertexTable.put(Tuple2.of(vertex.getPart(), vertex.getId()), Tuple3.of(null, null, null));
        return true;
    }

    @Override
    public synchronized boolean addEdge(DirectedEdge directedEdge) {
        Tuple2<Short, String> accessKey = Tuple2.of(getRuntimeContext().getCurrentPart(), directedEdge.getSrcId());
        localVertexTable.compute(accessKey, (key, val)->{
           if(val.f1 == null) val.f1 = new ObjectOpenHashSet<>();
           val.f1.add(Tuple2.of(directedEdge.getDestId(), directedEdge.getAttribute()));
           return val;
        });
        accessKey.f1 = directedEdge.getDestId();
        localVertexTable.compute(accessKey, (key, val)->{
            if(val.f0 == null) val.f0 = new ObjectOpenHashSet<>();
            val.f0.add(Tuple2.of(directedEdge.getSrcId(), directedEdge.getAttribute()));
            return val;
        });
        return true;
    }

    @Override
    public boolean addHyperEdge(HyperEdge hyperEdge) {
        throw new NotImplementedException("Delete not implemented yet");
    }

    @Override
    public boolean updateAttachedFeature(Feature<?, ?> feature, Feature<?, ?> memento) {
        if(feature.getAttachedElementType() == ElementType.VERTEX){
            int index = vertexFeatureInfo.get(feature.getName()).f2;
            ObjectArrayList<Object> features = localVertexTable.get(Tuple2.of(getRuntimeContext().getCurrentPart(), feature.getAttachedElementId())).f2;
            features.add(index, feature.value);
            return true;
        }
        return false;
    }

    @Override
    public boolean updateStandaloneFeature(Feature<?, ?> feature, Feature<?, ?> memento) {
        throw new NotImplementedException("Delete not implemented yet");
    }

    @Override
    public boolean updateVertex(Vertex vertex, Vertex memento) {
        return true;
    }

    @Override
    public boolean updateEdge(DirectedEdge directedEdge, DirectedEdge memento) {
        return true;
    }

    @Override
    public boolean updateHyperEdge(HyperEdge hyperEdge, HyperEdge memento) {
        throw new NotImplementedException("Delete not implemented yet");
    }

    @Override
    public boolean deleteAttachedFeature(Feature<?, ?> feature) {
        throw new NotImplementedException("Delete not implemented yet");
    }

    @Override
    public boolean deleteStandaloneFeature(Feature<?, ?> feature) {
        throw new NotImplementedException("Delete not implemented yet");
    }

    @Override
    public boolean deleteVertex(Vertex vertex) {
        throw new NotImplementedException("Delete not implemented yet");
    }

    @Override
    public boolean deleteEdge(DirectedEdge directedEdge) {
        throw new NotImplementedException("Delete not implemented yet");
    }

    @Override
    public boolean deleteHyperEdge(HyperEdge hyperEdge) {
        throw new NotImplementedException("Delete not implemented yet");
    }

    @Override
    public @Nullable Vertex getVertex(String vertexId) {
        short masterPart = vertexMasterTable.get(vertexId);
        return new Vertex(vertexId,masterPart);
    }

    @Override
    public Iterable<Vertex> getVertices() {
        short currentPart = getRuntimeContext().getCurrentPart();
        return () -> localVertexTable.keySet().stream().filter(key -> key.f0 == currentPart).map(item -> {
            short masterPart = vertexMasterTable.get(item.f1);
            return new Vertex(item.f1, masterPart);
        }).iterator();
    }

    @Override
    public @Nullable DirectedEdge getEdge(Tuple3<String, String, String> ids) {
        return new DirectedEdge(ids.f0, ids.f1, ids.f2);
    }

    @Override
    public Iterable<DirectedEdge> getIncidentEdges(Vertex vertex, EdgeType edge_type) {
        return Collections.emptyList();
    }

    @Override
    public @Nullable HyperEdge getHyperEdge(String hyperEdgeId) {
        throw new NotImplementedException("Delete not implemented yet");
    }

    @Override
    public Iterable<HyperEdge> getIncidentHyperEdges(Vertex vertex) {
        throw new NotImplementedException("Delete not implemented yet");
    }

    @Override
    public @Nullable Feature<?, ?> getAttachedFeature(Tuple3<ElementType, Object, String> ids) {
        if(ids.f0 == ElementType.VERTEX){
            Tuple3<Boolean, ConstructorAccess<? extends Feature>, Integer> featureInfo = vertexFeatureInfo.get(ids.f2);
            Object value = localVertexTable.get(Tuple2.of(getRuntimeContext().getCurrentPart(), ids.f1)).f2.get(featureInfo.f2);
            Feature feature = featureInfo.f1.newInstance();
            feature.value = value;
            feature.id.f0 = ids.f0;
            feature.id.f1 = ids.f1;
            feature.id.f2 = ids.f2;
            feature.halo = featureInfo.f0;
            return feature;
        }
        return null;
    }

    @Override
    public @Nullable Feature<?, ?> getStandaloneFeature(Tuple3<ElementType, Object, String> ids) {
        throw new NotImplementedException("Delete not implemented yet");
    }

    @Override
    public boolean containsVertex(String vertexId) {
        return localVertexTable.contains(Tuple2.of(getRuntimeContext().getCurrentPart(), vertexId));
    }

    @Override
    public boolean containsAttachedFeature(Tuple3<ElementType, Object, String> ids) {
        try {
            if (ids.f0 == ElementType.VERTEX) {
                int index = vertexFeatureInfo.get(ids.f2).f2;
                ObjectArrayList<Object> features = localVertexTable.get(Tuple2.of(getRuntimeContext().getCurrentPart(), ids.f1)).f2;
                return features.size() > index && features.get(index) != null;
            }
            return false;
        }catch (NullPointerException ignored) {
            return false;
        }
    }

    @Override
    public boolean containsStandaloneFeature(Tuple3<ElementType, Object, String> ids) {
        throw new NotImplementedException("Delete not implemented yet");
    }

    @Override
    public boolean containsEdge(Tuple3<String, String, String> ids) {
        Tuple2<Short,String> accessKey = Tuple2.of(getRuntimeContext().getCurrentPart(), ids.f0);
        if(localVertexTable.contains(accessKey)){
            return localVertexTable.get(accessKey).f1.contains(Tuple2.of(ids.f1, ids.f2));
        };
        return false;
    }

    @Override
    public boolean containsHyperEdge(String hyperEdgeId) {
        throw new NotImplementedException("Delete not implemented yet");
    }

    @Override
    public void cacheFeatures(GraphElement element, CacheFeatureContext context) {

    }

    @Override
    public void clear() {

    }
}
