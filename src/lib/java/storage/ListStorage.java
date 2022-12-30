package storage;

import com.esotericsoftware.reflectasm.ConstructorAccess;
import elements.*;
import elements.enums.CacheFeatureContext;
import elements.enums.EdgeType;
import elements.enums.ElementType;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
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
    Map<Short, Map<String, Tuple3<ObjectOpenHashSet<Tuple2<String,String>>, ObjectOpenHashSet<Tuple2<String,String>>, Object[]>>> localVertexTable = new NonBlockingHashMap<>();

    /**
     * Simple thread local for edge containement checks
     */
    ThreadLocal<Tuple2<String, String>> reuse = ThreadLocal.withInitial(Tuple2::new);

    @Override
    public boolean addAttachedFeature(Feature<?, ?> feature) {
        if(feature.getAttachedElementType() == ElementType.VERTEX){
            vertexFeatureInfo.computeIfAbsent(feature.getName(), (key)-> Tuple3.of(feature.isHalo(), ConstructorAccess.get(feature.getClass()), vertexFeatureIndex.getAndIncrement()));
            localVertexTable.get(getRuntimeContext().getCurrentPart()).compute((String) feature.getAttachedElementId(), (key, val)->{
                if(val.f2 == null) val.f2 = new Object[0];
                int indexOfFeature = vertexFeatureInfo.get(feature.getName()).f2;
                if(indexOfFeature >= val.f2.length){
                    Object[] tmp = new Object[indexOfFeature + 1];
                    System.arraycopy(val.f2,0, tmp, 0, val.f2.length);
                    val.f2 = tmp;
                }
                val.f2[indexOfFeature] = feature.value;
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
        localVertexTable.computeIfAbsent(getRuntimeContext().getCurrentPart(), (key)->new HashMap<>());
        localVertexTable.get(getRuntimeContext().getCurrentPart()).put(vertex.getId(), Tuple3.of(null, null, null));
        return true;
    }

    @Override
    public boolean addEdge(DirectedEdge directedEdge) {
        localVertexTable.get(getRuntimeContext().getCurrentPart()).compute(directedEdge.getSrcId(), (key, val)->{
           if(val.f1 == null) val.f1 = new ObjectOpenHashSet<>();
           val.f1.add(Tuple2.of(directedEdge.getDestId(), directedEdge.getAttribute()));
           return val;
        });
        localVertexTable.get(getRuntimeContext().getCurrentPart()).compute(directedEdge.getDestId(), (key, val)->{
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
            Object[] features = localVertexTable.get(getRuntimeContext().getCurrentPart()).get((String) feature.getAttachedElementId()).f2;
            features[index] = feature.value;
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
        return new Vertex(vertexId, masterPart);
    }

    @Override
    public Iterable<Vertex> getVertices() {
        try {
            return () -> localVertexTable.get(getRuntimeContext().getCurrentPart()).keySet().stream().map(item -> {
                short masterPart = vertexMasterTable.get(item);
                return new Vertex(item, masterPart);
            }).iterator();
        }
        catch (NullPointerException e) {
            return Collections.emptyList();
        }
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
            Object value = localVertexTable.get(getRuntimeContext().getCurrentPart()).get((String) ids.f1).f2[featureInfo.f2];
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
        try{
            return localVertexTable.get(getRuntimeContext().getCurrentPart()).containsKey(vertexId);
        }catch (NullPointerException e){
            return false;
        }
    }

    @Override
    public boolean containsAttachedFeature(Tuple3<ElementType, Object, String> ids) {
        try {
            if (ids.f0 == ElementType.VERTEX) {
                int index = vertexFeatureInfo.get(ids.f2).f2;
                Object[]features = localVertexTable.get(getRuntimeContext().getCurrentPart()).get((String) ids.f1).f2;
                return features.length > index && features[index] != null;
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
        try{
            Tuple2<String, String> accessKey = reuse.get();
            accessKey.f0 = ids.f1;
            accessKey.f1 = ids.f2;
            return localVertexTable.get(getRuntimeContext().getCurrentPart()).get(ids.f0).f1.contains(accessKey);
        }catch (NullPointerException e){
            return false;
        }
    }

    @Override
    public boolean containsHyperEdge(String hyperEdgeId) {
        throw new NotImplementedException("Delete not implemented yet");
    }

    @Override
    public void cacheFeatures(GraphElement element, CacheFeatureContext context) {
        if(element.getType() == ElementType.VERTEX) {
            Object[] features = localVertexTable.get(getRuntimeContext().getCurrentPart()).get((String) element.getId()).f2;
            for (Map.Entry<String, Tuple3<Boolean, ConstructorAccess<? extends Feature>, Integer>> stringTuple3Entry : vertexFeatureInfo.entrySet()) {
                if ((!stringTuple3Entry.getValue().f0 && context == CacheFeatureContext.HALO) || (stringTuple3Entry.getValue().f0 && context == CacheFeatureContext.NON_HALO))
                    continue; // This feature not needed to cache

            }
            for (Map.Entry<Tuple2<String, ElementType>, Tuple3<MapState<Object, Object>, Boolean, ConstructorAccess<? extends Feature>>> tuple2Tuple3Entry : attFeatureTable.entrySet()) {
                if ((!tuple2Tuple3Entry.getValue().f1 && context == CacheFeatureContext.HALO) || (tuple2Tuple3Entry.getValue().f1 && context == CacheFeatureContext.NON_HALO) || tuple2Tuple3Entry.getKey().f1 != element.getType() || !tuple2Tuple3Entry.getValue().f0.contains(element.getId()))
                    continue; // This feature not needed to cache
                if (element.features != null && element.features.stream().anyMatch(item -> item.getName().equals(tuple2Tuple3Entry.getKey().f0)))
                    return; // Already cached check
                Feature feature = tuple2Tuple3Entry.getValue().f2.newInstance();
                feature.id.f2 = tuple2Tuple3Entry.getKey().f0;
                feature.halo = tuple2Tuple3Entry.getValue().f1;
                feature.value = tuple2Tuple3Entry.getValue().f0.get(element.getId());
                feature.setElement(element, false);
            }
        }
    }

    @Override
    public void clear() {

    }
}
