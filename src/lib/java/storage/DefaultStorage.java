package storage;

import ai.djl.ndarray.LifeCycleControl;
import com.esotericsoftware.reflectasm.ConstructorAccess;
import elements.*;
import elements.enums.CacheFeatureContext;
import elements.enums.EdgeType;
import elements.enums.ElementType;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.fastutil.shorts.Short2ObjectOpenHashMap;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.state.taskshared.TaskSharedKeyedStateBackend;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public final class DefaultStorage extends GraphStorage {

    /**
     * Master Part table for vertices. This table is shared across tasks as vertices unique
     */
    private final Map<String, Short> vertexMasterTable = new NonBlockingHashMap<>(1000);

    /**
     * Vertex feature -> [halo, constructor, index in the vertex table, LifeCycleManager]
     */
    private final Map<String, Tuple4<Boolean, ConstructorAccess<? extends Feature>, Integer, Boolean>> vertexFeatureInfo = new ConcurrentHashMap<>(10);

    /**
     * Counter for vertex features, the localVertex table will hold feature in a list according to this index
     */
    private final AtomicInteger vertexFeatureIndex = new AtomicInteger(0);

    /**
     * Vertex table described by vertex id and part so it is unique for tasks
     * * (PartID, VertexId) -> [In-Edge-Table, Out-Edge-Table, [feature values]]
     */
    private final Map<Short, Map<String, Tuple3<ObjectOpenHashSet<Tuple2<String, String>>, ObjectOpenHashSet<Tuple2<String, String>>, Object[]>>> localVertexTable = new Short2ObjectOpenHashMap<>();

    /**
     * Simple thread local for edge containement checks
     */
    private final ThreadLocal<Tuple2<String, String>> reuse = ThreadLocal.withInitial(Tuple2::new);

    /**
     * Reuse scopes
     */
    private final ThreadLocal<ReuseScope> scopes = ThreadLocal.withInitial(ReuseScope::new);

    /**
     * Small elements cache for reuse scopes
     */
    private final ThreadLocal<ElementCacheHolderPerScopeLayer> elementCache = ThreadLocal.withInitial(ElementCacheHolderPerScopeLayer::new);


    @Override
    public boolean addAttachedFeature(Feature feature) {
        if (feature.getAttachedElementType() == ElementType.VERTEX) {
            vertexFeatureInfo.computeIfAbsent(feature.getName(), (key) -> Tuple4.of(feature.isHalo(), ConstructorAccess.get(feature.getClass()), vertexFeatureIndex.getAndIncrement(), (feature.value instanceof LifeCycleControl)));
            localVertexTable.get(getRuntimeContext().getCurrentPart()).compute((String) feature.getAttachedElementId(), (key, val) -> {
                if (val.f2 == null) val.f2 = new Object[0];
                Tuple4<?, ?, Integer, Boolean> featureInfo = vertexFeatureInfo.get(feature.getName());
                if (featureInfo.f2 >= val.f2.length) {
                    Object[] tmp = new Object[featureInfo.f2 + 1];
                    System.arraycopy(val.f2, 0, tmp, 0, val.f2.length);
                    val.f2 = tmp;
                }
                val.f2[featureInfo.f2] = feature.value;
                if (featureInfo.f3) ((LifeCycleControl) feature.value).delay();
                return val;
            });
            return true;
        }
        return false;
    }

    @Override
    public boolean addStandaloneFeature(Feature feature) {
        throw new NotImplementedException("Delete not implemented yet");
    }

    @Override
    public boolean addVertex(Vertex vertex) {
        vertexMasterTable.putIfAbsent(vertex.getId(), vertex.getMasterPart());
        localVertexTable.get(getRuntimeContext().getCurrentPart()).put(vertex.getId(), Tuple3.of(null, null, null));
        return true;
    }

    @Override
    public boolean addEdge(DirectedEdge directedEdge) {
        localVertexTable.get(getRuntimeContext().getCurrentPart()).compute(directedEdge.getSrcId(), (key, val) -> {
            if (val.f1 == null) val.f1 = new ObjectOpenHashSet<>();
            val.f1.add(Tuple2.of(directedEdge.getDestId(), directedEdge.getAttribute()));
            return val;
        });
        localVertexTable.get(getRuntimeContext().getCurrentPart()).compute(directedEdge.getDestId(), (key, val) -> {
            if (val.f0 == null) val.f0 = new ObjectOpenHashSet<>();
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
    public boolean updateAttachedFeature(Feature feature, Feature memento) {
        if (feature.getAttachedElementType() == ElementType.VERTEX) {
            Tuple4<?, ?, Integer, Boolean> featureInfo = vertexFeatureInfo.get(feature.getName());
            Object[] features = localVertexTable.get(getRuntimeContext().getCurrentPart()).get((String) feature.getAttachedElementId()).f2;
            if (featureInfo.f3) {
                ((LifeCycleControl) features[featureInfo.f2]).resume();
                ((LifeCycleControl) feature.value).delay();
            }
            features[featureInfo.f2] = feature.value;
            return true;
        }
        return false;
    }

    @Override
    public boolean updateStandaloneFeature(Feature feature, Feature memento) {
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
    public boolean deleteAttachedFeature(Feature feature) {
        throw new NotImplementedException("Delete not implemented yet");
    }

    @Override
    public boolean deleteStandaloneFeature(Feature feature) {
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
        if(scopes.get().isOpen()){
            Vertex v = elementCache.get().get().getVertex();
            if(v.features != null) v.features.clear();
            v.id = vertexId;
            v.masterPart = masterPart;
            return v;
        }
        return new Vertex(vertexId, masterPart);
    }

    @Override
    public Iterable<Vertex> getVertices() {
        try {
            if(scopes.get().isOpen()){
                Vertex reusable = elementCache.get().get().getVertex();
                return () -> localVertexTable.get(getRuntimeContext().getCurrentPart()).keySet().stream().map(item -> {
                    if(reusable.features != null) reusable.features.clear();
                    reusable.masterPart = vertexMasterTable.get(item);
                    reusable.id = item;
                    return reusable;
                }).iterator();
            }
            return () -> localVertexTable.get(getRuntimeContext().getCurrentPart()).keySet().stream().map(item -> {
                short masterPart = vertexMasterTable.get(item);
                return new Vertex(item, masterPart);
            }).iterator();
        } catch (NullPointerException e) {
            return Collections.emptyList();
        }
    }

    @Override
    public @Nullable DirectedEdge getEdge(Tuple3<String, String, String> ids) {
        if(scopes.get().isOpen()){
            DirectedEdge edge = elementCache.get().get().getDirectedEdge();
            if(edge.features != null) edge.features.clear();
            edge.id.f0 = ids.f0;
            edge.id.f1 = ids.f1;
            edge.id.f2 = ids.f2;
            edge.src = null;
            edge.dest = null;
            return edge;
        }
        return new DirectedEdge(ids.f0, ids.f1, ids.f2);
    }

    @Override
    public Iterable<DirectedEdge> getIncidentEdges(Vertex vertex, EdgeType edge_type) {
        try {
            Tuple3<ObjectOpenHashSet<Tuple2<String, String>>, ObjectOpenHashSet<Tuple2<String, String>>, ?> vertexTable = localVertexTable.get(getRuntimeContext().getCurrentPart()).get(vertex.getId());
            Iterator<DirectedEdge> srcEdgeIterable = Collections.emptyIterator();
            Iterator<DirectedEdge> destEdgeIterable = Collections.emptyIterator();
            if (edge_type == EdgeType.OUT || edge_type == EdgeType.BOTH) {
                if (vertexTable.f1 != null){
                    if(!scopes.get().isOpen()) destEdgeIterable = vertexTable.f1.stream().map(dstatt -> (new DirectedEdge(vertex.getId(), dstatt.f0, dstatt.f1))).iterator();
                    else{
                        DirectedEdge reusable = elementCache.get().get().getDirectedEdge();
                        destEdgeIterable = vertexTable.f1.stream().map(dstatt -> {
                            if(reusable.features != null) reusable.features.clear();
                            reusable.src = null;
                            reusable.dest = null;
                            reusable.id.f1 = dstatt.f0;
                            reusable.id.f2 = dstatt.f1;
                          return reusable;
                        }).iterator();
                    }
                }
            }
            if (edge_type == EdgeType.IN || edge_type == EdgeType.BOTH) {
                if (vertexTable.f0 != null){
                    if(!scopes.get().isOpen()) srcEdgeIterable = vertexTable.f0.stream().map(srcatt -> (new DirectedEdge(srcatt.f0, vertex.getId(), srcatt.f1))).iterator();
                    else{
                        DirectedEdge reusable = elementCache.get().get().getDirectedEdge();
                        destEdgeIterable = vertexTable.f1.stream().map(srcAtt -> {
                            if(reusable.features != null) reusable.features.clear();
                            reusable.src = null;
                            reusable.dest = null;
                            reusable.id.f0 = srcAtt.f0;
                            reusable.id.f2 = srcAtt.f1;
                            return reusable;
                        }).iterator();
                    }
                }
            }
            final Iterator<DirectedEdge> srcIteratorFinal = srcEdgeIterable;
            final Iterator<DirectedEdge> destIteratorFinal = destEdgeIterable;
            return () -> IteratorUtils.chainedIterator(srcIteratorFinal, destIteratorFinal);
        } catch (Exception ignored) {
            return Collections.emptyList();
        }
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
    public @Nullable Feature getAttachedFeature(Tuple3<ElementType, Object, String> ids) {
        if (ids.f0 == ElementType.VERTEX) {
            Tuple4<Boolean, ConstructorAccess<? extends Feature>, Integer, ?> featureInfo = vertexFeatureInfo.get(ids.f2);
            Object value = localVertexTable.get(getRuntimeContext().getCurrentPart()).get((String) ids.f1).f2[featureInfo.f2];
            Feature feature = scopes.get().isOpen()?elementCache.get().get().getVertexFeature(ids.f2):featureInfo.f1.newInstance();
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
    public Iterable<Feature> getAttachedFeatures(ElementType elementType, String featureName) {
        if(elementType == ElementType.VERTEX){
            Tuple4<Boolean, ConstructorAccess<? extends Feature>, Integer, ?> featureInfo = vertexFeatureInfo.get(featureName);
            if(!scopes.get().isOpen()){
                return ()-> localVertexTable.get(getRuntimeContext().getCurrentPart()).entrySet()
                        .stream()
                        .filter(entry -> entry.getValue().f2.length > featureInfo.f2 && entry.getValue().f2[featureInfo.f2]!=null)
                        .map(entry -> {
                            Feature feature = featureInfo.f1.newInstance();
                            feature.value = entry.getValue().f2[featureInfo.f2];
                            feature.id.f0 = elementType;
                            feature.id.f1 = entry.getKey();
                            feature.id.f2 = featureName;
                            feature.halo = featureInfo.f0;
                            return feature;
                        }).iterator();
            }else{
                Feature reusable = elementCache.get().get().getVertexFeature(featureName);
                return ()-> localVertexTable.get(getRuntimeContext().getCurrentPart()).entrySet()
                        .stream()
                        .filter(entry -> entry.getValue().f2.length > featureInfo.f2 && entry.getValue().f2[featureInfo.f2]!=null)
                        .map(entry -> {
                            if(reusable.features != null) reusable.features.clear();
                            reusable.element = null;
                            reusable.value = entry.getValue().f2[featureInfo.f2];
                            reusable.id.f0 = elementType;
                            reusable.id.f1 = entry.getKey();
                            reusable.id.f2 = featureName;
                            reusable.halo = featureInfo.f0;
                            return reusable;
                        }).iterator();
            }

        }
        return null;
    }

    @Override
    public @Nullable Feature getStandaloneFeature(String featureName) {
        throw new NotImplementedException("Delete not implemented yet");
    }

    @Override
    public Iterable<Feature> getStandaloneFeatures() {
        throw new NotImplementedException("Not implemented yet");
    }

    @Override
    public boolean containsVertex(String vertexId) {
        try {
            return localVertexTable.get(getRuntimeContext().getCurrentPart()).containsKey(vertexId);
        } catch (NullPointerException e) {
            return false;
        }
    }

    @Override
    public boolean containsAttachedFeature(Tuple3<ElementType, Object, String> ids) {
        try {
            if (ids.f0 == ElementType.VERTEX) {
                int index = vertexFeatureInfo.get(ids.f2).f2;
                Object[] features = localVertexTable.get(getRuntimeContext().getCurrentPart()).get((String) ids.f1).f2;
                return features.length > index && features[index] != null;
            }
            return false;
        } catch (NullPointerException ignored) {
            return false;
        }
    }

    @Override
    public boolean containsStandaloneFeature(String featureName) {
        throw new NotImplementedException("Delete not implemented yet");
    }

    @Override
    public boolean containsEdge(Tuple3<String, String, String> ids) {
        try {
            Tuple2<String, String> accessKey = reuse.get();
            accessKey.f0 = ids.f1;
            accessKey.f1 = ids.f2;
            return localVertexTable.get(getRuntimeContext().getCurrentPart()).get(ids.f0).f1.contains(accessKey);
        } catch (NullPointerException e) {
            return false;
        }
    }

    @Override
    public boolean containsHyperEdge(String hyperEdgeId) {
        throw new NotImplementedException("Delete not implemented yet");
    }

    @Override
    public void cacheFeatures(GraphElement element, CacheFeatureContext context) {
        try {
            if (element.getType() == ElementType.VERTEX) {
                Object[] features = localVertexTable.get(getRuntimeContext().getCurrentPart()).get((String) element.getId()).f2;
                for (Map.Entry<String, Tuple4<Boolean, ConstructorAccess<? extends Feature>, Integer, Boolean>> stringTuple3Entry : vertexFeatureInfo.entrySet()) {
                    if ((!stringTuple3Entry.getValue().f0 && context == CacheFeatureContext.HALO) || (stringTuple3Entry.getValue().f0 && context == CacheFeatureContext.NON_HALO) || features.length <= stringTuple3Entry.getValue().f2 || features[stringTuple3Entry.getValue().f2] == null)
                        continue; // This feature not needed to cache
                    if (element.features != null && element.features.stream().anyMatch(item -> item.getName().equals(stringTuple3Entry.getKey())))
                        continue; // Already cached check
                    Feature feature = stringTuple3Entry.getValue().f1.newInstance();
                    feature.id.f2 = stringTuple3Entry.getKey();
                    feature.halo = stringTuple3Entry.getValue().f0;
                    feature.value = features[stringTuple3Entry.getValue().f2];
                    feature.setElement(element, false);
                }
            }
        } catch (Exception ignored) {
            // pass
        }
    }

    @Override
    public synchronized void register(TaskSharedKeyedStateBackend<?> taskSharedStateBackend) {
        super.register(taskSharedStateBackend);
        for (Short thisOperatorPart : getRuntimeContext().getThisOperatorParts()) {
            localVertexTable.put(thisOperatorPart, new HashMap<>());
        }
    }

    @Override
    public void clear() {

    }

    @Override
    public ReuseScope withReuse() {
        return scopes.get().open();
    }

    public class ElementCacheHolderPerScopeLayer{
        private SmallElementsCache[] caches = new SmallElementsCache[0];

        public SmallElementsCache get(){
            if(caches.length < scopes.get().getOpenCount()){
                SmallElementsCache[] tmpNew = new SmallElementsCache[scopes.get().getOpenCount()];
                System.arraycopy(caches,0, tmpNew,0, caches.length);
                caches = tmpNew;
            }
            if(caches[scopes.get().getOpenCount() - 1] == null) caches[scopes.get().getOpenCount() - 1] = new SmallElementsCache();
            return caches[scopes.get().getOpenCount() - 1];
        }
    }

    public class SmallElementsCache {

        private Vertex vertex = new Vertex();

        private DirectedEdge directedEdge = new DirectedEdge();

        Map<String, Feature> vertexFeatures = new Object2ObjectOpenHashMap<>(5);

        public Vertex getVertex() {
            return vertex;
        }

        public DirectedEdge getDirectedEdge() {
            return directedEdge;
        }

        public Feature getVertexFeature(String featureName) {
            Feature feature = vertexFeatures.get(featureName);
            if(feature != null) return feature;
            Tuple4<Boolean, ConstructorAccess<? extends Feature>, Integer, Boolean> info = vertexFeatureInfo.get(featureName);
            feature = info.f1.newInstance();
            feature.id.f2 = featureName;
            feature.halo = info.f0;
            vertexFeatures.put(featureName, feature);
            return feature;
        }
    }

}
