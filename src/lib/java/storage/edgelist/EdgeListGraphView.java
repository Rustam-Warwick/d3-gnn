package storage.edgelist;

import elements.DirectedEdge;
import elements.Feature;
import elements.Vertex;
import elements.enums.ElementType;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.shorts.Short2ObjectOpenHashMap;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.streaming.api.operators.graph.interfaces.GraphRuntimeContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import storage.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

class EdgeListGraphView extends GraphView {

    protected final Map<String, Short> vertex2MasterPart;
    protected final Map<String, AttachedFeatureInfo> vertexFeatureName2FeatureInfo;
    protected final Int2ObjectOpenHashMap<AttachedFeatureInfo> vertexFeatureIndex2FeatureInfo;
    protected final AtomicInteger uniqueVertexFeatureCounter;
    protected final Short2ObjectOpenHashMap<Map<String, VertexInfo>> part2Vertex2VertexInfo;
    protected int edgeCount;
    protected final EdgeListObjectPool objectScope = new EdgeListObjectPool();
    protected final VerticesView globalVerticesView = new GlobalVerticesView();
    protected final EdgesView globalEdgesView = new GlobalEdgesView();

    public EdgeListGraphView(GraphRuntimeContext runtimeContext, Map<String, Short> vertex2MasterPart, Map<String, AttachedFeatureInfo> vertexFeatureName2FeatureInfo, Int2ObjectOpenHashMap<AttachedFeatureInfo> vertexFeatureIndex2FeatureInfo, AtomicInteger uniqueVertexFeatureCounter, Short2ObjectOpenHashMap<Map<String, VertexInfo>> part2Vertex2VertexInfo) {
        super(runtimeContext);
        this.vertex2MasterPart = vertex2MasterPart;
        this.vertexFeatureName2FeatureInfo = vertexFeatureName2FeatureInfo;
        this.vertexFeatureIndex2FeatureInfo = vertexFeatureIndex2FeatureInfo;
        this.uniqueVertexFeatureCounter = uniqueVertexFeatureCounter;
        this.part2Vertex2VertexInfo = part2Vertex2VertexInfo;
        synchronized (part2Vertex2VertexInfo){
            runtimeContext.getThisOperatorParts().forEach(part -> part2Vertex2VertexInfo.putIfAbsent(part, new HashMap<>()));
        }
    }

    @Override
    public VerticesView getVertices() {
        return globalVerticesView;
    }

    @Override
    public EdgesView getEdges() {
        return globalEdgesView;
    }

    @Override
    public FeaturesView getStandaloneFeatures() {
        throw new NotImplementedException("");
    }

    @Override
    public ObjectPoolScope openObjectPoolScope() {
        return objectScope.open();
    }

    /**
     * {@link VerticesView} view of all the vertices for this Graph
     */
    final class GlobalVerticesView implements VerticesView{

        private final Values values = new Values();

        @Override
        public FeaturesView getFeatures(String key) {
            return new VertexFeaturesView(part2Vertex2VertexInfo.get(getRuntimeContext().getCurrentPart()).get(key), key);
        }

        @Override
        public Collection<Feature> filterFeatures(String featureName) {
            return null;
        }

        @Override
        public int size() {
            return part2Vertex2VertexInfo.get(getRuntimeContext().getCurrentPart()).size();
        }

        @Override
        public boolean isEmpty() {
            return part2Vertex2VertexInfo.get(getRuntimeContext().getCurrentPart()).isEmpty();
        }

        @Override
        public boolean containsKey(Object key) {
            return part2Vertex2VertexInfo.get(getRuntimeContext().getCurrentPart()).containsKey(key);
        }

        @Override
        public Vertex get(Object key) {
            if (objectScope.isOpen()) {
                return objectScope.getVertex((String) key, vertex2MasterPart.get(key));
            }
            return new Vertex((String) key, vertex2MasterPart.get(key));
        }

        @Override
        public Vertex put(String key, Vertex value) {
            vertex2MasterPart.putIfAbsent(value.getId(), value.getMasterPart());
            part2Vertex2VertexInfo.get(getRuntimeContext().getCurrentPart()).putIfAbsent(value.getId(), new VertexInfo());
            return value;
        }

        @Override
        public Vertex remove(Object key) {
            part2Vertex2VertexInfo.get(getRuntimeContext().getCurrentPart()).remove(key);
            return null;
        }

        @Override
        public void clear() {
            part2Vertex2VertexInfo.get(getRuntimeContext().getCurrentPart()).clear();
        }

        @NotNull
        @Override
        public Set<String> keySet() {
            return part2Vertex2VertexInfo.get(getRuntimeContext().getCurrentPart()).keySet();
        }

        @NotNull
        @Override
        public Collection<Vertex> values() {
            return values;
        }

        final class Values extends AbstractCollection<Vertex> {
            public int size()                 { return GlobalVerticesView.this.size(); }
            public void clear()               { GlobalVerticesView.this.clear(); }
            public Iterator<Vertex> iterator()     {
                return new ValuesIterator();
            }
            public boolean contains(Object o) { return containsValue(o); }
            public Spliterator<Vertex> spliterator() {
                return new ValuesSpliterator();
            }
            public void forEach(Consumer<? super Vertex> action) {
                GlobalVerticesView.this.keySet().forEach(key -> {
                    action.accept(GlobalVerticesView.this.get(key));
                });
            }
        }

        final class ValuesSpliterator implements Spliterator<Vertex>{
            final Spliterator<String> keySpliterator;

            public ValuesSpliterator() {
                this.keySpliterator = part2Vertex2VertexInfo.get(getRuntimeContext().getCurrentPart()).keySet().spliterator();
            }

            public ValuesSpliterator(Spliterator<String> keySpliterator) {
                this.keySpliterator = keySpliterator;
            }

            @Override
            public boolean tryAdvance(Consumer<? super Vertex> action) {
                return keySpliterator.tryAdvance(vertexId -> {
                    action.accept(GlobalVerticesView.this.get(vertexId));
                });
            }

            @Override
            public Spliterator<Vertex> trySplit() {
                return new ValuesSpliterator(keySpliterator.trySplit());
            }

            @Override
            public long estimateSize() {
                return keySpliterator.estimateSize();
            }

            @Override
            public int characteristics() {
                return keySpliterator.characteristics();
            }
        }

        final class ValuesIterator implements Iterator<Vertex>{
            final Iterator<String> keyIterator = part2Vertex2VertexInfo.get(getRuntimeContext().getCurrentPart()).keySet().iterator();
            @Override
            public boolean hasNext() {
                return keyIterator.hasNext();
            }

            @Override
            public Vertex next() {
                return GlobalVerticesView.this.get(keyIterator.next());
            }
        }
    }

    /**
     * {@link FeaturesView} of all the vertex-attached features
     */
    class VertexFeaturesView implements FeaturesView {
        final VertexInfo vertexInfo;

        final String vertexId;

        final Boolean isHalo;

        private VertexFeaturesView(VertexInfo vertexInfo, String vertexId, Boolean isHalo) {
            this.vertexInfo = vertexInfo;
            this.vertexId = vertexId;
            this.isHalo = isHalo;
        }

        public VertexFeaturesView(VertexInfo vertexInfo, String vertexId) {
            this.vertexInfo = vertexInfo;
            this.vertexId = vertexId;
            this.isHalo = null;
        }

        @Override
        public FeaturesView filter(boolean isHalo) {
            return new VertexFeaturesView(vertexInfo, vertexId, isHalo);
        }

        @Override
        public int size() {
            if(vertexInfo.featureValues == null) return 0;
            int size = 0;
            for (int i = 0; i < vertexInfo.featureValues.length; i++) {
                if(vertexInfo.featureValues[i] != null && (isHalo == null || vertexFeatureIndex2FeatureInfo.get(i).halo == isHalo)){
                    size++;
                }
            }
            return size;
        }

        @Override
        public boolean isEmpty() {
            return size() == 0;
        }

        @Override
        public boolean containsKey(Object key) {
            AttachedFeatureInfo featureInfo = vertexFeatureName2FeatureInfo.get(key);
            return featureInfo != null && (isHalo == null || isHalo == featureInfo.halo) && vertexInfo.hasFeatureInPosition(featureInfo.position);
        }

        @Override
        public Feature get(Object key) {
            AttachedFeatureInfo vertexFeatureInfo = vertexFeatureName2FeatureInfo.get(key);
            Object value = vertexInfo.featureValues[vertexFeatureInfo.position];
            if (objectScope.isOpen()) {
                return objectScope.getVertexFeature(vertexId, (String) key, value, vertexFeatureInfo);
            } else {
                Feature feature = vertexFeatureInfo.constructorAccess.newInstance();
                feature.value = value;
                feature.id.f0 = ElementType.VERTEX;
                feature.id.f1 = vertexId;
                feature.id.f2 = key;
                feature.halo = vertexFeatureInfo.halo;
                return feature;
            }
        }

        @Nullable
        @Override
        public Feature put(String key, Feature value) {
            if(isHalo != null && isHalo != value.halo) throw new IllegalStateException("Filtered");
            AttachedFeatureInfo attachedFeatureInfo = vertexFeatureName2FeatureInfo.computeIfAbsent(value.getName(), (ignored) ->{
                int position = uniqueVertexFeatureCounter.getAndIncrement();
                AttachedFeatureInfo featureInfo = new AttachedFeatureInfo(value, position);
                vertexFeatureIndex2FeatureInfo.put(position, featureInfo);
                return featureInfo;
            });
            vertexInfo.addOrUpdateFeature(value, attachedFeatureInfo);
            return value;
        }

        @Override
        public Feature remove(Object key) {
            throw new NotImplementedException("Remove not handled yet");
        }

        @Override
        public void clear() {
            throw new NotImplementedException("Remove not handled yet");
        }

        @NotNull
        @Override
        public Set<String> keySet() {
            return new KeySet();
        }

        @NotNull
        @Override
        public Collection<Feature> values() {
            return new Values();
        }

        final class Values extends AbstractCollection<Feature>{
            @Override
            public Iterator<Feature> iterator() {
                return new ValuesIterator();
            }

            @Override
            public int size() {
                return VertexFeaturesView.this.size();
            }
        }

        final class ValuesIterator implements Iterator<Feature>{

            int i;

            int processed;

            int size = VertexFeaturesView.this.size();

            @Override
            public boolean hasNext() {
                return processed < size;
            }

            @Override
            public Feature next() {
                for (; i < vertexInfo.featureValues.length; i++) {
                    AttachedFeatureInfo featureInfo = null;
                    if(vertexInfo.featureValues[i] != null && (isHalo == null || (featureInfo = vertexFeatureIndex2FeatureInfo.get(i)).halo == isHalo)) {
                        processed++;
                        if (objectScope.isOpen()) {
                            return objectScope.getVertexFeature(vertexId, (String) vertexId, vertexInfo.featureValues[i], featureInfo);
                        } else {
                            Feature feature = featureInfo.constructorAccess.newInstance();
                            feature.value = vertexInfo.featureValues[i];
                            feature.id.f0 = ElementType.VERTEX;
                            feature.id.f1 = vertexId;
                            feature.id.f2 = featureInfo.name;
                            feature.halo = featureInfo.halo;
                            return feature;
                        }
                    }
                }
                return null;
            }
        }

        final class KeySet extends AbstractSet<String> {
            public int size()                 { return VertexFeaturesView.this.size(); }
            public void clear()               { VertexFeaturesView.this.clear();}
            public Iterator<String> iterator() {
                return new KeyIterator();
            }
            public boolean contains(Object o) { return containsKey(o); }
            public boolean remove(Object key) {
                return remove(key);
            }
            public Spliterator<String> spliterator() {
                throw new NotImplementedException("");
            }
            public void forEach(Consumer<? super String> action) {
                for (int i = 0; i < vertexInfo.featureValues.length; i++) {
                    AttachedFeatureInfo featureInfo = null;
                    if(vertexInfo.featureValues[i] != null && (isHalo == null || (featureInfo = vertexFeatureIndex2FeatureInfo.get(i)).halo == isHalo)){
                        action.accept(featureInfo.name);
                    }
                }
            }
        }

        final class KeyIterator implements Iterator<String>{
            int i;

            int processed;

            int size = VertexFeaturesView.this.size();
            @Override
            public boolean hasNext() {
                return processed < size;
            }

            @Override
            public String next() {
                for (; i < vertexInfo.featureValues.length; i++) {
                    AttachedFeatureInfo featureInfo = null;
                    if(vertexInfo.featureValues[i] != null && (isHalo == null || (featureInfo = vertexFeatureIndex2FeatureInfo.get(i)).halo == isHalo)) {
                        processed++;
                        return featureInfo.name;
                    }
                }
                return null;
            }
        }

    }

    /**
     * Global view of all the {@link DirectedEdge} in the graph
     */
    class GlobalEdgesView extends AbstractCollection<DirectedEdge> implements EdgesView{
        @Override
        public EdgesView filterSrcId(String srcId) {
            return new FilteredEdgesView(srcId, Objects.requireNonNullElse(part2Vertex2VertexInfo.get(getRuntimeContext().getCurrentPart()).get(srcId).outEdges, Collections.emptyList()), Collections.emptyList());
        }

        @Override
        public EdgesView filterDestId(String destId) {
            return new FilteredEdgesView(destId, Collections.emptyList(), Objects.requireNonNullElse(part2Vertex2VertexInfo.get(getRuntimeContext().getCurrentPart()).get(destId).inEdges, Collections.emptyList()));
        }

        @Override
        public EdgesView filterVertexId(String vertexId) {
            return new FilteredEdgesView(vertexId,Objects.requireNonNullElse(part2Vertex2VertexInfo.get(getRuntimeContext().getCurrentPart()).get(vertexId).outEdges, Collections.emptyList()), Objects.requireNonNullElse(part2Vertex2VertexInfo.get(getRuntimeContext().getCurrentPart()).get(vertexId).inEdges, Collections.emptyList()));
        }

        @Override
        public EdgesView filterAttribute(String attribute) {
           throw new NotImplementedException("Attributes not supported");
        }

        @Override
        public EdgesView filterSrcAndDest(String srcId, String destId) {
            throw new NotImplementedException("Filtering of src and dest not supported");
        }

        @Override
        public Iterator<DirectedEdge> iterator() {
            throw new NotImplementedException("Filtering of src and dest not supported");
        }

        @Override
        public int size() {
            return edgeCount;
        }

        @Override
        public boolean remove(String srcId, String destId, @Nullable String attribute) {
            List<String> outEdges = part2Vertex2VertexInfo.get(getRuntimeContext().getCurrentPart()).get(srcId).outEdges;
            List<String> inEdges = part2Vertex2VertexInfo.get(getRuntimeContext().getCurrentPart()).get(destId).inEdges;
            outEdges.remove(outEdges.lastIndexOf(destId));
            inEdges.remove(inEdges.lastIndexOf(srcId));
            edgeCount--;
            return true;
        }

        @Override
        public DirectedEdge get(String srcId, String destId, @Nullable String attribute) {
            if (objectScope.isOpen()) {
                return objectScope.getEdge(srcId, destId, attribute);
            }
            return new DirectedEdge(srcId, destId, attribute);
        }

        @Override
        public boolean contains(String srcId, String destId, @Nullable String attribute) {
            if(attribute != null) return false;
            List<String> outEdges = part2Vertex2VertexInfo.get(getRuntimeContext().getCurrentPart()).get(srcId).outEdges;
            return outEdges != null && outEdges.contains(destId);
        }

        @Override
        public boolean add(DirectedEdge directedEdge) {
            part2Vertex2VertexInfo.get(getRuntimeContext().getCurrentPart()).get(directedEdge.getSrcId()).addOutEdge(directedEdge);
            part2Vertex2VertexInfo.get(getRuntimeContext().getCurrentPart()).get(directedEdge.getDestId()).addInEdge(directedEdge);
            edgeCount++;
            return true;
        }

    }

    class FilteredEdgesView extends AbstractCollection<DirectedEdge> implements EdgesView{

        protected final String mainVertexId;

        protected final List<String> destVertexIds;

        protected final List<String> srcVertexIds;

        public FilteredEdgesView(String mainVertexId, List<String> destVertexIds, List<String> srcVertexIds) {
            this.mainVertexId = mainVertexId;
            this.destVertexIds = destVertexIds;
            this.srcVertexIds = srcVertexIds;
        }

        @Override
        public EdgesView filterSrcId(String srcId) {
            throw new NotImplementedException("Double filtering not supported");
        }

        @Override
        public EdgesView filterDestId(String destId) {
            throw new NotImplementedException("Double filtering not supported");
        }

        @Override
        public EdgesView filterVertexId(String vertexId) {
            throw new NotImplementedException("Double filtering not supported");
        }

        @Override
        public EdgesView filterAttribute(String attribute) {
            throw new NotImplementedException("Double filtering not supported");
        }

        @Override
        public EdgesView filterSrcAndDest(String srcId, String destId) {
            throw new NotImplementedException("Double filtering not supported");
        }

        @Override
        public int size() {
            return destVertexIds.size() + srcVertexIds.size();
        }

        @Override
        public Iterator<DirectedEdge> iterator() {
            return new ValuesIterator();
        }

        @Override
        public boolean remove(String srcId, String destId, @Nullable String attribute) {
            return getEdges().remove(srcId, destId, attribute);
        }

        @Override
        public DirectedEdge get(String srcId, String destId, @Nullable String attribute) {
            return getEdges().get(srcId, destId, attribute);
        }

        @Override
        public boolean contains(String srcId, String destId, @Nullable String attribute) {
            if(attribute != null) return false;
            if(mainVertexId.equals(srcId)) return destVertexIds.contains(destId);
            else if(mainVertexId.equals(destId)) return srcVertexIds.contains(srcId);
            return false;
        }

        final class ValuesIterator implements Iterator<DirectedEdge>{
            int i;
            @Override
            public boolean hasNext() {
                return i < FilteredEdgesView.this.size();
            }

            @Override
            public DirectedEdge next() {
                if(i < destVertexIds.size()){
                    return get(mainVertexId, destVertexIds.get(i++), null);
                }else{
                    return get(srcVertexIds.get(i++ - destVertexIds.size()), mainVertexId, null);
                }
            }

        }
    }

}
