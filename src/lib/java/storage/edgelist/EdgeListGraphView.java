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

    /**
     * Master Part table for vertices. This table is shared across tasks as vertices unique
     */
    protected final Map<String, Short> vertexMasterTable;

    /**
     * Vertex Feature Info
     */
    protected final Map<String, AttachedFeatureInfo> vertexFeatureInfoTable;

    /**
     * Index to feature info table
     */
    protected final Int2ObjectOpenHashMap<AttachedFeatureInfo> indexVertexFeatureInfoTable;

    /**
     * Unique Vertex Feature Counter
     */
    protected final AtomicInteger uniqueVertexFeatureCounter;

    /**
     * Vertex Map
     */
    protected final Short2ObjectOpenHashMap<Map<String, VertexInfo>> vertexMap;

    /**
     * {@link ObjectPoolScope}
     */
    protected final EdgeListObjectPool objectPool = new EdgeListObjectPool();

    /**
     * {@link VerticesView}
     */
    protected final VerticesView verticesView = new GlobalVerticesView();

    /**
     * {@link EdgesView}
     */
    protected final EdgesView edgesView = new PossiblyFilteredEdgesView();

    public EdgeListGraphView(GraphRuntimeContext runtimeContext, Map<String, Short> vertexMasterTable, Map<String, AttachedFeatureInfo> vertexFeatureInfoTable, Int2ObjectOpenHashMap<AttachedFeatureInfo> indexVertexFeatureInfoTable, AtomicInteger uniqueVertexFeatureCounter, Short2ObjectOpenHashMap<Map<String, VertexInfo>> vertexMap) {
        super(runtimeContext);
        this.vertexMasterTable = vertexMasterTable;
        this.vertexFeatureInfoTable = vertexFeatureInfoTable;
        this.indexVertexFeatureInfoTable = indexVertexFeatureInfoTable;
        this.uniqueVertexFeatureCounter = uniqueVertexFeatureCounter;
        this.vertexMap = vertexMap;
        synchronized (vertexMap){
            runtimeContext.getThisOperatorParts().forEach(part -> vertexMap.putIfAbsent(part, new HashMap<>()));
        }
    }

    @Override
    public VerticesView getVertices() {
        return verticesView;
    }

    @Override
    public EdgesView getEdges() {
        return edgesView;
    }

    @Override
    public FeaturesView getStandaloneFeatures() {
        throw new NotImplementedException("");
    }

    @Override
    public ObjectPoolScope openObjectPoolScope() {
        return objectPool.open();
    }

    class GlobalVerticesView implements VerticesView{

        protected final Values values = new Values();

        @Override
        public FeaturesView getFeatures(String key) {
            return new VertexFeaturesView(vertexMap.get(getRuntimeContext().getCurrentPart()).get(key), key);
        }

        @Override
        public Collection<Feature> filterFeatures(String featureName) {
            return null;
        }

        @Override
        public int size() {
            return vertexMap.get(getRuntimeContext().getCurrentPart()).size();
        }

        @Override
        public boolean isEmpty() {
            return vertexMap.get(getRuntimeContext().getCurrentPart()).isEmpty();
        }

        @Override
        public boolean containsKey(Object key) {
            return vertexMap.get(getRuntimeContext().getCurrentPart()).containsKey(key);
        }

        @Override
        public boolean containsValue(Object value) {
            Vertex v = (Vertex) value;
            return vertexMap.get(getRuntimeContext().getCurrentPart()).containsKey(v.getId());
        }

        @Override
        public Vertex get(Object key) {
            if (objectPool.isOpen()) {
                return objectPool.getVertex((String) key, vertexMasterTable.get(key));
            }
            return new Vertex((String) key, vertexMasterTable.get(key));
        }

        @Nullable
        @Override
        public Vertex put(String key, Vertex value) {
            vertexMasterTable.putIfAbsent(value.getId(), value.getMasterPart());
            vertexMap.get(getRuntimeContext().getCurrentPart()).putIfAbsent(value.getId(), new VertexInfo());
            return value;
        }

        @Override
        public Vertex remove(Object key) {
            throw new NotImplementedException("Remove not handled yet");
        }

        @Override
        public void putAll(@NotNull Map<? extends String, ? extends Vertex> m) {
            m.forEach(this::put);
        }

        @Override
        public void clear() {
            throw new NotImplementedException("Remove not handled yet");
        }

        @NotNull
        @Override
        public Set<String> keySet() {
            return vertexMap.get(getRuntimeContext().getCurrentPart()).keySet();
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
                    Vertex v = GlobalVerticesView.this.get(key);
                    action.accept(v);
                });
            }
        }

        final class ValuesSpliterator implements Spliterator<Vertex>{
            final Spliterator<String> keySpliterator;

            public ValuesSpliterator() {
                this.keySpliterator = vertexMap.get(getRuntimeContext().getCurrentPart()).keySet().spliterator();
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
            final Iterator<String> keyIterator = vertexMap.get(getRuntimeContext().getCurrentPart()).keySet().iterator();
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

    class VertexFeaturesView implements FeaturesView{
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
            int size = 0;
            for (int i = 0; i < vertexInfo.featureValues.length; i++) {
                if(vertexInfo.featureValues[i] != null && (isHalo == null || indexVertexFeatureInfoTable.get(i).halo == isHalo)){
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
            AttachedFeatureInfo featureInfo = vertexFeatureInfoTable.get(key);
            return featureInfo != null && (isHalo == null || isHalo == featureInfo.halo) && vertexInfo.hasFeatureInPosition(featureInfo.position);
        }

        @Override
        public boolean containsValue(Object value) {
            Feature feature = (Feature) value;
            AttachedFeatureInfo featureInfo = vertexFeatureInfoTable.get(feature.getName());
            return (isHalo == null || isHalo == featureInfo.halo) && vertexInfo.hasFeatureInPosition(featureInfo.position);
        }

        @Override
        public Feature get(Object key) {
            AttachedFeatureInfo vertexFeatureInfo = vertexFeatureInfoTable.get(key);
            if(isHalo != null && vertexFeatureInfo.halo != isHalo) return null;
            Object value = vertexInfo.featureValues[vertexFeatureInfo.position];
            if (objectPool.isOpen()) {
                return objectPool.getVertexFeature(vertexId, (String) key, value, vertexFeatureInfo);
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
            if(isHalo != null && value.isHalo() != isHalo) return null;
            AttachedFeatureInfo attachedFeatureInfo = vertexFeatureInfoTable.computeIfAbsent(value.getName(), (ignored) ->{
                int position = uniqueVertexFeatureCounter.getAndIncrement();
                AttachedFeatureInfo featureInfo = new AttachedFeatureInfo(value, position);
                indexVertexFeatureInfoTable.put(position, featureInfo);
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
        public void putAll(@NotNull Map<? extends String, ? extends Feature> m) {
            m.forEach(this::put);
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
                    if(vertexInfo.featureValues[i] != null && (isHalo == null || (featureInfo = indexVertexFeatureInfoTable.get(i)).halo == isHalo)) {
                        processed++;
                        if (objectPool.isOpen()) {
                            return objectPool.getVertexFeature(vertexId, (String) vertexId, vertexInfo.featureValues[i], featureInfo);
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
                    if(vertexInfo.featureValues[i] != null && (isHalo == null || (featureInfo = indexVertexFeatureInfoTable.get(i)).halo == isHalo)){
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
                    if(vertexInfo.featureValues[i] != null && (isHalo == null || (featureInfo = indexVertexFeatureInfoTable.get(i)).halo == isHalo)) {
                        processed++;
                        return featureInfo.name;
                    }
                }
                return null;
            }
        }

    }

    class PossiblyFilteredEdgesView implements EdgesView{

        protected final String mainVertexId;

        protected final List<String> destVertexIds;

        protected final List<String> srcVertexIds;

        public PossiblyFilteredEdgesView(String mainVertexId, List<String> destVertexIds, List<String> srcVertexIds) {
            this.mainVertexId = mainVertexId;
            this.destVertexIds = destVertexIds;
            this.srcVertexIds = srcVertexIds;
        }

        public PossiblyFilteredEdgesView() {
            mainVertexId = null;
            destVertexIds = null;
            srcVertexIds = null;
        }

        @Override
        public EdgesView filterSrcId(String srcId) {
            return new PossiblyFilteredEdgesView(srcId, vertexMap.get(getRuntimeContext().getCurrentPart()).get(srcId).outEdges, Collections.emptyList());
        }

        @Override
        public EdgesView filterDestId(String destId) {
            return new PossiblyFilteredEdgesView(destId, Collections.emptyList(), vertexMap.get(getRuntimeContext().getCurrentPart()).get(destId).inEdges);
        }

        @Override
        public EdgesView filterVertexId(String vertexId) {
            return new PossiblyFilteredEdgesView(vertexId, vertexMap.get(getRuntimeContext().getCurrentPart()).get(vertexId).outEdges, vertexMap.get(getRuntimeContext().getCurrentPart()).get(vertexId).inEdges);
        }

        @Override
        public EdgesView filterAttribute(String attribute) {
            throw new NotImplementedException("Not implemented");
        }

        @Override
        public EdgesView filterSrcAndDest(String srcId, String destId) {
            throw new NotImplementedException("Not implemented");
        }

        @Override
        public DirectedEdge get(String srcId, String destId, @Nullable String attribute) {
            if (objectPool.isOpen()) {
                return objectPool.getEdge(srcId, destId, attribute);
            }
            return new DirectedEdge(srcId, destId, attribute);
        }

        @Override
        public boolean contains(String srcId, String destId, @Nullable String attribute) {
            if(attribute != null) return false;
            if (srcId.equals(mainVertexId)) return destVertexIds.contains(destId);
            if (destId.equals(mainVertexId)) return srcVertexIds.contains(srcId);
            return false;
        }

        @Override
        public int size() {
            return srcVertexIds.size() + destVertexIds.size();
        }

        @Override
        public boolean isEmpty() {
            return size() == 0;
        }

        @Override
        public boolean contains(Object o) {
            DirectedEdge edge = (DirectedEdge) o;
            return contains(edge.getSrcId(), edge.getDestId(), edge.getAttribute());
        }

        @NotNull
        @Override
        public Iterator<DirectedEdge> iterator() {
            return null;
        }

        @NotNull
        @Override
        public Object[] toArray() {
            throw new NotImplementedException("Not implemented");
        }

        @NotNull
        @Override
        public <T> T[] toArray(@NotNull T[] a) {
            throw new NotImplementedException("Not implemented");
        }

        @Override
        public boolean add(DirectedEdge directedEdge) {
            vertexMap.get(getRuntimeContext().getCurrentPart()).get(directedEdge.getSrcId()).addOutEdge(directedEdge);
            vertexMap.get(getRuntimeContext().getCurrentPart()).get(directedEdge.getDestId()).addInEdge(directedEdge);
            return true;
        }

        @Override
        public boolean remove(Object o) {
            throw new NotImplementedException("Not implemented");
        }

        @Override
        public boolean containsAll(@NotNull Collection<?> c) {
            throw new NotImplementedException("Not implemented");
        }

        @Override
        public boolean addAll(@NotNull Collection<? extends DirectedEdge> c) {
            throw new NotImplementedException("Not implemented");
        }

        @Override
        public boolean addAll(int index, @NotNull Collection<? extends DirectedEdge> c) {
            throw new NotImplementedException("Not implemented");
        }

        @Override
        public boolean removeAll(@NotNull Collection<?> c) {
            throw new NotImplementedException("Not implemented");
        }

        @Override
        public boolean retainAll(@NotNull Collection<?> c) {
            throw new NotImplementedException("Not implemented");
        }

        @Override
        public void clear() {
            throw new NotImplementedException("Not implemented");
        }

        @Override
        public DirectedEdge get(int index) {
            if(index < destVertexIds.size()){
                return get(mainVertexId, destVertexIds.get(index), null);
            }else{
                return get(srcVertexIds.get(index - destVertexIds.size()), mainVertexId, null);
            }
        }

        @Override
        public DirectedEdge set(int index, DirectedEdge element) {
            throw new NotImplementedException("Not implemented");
        }

        @Override
        public void add(int index, DirectedEdge element) {
            throw new NotImplementedException("Not implemented");
        }

        @Override
        public DirectedEdge remove(int index) {
            throw new NotImplementedException("Not implemented");
        }

        @Override
        public int indexOf(Object o) {
            throw new NotImplementedException("Not implemented");        }

        @Override
        public int lastIndexOf(Object o) {
            throw new NotImplementedException("Not implemented");        }

        @NotNull
        @Override
        public ListIterator<DirectedEdge> listIterator() {
            throw new NotImplementedException("Not implemented");        }

        @NotNull
        @Override
        public ListIterator<DirectedEdge> listIterator(int index) {
            throw new NotImplementedException("Not implemented");        }

        @NotNull
        @Override
        public List<DirectedEdge> subList(int fromIndex, int toIndex) {
            throw new NotImplementedException("Not implemented");
        }
    }

}
