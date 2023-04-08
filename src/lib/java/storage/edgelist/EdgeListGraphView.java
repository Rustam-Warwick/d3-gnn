package storage.edgelist;

import elements.DirectedEdge;
import elements.Feature;
import elements.Vertex;
import elements.enums.ElementType;
import it.unimi.dsi.fastutil.shorts.Short2ObjectOpenHashMap;
import org.apache.commons.collections.IteratorUtils;
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
    protected final EdgeListObjectPool edgeListObjectPool = new EdgeListObjectPool();

    /**
     * {@link VerticesView}
     */
    protected final VerticesView verticesView = new GlobalVerticesView();

    /**
     * {@link EdgesView}
     */
    protected final EdgesView edgesView = new PossiblyFilteredEdgesView();

    public EdgeListGraphView(GraphRuntimeContext runtimeContext, Map<String, Short> vertexMasterTable, Map<String, AttachedFeatureInfo> vertexFeatureInfoTable, AtomicInteger uniqueVertexFeatureCounter, Short2ObjectOpenHashMap<Map<String, VertexInfo>> vertexMap) {
        super(runtimeContext);
        this.vertexMasterTable = vertexMasterTable;
        this.vertexFeatureInfoTable = vertexFeatureInfoTable;
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
        return edgeListObjectPool.open();
    }

    class GlobalVerticesView implements VerticesView{

        protected Values values = null;

        @Override
        public FeaturesView getFeatures(String key) {
            return new VertexFeaturesView(vertexMap.get(getRuntimeContext().getCurrentPart()).get(key), key);
        }

        @Override
        public List<Feature> filterFeatures(String featureName) {
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
            if (edgeListObjectPool.isOpen()) {
                return edgeListObjectPool.getVertex((String) key, vertexMasterTable.get(key));
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
            throw new NotImplementedException("");
        }

        @Override
        public void putAll(@NotNull Map<? extends String, ? extends Vertex> m) {
            m.forEach(this::put);
        }

        @Override
        public void clear() {
            throw new NotImplementedException("");
        }

        @NotNull
        @Override
        public Set<String> keySet() {
            return vertexMap.get(getRuntimeContext().getCurrentPart()).keySet();
        }

        @NotNull
        @Override
        public Collection<Vertex> values() {
            if(values == null) values = new Values();
            return values;
        }

        @NotNull
        @Override
        public Set<Entry<String, Vertex>> entrySet() {
            throw new NotImplementedException("");
        }

        final class Values extends AbstractCollection<Vertex> {
            public int size()                 { return GlobalVerticesView.this.size(); }
            public void clear()               { GlobalVerticesView.this.clear(); }
            public Iterator<Vertex> iterator()     {
                return IteratorUtils.transformedIterator(GlobalVerticesView.this.keySet().iterator(), GlobalVerticesView.this::get);
            }
            public boolean contains(Object o) { return containsValue(o); }
            public Spliterator<Vertex> spliterator() {
                throw new NotImplementedException("");
            }
            public void forEach(Consumer<? super Vertex> action) {
                GlobalVerticesView.this.keySet().forEach(key -> {
                    Vertex v = GlobalVerticesView.this.get(key);
                    action.accept(v);
                });
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
            return 0;
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public boolean containsKey(Object key) {
            return vertexInfo.hasFeatureInPosition(vertexFeatureInfoTable.get(key).position);
        }

        @Override
        public boolean containsValue(Object value) {
            Feature feature = (Feature) value;
            return vertexInfo.hasFeatureInPosition(vertexFeatureInfoTable.get(feature.getName()).position);
        }

        @Override
        public Feature get(Object key) {
            AttachedFeatureInfo vertexFeatureInfo = vertexFeatureInfoTable.get(key);
            Object value = vertexInfo.featureValues[vertexFeatureInfo.position];
            if (edgeListObjectPool.isOpen()) {
                return edgeListObjectPool.getVertexFeature(vertexId, (String) key, value, vertexFeatureInfo);
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
            return null;
        }

        @Override
        public Feature remove(Object key) {
            throw new NotImplementedException("");
        }

        @Override
        public void putAll(@NotNull Map<? extends String, ? extends Feature> m) {
            throw new NotImplementedException("");
        }

        @Override
        public void clear() {
            throw new NotImplementedException("");
        }

        @NotNull
        @Override
        public Set<String> keySet() {
            return null;
        }

        @NotNull
        @Override
        public Collection<Feature> values() {
            return null;
        }

        @NotNull
        @Override
        public Set<Entry<String, Feature>> entrySet() {
            return null;
        }
    }

    class PossiblyFilteredEdgesView implements EdgesView{
        @Override
        public EdgesView filterSrcId(String srcId) {
            return null;
        }

        @Override
        public EdgesView filterDestId(String destId) {
            return null;
        }

        @Override
        public EdgesView filterVertexId(String vertexId) {
            return null;
        }

        @Override
        public EdgesView filterAttribute(String attribute) {
            return null;
        }

        @Override
        public EdgesView filterSrcAndDest(String srcId, String destId) {
            return null;
        }

        @Override
        public DirectedEdge get(String srcId, String destId, @Nullable String attribute) {
            return null;
        }

        @Override
        public boolean contains(String srcId, String destId, @Nullable String attribute) {
            return false;
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public boolean contains(Object o) {
            return false;
        }

        @NotNull
        @Override
        public Iterator<DirectedEdge> iterator() {
            return null;
        }

        @NotNull
        @Override
        public Object[] toArray() {
            return new Object[0];
        }

        @NotNull
        @Override
        public <T> T[] toArray(@NotNull T[] a) {
            return null;
        }

        @Override
        public boolean add(DirectedEdge directedEdge) {
            return false;
        }

        @Override
        public boolean remove(Object o) {
            return false;
        }

        @Override
        public boolean containsAll(@NotNull Collection<?> c) {
            return false;
        }

        @Override
        public boolean addAll(@NotNull Collection<? extends DirectedEdge> c) {
            return false;
        }

        @Override
        public boolean addAll(int index, @NotNull Collection<? extends DirectedEdge> c) {
            return false;
        }

        @Override
        public boolean removeAll(@NotNull Collection<?> c) {
            return false;
        }

        @Override
        public boolean retainAll(@NotNull Collection<?> c) {
            return false;
        }

        @Override
        public void clear() {

        }

        @Override
        public DirectedEdge get(int index) {
            return null;
        }

        @Override
        public DirectedEdge set(int index, DirectedEdge element) {
            return null;
        }

        @Override
        public void add(int index, DirectedEdge element) {

        }

        @Override
        public DirectedEdge remove(int index) {
            return null;
        }

        @Override
        public int indexOf(Object o) {
            return 0;
        }

        @Override
        public int lastIndexOf(Object o) {
            return 0;
        }

        @NotNull
        @Override
        public ListIterator<DirectedEdge> listIterator() {
            return null;
        }

        @NotNull
        @Override
        public ListIterator<DirectedEdge> listIterator(int index) {
            return null;
        }

        @NotNull
        @Override
        public List<DirectedEdge> subList(int fromIndex, int toIndex) {
            return null;
        }
    }

}
