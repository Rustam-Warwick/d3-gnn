package storage.edgelist;

import elements.DirectedEdge;
import elements.Feature;
import elements.Vertex;
import it.unimi.dsi.fastutil.shorts.Short2ObjectOpenHashMap;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.streaming.api.operators.graph.interfaces.GraphRuntimeContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import storage.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

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
    protected final EdgesView edgesView = new GlobalEdgeView();

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
        @Override
        public FeaturesView getFeatures(String key) {
            return null;
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
            throw new NotImplementedException("");
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
            throw new NotImplementedException("");
        }

        @NotNull
        @Override
        public Set<Entry<String, Vertex>> entrySet() {
            throw new NotImplementedException("");
        }
    }

    class VertexFeaturesView implements FeaturesView{
        VertexInfo vertexInfo;

        @Override
        public FeaturesView filter(boolean isHalo) {
            return null;
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
            return false;
        }

        @Override
        public boolean containsValue(Object value) {
            return false;
        }

        @Override
        public Feature get(Object key) {
            return null;
        }

        @Nullable
        @Override
        public Feature put(String key, Feature value) {
            return null;
        }

        @Override
        public Feature remove(Object key) {
            return null;
        }

        @Override
        public void putAll(@NotNull Map<? extends String, ? extends Feature> m) {

        }

        @Override
        public void clear() {

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

        public VertexFeaturesView(VertexInfo vertexInfo) {
            this.vertexInfo = vertexInfo;
        }
    }

    class GlobalEdgeView implements EdgesView{
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
