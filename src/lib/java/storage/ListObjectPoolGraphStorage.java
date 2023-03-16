package storage;

import com.esotericsoftware.reflectasm.ConstructorAccess;
import elements.*;
import elements.enums.CacheFeatureContext;
import elements.enums.EdgeType;
import elements.enums.ElementType;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.operators.graph.interfaces.GraphRuntimeContext;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ListObjectPoolGraphStorage extends BaseStorage {

    /**
     * Master Part table for vertices. This table is shared across tasks as vertices unique
     */
    private final Map<String, Short> vertexMasterTable = new NonBlockingHashMap<>(1000);

    /**
     * Vertex Feature Info
     */
    private final Map<String, VertexFeatureInfo> vertexFeatureInfoTable = new ConcurrentHashMap<>();

    /**
     * Unique Vertex Feature Counter
     */
    private AtomicInteger uniqueVertexFeatureCounter = new AtomicInteger(0);

    /**
     * Vertex Map
     */
    private Map<Short, Map<String, VertexInfo>> vertexMap = new NonBlockingHashMap<>();

    @Override
    public GraphView createGraphStorageView(GraphRuntimeContext runtimeContext) {
        return new ListGraphView(runtimeContext);
    }

    @Override
    public void clear() {}

    /**
     * {@inheritDoc}
     */
    public class ListGraphView extends GraphView {

        /**
         * Scope pool object
         */
        protected final ObjectPool scopePool = new ObjectPool();

        /**
         * 3 tuple id for the edges
         */
        protected final Tuple3<String, String, String> reuseEdgeId = new Tuple3<>();

        public ListGraphView(GraphRuntimeContext runtimeContext) {
            super(runtimeContext);
            runtimeContext.getThisOperatorParts().forEach(part -> {
                vertexMap.putIfAbsent(part, new HashMap<>());
            });
        }

        @Override
        public void addAttachedFeature(Feature feature) {
            if(feature.getAttachedElementType() == ElementType.VERTEX){
                vertexFeatureInfoTable.computeIfAbsent(feature.getName(), (key)->new VertexFeatureInfo(feature, uniqueVertexFeatureCounter.getAndIncrement()));
                vertexMap.get(getRuntimeContext().getCurrentPart()).get(feature.getAttachedElementId()).addOrUpdateFeature(feature, vertexFeatureInfoTable.get(feature.getName()));
            }
            else{
                throw new IllegalStateException("NOT IMPLEMENTED");
            }
        }

        @Override
        public void addStandaloneFeature(Feature feature) {
            throw new IllegalStateException("NOT IMPLEMENTED");
        }

        @Override
        public void addVertex(Vertex vertex) {
            vertexMasterTable.putIfAbsent(vertex.getId(), vertex.getMasterPart());
            vertexMap.get(getRuntimeContext().getCurrentPart()).put(vertex.getId(), new VertexInfo());
        }

        @Override
        public void addEdge(DirectedEdge directedEdge) {
            vertexMap.get(getRuntimeContext().getCurrentPart()).get(directedEdge.getSrcId()).addOutEdge(directedEdge);
            vertexMap.get(getRuntimeContext().getCurrentPart()).get(directedEdge.getDestId()).addInEdge(directedEdge);
        }

        @Override
        public void addHyperEdge(HyperEdge hyperEdge) {
            throw new IllegalStateException("NOT IMPLEMENTED");
        }

        @Override
        public void updateAttachedFeature(Feature feature, Feature memento) {
            vertexMap.get(getRuntimeContext().getCurrentPart()).get(feature.getAttachedElementId()).addOrUpdateFeature(feature, vertexFeatureInfoTable.get(feature.getName()));
        }

        @Override
        public void updateStandaloneFeature(Feature feature, Feature memento) {
            throw new IllegalStateException("NOT IMPLEMENTED");
        }

        @Override
        public void updateVertex(Vertex vertex, Vertex memento) {
            // No need to do anything
        }

        @Override
        public void updateEdge(DirectedEdge directedEdge, DirectedEdge memento) {
            // No need to do anything
        }

        @Override
        public void updateHyperEdge(HyperEdge hyperEdge, HyperEdge memento) {
            throw new IllegalStateException("NOT IMPLEMENTED");
        }

        @Override
        public void deleteAttachedFeature(Feature feature) {
            throw new IllegalStateException("NOT IMPLEMENTED");
        }

        @Override
        public void deleteStandaloneFeature(Feature feature) {
            throw new IllegalStateException("NOT IMPLEMENTED");
        }

        @Override
        public void deleteVertex(Vertex vertex) {
            throw new IllegalStateException("NOT IMPLEMENTED");
        }

        @Override
        public void deleteEdge(DirectedEdge directedEdge) {
            throw new IllegalStateException("NOT IMPLEMENTED");
        }

        @Override
        public void deleteHyperEdge(HyperEdge hyperEdge) {
            throw new IllegalStateException("NOT IMPLEMENTED");
        }

        @Override
        public @Nullable Vertex getVertex(String vertexId) {
            if(scopePool.isOpen()){
                return scopePool.getVertex(vertexId, vertexMasterTable.get(vertexId));
            }
            return new Vertex(vertexId, vertexMasterTable.get(vertexId));
        }

        @Override
        public Iterable<Vertex> getVertices() {
            return () -> vertexMap.get(getRuntimeContext().getCurrentPart()).keySet().stream().map(this::getVertex).iterator();
        }

        @Override
        public @Nullable DirectedEdge getEdge(Tuple3<String, String, String> id) {
            if(scopePool.isOpen()){
                return scopePool.getEdge(id);
            }
            return new DirectedEdge(id.f0, id.f1, id.f2);
        }

        @Override
        public Iterable<DirectedEdge> getIncidentEdges(Vertex vertex, EdgeType edge_type) {
            VertexInfo vertexInfo = vertexMap.get(getRuntimeContext().getCurrentPart()).get(vertex.getId());
            Iterator<DirectedEdge> inEdgeIterable = IteratorUtils.emptyIterator();
            Iterator<DirectedEdge> outEdgeIterable = IteratorUtils.emptyIterator();
            if(vertexInfo.outEdges!=null && (edge_type == EdgeType.OUT || edge_type == EdgeType.BOTH)){
                outEdgeIterable = vertexInfo.outEdges.stream().map(partialIds-> {
                    reuseEdgeId.f0 = vertex.getId();
                    reuseEdgeId.f1 = partialIds[0];
                    reuseEdgeId.f2 = partialIds.length == 2 ? partialIds[1] : null;
                    return getEdge(reuseEdgeId);
                }).iterator();
            }
            if(vertexInfo.inEdges!=null && (edge_type == EdgeType.IN || edge_type == EdgeType.BOTH)){
                inEdgeIterable = vertexInfo.inEdges.stream().map(partialIds->{
                    reuseEdgeId.f0 = partialIds[0];
                    reuseEdgeId.f1 = vertex.getId();
                    reuseEdgeId.f2 = partialIds.length == 2 ? partialIds[1] : null;
                    return getEdge(reuseEdgeId);
                }).iterator();
            }
            Iterator<DirectedEdge> res = IteratorUtils.chainedIterator(inEdgeIterable, outEdgeIterable);
            return () -> res;
        }

        @Override
        public @Nullable HyperEdge getHyperEdge(String hyperEdgeId) {
            throw new IllegalStateException("NOT IMPLEMENTED");
        }

        @Override
        public Iterable<HyperEdge> getIncidentHyperEdges(Vertex vertex) {
            throw new IllegalStateException("NOT IMPLEMENTED");
        }

        @Override
        public @Nullable Feature getAttachedFeature(Tuple3<ElementType, Object, String> id) {
            if(id.f0 == ElementType.VERTEX){
                VertexFeatureInfo featureInfo = vertexFeatureInfoTable.get(id.f2);
                Object value = vertexMap.get(getRuntimeContext().getCurrentPart()).get(id.f1).featureValues[featureInfo.position];
                Feature feature = featureInfo.constructorAccess.newInstance();
                feature.value = value;
                feature.id.f0 = id.f0;
                feature.id.f1 = id.f1;
                feature.id.f2 = id.f2;
                feature.halo = featureInfo.halo;
                return feature;
            }
            throw new IllegalStateException("NOT IMPLEMENTED");
        }

        @Override
        public Iterable<Feature> getAttachedFeatures(ElementType elementType, String featureName) {
            if(elementType == ElementType.VERTEX){
                VertexFeatureInfo vertexFeatureInfo = vertexFeatureInfoTable.get(featureName);
                return () -> vertexMap.get(getRuntimeContext().getCurrentPart()).entrySet()
                        .stream()
                        .filter(entrySet->entrySet.getValue().hasFeatureInPosition(vertexFeatureInfo.position))
                        .map(entrySet->{
                            Object value = entrySet.getValue().featureValues[vertexFeatureInfo.position];
                            Feature feature = vertexFeatureInfo.constructorAccess.newInstance();
                            feature.value = value;
                            feature.id.f0 = ElementType.VERTEX;
                            feature.id.f1 = entrySet.getKey();
                            feature.id.f2 = featureName;
                            feature.halo = vertexFeatureInfo.halo;
                            return feature;
                        }).iterator();

            }
            throw new IllegalStateException("NOT IMPLEMENTED");
        }

        @Override
        public @Nullable Feature getStandaloneFeature(String featureName) {
            throw new IllegalStateException("NOT IMPLEMENTED");
        }

        @Override
        public Iterable<Feature> getStandaloneFeatures() {
            throw new IllegalStateException("NOT IMPLEMENTED");
        }

        @Override
        public boolean containsVertex(String vertexId) {
            return vertexMap.get(getRuntimeContext().getCurrentPart()).containsKey(vertexId);
        }

        @Override
        public boolean containsAttachedFeature(Tuple3<ElementType, Object, String> id) {
            if(id.f0 == ElementType.VERTEX){
                if(!vertexFeatureInfoTable.containsKey(id.f2) || !vertexMap.get(getRuntimeContext().getCurrentPart()).containsKey(id.f1)) return false;
                return vertexMap.get(getRuntimeContext().getCurrentPart()).get(id.f1).hasFeatureInPosition(vertexFeatureInfoTable.get(id.f2).position);
            }
            throw new IllegalStateException("NOT IMPLEMENTED");
        }

        @Override
        public boolean containsStandaloneFeature(String featureName) {
            throw new IllegalStateException("NOT IMPLEMENTED");
        }

        @Override
        public boolean containsEdge(Tuple3<String, String, String> id) {
            throw new IllegalStateException("NOT IMPLEMENTED");
        }

        @Override
        public boolean containsHyperEdge(String hyperEdgeId) {
            throw new IllegalStateException("NOT IMPLEMENTED");
        }

        @Override
        public void cacheAttachedFeatures(GraphElement element, CacheFeatureContext context) {
            if(element.getType() == ElementType.VERTEX){
                VertexInfo vertexFeatureInfo = vertexMap.get(getRuntimeContext().getCurrentPart()).get(element.getId());
                vertexFeatureInfoTable.forEach((featureName, featureInfo)->{
                    if ((!featureInfo.halo && context == CacheFeatureContext.HALO)
                            || (featureInfo.halo && context == CacheFeatureContext.NON_HALO)
                            || !vertexFeatureInfo.hasFeatureInPosition(featureInfo.position))
                        return;
                    element.getFeature(featureName); // Try to get it so element will cache

                });
                return;
            }
            throw new IllegalStateException("NOT IMPLEMENTED");
        }

        @Override
        public ObjectPoolScope openObjectPoolScope() {
            return scopePool.open();
        }

    }

    /**
     * Information stored per Vertex
     */
    public static class VertexInfo{

        protected Object[] featureValues;

        protected List<String[]> outEdges;

        protected List<String[]> inEdges;

        protected void addOrUpdateFeature(Feature feature, VertexFeatureInfo featureInfo){
            if(featureValues == null) featureValues = new Object[featureInfo.position + 1];
            if (featureInfo.position >= featureValues.length) {
                Object[] tmp = new Object[featureInfo.position + 1];
                System.arraycopy(featureValues, 0, tmp, 0, featureValues.length);
                featureValues = tmp;
            }
            List<Feature<?,?>> subFeatures = feature.features;
            feature.features = null;
            if(featureValues[featureInfo.position] != null){
                // Is update
                Object oldFeatureValue = featureValues[featureInfo.position];
                if(oldFeatureValue != feature.value){
                    // Values are different (non-in-place). Delay resume add and return
                    feature.delay();
                    featureValues[featureInfo.position] = feature.value;
                    feature.value = oldFeatureValue;
                    feature.resume();
                    feature.value = featureValues[featureInfo.position];
                }
            }else{
                // Is create: Delay and add
                feature.delay();
                featureValues[featureInfo.position] = feature.value;
            }
            feature.features = subFeatures;
        }

        protected boolean hasFeatureInPosition(int position){
            return featureValues != null && featureValues.length > position && featureValues[position] != null;
        }

        protected void addOutEdge(DirectedEdge edge){
            if(outEdges == null) outEdges = new ObjectArrayList<>(4);
            if(edge.getAttribute() == null) outEdges.add(new String[]{edge.getDestId()});
            else outEdges.add(new String[]{edge.getDestId(), edge.getAttribute()});
        }

        protected void addInEdge(DirectedEdge edge){
            if(inEdges == null) inEdges = new ObjectArrayList<>(4);
            if(edge.getAttribute() == null) inEdges.add(new String[]{edge.getSrcId()});
            else inEdges.add(new String[]{edge.getSrcId(), edge.getAttribute()});
        }

    }

    /**
     * Information about the Vertex Feature storing halo state constructor and etc.
     */
    public static class VertexFeatureInfo{

        boolean halo;

        int position;

        Class<? extends Feature> clazz;

        ConstructorAccess<? extends Feature> constructorAccess;

        protected VertexFeatureInfo(Feature<?,?> feature, int position){
            this.position = position;
            this.halo = feature.isHalo();
            this.clazz = feature.getClass();
            this.constructorAccess = ConstructorAccess.get(this.clazz);
        }
    }

    /**
     * Reuse scope with elements cache per block
     */
    public class ObjectPool extends ObjectPoolScope {

        List<Vertex> vertices = new ObjectArrayList<>(10);

        int usingVerticesUpTo = 0;

        List<DirectedEdge> edges = new ObjectArrayList<>(10);

        int usingEdgesUpTo = 0;

        List<Feature>[] vertexFeatures = new List[0];

        int[] vertexFeaturesUpTo = new int[0];

        public Vertex getVertex(String id, short masterPart){
            if(vertices.size() <= usingVerticesUpTo) vertices.add(new Vertex());
            Vertex v = vertices.get(usingVerticesUpTo++);
            v.id = id;
            v.masterPart = masterPart;
            if(v.features != null) v.features.clear();
            return v;
        }

        public DirectedEdge getEdge(Tuple3<String, String, String> id){
            if(edges.size() <= usingEdgesUpTo) edges.add(new DirectedEdge());
            DirectedEdge edge = edges.get(usingEdgesUpTo++);
            edge.src = null;
            edge.dest = null;
            edge.id.f0 = id.f0;
            edge.id.f1 = id.f1;
            edge.id.f2 = id.f2;
            if(edge.features != null) edge.features.clear();
            return edge;
        }


        @Override
        public void close() {
            super.close();
            usingVerticesUpTo = 0;
            usingEdgesUpTo = 0;
        }
    }

}
