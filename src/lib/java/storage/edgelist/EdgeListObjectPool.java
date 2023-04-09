package storage.edgelist;

import elements.DirectedEdge;
import elements.Feature;
import elements.Vertex;
import elements.enums.ElementType;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.flink.api.java.tuple.Tuple2;
import org.jetbrains.annotations.Nullable;
import storage.ObjectPoolScope;

import java.util.Collections;
import java.util.List;

/**
 * Reuse scope with elements cache per block
 * Caches {@link Vertex} {@link DirectedEdge} and attached-{@link Feature} objects
 */
class EdgeListObjectPool extends ObjectPoolScope {

    List<Vertex> vertices = new ObjectArrayList<>(10);

    IntList usingVerticesUpTo = new IntArrayList(List.of(0));

    List<DirectedEdge> edges = new ObjectArrayList<>(10);

    IntList usingEdgesUpTo = new IntArrayList(List.of(0));

    Int2ObjectMap<Tuple2<List<Feature>, IntList>> vertexFeaturesMap = new Int2ObjectOpenHashMap<>();

    public Vertex getVertex(String id, short masterPart) {
        if (vertices.size() <= usingVerticesUpTo.getInt(openCount)) vertices.add(new Vertex());
        Vertex v = vertices.get(usingVerticesUpTo.getInt(openCount));
        usingVerticesUpTo.set(openCount, usingVerticesUpTo.getInt(openCount) + 1);
        v.id = id;
        v.masterPart = masterPart;
        if (v.features != null) v.features.clear();
        return v;
    }

    public DirectedEdge getEdge(String srcId, String destId, @Nullable String attributeId) {
        if (edges.size() <= usingEdgesUpTo.getInt(openCount)) edges.add(new DirectedEdge());
        DirectedEdge edge = edges.get(usingEdgesUpTo.getInt(openCount));
        usingEdgesUpTo.set(openCount, usingEdgesUpTo.getInt(openCount) + 1);
        edge.src = null;
        edge.dest = null;
        edge.id.f0 = srcId;
        edge.id.f1 = destId;
        edge.id.f2 = attributeId;
        if (edge.features != null) edge.features.clear();
        return edge;
    }

    public Feature getVertexFeature(Object vertexId, Object value, AttachedFeatureInfo attachedFeatureInfo) {
        Tuple2<List<Feature>, IntList> vertexFeatureTuple = vertexFeaturesMap.computeIfAbsent(attachedFeatureInfo.position, (position) -> Tuple2.of(new ObjectArrayList<>(), new IntArrayList(Collections.nCopies(openCount + 1, 0))));
        if (vertexFeatureTuple.f0.size() <= vertexFeatureTuple.f1.getInt(openCount))
            vertexFeatureTuple.f0.add(attachedFeatureInfo.constructorAccess.newInstance());
        Feature feature = vertexFeatureTuple.f0.get(vertexFeatureTuple.f1.getInt(openCount));
        vertexFeatureTuple.f1.set(openCount, vertexFeatureTuple.f1.getInt(openCount) + 1);
        feature.element = null;
        feature.halo = attachedFeatureInfo.halo;
        feature.value = value;
        feature.id.f0 = ElementType.VERTEX;
        feature.id.f1 = vertexId;
        feature.id.f2 = attachedFeatureInfo.name;
        if (feature.features != null) feature.features.clear();
        return feature;
    }

    @Override
    protected ObjectPoolScope open() {
        usingVerticesUpTo.add(usingVerticesUpTo.getInt(openCount));
        usingEdgesUpTo.add(usingEdgesUpTo.getInt(openCount));
        vertexFeaturesMap.forEach((key, val) -> val.f1.add(val.f1.getInt(openCount)));
        return super.open();
    }

    @Override
    public void close() {
        usingVerticesUpTo.removeInt(openCount);
        usingEdgesUpTo.removeInt(openCount);
        vertexFeaturesMap.forEach((key, val) -> val.f1.removeInt(openCount));
        super.close();
    }
}