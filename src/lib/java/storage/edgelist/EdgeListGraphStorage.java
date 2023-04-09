package storage.edgelist;

import elements.Feature;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.shorts.Short2ObjectOpenHashMap;
import org.apache.flink.streaming.api.operators.graph.interfaces.GraphRuntimeContext;
import org.jctools.maps.NonBlockingHashMap;
import storage.*;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Edge-list based GraphStorage
 */
public class EdgeListGraphStorage extends GraphStorage {

    /**
     * Master Part table for vertices. This table is shared across tasks as vertices unique
     */
    protected final Map<String, Short> vertex2MasterPart = new NonBlockingHashMap<>(1000);

    /**
     * Vertex Feature Info
     */
    protected final Map<String, AttachedFeatureInfo> vertexFeatureName2FeatureInfo = new ConcurrentHashMap<>();

    /**
     * Indec to feature info table
     */
    protected final Int2ObjectOpenHashMap<AttachedFeatureInfo> vertexFeatureIndex2FeatureInfo = new Int2ObjectOpenHashMap<>();

    /**
     * Unique Vertex Feature Counter
     */
    protected final AtomicInteger uniqueVertexFeatureCounter = new AtomicInteger(0);

    /**
     * Vertex Map
     */
    protected final Short2ObjectOpenHashMap<Map<String, VertexInfo>> part2Vertex2VertexInfo = new Short2ObjectOpenHashMap<>();

    @Override
    public void clear() {
        Map<Integer, Feature> featureTmpMap = vertexFeatureName2FeatureInfo.entrySet().stream().collect(Collectors.toMap(item -> item.getValue().position, item -> item.getValue().constructorAccess.newInstance()));
        part2Vertex2VertexInfo.forEach((part, vertexMapInternal) -> {
            vertexMapInternal.forEach((vertexId, vertexData) -> {
                if (vertexData.featureValues != null) {
                    for (int i = 0; i < vertexData.featureValues.length; i++) {
                        if (vertexData.featureValues[i] != null) {
                            Feature tmp = featureTmpMap.get(i);
                            tmp.value = vertexData.featureValues[i];
                            tmp.destroy();
                        }
                    }
                }
            });
            vertexMapInternal.clear();
        });
        part2Vertex2VertexInfo.clear();
        vertexFeatureName2FeatureInfo.clear();
        vertex2MasterPart.clear();
    }

    @Override
    public GraphView getGraphStorageView(GraphRuntimeContext runtimeContext) {
        return new EdgeListGraphView(runtimeContext, vertex2MasterPart, vertexFeatureName2FeatureInfo, vertexFeatureIndex2FeatureInfo, uniqueVertexFeatureCounter, part2Vertex2VertexInfo);
    }

}
