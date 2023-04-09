package storage.edgelist;

import elements.DirectedEdge;
import elements.Feature;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.jetbrains.annotations.Nullable;

import java.util.List;

/**
 * Information about a single vertex
 */
class VertexInfo {
    @Nullable
    Object[] featureValues;
    @Nullable
    List<String> outEdges;
    @Nullable
    List<String> inEdges;

    protected void addOrUpdateFeature(Feature feature, AttachedFeatureInfo featureInfo) {
        if (featureValues == null) featureValues = new Object[featureInfo.position + 1];
        if (featureInfo.position >= featureValues.length) {
            Object[] tmp = new Object[featureInfo.position + 1];
            System.arraycopy(featureValues, 0, tmp, 0, featureValues.length);
            featureValues = tmp;
        }
        List<Feature<?, ?>> subFeatures = feature.features;
        feature.features = null;
        if (featureValues[featureInfo.position] != null) {
            // Is update
            Object oldFeatureValue = featureValues[featureInfo.position];
            if (oldFeatureValue != feature.value) {
                // Values are different (non-in-place). Delay resume add and return
                feature.delay();
                featureValues[featureInfo.position] = feature.value;
                feature.value = oldFeatureValue;
                feature.resume();
                feature.value = featureValues[featureInfo.position];
            }
        } else {
            // Is create: Delay and add
            feature.delay();
            featureValues[featureInfo.position] = feature.value;
        }
        feature.features = subFeatures;
    }

    boolean hasFeatureInPosition(int position) {
        return featureValues != null && featureValues.length > position && featureValues[position] != null;
    }

    void addOutEdge(DirectedEdge edge) {
        if (outEdges == null) outEdges = new ObjectArrayList<>(4);
        outEdges.add(edge.getDestId());
    }

    void addInEdge(DirectedEdge edge) {
        if (inEdges == null) inEdges = new ObjectArrayList<>(4);
        inEdges.add(edge.getSrcId());
    }
}
