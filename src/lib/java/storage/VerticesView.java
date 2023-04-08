package storage;

import elements.Feature;
import elements.Vertex;

import java.util.Collection;
import java.util.Map;

/**
 * View for {@link Vertex} in the {@link GraphStorage}
 */
public interface VerticesView extends Map<String, Vertex> {

    /**
     * Get attached {@link FeaturesView} of {@link Vertex} defined by its ID
     */
    FeaturesView getFeatures(String key);

    /**
     * Get all the Vertex-attached {@link Feature}s with the given name
     */
    Collection<Feature> filterFeatures(String featureName);
}
