package storage;

import elements.Feature;
import elements.Vertex;
import org.apache.commons.lang3.NotImplementedException;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * View for {@link Vertex} in the {@link GraphStorage}
 * @implNote All the get & remove methods assume that contains is checked before
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

    @Override
    default boolean containsValue(Object value) {
        Vertex v = (Vertex) value;
        return containsKey(v.getId());
    }

    @Override
    default void putAll(@NotNull Map<? extends String, ? extends Vertex> m) {
        m.forEach(this::put);
    }

    @NotNull
    @Override
    default Set<Entry<String, Vertex>> entrySet(){
        throw new NotImplementedException("Entry set not needed");
    };

}
