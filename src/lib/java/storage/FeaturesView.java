package storage;

import elements.Feature;
import org.apache.commons.lang3.NotImplementedException;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.Set;

/**
 * View for {@link Feature} attached or standalone in the {@link GraphStorage}
 * @implNote All the get & remove methods assume that contains is checked before
 */
public interface FeaturesView extends Map<String, Feature> {

    /**
     * Filter based on weather the {@link Feature} is halo or not
     */
    FeaturesView filter(boolean isHalo);

    @Override
    default boolean containsValue(Object value){
        Feature feature = (Feature) value;
        return containsKey(feature.getName());
    };

    @Override
    default void putAll(@NotNull Map<? extends String, ? extends Feature> m){
        m.forEach(this::put);
    };

    @NotNull
    @Override
    default Set<Entry<String, Feature>> entrySet(){
        throw new NotImplementedException("Entry set not needed");
    };
}
