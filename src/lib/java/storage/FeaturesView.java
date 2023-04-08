package storage;

import elements.Feature;
import org.apache.commons.lang3.NotImplementedException;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.Set;

/**
 * View for {@link Feature} attached or standalone in the {@link GraphStorage}
 */
public interface FeaturesView extends Map<String, Feature> {

    /**
     * Filter based on wether the {@link Feature} is halo or not
     */
    FeaturesView filter(boolean isHalo);

    @NotNull
    @Override
    default Set<Entry<String, Feature>> entrySet(){
        throw new NotImplementedException("Entry set not needed");
    };
}
