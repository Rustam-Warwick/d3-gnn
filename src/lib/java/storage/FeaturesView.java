package storage;

import elements.Feature;

import java.util.Map;

/**
 * View for {@link Feature} attached or standalone in the {@link GraphStorage}
 */
public interface FeaturesView extends Map<String, Feature> {

    /**
     * Filter based on wether the {@link Feature} is halo or not
     */
    FeaturesView filter(boolean isHalo);

}
