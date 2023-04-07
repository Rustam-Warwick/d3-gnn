package storage.edgelist;

import com.esotericsoftware.reflectasm.ConstructorAccess;
import elements.Feature;

/**
 * Information about a feature that is attached
 */
class AttachedFeatureInfo {

    boolean halo;

    int position;

    Class<? extends Feature> clazz;

    ConstructorAccess<? extends Feature> constructorAccess;

    AttachedFeatureInfo(Feature<?, ?> feature, int position) {
        this.position = position;
        this.halo = feature.isHalo();
        this.clazz = feature.getClass();
        this.constructorAccess = ConstructorAccess.get(this.clazz);
    }
}
