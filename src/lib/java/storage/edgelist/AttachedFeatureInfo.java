package storage.edgelist;

import com.esotericsoftware.reflectasm.ConstructorAccess;
import elements.Feature;

/**
 * Information about a feature that is attached
 */
class AttachedFeatureInfo {

    final boolean halo;

    final int position;

    final Class<? extends Feature> clazz;

    final ConstructorAccess<? extends Feature> constructorAccess;

    final String name;

    AttachedFeatureInfo(Feature<?, ?> feature, int position) {
        this.position = position;
        this.halo = feature.isHalo();
        this.clazz = feature.getClass();
        this.constructorAccess = ConstructorAccess.get(this.clazz);
        this.name = feature.getName();
    }
}
