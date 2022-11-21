package elements;

import ai.djl.ndarray.LifeCycleControl;
import elements.enums.CacheFeatureContext;
import elements.enums.CopyContext;
import elements.enums.ElementType;
import elements.enums.ReplicaState;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.jetbrains.annotations.Nullable;
import storage.BaseStorage;
import typeinfo.recursivepojoinfo.DeSerializationListener;
import typeinfo.recursivepojoinfo.RecursivePojoTypeInfoFactory;

import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Abstract class representing a GraphElement.
 */
@TypeInfo(RecursivePojoTypeInfoFactory.class)
public abstract class GraphElement implements Serializable, LifeCycleControl, DeSerializationListener {

    protected static final Tuple2<Consumer<BaseStorage>, GraphElement> reuse = Tuple2.of(null, null);

    /**
     * Features attached to this GraphElement
     * <strong>
     * There is not always a 1-1 relation between them.
     * Many same Features can point to this element
     * While this element always has the reference to the first one obtained
     * </strong>
     */
    @Nullable
    public List<Feature<?, ?>> features;

    public GraphElement() {

    }

    public GraphElement(GraphElement element, CopyContext context) {
        if (context == CopyContext.SYNC) {
            // Copy all the non-halo features
            getStorage().cacheFeatures(element, CacheFeatureContext.NON_HALO);
            if (element.features != null && !element.features.isEmpty()) {
                for (Feature<?, ?> feature : element.features) {
                    if (!feature.isHalo()) {
                        feature.copy(context).setElement(this, false);
                    }
                }
            }
        }
    }

    /**
     * Helper method for getting the storage object or null if we are not at storage operator right now
     */
    public static BaseStorage getStorage() {
        return BaseStorage.STORAGES.get();
    }

    /**
     * Copy this element
     *
     * @param context the context for copying.
     */
    abstract public GraphElement copy(CopyContext context);

    /**
     * Part of creation relating to storage
     * <strong>Creating all features as well</strong>
     */
    protected Consumer<BaseStorage> createInternal() {
        Consumer<BaseStorage> callback = null;
        if (getStorage().addElement(this)) {
            callback = storage -> storage.getPlugins().forEach(plugin -> plugin.addElementCallback(this));
            if (features != null) {
                for (Feature<?, ?> feature : features) {
                    callback = callback.andThen(feature.createInternal());
                }
            }
        }
        return callback;
    }

    /**
     * Part of deletion relating to storage
     * <strong>Deleting all features as well</strong>
     */
    protected Consumer<BaseStorage> deleteInternal() {
        getStorage().cacheFeatures(this, CacheFeatureContext.ALL);
        Consumer<BaseStorage> callback = null;
        if (features != null) {
            for (Feature<?, ?> feature : features) {
                callback = callback == null ? feature.deleteInternal() : feature.deleteInternal().andThen(callback);
            }
        }
        getStorage().deleteElement(this);
        callback = callback == null ? storage -> storage.getPlugins().forEach(plugin -> plugin.deleteElementCallback(this)) : callback.andThen(storage -> storage.getPlugins().forEach(plugin -> plugin.deleteElementCallback(this)));
        return callback;
    }

    /**
     * Part of update relating to storage
     * <p>
     * Creating or updating all features as well
     * If memento is not-null, update is successful
     * </p>
     */
    protected Tuple2<Consumer<BaseStorage>, GraphElement> updateInternal(GraphElement newElement, @Nullable GraphElement memento) {
        Consumer<BaseStorage> callback = null;
        if (newElement.features != null && !newElement.features.isEmpty()) {
            for (Iterator<Feature<?, ?>> iterator = newElement.features.iterator(); iterator.hasNext(); ) {
                Feature<?, ?> newFeature = iterator.next();
                if (containsFeature(newFeature.getName())) {
                    // This is Feature update
                    Feature<?, ?> thisFeature = getFeature(newFeature.getName());
                    Tuple2<Consumer<BaseStorage>, GraphElement> tmp = thisFeature.updateInternal(newFeature, null);
                    if (tmp.f0 != null) {
                        memento = memento == null ? copy(CopyContext.MEMENTO) : memento;
                        callback = callback == null ? tmp.f0 : callback.andThen(tmp.f0);
                        ((Feature<?, ?>) tmp.f1).setElement(memento, false);
                    }
                } else {
                    memento = memento == null ? copy(CopyContext.MEMENTO) : memento;
                    iterator.remove();
                    newFeature.setElement(this, false);
                    Consumer<BaseStorage> tmp = newFeature.createInternal();
                    if (tmp != null) {
                        callback = callback == null ? tmp : callback.andThen(tmp);
                    }
                }
            }
        }
        if (memento != null) {
            getStorage().updateElement(this, memento);
            GraphElement finalMemento = memento;
            Consumer<BaseStorage> tmp = storage -> storage.getPlugins().forEach(plugin -> plugin.updateElementCallback(this, finalMemento));
            callback = callback == null ? tmp : callback.andThen(tmp);
            return Tuple2.of(callback, memento);
        }
        return reuse;
    }

    /**
     * Part of creation relating to replication and external things
     */
    public Consumer<BaseStorage> create() {
        return createInternal();
    }

    /**
     * Part of deletion relating to replication and external things
     */
    public Consumer<BaseStorage> delete() {
        return deleteInternal();
    }

    /**
     * Part of update relating to replication and external things
     */
    public Tuple2<Consumer<BaseStorage>, GraphElement> update(GraphElement newElement) {
        return updateInternal(newElement, null);
    }

    /**
     * Sync this {@link GraphElement}
     */
    public void sync(GraphElement newElement) {
        throw new NotImplementedException("Replica Elements should override this method");
    }

    /**
     * Type of this element
     */
    public ElementType getType() {
        return ElementType.NONE;
    }

    /**
     * Is this element replicable
     */
    public boolean isReplicable() {
        return false;
    }

    /**
     * Master part of this element
     */
    public short getMasterPart() {
        return getPart();
    }

    /**
     * If this element is HALO
     */
    public boolean isHalo() {
        return false;
    }

    /**
     * State of this element (MASTER, REPLICA, UNDEFINED)
     */
    public ReplicaState state() {
        if (getPart() == -1) return ReplicaState.UNDEFINED;
        if (getPart() == getMasterPart()) return ReplicaState.MASTER;
        return ReplicaState.REPLICA;
    }

    /**
     * List of replica parts
     */
    public List<Short> getReplicaParts() {
        return Collections.emptyList();
    }

    /**
     * ID of GraphElement
     */
    abstract public String getId();

    /**
     * Current Part of this element
     * <strong>To be called in Storage parts only, otherwise {@link NullPointerException}</strong>
     */
    public short getPart() {
        return getStorage().layerFunction.getCurrentPart();
    }

    /**
     * Retrieves {@link Feature} from cache if exists, otherwise from storage.
     *
     * @implNote <strong> Developer should make sure to call this method knowing that the feature exists</strong>
     */
    public Feature<?, ?> getFeature(String name) {
        if (features != null) {
            for (Feature<?, ?> feature : features) {
                if (feature.getName().equals(name)) return feature;
            }
        }
        if (Objects.nonNull(getStorage())) {
            Feature<?, ?> feature = getStorage().getAttachedFeature(getType(), getId(), name, null);
            feature.setElement(this, false);
        }
        return null;
    }

    /**
     * Returns if {@link Feature} with this name is available either here or in storage
     */
    public Boolean containsFeature(String name) {
        if (features != null) {
            for (Feature<?, ?> feature : features) {
                if (feature.getName().equals(name)) return true;
            }
        }
        if (getStorage() != null) {
            return getStorage().containsAttachedFeature(getType(), getId(), name, null);
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delay() {
        if (features != null) features.forEach(LifeCycleControl::delay);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void resume() {
        if (features != null) features.forEach(LifeCycleControl::resume);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onDeserialized() {
        if (features != null) {
            features.forEach(feature -> {
                feature.element = this;
                feature.onDeserialized();
            });
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return getType() + "{" +
                "id='" + getId() + '\'' +
                '}';
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GraphElement that = (GraphElement) o;
        return Objects.equals(getId(), that.getId()) && Objects.equals(getType(), that.getType());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hash(getId(), getType());
    }

}
