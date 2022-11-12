package elements;

import ai.djl.ndarray.LifeCycleControl;
import elements.annotations.OmitStorage;
import elements.enums.CacheFeatureContext;
import elements.enums.CopyContext;
import elements.enums.ElementType;
import elements.enums.ReplicaState;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import storage.BaseStorage;
import typeinfo.recursivepojoinfo.RecursivePojoTypeInfoFactory;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.*;
import java.util.function.Consumer;

/**
 * Abstract class representing a GraphElement.
 * CRUD Methods for interacting with the storage layer
 */
@TypeInfo(RecursivePojoTypeInfoFactory.class)
public abstract class GraphElement implements Serializable, LifeCycleControl {
    protected static final Tuple2<Consumer<BaseStorage>, GraphElement> reuse = Tuple2.of(null, null);
    @OmitStorage
    public short partId = -1; // Part id where this object is/was located

    @Nullable
    public transient BaseStorage storage;

    @OmitStorage
    @Nullable
    public List<Feature<?, ?>> features;

    public GraphElement() {

    }

    public GraphElement(GraphElement element, CopyContext context) {
        partId = element.partId;
        if (context == CopyContext.SYNC) {
            // Copy all the non-halo features
            element.storage.cacheFeatures(element, CacheFeatureContext.NON_HALO);
            if (element.features != null && !element.features.isEmpty()) {
                for (Feature<?, ?> feature : element.features) {
                    if (!feature.isHalo()) {
                        if (features == null) features = new ArrayList<>(3);
                        features.add(feature.copy(context));
                    }
                }
            }
        } else if (context == CopyContext.RMI) storage = element.storage;
    }

    /**
     * Shallow copy this element
     *
     * @param context the context for copying.
     */
    abstract public GraphElement copy(CopyContext context);

    /**
     * Create this element and all its features
     *
     * @return Callback or null if you cannot create
     */
    protected Consumer<BaseStorage> createElement() {
        Consumer<BaseStorage> callback = null;
        if (storage.addElement(this)) {
            callback = storage -> {
                storage.getPlugins().forEach(item -> item.addElementCallback(this));
            };
            if (features != null) {
                for (Feature<?, ?> feature : features) {
                    callback = callback.andThen(feature.createElement());
                }
            }
        }
        return callback;
    }

    /**
     * Deleted this element and all its features
     *
     * @return Callback or null if you cannot create
     */
    protected Consumer<BaseStorage> deleteElement() {
        storage.cacheFeatures(this, CacheFeatureContext.ALL);
        Consumer<BaseStorage> callback = null;
        if (features != null) {
            for (Feature<?, ?> feature : features) {
                callback = callback == null ? feature.deleteElement() : feature.deleteElement().andThen(callback);
            }
        }
        storage.deleteElement(this);
        callback = callback == null ? storage -> storage.getPlugins().forEach(plugin -> plugin.deleteElementCallback(this)) : callback.andThen(storage -> storage.getPlugins().forEach(plugin -> plugin.deleteElementCallback(this)));
        return callback;
    }

    /**
     * If memento is null, will try to update features with the features of newElement. If update is found will create a copy of this element called memento
     * If memento is not-null it means that this element must be updated even if not updates are found in Features. Passing memento is needed if your subclass has some additional data that should be updated.
     * Memento stores the difference between the updated value of this element vs the old value.
     *
     * @param newElement newElement to update with
     * @return (is updated, previous value)
     */
    protected Tuple2<Consumer<BaseStorage>, GraphElement> updateElement(GraphElement newElement, @Nullable GraphElement memento) {
        Consumer<BaseStorage> callback = null;
        if (newElement.features != null && !newElement.features.isEmpty()) {
            for (Iterator<Feature<?, ?>> iterator = newElement.features.iterator(); iterator.hasNext(); ) {
                Feature<?, ?> feature = iterator.next();
                if (containsFeature(feature.getName())) {
                    // This is Feature update
                    Feature<?, ?> thisFeature = getFeature(feature.getName());
                    Tuple2<Consumer<BaseStorage>, GraphElement> tmp = thisFeature.updateElement(feature, null);
                    if (tmp.f0 != null) {
                        memento = memento == null ? copy(CopyContext.MEMENTO) : memento;
                        callback = callback == null ? tmp.f0 : callback.andThen(tmp.f0);
                        ((Feature<?, ?>) tmp.f1).setElement(memento);
                    }
                } else {
                    memento = memento == null ? copy(CopyContext.MEMENTO) : memento;
                    iterator.remove();
                    feature.setStorage(storage);
                    feature.setElement(this);
                    Consumer<BaseStorage> tmp = feature.createElement();
                    if (tmp != null) {
                        callback = callback == null ? tmp : callback.andThen(tmp);
                    }
                }
            }
        }
        if (memento != null) {
            storage.updateElement(this, memento);
            GraphElement finalMemento = memento;
            Consumer<BaseStorage> tmp = item -> item.getPlugins().forEach(plugin -> plugin.updateElementCallback(this, finalMemento));
            callback = callback == null ? tmp : callback.andThen(tmp);
            return Tuple2.of(callback, memento);
        }
        return reuse;
    }

    /**
     * External Create GraphElement
     */
    public void create() {
        // 
        storage.runCallback(createElement());

    }

    /**
     * External Query to delete GraphElement
     */
    public void delete() {
        // 
        storage.runCallback(deleteElement());
    }

    /**
     * External Query to sync masters and replicas
     *
     * @param newElement element that requires syncing
     */
    public void sync(GraphElement newElement) {
        // No action
    }

    /**
     * External Query to update GraphElement
     *
     * @param newElement external update element
     */
    public void update(GraphElement newElement) {
        // 
        storage.runCallback(updateElement(newElement, null).f0);
    }

    /**
     * @return Type of this element
     */
    public ElementType elementType() {
        return ElementType.NONE;
    }

    /**
     * @return is this element replicable
     */
    public boolean isReplicable() {
        return false;
    }

    /**
     * @return master part of this element
     */
    public short masterPart() {
        return getPartId();
    }

    /**
     * @return if this element is HALO
     */
    public boolean isHalo() {
        return false;
    }

    /**
     * @return state of this element (MASTER, REPLICA, UNDEFINED)
     */
    public ReplicaState state() {
        if (getPartId() == -1) return ReplicaState.UNDEFINED;
        if (getPartId() == masterPart()) return ReplicaState.MASTER;
        return ReplicaState.REPLICA;
    }

    /**
     * @return list of replica parts
     */
    public List<Short> replicaParts() {
        return Collections.emptyList();
    }

    /**
     * @return id of GraphElement
     */
    abstract public String getId();

    /**
     * Get the part id of this element. If attached to storage default is current processing part
     *
     * @return Element part id
     */
    public short getPartId() {
        return partId;
    }

    /**
     * Attaches storage to this element, so that element can use the storage functions
     * Setting storage also affects part id as well id ids of subFeatures
     * In this step we also assign this as element of subFeatures
     *
     * @param storage BaseStorage to be attached to
     */
    public void setStorage(BaseStorage storage) {
        if (this.storage != null) return;
        this.storage = storage;
        this.partId = storage != null ? storage.layerFunction.getCurrentPart() : partId;
        if (features != null) {
            for (Feature<?, ?> feature : features) {
                feature.setStorage(storage);
                feature.setElement(this);
            }
        }
    }

    /**
     * Retrieves feature from cache if exists, otherwise from storage
     *
     * @param name name of the feature
     * @return Feature or NULL
     * @implNote that a cached feature will not be queried a second time from storage
     */
    @Nullable
    public Feature<?, ?> getFeature(String name) {
        if (features != null) {
            for (Feature<?, ?> feature : features) {
                if (feature.getName().equals(name)) return feature;
            }
        }
        if (storage != null) {
            Feature<?, ?> feature = storage.getAttachedFeature(elementType(), getId(), name, null);
            if (feature != null) {
                feature.setElement(this);
                return feature;
            }
        }
        return null;
    }

    /**
     * Returns if Feature with this name is available either here or in storage
     */
    public Boolean containsFeature(String name) {
        if (features != null) {
            for (Feature<?, ?> feature : features) {
                if (feature.getName().equals(name)) return true;
            }
        }
        if (storage != null) {
            return storage.containsAttachedFeature(elementType(), getId(), name, null);
        }
        return false;
    }

    /**
     * If the feature already exists this will not do anything
     * Otherwise it will try to create the feature in storage or at least append to feature list
     *
     * @param name    name of the feature to be added
     * @param feature feature itself
     */
    public void setFeature(String name, Feature<?, ?> feature) {
        if (!containsFeature(name)) {
            feature.setName(name);
            feature.setStorage(storage);
            feature.setElement(this);
            if (Objects.nonNull(storage)) {
                feature.create();
            }
        }
    }

    @Override
    public void delay() {
        if (features != null) features.forEach(LifeCycleControl::delay);
    }

    @Override
    public void resume() {
        if (features != null) features.forEach(LifeCycleControl::resume);
    }

    @Override
    public String toString() {
        return elementType() + "{" +
                "id='" + getId() + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GraphElement that = (GraphElement) o;
        return Objects.equals(getId(), that.getId()) && Objects.equals(elementType(), that.elementType());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId(), elementType());
    }


}
