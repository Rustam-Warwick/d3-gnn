package elements;

import ai.djl.ndarray.NDArray;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import storage.BaseStorage;
import typeinfo.ListTypeInformationFactory;
import typeinfo.RecursiveListFieldsTypeInfoFactory;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

@TypeInfo(RecursiveListFieldsTypeInfoFactory.class)
public class GraphElement implements Serializable {
    protected static final transient Tuple2<Boolean, GraphElement> reuse = Tuple2.of(false, null);

    @OmitStorage
    @Nullable
    public String id;

    @OmitStorage
    @Nullable
    public Short partId;

    @Nullable
    public Long ts;

    @Nullable
    public transient BaseStorage storage;

    @OmitStorage
    @Nullable
    @TypeInfo(ListTypeInformationFactory.class)
    public List<Feature<?, ?>> features;

    public GraphElement() {

    }

    public GraphElement(@Nullable String id) {
        this.id = id;
        this.partId = null;
        this.storage = null;
        this.features = null;
    }

    public GraphElement(GraphElement element, boolean deepCopy) {
        this.id = element.id;
        this.partId = element.partId;
        this.ts = element.ts;
    }

    /**
     * Copy bare element, without storage and features
     *
     * @return copied element
     */
    public GraphElement copy() {
        return new GraphElement(this, false);
    }

    /**
     * Copy everything including storage, element features
     *
     * @return copied element
     */
    public GraphElement deepCopy() {
        return new GraphElement(this, true);
    }

    /**
     * Creates element and all attached Features
     * Notifies the listener plugins if the boolean flag is true
     * Here we should encode the logic of transforming the element locally if needed
     * @return Was element created
     * @implNote Does not rollback if some Feature is not created
     */
    protected Boolean createElement() {
        assert storage != null;
        boolean is_created = storage.addElement(this);
        if (is_created) {
            storage.runCallback(item -> item.addElementCallback(this));
            if (features != null) {
                for (Feature<?, ?> el : features) {
                    el.createElement();
                }
            }
        }
        return is_created;
    }

    /**
     * Deletes element and all attached Features
     *
     * @return Was element deleted
     * @implNote Does not rollback after deletion
     */
    protected Boolean deleteElement() {
        assert storage != null;
        cacheFeatures();
        if (features != null) {
            for (GraphElement el : features) {
                el.deleteElement();
            }
        }
        boolean is_deleted = storage.deleteElement(this);
        if (is_deleted) {
            storage.runCallback(item -> item.deleteElementCallback(this));
        }
        return is_deleted;
    }

    /**
     * If memento is null, will try to update features with the features of newElement. If update is found will create a copy of this element called memento
     * If memnto is not-null it means that this element must be updated even if not updates are found in Features. Passing memento is needed if your sub-class has some additional data that should be updated.
     * Memento stores the difference between the updated value of this element vs the old value.
     *
     * @param newElement newElement to update with
     * @return (is updated, previous value)
     */
    protected Tuple2<Boolean, GraphElement> updateElement(GraphElement newElement, @Nullable GraphElement memento) {
        assert storage != null;
        if (newElement.features != null && !newElement.features.isEmpty()) {
            for (Iterator<Feature<?, ?>> iterator = newElement.features.iterator(); iterator.hasNext(); ) {
                Feature<?, ?> feature = iterator.next();
                Feature<?, ?> thisFeature = this.getFeature(feature.getName());
                if (Objects.nonNull(thisFeature)) {
                    Tuple2<Boolean, GraphElement> tmp = thisFeature.updateElement(feature, null);
                    if (tmp.f0) {
                        memento = memento == null ? this.copy() : memento;
                        memento.setFeature(feature.getName(), (Feature<?, ?>) tmp.f1);
                    }
                } else {
                    memento = memento == null ? this.copy() : memento;
                    iterator.remove();
                    feature.setStorage(storage);
                    feature.setElement(this);
                    feature.createElement();
                }
            }
        }

        if (memento != null) {
            resolveTimestamp(newElement.getTimestamp());
            storage.updateElement(this);
            GraphElement finalMemento = memento;
            storage.runCallback(item -> item.updateElementCallback(this, finalMemento));
            return Tuple2.of(true, memento);
        }
        return reuse;
    }

    /**
     * External delete query
     *
     */
    public void delete() {
        deleteElement();
    }

    /**
     * External Create logic
     *
     */
    public void create() {
        createElement();
    }

    /**
     * External Sync logic
     *
     * @param newElement element that requires syncing
     */
    public void sync(GraphElement newElement) {
        // No action
    }

    /**
     * External Update logic
     *
     * @param newElement external update element
     */
    public void update(GraphElement newElement) {
         updateElement(newElement, null);
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
    public Boolean isReplicable() {
        return false;
    }

    /**
     * Master part of this element, default is current part
     *
     * @return master part of this element
     */
    @Nullable
    public Short masterPart() {
        return getPartId();
    }

    /**
     * Default is false, halo does make sense only for replicableElements
     *
     * @return is this element Halo()
     */
    public Boolean isHalo() {
        return false;
    }

    /**
     * MASTER, REPLICA, UNDEFINED. UNDEFINED is if not yet in storage
     *
     * @return state of this element
     */
    public ReplicaState state() {
        if (getPartId() == null || masterPart() == null) return ReplicaState.UNDEFINED;
        if (Objects.equals(getPartId(), this.masterPart())) return ReplicaState.MASTER;
        return ReplicaState.REPLICA;
    }

    /**
     * List of replica part, default is empty list
     *
     * @return list of replica parts
     */
    public List<Short> replicaParts() {
        return Collections.emptyList();
    }

    /**
     * Element Timestamp
     * If the timestamp does not exist take the current element's timestamp in the pipeline
     *
     * @return timestamp
     */
    @Nullable
    public Long getTimestamp() {
        return ts;
    }

    /**
     * Set element timestamp
     *
     * @param ts timestamp to be added
     */
    public void setTimestamp(Long ts) {
        this.ts = ts;
    }

    /**
     * get id of this element
     *
     * @return element id
     */
    @Nullable
    public String getId() {
        return id;
    }

    /**
     * Set id of this element
     *
     * @param id id to be set
     */
    public void setId(@Nullable String id) {
        this.id = id;
    }

    /**
     * Get the part id of this element. If attached to storage default is current processing part
     *
     * @return Element part id
     */
    @Nullable
    public Short getPartId() {
        return partId;
    }

    /**
     * Set the part id of this element
     *
     * @param partId part id
     */
    public void setPartId(@Nullable Short partId) {
        this.partId = partId;
    }

    /**
     * Attaches storage to this element, so that element can use the storage functions
     * Setting storage also affects part id as well id ids of subFeatures
     * In this step we also assign this as element of subFeatures
     *
     * @param storage BaseStorage to be attached to
     */
    public void setStorage(BaseStorage storage) {
        this.storage = storage;
        this.partId = storage != null ? storage.layerFunction.getCurrentPart() : partId;
        if (features != null) {
            for (Feature<?, ?> ft : this.features) {
                ft.setStorage(storage);
                ft.setElement(this);
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
        Feature<?, ?> result = features != null ? features.stream().filter(item -> Objects.equals(item.getName(), name)).findAny().orElse(null) : null;
        if (result == null && storage != null && storage.containsFeature(Feature.encodeAttachedFeatureId(name, getId()))) {
            result = storage.getFeature(Feature.encodeAttachedFeatureId(name, getId()));
        }
        if (Objects.nonNull(result)) result.setElement(this);
        return result;
    }

    /**
     * Returns if Feature with this name is available either here or in storage
     */
    public Boolean containsFeature(String name) {
        boolean hasLocallyAvailable = features != null && features.stream().anyMatch(item -> Objects.equals(item.getName(), name));
        return hasLocallyAvailable || (storage != null && storage.containsFeature(Feature.encodeAttachedFeatureId(name, getId())));
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
            feature.setElement(this); // This also adds this feature to my element
            if (Objects.nonNull(storage)) {
                feature.create();
            }
        }
    }

    /**
     * Retrieves all features of this graph element from the storage
     */
    public void cacheFeatures() {
        assert storage != null;
        storage.cacheFeaturesOf(this);
    }

    /**
     * Clear the cached features on this GraphElement
     */
    public void clearFeatures() {
        if (features != null) features.clear();
    }

    /**
     * Resolve timestamp of the new Element that just came in
     *
     * @param newTimestamp New Element timestamp
     */
    public void resolveTimestamp(Long newTimestamp) {
        if (Objects.nonNull(newTimestamp)) {
            if (Objects.nonNull(ts)) {
                ts = Math.max(ts, newTimestamp);
            } else {
                ts = newTimestamp;
            }
        }

    }

    /**
     * Consumer for all NDArrays, traverses all
     */
    public void applyForNDArrays(Consumer<NDArray> operation) {
        if (features != null) {
            features.forEach(item -> item.applyForNDArrays(operation));
        }
    }

    @Override
    public String toString() {
        return elementType()+"{" +
                "id='" + id + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GraphElement that = (GraphElement) o;
        return Objects.equals(getId(), that.getId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId());
    }


}
