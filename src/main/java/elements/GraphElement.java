package elements;

import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import storage.BaseStorage;
import typeinfo.ListTypeInformationFactory;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;


public class GraphElement implements Serializable {
    @Nullable
    public String id;
    @Nullable
    public Short partId;
    @Nullable
    public Long ts;
    @Nullable
    public transient BaseStorage storage;
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

    public GraphElement(GraphElement element, boolean deepCopy){
        this.id = element.id;
        this.partId = element.partId;
        this.ts = element.ts;
        if(deepCopy){
            this.storage = element.storage;
            if(element.features !=null){
                this.features = new ArrayList<>(element.features);
            }
        }
    }

    /**
     * Helper method to add feature if it does not exist
     *
     * @param  el  Element to add Feature to
     * @param feature feature we are willing to add
     * @return could we add it
     */
    public static boolean addCachedFeatureOrExists(GraphElement el, Feature<?, ?> feature) {
        if(el.features==null)el.features = new ArrayList<>(3);
        if (!el.features.contains(feature)) {
            el.features.add(feature);
            return true;
        }
        return false;
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
     * Element creation logic
     *
     * @return Was element created
     */
    public Boolean createElement() {
        assert storage != null;
        boolean is_created = storage.addElement(this);
        if (is_created) {
            if(features!=null){
                for (GraphElement el : features) {
                    el.createElement();
                }
            }
            storage.getPlugins().forEach(item -> item.addElementCallback(this));
        }
        return is_created;
    }

    /**
     * Element deletion logic
     *
     * @return Was element deleted
     */
    public Boolean deleteElement() {
        assert storage !=null;
        cacheFeatures();
        if(features!=null){
            for (GraphElement el : features) {
                el.deleteElement();
            }
        }
        boolean is_deleted = storage.deleteElement(this);
        if (is_deleted) {
            storage.getPlugins().forEach(item -> item.deleteElementCallback(this));
        }
        return is_deleted;
    }

    /**
     * Element update logic
     *
     * @param newElement newElement to update with
     * @return (is updated, previous values)
     */
    public Tuple2<Boolean, GraphElement> updateElement(GraphElement newElement) {
        assert storage!=null;
        GraphElement memento = this.copy(); // No features to storage
        boolean is_updated = false;
        if(newElement.features!=null) {
            for (Feature<?, ?> feature : newElement.features) {
                Feature<?, ?> thisFeature = this.getFeature(feature.getName());
                if (Objects.nonNull(thisFeature)) {
                    Tuple2<Boolean, GraphElement> tmp = thisFeature.updateElement(feature);
                    is_updated |= tmp.f0;
                    memento.setFeature(feature.getName(), (Feature<?, ?>) tmp.f1);
                } else {
                    Feature<?, ?> featureCopy = feature.copy();
                    featureCopy.setStorage(storage);
                    featureCopy.setElement(this);
                    featureCopy.createElement();
                    is_updated = true;
                }
            }
            if (is_updated) {
                resolveTimestamp(newElement.getTimestamp());
                this.storage.updateElement(this);
                this.storage.getPlugins().forEach(item -> item.updateElementCallback(this, memento));
            }
        }
        return new Tuple2<>(is_updated, memento);
    }

    /**
     * External delete query
     *
     * @return is deleted
     */
    public Boolean delete() {
        return deleteElement();
    }

    /**
     * External Create logic
     *
     * @return is created
     */
    public Boolean create() {
        return createElement();
    }

    /**
     * External Sync logic
     *
     * @param newElement element that requires syncing
     * @return (isSynced, previous element)
     */
    public Tuple2<Boolean, GraphElement> sync(GraphElement newElement) {
        return new Tuple2<>(false, this);
    }

    /**
     * External Update logic
     *
     * @param newElement external update element
     * @return (isUpdated, previous element)
     */
    public Tuple2<Boolean, GraphElement> update(GraphElement newElement) {
        return updateElement(newElement);
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
        if (getPartId() == null) return ReplicaState.UNDEFINED;
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
        this.partId = storage==null?this.partId:storage.layerFunction.getCurrentPart();
        if(features!=null){
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
        Feature<?, ?> result = features!=null?features.stream().filter(item -> Objects.equals(item.getName(), name)).findAny().orElse(null):null;
        if (result == null && storage != null) {
            result = storage.getFeature(decodeFeatureId(name));
            if (Objects.nonNull(result)) result.setElement(this);
        }
        return result;
    }

    /**
     * If the feature already exists this will not do anything
     * Otherwise it will try to create the feature in storage or at least append to feature list
     *
     * @param name    name of the feature to be added
     * @param feature feature itself
     */
    public void setFeature(String name, Feature<?, ?> feature) {
        if (Objects.isNull(getFeature(name))) {
            feature.setId(name);
            feature.setStorage(storage);
            feature.setElement(this); // This also adds this feature to my element
            if (Objects.nonNull(storage)) {
                feature.create();
            }
        }
    }

    /**
     * Helper to setFeature with a specific timestamp
     *
     * @param name      Feature name
     * @param feature   Feature object
     * @param timestamp Timestamp
     */
    public void setFeature(String name, Feature<?, ?> feature, Long timestamp) {
        feature.setTimestamp(timestamp);
        setFeature(name, feature);
    }

    /**
     * Retrieves all features of this graph element from the storage
     */
    public void cacheFeatures() {
        if (Objects.nonNull(storage)) {
            storage.cacheFeaturesOf(this);
        }
    }

    /**
     * Clear the cached features on this GraphElement
     */
    public void clearFeatures() {
        if(features!=null)features.clear();
    }

    /**
     * Helper method that decodes feature id from the featureName
     *
     * @param name featureName
     * @return full feature id
     */
    public String decodeFeatureId(String name) {
        return getId() +":"+ name;
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

    @Override
    public String toString() {
        return "GraphElement{" +
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
