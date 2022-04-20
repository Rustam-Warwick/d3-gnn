package elements;

import scala.Serializable;
import scala.Tuple2;
import storage.BaseStorage;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class GraphElement implements Serializable {
    public String id;
    public short partId;
    public transient BaseStorage storage;
    public List<Feature> features;

    public GraphElement() {
        this.id = null;
        this.partId = -1;
        this.storage = null;
        this.features = new ArrayList<>();
    }

    public GraphElement(String id) {
        this.id = id;
        this.partId = -1;
        this.storage = null;
        this.features = new ArrayList<>();
    }

    public static void addIfNotExists(List<Feature> list, Feature feature) {
        if (!list.contains(feature)) {
            list.add(feature);
        }
    }

    /**
     * Copy bare element, without storage and features
     *
     * @return copied element
     */
    public GraphElement copy() {
        GraphElement tmp = new GraphElement(this.id);
        tmp.partId = this.partId;
        return tmp;
    }

    /**
     * Copy everything including storage, element features
     *
     * @return copied element
     */
    public GraphElement deepCopy() {
        GraphElement tmp = new GraphElement(this.id);
        tmp.partId = this.partId;
        tmp.storage = this.storage;
        tmp.features.addAll(this.features);
        return tmp;
    }

    public Boolean createElement() {
        boolean is_created = this.storage.addElement(this);
        if (is_created) {
            for (GraphElement el : this.features) {
                el.createElement();
            }
            this.storage.getPlugins().forEach(item -> item.addElementCallback(this));
        }
        return is_created;
    }

    public Boolean deleteElement() {
        cacheFeatures();
        for (GraphElement el : this.features) {
            el.deleteElement();
        }
        boolean is_deleted = this.storage.deleteElement(this);
        if (is_deleted) {
            this.storage.getPlugins().forEach(item -> item.deleteElementCallback(this));
        }
        return is_deleted;
    }

    public Tuple2<Boolean, GraphElement> updateElement(GraphElement newElement) {
        GraphElement memento = this.copy();
        boolean is_updated = false;
        for (Feature feature : newElement.features) {
            Feature thisFeature = this.getFeature(feature.getFieldName());
            if (Objects.nonNull(thisFeature)) {
                Tuple2<Boolean, GraphElement> tmp = thisFeature.updateElement(feature);
                is_updated |= tmp._1();
                addIfNotExists(memento.features, (Feature) tmp._2);
            } else {
                feature.setElement(this);
                feature.setStorage(this.storage);
                feature.createElement();
                addIfNotExists(this.features, feature);
                is_updated = true;
            }
        }

        if (is_updated) {
            this.storage.updateElement(this);
            this.storage.getPlugins().forEach(item -> item.updateElementCallback(this, memento));
        }

        return new Tuple2<>(is_updated, memento);
    }

    public Boolean delete() {
        return deleteElement();
    }

    public Boolean create() {
        return createElement();
    }

    public Tuple2<Boolean, GraphElement> sync(GraphElement newElement) {
        return new Tuple2<>(false, this);
    }

    public Tuple2<Boolean, GraphElement> update(GraphElement newElement) {
        return updateElement(newElement);
    }

    // Typing stuff
    public ElementType elementType() {
        return ElementType.NONE;
    }

    public Boolean isReplicable() {
        return false;
    }

    public short masterPart() {
        return this.partId;
    }

    public ReplicaState state() {
        if (this.partId == -1) return ReplicaState.UNDEFINED;
        if (this.partId == this.masterPart()) return ReplicaState.MASTER;
        return ReplicaState.REPLICA;
    }

    @Nullable
    public List<Short> replicaParts() {
        return Collections.emptyList();
    }
    // Getters and Setters

    public Boolean isHalo() {
        return false;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public short getPartId() {
        if (Objects.nonNull(this.storage)) return storage.layerFunction.getCurrentPart();
        return partId;
    }

    public void setPartId(short partId) {
        this.partId = partId;
    }

    public void setStorage(BaseStorage storage) {
        this.storage = storage;
        if (Objects.nonNull(storage)) this.partId = storage.layerFunction.getCurrentPart();
        for (Feature ft : this.features) {
            ft.setStorage(storage);
        }
    }

    public String decodeFeatureId(String fieldName) {
        return getId() + fieldName;
    }

    public Feature getFeature(String name) {
        Feature result = this.features.stream().filter(item -> item.getFieldName().equals(name)).findAny().orElse(null);
        if (result == null && this.storage != null) {
            result = this.storage.getFeature(decodeFeatureId(name));
            if (Objects.nonNull(result)) {
                result.setElement(this);
                addIfNotExists(this.features, result);
            }
        }
        return result;
    }

    public void setFeature(String name, Feature feature) {
        Feature exists = this.getFeature(name);
        if (Objects.nonNull(exists)) return;
        feature.setId(name);
        feature.setElement(this);
        feature.setStorage(this.storage);
        if (Objects.nonNull(this.storage)) {
            if (feature.create()) {
                addIfNotExists(this.features, feature);
            }
        } else {
            addIfNotExists(this.features, feature);
        }
    }

    public void cacheFeatures() {
        if (Objects.nonNull(storage)) {
            storage.cacheFeaturesOf(this);
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
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }


}
