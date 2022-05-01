package elements;

import iterations.MessageDirection;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * Represents a Feature either attached to another element or not
 *
 * @param <T> Type that is actually stored in the Feature Object
 * @param <V> Type that is exposed through getValue method
 */
public class Feature<T, V> extends ReplicableGraphElement {
    public T value;
    public transient GraphElement element;
    public Tuple2<ElementType, String> attachedTo = new Tuple2<>(ElementType.NONE, null);

    public Feature() {
        super();
        this.value = null;
    }

    public Feature(T value) {
        super();
        this.value = value;
    }

    public Feature(T value, boolean halo) {
        super(null, halo);
        this.value = value;
    }

    public Feature(T value, boolean halo, short master) {
        super(null, halo, master);
        this.value = value;
    }

    public Feature(String id, T value) {
        super(id);
        this.value = value;
    }

    public Feature(String id, T value, boolean halo) {
        super(id, halo);
        this.value = value;
    }

    public Feature(String id, T value, boolean halo, short master) {
        super(id, halo, master);
        this.value = value;
    }

    @Override
    public Feature<T, V> copy() {
        Feature<T, V> tmp = new Feature<>(this.id, this.value, this.halo, this.master);
        tmp.ts = this.ts;
        tmp.attachedTo = this.attachedTo;
        tmp.partId = this.partId;
        return tmp;
    }

    @Override
    public Feature<T, V> deepCopy() {
        Feature<T, V> tmp = new Feature<>(this.id, this.value, this.halo, this.master);
        tmp.ts = this.ts;
        tmp.attachedTo = this.attachedTo;
        tmp.partId = this.partId;
        tmp.element = this.element;
        tmp.storage = this.storage;
        tmp.features.addAll(this.features);
        return tmp;
    }

    /**
     * Features attached to elements should arrive at corresponding masters first,
     * hence the different in main logic is that master should then create them on replica parts
     *
     * @return is created successfully
     */
    @Override
    public Boolean create() {
        if (this.attachedTo._1 == ElementType.NONE) return super.create();
        else {
            boolean is_created = createElement();
            if (is_created && state() == ReplicaState.MASTER && !isHalo()) {
                replicaParts().forEach(part -> storage.layerFunction.message(new GraphOp(Op.COMMIT, part, this, MessageDirection.ITERATE, getTimestamp())));
            }
            return is_created;
        }
    }

    /**
     * Update element is different for feature since we also need to update the value stored in the feature
     *
     * @param newElement newElement to update with
     * @return (isUpdated, oldElement)
     */
    @Override
    public Tuple2<Boolean, GraphElement> updateElement(GraphElement newElement) {
        Feature<T, V> memento = this.copy();
        Feature<T, V> newFeature = (Feature<T, V>) newElement;
        boolean isUpdated = !this.valuesEqual(newFeature.value, this.value);
        if (isUpdated) this.value = newFeature.value;
        if (this.attachedTo._1 == ElementType.NONE) {
            // If none sub-features may exist
            for (Feature<?, ?> newSubFeature : newElement.features) {
                Feature<?, ?> thisSubFeature = this.getFeature(newSubFeature.getName());
                if (Objects.nonNull(thisSubFeature)) {
                    Tuple2<Boolean, GraphElement> tmp = thisSubFeature.updateElement(newSubFeature);
                    isUpdated |= tmp._1();
                    addIfNotExists(memento.features, (Feature<?, ?>) tmp._2);
                } else {
                    Feature<?, ?> featureCopy = newSubFeature.copy();
                    featureCopy.setElement(this);
                    featureCopy.setStorage(this.storage);
                    featureCopy.createElement();
                    addIfNotExists(this.features, featureCopy);
                    isUpdated = true;
                }
            }
        }

        if (isUpdated) {
            this.setTimestamp(Math.max(getTimestamp(), newElement.getTimestamp())); // Timestamps always max out
            this.storage.updateFeature(this);
            this.storage.getPlugins().forEach(item -> item.updateElementCallback(this, memento));
        }
        return new Tuple2<>(isUpdated, memento);
    }


    /**
     * Gets the value of the interface V that is stored here
     *
     * @return V
     */
    public V getValue() {
        return (V) this.value;
    }

    /**
     * Given 2 Ts if they are both equal
     *
     * @param v1 first T
     * @param v2 second T
     * @return if v1 === v2
     * @implNote This function is used to decide if an update is really needed
     */
    public boolean valuesEqual(T v1, T v2) {
        return false;
    }

    /**
     * If this element is an attached feature, master part is the one of the attached graph element
     *
     * @return master part
     */
    @Override
    public short masterPart() {
        if (Objects.nonNull(this.getElement())) {
            return this.getElement().masterPart();
        }
        return super.masterPart();
    }

    /**
     * If this element is an attached feature, replica parts are the ones attached to the graph element
     *
     * @return replicated parts
     */
    @Override
    public List<Short> replicaParts() {
        if (Objects.nonNull(this.getElement())) {
            return this.getElement().replicaParts();
        }
        return super.replicaParts();
    }

    /**
     * If this element is an attached feature, id = attachedId + this.id
     *
     * @return id of the feature
     * @implNote this method should be used for storing the elements as well as keying
     */
    @Override
    public String getId() {
        if (this.attachedTo._1 == ElementType.NONE) return super.getId();
        return this.attachedTo._2 + this.id;
    }

    /**
     * Name is the actualy feature name, this is equal id if not attached feature
     *
     * @return name of the feature
     */
    public String getName() {
        return this.id;
    }

    /**
     * If element is cached here return it, otherwise ask the DB to retrieve the element
     *
     * @return GraphElement
     */
    @Nullable
    public GraphElement getElement() {
        if (attachedTo._1 == ElementType.NONE) return null;
        if (element == null && storage != null) {
            setElement(storage.getElement(attachedTo._2, attachedTo._1));
        }
        return element;
    }

    /**
     * Caches the given element, adds current feature to feature of the element if that does not exist
     *
     * @param attachingElement element that want to attach itself to this feature
     * @implNote Attaching a feature to element is done here, reasing is that we want to have rigid link
     * between element.features <--> feature.element.
     */
    public void setElement(GraphElement attachingElement) {
        if (element == null && attachingElement != null && addIfNotExists(attachingElement.features, this)) {
            attachedTo = new Tuple2<>(attachingElement.elementType(), attachingElement.getId());
            element = attachingElement;
        }
    }


    /**
     * Attached Features cannot have subFeatures otherwise behaves as usual
     *
     * @param name name of the feature
     * @return Feature
     */
    @Override
    @Nullable
    public Feature<?, ?> getFeature(String name) {
        if (attachedTo._1 == ElementType.NONE) return super.getFeature(name);
        return null;
    }

    /**
     * Attached Features cannot have subFeatures otherwise begaves as usual
     *
     * @param name    name of the feature to be added
     * @param feature feature itself
     */
    @Override
    public void setFeature(String name, Feature<?, ?> feature) {
        if (attachedTo._1 == ElementType.NONE) super.setFeature(name, feature);
        throw new IllegalStateException("Nested features not allowed");
    }

    @Override
    public ElementType elementType() {
        return ElementType.FEATURE;
    }
}
