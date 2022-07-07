package elements;

import elements.iterations.MessageDirection;
import org.apache.flink.api.java.tuple.Tuple2;

import javax.annotation.Nullable;
import java.util.ArrayList;
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
    @Nullable
    public transient GraphElement element;
    @Nullable
    public Tuple2<ElementType, String> attachedTo;

    public Feature() {
        super();
    }

    public Feature(Feature<T, V> f, boolean deepCopy) {
        super(f, deepCopy);
        this.attachedTo = f.attachedTo;
        this.value = f.value;
    }

    public Feature(T value) {
        super();
        this.value = value;
    }

    public Feature(T value, boolean halo, Short master) {
        super(null, halo, master);
        this.value = value;
    }

    public Feature(String id, T value) {
        super(id, false, (short) -1);
        this.value = value;
    }

    public Feature(String id, T value, boolean halo, Short master) {
        super(id, halo, master);
        this.value = value;
    }

    @Override
    public Feature<T, V> copy() {
        return new Feature<>(this, false);
    }

    @Override
    public Feature<T, V> deepCopy() {
        return new Feature<>(this, true);
    }

    /**
     * Features attached to elements should arrive at corresponding masters first,
     * hence the different in main logic is that master should then create them on replica parts
     *
     * @return is created successfully
     */
    @Override
    public Boolean create() {
        if (this.attachedTo == null) return super.create();
        else {
            boolean is_created = createElement();
            if (is_created && state() == ReplicaState.MASTER && !isHalo()) {
                replicaParts().forEach(part -> storage.layerFunction.message(new GraphOp(Op.COMMIT, part, this), MessageDirection.ITERATE));
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
    public Tuple2<Boolean, GraphElement> updateElement(GraphElement newElement, GraphElement memento) {
        assert storage != null;
        Feature<T, V> newFeature = (Feature<T, V>) newElement;
        if (!valuesEqual(newFeature.value, this.value)) {
            memento = this.copy();
            value = newFeature.value;
        }
        return super.updateElement(newElement, memento);
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
    public Short masterPart() {
        if (Objects.nonNull(getElement())) {
            return getElement().masterPart();
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
        if (Objects.nonNull(getElement())) {
            return getElement().replicaParts();
        }
        return super.replicaParts();
    }

    @Override
    public void setId(@Nullable String id) {
        throw new IllegalStateException("Use .setName() for Features");
    }

    /**
     * If this element is an attached feature, id = attachedId + this.id
     *
     * @return id of the feature
     * @implNote this method should be used for storing the elements as well as keying
     */
    @Override
    public String getId() {
        if (this.attachedTo == null) return super.getId();
        return attachedTo.f1 + ":" + this.id;
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
     * Set the name(actually id). SetId is made private to not have confusion
     */
    public void setName(@Nullable String name){
        this.id = name;
    }
    /**
     * If element is cached here return it, otherwise ask the DB to retrieve the element
     *
     * @return GraphElement
     */
    @Nullable
    public GraphElement getElement() {
        if (attachedTo == null) return null;
        if (element == null && storage != null) {
            setElement(storage.getElement(attachedTo.f1, attachedTo.f0));
        }
        return element;
    }

    /**
     * Caches the given element, adds current feature to feature of the element if that does not exist there.
     * Removes reference from other variable if already attached
     *
     * @param attachingElement element that want to attach itself to this feature
     * @implNote Make sure to properly track the change of references from Graph Elements
     * @implNote If this element is already attached to some other element, it should be removed from that elements feature list before attaching this to other element
     * @implNote Attaching element cannot contain a Feature with the same id.
     * @implNote Attaching a feature to element is done here, reasing is that we want to have rigid link
     * between element.features <--> feature.element.
     */
    public void setElement(GraphElement attachingElement) {
        if (attachingElement != null) {
            if (element == attachingElement) return; // Already attached
            if (element != null && element.features.contains(this)) {
                throw new IllegalStateException("This Feature has an attachee, make sure to remove it from element.featue before proceeding");
            }
            attachedTo = attachedTo == null ? new Tuple2<>(attachingElement.elementType(), attachingElement.getId()) : attachedTo;
            element = attachingElement;
            if (attachingElement.features == null) attachingElement.features = new ArrayList<>(4);
            if (attachingElement.features.stream().anyMatch(item -> item == this)) return;
            if (attachingElement.features.contains(this)) {
                throw new IllegalStateException("This Element already has a similar feature, use updateFeature instead");
            }
            attachingElement.features.add(this);
        }
    }

    @Override
    public void setFeature(String name, Feature<?, ?> feature) {
        if (attachedTo != null)
            throw new IllegalStateException("Instead of using nested Features, go with flat design");
        super.setFeature(name, feature);
    }

    @Nullable
    @Override
    public Feature<?, ?> getFeature(String name) {
        if (attachedTo != null) return null;
        return super.getFeature(name);
    }

    @Override
    public ElementType elementType() {
        return ElementType.FEATURE;
    }
}
