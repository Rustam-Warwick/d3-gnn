package elements;

import elements.annotations.OmitStorage;
import elements.enums.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import storage.BaseStorage;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Represents a Feature either attached to another element or not
 *
 * @param <T> Type that is actually stored in the Feature Object
 * @param <V> Type that is exposed through getValue method
 */
public class Feature<T, V> extends ReplicableGraphElement {

    public static String DELIMITER = "/";

    public static ElementType[] ELEMENT_VALUES = ElementType.values();

    public T value; // Value stored in this feature

    public boolean halo = false; // Is this Feature halo

    @Nullable
    public transient GraphElement element; // GraphElement attached

    @OmitStorage
    public Tuple3<ElementType, String, String> attachedTo = Tuple3.of(ElementType.NONE, null, null); // [ElementType, element id, feature name]

    public Feature() {
        super();
    }

    public Feature(T value) {
        super();
        this.value = value;
    }

    public Feature(T value, boolean halo, short master) {
        super(master);
        this.halo = halo;
        this.value = value;
    }

    public Feature(String name, T value) {
        super();
        this.value = value;
        attachedTo.f2 = name;
    }

    public Feature(String id, T value, boolean halo, short master) {
        super(master);
        this.halo = halo;
        this.value = value;
        attachedTo.f2 = id;
    }

    public Feature(Feature<T, V> f, CopyContext context) {
        super(f, context);
        attachedTo.f0 = f.attachedTo.f0;
        attachedTo.f1 = f.attachedTo.f1;
        attachedTo.f2 = f.attachedTo.f2;
        value = f.value;
        halo = f.halo;
        element = f.element;
    }

    /**
     * Given featureName and attached Element id return the unique id for this feature
     */
    public static String encodeFeatureId(ElementType type, String attachedElementId, String featureName) {
        if (type == ElementType.NONE) return featureName;
        return attachedElementId + DELIMITER + featureName + DELIMITER + type.ordinal();
    }

    /**
     * Given an attached Feature id, decode it returns an array of [Element Type, element Id, Feature Name]
     */
    public static Tuple3<ElementType, String, String> decodeAttachedFeatureId(String attachedFeatureId) {
        String[] val = attachedFeatureId.split(DELIMITER);
        return Tuple3.of(ELEMENT_VALUES[Integer.parseInt(val[2])], val[0], val[1]);
    }

    /**
     * Does this id belong to attached feature or not
     */
    public static boolean isAttached(String featureId) {
        return featureId.contains(DELIMITER);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Feature<T, V> copy(CopyContext context) {
        return new Feature<>(this, context);
    }

    /**
     * Features attached to elements should arrive at corresponding masters first,
     * hence the different in main logic is that master should then create them on replica parts
     * Handles the case for late Vertex Feature, since they arrive at masters first
     * But not for Edge since edge Vertices can be replicated
     */
    @Override
    public void create() {
        if (attachedTo.f0 == ElementType.NONE) super.create();
        else {
            if (!storage.containsElement(attachedTo.f1, attachedTo.f0)) {
                // Parent element not yet here
                storage.delayEvent(attachedTo.f1, attachedTo.f0, storage.layerFunction.getWrapperContext().getElement().getValue());
                return;
            }
            Consumer<BaseStorage> callback = createElement();
            if (callback != null && !isHalo() && isReplicable() && !replicaParts().isEmpty() && (state() == ReplicaState.MASTER)) {
                GraphElement cpy = copy(CopyContext.SYNC); // Make a copy do not actually send this element
                replicaParts().forEach(part_id -> this.storage.layerFunction.message(new GraphOp(Op.COMMIT, part_id, cpy), MessageDirection.ITERATE));
            }
            storage.runCallback(callback);
        }
    }

    /**
     * Update element is different for feature since we also need to update the value stored in the feature
     *
     * @param newElement newElement to update with
     * @return (isUpdated, oldElement)
     */
    @Override
    public Tuple2<Consumer<BaseStorage>, GraphElement> updateElement(GraphElement newElement, GraphElement memento) {
        Feature<T, V> newFeature = (Feature<T, V>) newElement;
        if (!valuesEqual(newFeature.value, this.value)) {
            memento = copy(CopyContext.MEMENTO);
            value = newFeature.value;
        }
        return super.updateElement(newElement, memento);
    }

    /**
     * @return V value of this element
     */
    public V getValue() {
        return (V) this.value;
    }

    /**
     * @return if 2 values are equal, or updated
     */
    public boolean valuesEqual(T v1, T v2) {
        return false;
    }

    public TypeInformation<?> getValueTypeInfo() {
        return Types.GENERIC(Object.class);
    }

    /**
     * If this element is an attached feature, master part is the one of the attached graph element
     *
     * @return master part
     */
    @Override
    public short masterPart() {
        if (Objects.nonNull(getElement())) {
            return getElement().masterPart();
        }
        return super.masterPart();
    }

    @Override
    public boolean isHalo() {
        return halo;
    }

    /**
     * If this element is an attached feature, replica parts are the ones attached to the graph element
     * Otherwise own ones
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
    public boolean isReplicable() {
        if (Objects.nonNull(getElement())) {
            return getElement().isReplicable();
        } else {
            return super.isReplicable();
        }
    }

    /**
     * If this element is an attached feature, id = attachedId + this.id
     *
     * @return id of the feature
     * @implNote this method should be used for storing the elements as well as keying
     */
    @Override
    public String getId() {
        return encodeFeatureId(attachedTo.f0, attachedTo.f1, attachedTo.f2);
    }

    /**
     * Name is the feature name, this is equal id if not attached feature
     *
     * @return name of the feature
     */
    public String getName() {
        return attachedTo.f2;
    }

    /**
     * Set the name(actually id). SetId is made private to not have confusion
     */
    public void setName(String name) {
        attachedTo.f2 = name;
    }

    /**
     * If element is cached here return it, otherwise ask the DB to retrieve the element
     *
     * @return GraphElement
     */
    @Nullable
    public GraphElement getElement() {
        if (attachedTo.f0 == ElementType.NONE) return null;
        if (element == null && storage != null && storage.containsElement(attachedTo.f1, attachedTo.f0)) {
            setElement(storage.getElement(attachedTo.f1, attachedTo.f0));
        }
        return element;
    }

    /**
     * Caches the given element, adds current feature to feature of the element if that does not exist there.
     *
     * @param attachingElement element that want to attach itself to this feature
     * @implNote Make sure to properly track the change of references from Graph Elements
     * @implNote If this element is already attached to some other element, it should be removed from that elements feature list before attaching this to other element
     * @implNote Attaching element cannot contain a Feature with the same id.
     * @implNote Attaching a feature to element is done here, realising is that we want to have rigid link
     * between element.features <--> feature.element.
     */
    public void setElement(GraphElement attachingElement) {
        if (element == attachingElement) return; // Already attached to this element
        if (attachingElement.features != null && attachingElement.features.contains(this))
            throw new IllegalStateException("Already attached to this element");
        attachedTo.f0 = attachingElement.elementType();
        attachedTo.f1 = attachingElement.getId();
        if (attachingElement.features == null) attachingElement.features = new ArrayList<>(3);
        element = attachingElement;
        attachingElement.features.add(this);
    }

    @Override
    public void setFeature(String name, Feature<?, ?> feature) {
        if (attachedTo.f0 != ElementType.NONE)
            throw new IllegalStateException("Using sub-sub Features are not allowed");
        super.setFeature(name, feature);
    }

    @Nullable
    @Override
    public Feature<?, ?> getFeature(String name) {
        if (attachedTo.f0 != ElementType.NONE)
            throw new IllegalStateException("Using sub-sub Features are not allowed");
        return super.getFeature(name);
    }

    @Override
    public ElementType elementType() {
        return attachedTo.f0 == ElementType.NONE ? ElementType.STANDALONE_FEATURE : ElementType.ATTACHED_FEATURE;
    }

}
