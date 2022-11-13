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

    public T value;

    public boolean halo = false;

    @Nullable
    public transient GraphElement element;

    @OmitStorage
    public Tuple3<ElementType, String, String> attachedTo; // [ElementType, element id, feature name]

    public Feature() {
        super();
        attachedTo = Tuple3.of(ElementType.NONE, null, null);
    }

    public Feature(T value) {
        super();
        this.value = value;
        attachedTo = Tuple3.of(ElementType.NONE, null, null);
    }

    public Feature(T value, boolean halo, short master) {
        super(master);
        this.halo = halo;
        this.value = value;
        attachedTo = Tuple3.of(ElementType.NONE, null, null);
    }

    public Feature(String name, T value) {
        super();
        this.value = value;
        attachedTo = Tuple3.of(ElementType.NONE, null, name);
    }

    public Feature(String name, T value, boolean halo, short master) {
        super(master);
        this.halo = halo;
        this.value = value;
        attachedTo = Tuple3.of(ElementType.NONE, null, name);
    }

    public Feature(Feature<T, V> f, CopyContext context) {
        super(f, context);
        attachedTo = f.attachedTo;
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
     * {@inheritDoc}
     * STANDALONE -> Regular Replicable Element
     * ATTACHED -> If parent not here delay. Otherwise, create and copy to replicas if not halo and master
     */
    @Override
    public void create() {
        if (elementType() == ElementType.STANDALONE_FEATURE) super.create();
        else {
            if (!storage.containsElement(attachedTo.f1, attachedTo.f0)) {
                // Parent element not yet here
                storage.createLateElement(attachedTo.f1, attachedTo.f0);
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
     * {@inheritDoc}
     * If values are not-equal update the value and continue with {@link GraphElement} updateElement
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

    /**
     * Helper TypeInfo for the storage layers
     */
    public TypeInformation<?> getValueTypeInfo() {
        return Types.GENERIC(Object.class);
    }

    /**
     * STANDALONE -> Regular
     * ATTACHED -> Parent
     */
    @Override
    public short masterPart() {
        if (Objects.nonNull(getElement())) {
            return getElement().masterPart();
        }
        return super.masterPart();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isHalo() {
        return halo;
    }

    /**
     * STANDALONE -> Regular
     * ATTACHED -> Parent
     */
    @Override
    public List<Short> replicaParts() {
        if (Objects.nonNull(getElement())) {
            return getElement().replicaParts();
        }
        return super.replicaParts();
    }

    /**
     * {@inheritDoc}
     * STANDALONE -> Regular
     * ATTACHED -> Parent
     *
     * @return
     */
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
     * @return name of the feature
     */
    public String getName() {
        return attachedTo.f2;
    }

    /**
     * Set the name of this feature
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
     * If this element exists in attachingElement throw {@link IllegalStateException}
     */
    public void setElement(GraphElement attachingElement) {
        if (element == attachingElement) return; // Already attached to this element
        attachedTo.f0 = attachingElement.elementType();
        attachedTo.f1 = attachingElement.getId();
        if (attachingElement.features == null) attachingElement.features = new ArrayList<>(3);
        element = attachingElement;
        attachingElement.features.add(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ElementType elementType() {
        return attachedTo.f0 == ElementType.NONE ? ElementType.STANDALONE_FEATURE : ElementType.ATTACHED_FEATURE;
    }

}
