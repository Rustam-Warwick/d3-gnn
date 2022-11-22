package elements;

import elements.enums.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.jetbrains.annotations.Nullable;
import storage.BaseStorage;

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

    /**
     * Delimiter used to create id for attached features
     */
    public static String DELIMITER = "/";

    /**
     * List of Element types
     */
    public static ElementType[] ELEMENT_VALUES = ElementType.values();

    /**
     * Actual value stored in this Feature object
     */
    public T value;

    /**
     * If this Feature is halo
     */
    public boolean halo = false;

    /**
     * Attached {@link  GraphElement} if it exists
     */
    @Nullable
    public transient GraphElement element;

    /**
     * Ids of this Feature [Attach Element Type, Attached Element Id, Feature Name]
     */
    public Tuple3<ElementType, String, String> ids;

    public Feature() {
        super();
        ids = Tuple3.of(ElementType.NONE, null, null);
    }

    public Feature(String name, T value) {
        super();
        this.value = value;
        ids = Tuple3.of(ElementType.NONE, null, name);
    }

    public Feature(String name, T value, boolean halo){
        super();
        this.value = value;
        ids = Tuple3.of(ElementType.NONE, null, name);
        this.halo = halo;
    }

    public Feature(String name, T value, boolean halo, short master) {
        super(master);
        this.halo = halo;
        this.value = value;
        ids = Tuple3.of(ElementType.NONE, null, name);
    }

    public Feature(Feature<T, V> feature, CopyContext context) {
        super(feature, context);
        ids = feature.ids;
        value = feature.value;
        halo = feature.halo;
        element = feature.element;
    }

    /**
     * Given featureName and attached Element id return the unique id for this feature
     */
    public static String encodeFeatureId(ElementType type, @Nullable String attachedElementId, String featureName) {
        if (type == ElementType.NONE) return featureName;
        return attachedElementId + DELIMITER + featureName + DELIMITER + type.ordinal();
    }

    /**
     * Given an attached Feature id, decode it returns an array of [Element Type, element_id , Feature Name]
     */
    public static Tuple3<ElementType, String, String> decodeAttachedFeatureId(String attachedFeatureId) {
        String[] val = attachedFeatureId.split(DELIMITER);
        return Tuple3.of(ELEMENT_VALUES[Integer.parseInt(val[2])], val[0], val[1]);
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
     * STANDALONE -> Regular {@link ReplicableGraphElement}
     * ATTACHED -> COMMIT in parent {@link GraphElement} replicas after creation
     */
    @Override
    public Consumer<BaseStorage> create() {
        if (getType() == ElementType.STANDALONE_FEATURE) return super.create();
        else {
            if (!getStorage().containsElement(ids.f1, ids.f0)) {
                setElement(getStorage().createLateElement(ids.f1, ids.f0),false);
            }
            Consumer<BaseStorage> callback = createInternal();
            if (callback != null && !isHalo() && isReplicable() && !getReplicaParts().isEmpty() && (state() == ReplicaState.MASTER)) {
                GraphElement cpy = copy(CopyContext.SYNC); // Make a copy do not actually send this element
                getReplicaParts().forEach(part_id -> getStorage().layerFunction.message(new GraphOp(Op.COMMIT, part_id, cpy), MessageDirection.ITERATE));
            }
            return callback;
        }
    }

    /**
     * {@inheritDoc}
     * If values are not-equal triggerUpdate the value and continue with {@link GraphElement} updateInternal
     */
    @Override
    public Tuple2<Consumer<BaseStorage>, GraphElement> updateInternal(GraphElement newElement, GraphElement memento) {
        Feature<T, V> newFeature = (Feature<T, V>) newElement;
        if (!valuesEqual(newFeature.value, this.value)) {
            memento = copy(CopyContext.MEMENTO);
            value = newFeature.value;
        }
        return super.updateInternal(newElement, memento);
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
     * {@inheritDoc}
     * Delegate to attached element if attached
     */
    @Override
    public short getMasterPart() {
        if (Objects.nonNull(getElement())) {
            return getElement().getMasterPart();
        }
        return super.getMasterPart();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isHalo() {
        return halo;
    }

    /**
     * {@inheritDoc}
     * Delegate to attached element if attached
     */
    @Override
    public List<Short> getReplicaParts() {
        if (Objects.nonNull(getElement())) {
            return getElement().getReplicaParts();
        }
        return super.getReplicaParts();
    }

    /**
     * {@inheritDoc}
     * Delegate to attached element if attached
     */
    @Override
    public boolean isReplicable() {
        if (Objects.nonNull(getElement())) {
            return getElement().isReplicable();
        }
        return super.isReplicable();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getId() {
        return encodeFeatureId(ids.f0, ids.f1, ids.f2);
    }

    /**
     * Name of the feature
     */
    public String getName() {
        return ids.f2;
    }

    /**
     * Get id of attached {@link GraphElement}
     */
    public String getAttachedElementId() {
        return ids.f1;
    }

    /**
     * Get {@link ElementType} of the attached {@link GraphElement}
     */
    @Nullable
    public ElementType getAttachedElementType() {
        return ids.f0;
    }

    /**
     * If element is cached here return it, otherwise ask the DB to retrieve the element
     */
    @Nullable
    public GraphElement getElement() {
        if (ids.f0 == ElementType.NONE) return null;
        if (element == null && getStorage() != null) {
            setElement(getStorage().getElement(ids.f1, ids.f0), true);
        }
        return element;
    }

    /**
     * Caches the given element, also adds this {@link Feature} to {@link GraphElement}
     * @param testIfExistsInElement If we should check for existance of duplicated {@link Feature} in {@link GraphElement}
     */
    public void setElement(GraphElement attachingElement, boolean testIfExistsInElement) {
        element = attachingElement;
        ids.f0 = attachingElement.getType();
        ids.f1 = attachingElement.getId();
        if (attachingElement.features == null) attachingElement.features = new ArrayList<>(4);
        if (testIfExistsInElement) {
            for (Feature<?, ?> feature : attachingElement.features) {
                if (feature.getName().equals(getName())) return;
            }
        }
        attachingElement.features.add(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ElementType getType() {
        return ids.f0 == ElementType.NONE ? ElementType.STANDALONE_FEATURE : ElementType.ATTACHED_FEATURE;
    }

}
