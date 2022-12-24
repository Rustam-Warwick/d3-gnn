package elements;

import elements.enums.CopyContext;
import elements.enums.ElementType;
import elements.enums.Op;
import elements.enums.ReplicaState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.operators.graph.OutputTags;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Represents a Feature either attached to another element or not
 *
 * @param <T> Type that is actually stored in the Feature Object
 * @param <V> Type that is exposed through getValue method
 */
@SuppressWarnings({"unused", "unchecked"})
public class Feature<T, V> extends ReplicableGraphElement {

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
     * Ids of this Feature [Attach Element Type, Attached Element ID, Feature Name]
     */
    public Tuple3<ElementType, Object, String> id;

    public Feature() {
        super();
        this.id = Tuple3.of(ElementType.NONE, null, null);
    }

    public Feature(String name, T value) {
        super();
        this.value = value;
        this.id = Tuple3.of(ElementType.NONE, null, name);
    }

    public Feature(String name, T value, boolean halo) {
        super();
        this.value = value;
        this.id = Tuple3.of(ElementType.NONE, null, name);
        this.halo = halo;
    }

    public Feature(String name, T value, boolean halo, short master) {
        super(master);
        this.halo = halo;
        this.value = value;
        this.id = Tuple3.of(ElementType.NONE, null, name);
    }

    public Feature(Feature<T, V> feature, CopyContext context) {
        super(feature, context);
        id = feature.id;
        value = feature.value;
        halo = feature.halo;
        element = feature.element;
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
     * <p>
     * Standalone Features behave just like {@link ReplicableGraphElement}
     * For Attached Features 3 cases are possible:
     *      <ol>
     *          <li>
     *              If attached element contains replicas send commit output to them
     *          </li>
     *          <li>
     *              If attached element has only 1 master simply create
     *          </li>
     *          <li>
     *              If attached element doest not exist yet, attempt to create it by first attaching this feature
     *          </li>
     *      </ol>
     * </p>
     */
    @Override
    public void create() {
        if (getType() == ElementType.STANDALONE_FEATURE) super.create();
        else if (!getGraphRuntimeContext().getStorage().containsElement(id.f1, id.f0)) {
            GraphElement el = getGraphRuntimeContext().getStorage().getDummyElement(id.f1, id.f0);
            setElement(el, false);
            el.create();
        } else {
            if (!isHalo() && isReplicable() && !getReplicaParts().isEmpty() && (state() == ReplicaState.MASTER)) {
                GraphOp message = new GraphOp(Op.UPDATE, copy(CopyContext.SYNC));
                getGraphRuntimeContext().broadcast(message, OutputTags.ITERATE_OUTPUT_TAG, getReplicaParts());
            }
            createInternal();
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * If there is an actual update on the values swap the values of newElement and this
     * So <code>this</code> will hold the updated value
     * </p>
     */
    @Override
    public void updateInternal(GraphElement newElement) {
        Feature<T, V> newFeature = (Feature<T, V>) newElement;
        if (!valuesEqual(newFeature.value, this.value)) {
            T tmp = newFeature.value;
            newFeature.value = value;
            value = tmp;
            super.updateInternal(newElement);
        }
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
    public Tuple3<ElementType, Object, String> getId() {
        return id;
    }

    /**
     * Name of the feature
     */
    public String getName() {
        return id.f2;
    }

    /**
     * Get id of attached {@link GraphElement}
     */
    public Object getAttachedElementId() {
        return id.f1;
    }

    /**
     * Get {@link ElementType} of the attached {@link GraphElement}
     */
    @Nullable
    public ElementType getAttachedElementType() {
        return id.f0;
    }

    /**
     * If element is cached here return it, otherwise ask the DB to retrieve the element
     */
    @Nullable
    public GraphElement getElement() {
        if (id.f0 == ElementType.NONE) return null;
        if (element == null && getGraphRuntimeContext() != null) {
            setElement(getGraphRuntimeContext().getStorage().getElement(id.f1, id.f0), true);
        }
        return element;
    }

    /**
     * Caches the given element, also adds this {@link Feature} to {@link GraphElement}
     *
     * @param testIfExistsInElement If we should check for existence of duplicated {@link Feature} in {@link GraphElement}
     *                              <p> If test is true, this Feature will not be added to GraphElement if there is a feature with that name already </p>
     */
    public void setElement(GraphElement attachingElement, boolean testIfExistsInElement) {
        element = attachingElement;
        id.f0 = attachingElement.getType();
        id.f1 = attachingElement.getId();
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
    public String toString() {
        return getType() + "{" +
                "id='" + getId() + '\'' +
                "master='" + getMasterPart() + '\'' +
                "value='" + value + '\'' +
                '}';
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ElementType getType() {
        return id.f0 == ElementType.NONE ? ElementType.STANDALONE_FEATURE : ElementType.ATTACHED_FEATURE;
    }

}
