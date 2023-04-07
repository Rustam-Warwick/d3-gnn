package elements;

import ai.djl.ndarray.LifeCycleControl;
import elements.enums.CopyContext;
import elements.enums.ElementType;
import elements.enums.ReplicaState;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.streaming.api.operators.graph.interfaces.GraphRuntimeContext;
import org.jetbrains.annotations.Nullable;
import storage.GraphStorage;
import typeinfo.recursivepojoinfo.DeSerializationListener;
import typeinfo.recursivepojoinfo.RecursivePojoTypeInfoFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Abstract class representing all the elements in the graph.
 * <p>
 * All RUD operation on GraphElement should be performed by first fetching the element from storage
 * Since some {@link GraphStorage} implementations might store everything in memory we need to make sure all attached elements are in sync
 * </p>
 * @todo Check on this issue, it works for now because we don't have nested-features & Feature.valueEquals() is not properly used
 */
@TypeInfo(RecursivePojoTypeInfoFactory.class)
public abstract class GraphElement implements Serializable, LifeCycleControl, DeSerializationListener {

    /**
     * Features attached to this GraphElement
     * <strong>
     * There is not always a 1-1 relation between them.
     * Many same Features can point to this element
     * While this element always has the reference to the first one obtained
     * </strong>
     */
    @Nullable
    public List<Feature<?, ?>> features;

    public GraphElement() {

    }

    public GraphElement(GraphElement element, CopyContext context) {
        if (context == CopyContext.SYNC) {
            // Copy all the non-halo features
            if (element.features != null) {
                for (Feature<?, ?> feature : element.features) {
                    if (!feature.isHalo()) {
                        // Don't use setElement here since this.getId() might be null at this point
                        if (features == null) features = new ArrayList<>(4);
                        Feature<?, ?> cpyFeature = feature.copy(context);
                        cpyFeature.element = this;
                        features.add(cpyFeature);
                    }
                }
            }
        }
    }

    /**
     * Helper method for getting {@link GraphRuntimeContext} object from {@link ThreadLocal}
     */
    public static GraphRuntimeContext getGraphRuntimeContext() {
        return GraphRuntimeContext.CONTEXT_THREAD_LOCAL.get();
    }

    /**
     * Copy this element
     * @param context the context for copying.
     */
    abstract public GraphElement copy(CopyContext context);

    /**
     * Part of creation relating to storage and plugin callbacks
     * @implNote features will be cleared after exiting this method
     * <ol>
     *     <li>Create in Storage</li>
     *     <ol>
     *         <li>Attach Feature</li>
     *         <li>feature.createInternal()</li>
     *     </ol>
     *     <li>Plugin Callback</li>
     * </ol>
     */
    public void createInternal() {
        List<Feature<?, ?>> copyFeatures = null;
        if (features != null && !features.isEmpty()) {
            copyFeatures = features;
            features = new ArrayList<>(copyFeatures.size());
        }
        getGraphRuntimeContext().getStorage().addElement(this);
        getGraphRuntimeContext().addElementCallback(this);
        if (copyFeatures != null && !copyFeatures.isEmpty()) {
            for (Feature<?, ?> feature : copyFeatures) {
                feature.setElement(this, false);
                feature.createInternal();
            }
        }
    }

    /**
     * Part of deletion relating to storage
     * <strong>Deleting all features as well</strong>
     */
    public void deleteInternal() {
        throw new NotImplementedException("Delete events are not implemented yet");
    }

    /**
     * Part of update relating to storage and plugin interactions
     *
     * @implNote As the callbacks are called the newElement will gradually transform into a <strong>>memento</strong> holding the old state
     * <ol>
     *     <ol>
     *         <code>if feature exists </code>
     *         <li>feature.updateInternal(newFeature)</li>
     *         <code>else</code>
     *         <li>newFeature attach here</li>
     *         <li>newFeature.createInternal()</li>
     *     </ol>
     *     <li>update storage & call plugin callbacks</li>
     * </ol>
     */
    public void updateInternal(GraphElement newElement) {
        if (newElement.features != null && !newElement.features.isEmpty()) {
            for (Feature<?, ?> newFeature : newElement.features) {
                if (containsFeature(newFeature.getName())) {
                    Feature<?, ?> oldFeature = getFeature(newFeature.getName());
                    oldFeature.updateInternal(newFeature);
                } else {
                    newFeature.setElement(this, false);
                    newFeature.createInternal();
                }
            }
        }
        getGraphRuntimeContext().getStorage().updateElement(this, newElement);
        getGraphRuntimeContext().updateElementCallback(this, newElement);
    }

    /**
     * Part of creation relating to replication and external communication
     * Delegate to createInternal()
     */
    public void create() {
        createInternal();
    }

    /**
     * Part of deletion relating to replication and external things
     */
    @SuppressWarnings("unused")
    public void delete() {
        deleteInternal();
    }

    /**
     * Part of triggerUpdate relating to replication and external communication
     * Delegate to updateInternal()
     */
    public void update(GraphElement newElement) {
        updateInternal(newElement);
    }

    /**
     * Master -> Replica sync for {@link GraphElement}
     */
    public void sync(GraphElement newElement) {
        throw new NotImplementedException("Replica Elements should override this method");
    }

    /**
     * Replica -> Master sync request {@link SyncRequest}
     */
    public void syncRequest(GraphElement newElement) {
        throw new NotImplementedException("Replica Elements should override this method");
    }

    /**
     * Type of this element
     */
    public ElementType getType() {
        return ElementType.NONE;
    }

    /**
     * Is this element replicable
     */
    public boolean isReplicable() {
        return false;
    }

    /**
     * Master part of this element
     */
    public short getMasterPart() {
        return getPart();
    }

    /**
     * If this element is of halo type, meaning never replicated
     */
    public boolean isHalo() {
        return false;
    }

    /**
     * State of this element (MASTER, REPLICA, UNDEFINED)
     */
    public ReplicaState state() {
        if (getPart() == -1) return ReplicaState.UNDEFINED;
        if (getPart() == getMasterPart()) return ReplicaState.MASTER;
        return ReplicaState.REPLICA;
    }

    /**
     * List of replica parts
     */
    public List<Short> getReplicaParts() {
        return Collections.emptyList();
    }

    /**
     * ID of GraphElement
     */
    abstract public Object getId();

    /**
     * Current Part of this element
     * <strong>To be called in {@link GraphStorage} parts only, otherwise {@link NullPointerException}</strong>
     */
    public short getPart() {
        return getGraphRuntimeContext().getCurrentPart();
    }

    /**
     * Retrieves {@link Feature} from cache if exists, otherwise from storage.
     *
     * @implNote If {@link Feature} does not exist throws {@link NullPointerException}
     * @implNote <strong> Developer should make sure to call this method knowing that the feature exists</strong>
     */
    @SuppressWarnings("ConstantConditions")
    public Feature<?, ?> getFeature(String name) {
        if (features != null) {
            for (Feature<?, ?> feature : features) {
                if (feature.getName().equals(name)) return feature;
            }
        }
        return null;
    }

    /**
     * Returns if {@link Feature} with this name is available either here or in storage
     */
    public Boolean containsFeature(String name) {
        if (features != null) {
            for (Feature<?, ?> feature : features) {
                if (feature.getName().equals(name)) return true;
            }
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delay() {
        if (features != null) features.forEach(LifeCycleControl::delay);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void resume() {
        if (features != null) features.forEach(LifeCycleControl::resume);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void destroy() {
        if (features != null) features.forEach(LifeCycleControl::destroy);
    }

    /**
     * {@inheritDoc}
     * Attach all feature back to this element. Needed since they are <code>transient</code>
     */
    @Override
    public void onDeserialized() {
        if (features != null) {
            features.forEach(feature -> {
                feature.element = this;
                feature.onDeserialized();
            });
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return getType() + "{" +
                "id='" + getId() + '\'' +
                '}';
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GraphElement that = (GraphElement) o;
        return Objects.equals(getId(), that.getId()) && Objects.equals(getType(), that.getType());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hash(getId(), getType());
    }

}
