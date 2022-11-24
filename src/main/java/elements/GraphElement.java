package elements;

import ai.djl.ndarray.LifeCycleControl;
import elements.enums.CacheFeatureContext;
import elements.enums.CopyContext;
import elements.enums.ElementType;
import elements.enums.ReplicaState;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.jetbrains.annotations.Nullable;
import storage.BaseStorage;
import typeinfo.recursivepojoinfo.DeSerializationListener;
import typeinfo.recursivepojoinfo.RecursivePojoTypeInfoFactory;

import java.io.Serializable;
import java.util.*;
import java.util.function.Consumer;

/**
 * Abstract class representing a GraphElement.
 * <h2>
 *     3 Issues need to be taken into account
 *     <ol>
 *         <li>
 *             Ideally all the sync logic should precede the plugin callback logic
 *             Similar problem exists in {@link Feature} create. What if createInternal fails or changes the Feature
 *         </li>
 *         <li>
 *             No way of {@link Feature} update callback failing, how to deal with such cases?
 *         </li>
 *         <li>
 *             What if element accessed a sub-Feature during RMI copy process
 *         </li>
 *     </ol>
 * </h2>
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
        if (context == CopyContext.SYNC_CACHE_FEATURES || context == CopyContext.SYNC_NOT_CACHE_FEATURES) {
            // Copy all the non-halo features
            if(context == CopyContext.SYNC_CACHE_FEATURES) getStorage().cacheFeatures(element, CacheFeatureContext.NON_HALO);
            if (element.features != null && !element.features.isEmpty()) {
                for (Feature<?, ?> feature : element.features) {
                    if (!feature.isHalo()) {
                        // Don't use setElement here since this.getId() might be null at this point
                        if(features == null) features = new ArrayList<>(4);
                        Feature<?,?> cpyFeature = feature.copy(context);
                        cpyFeature.element = this;
                        features.add(feature.copy(context));
                    }
                }
            }
        }
    }

    /**
     * Helper method for getting the storage object or null if we are not at storage operator right now
     */
    public static BaseStorage getStorage() {
        return BaseStorage.STORAGES.get();
    }

    /**
     * Helper function to chain multiple callbacks
     */
    public static <T> Consumer<T> chain(@Nullable Consumer<T> previousChain, Consumer<T> addCallback){
        if(previousChain == null) return addCallback;
        return previousChain.andThen(addCallback);
    }

    /**
     * Copy this element
     *
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
    protected Consumer<BaseStorage> createInternal() {
        Consumer<BaseStorage> callback = storage -> storage.addElement(this);
        if (features != null) {
            for (Feature<?, ?> feature : features) {
                callback = callback
                        .andThen(ignored -> feature.setElement(this, false))
                        .andThen(feature.createInternal());
            }
            features.clear(); // Will be added incrementally in the callback chain to not have information leakage
        }
        return callback.andThen(storage -> storage.plugins.values().forEach(plugin -> plugin.addElementCallback(this)));
    }

    /**
     * Part of deletion relating to storage
     * <strong>Deleting all features as well</strong>
     */
    protected Consumer<BaseStorage> deleteInternal() {
        throw new NotImplementedException("Delte events are not implemented yet");
    }

    /**
     * Part of update relating to storage and plugin interactions
     * @implNote As the callbacks are called the newElement will gradually transform into a <strong>>memento</strong> holding the old state
     *  <ol>
     *      <ol>
     *          <code>if feature exists </code>
     *          <li>feature.updateInternal(newFeature)</li>
     *          <code>else</code>
     *          <li>newFeature attach here</li>
     *          <li>newFeature.createInternal()</li>
     *      </ol>
     *      <li>update storage & call plugin callbacks</li>
     *  </ol>
     */
    protected Consumer<BaseStorage> updateInternal(GraphElement newElement) {
        Consumer<BaseStorage> callback = null;
        if (newElement.features != null && !newElement.features.isEmpty()) {
            for (Iterator<Feature<?, ?>> iterator = newElement.features.iterator(); iterator.hasNext(); ) {
                Feature<?, ?> newFeature = iterator.next();
                if (containsFeature(newFeature.getName())) {
                    Feature<?, ?> oldFeature = getFeature(newFeature.getName());
                    callback = chain(callback, oldFeature.updateInternal(newFeature));
                } else {
                    iterator.remove();
                    callback = chain(callback,
                            ((Consumer<BaseStorage>) (ignored -> newFeature.setElement(this, false))).andThen(newFeature.createInternal())
                    );
                }
            }
        }
        return chain(callback, storage -> {
                storage.updateElement(this, newElement); // Already a memento object (newElement)
                storage.plugins.values().forEach(plugin -> plugin.updateElementCallback(this, newElement));
            });
    }

    /**
     * Part of creation relating to replication and external communication
     * Delegate to createInternal()
     */
    public Consumer<BaseStorage> create() {
        return createInternal();
    }

    /**
     * Part of deletion relating to replication and external things
     */
    public Consumer<BaseStorage> delete() {
        return deleteInternal();
    }

    /**
     * Part of triggerUpdate relating to replication and external communication
     * Delegate to updateInternal()
     */
    public Consumer<BaseStorage> update(GraphElement newElement) {
        return updateInternal(newElement);
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
    public void syncRequest(GraphElement newElement){
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
     * If this element is HALO
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
    abstract public String getId();

    /**
     * Current Part of this element
     * <strong>To be called in Storage parts only, otherwise {@link NullPointerException}</strong>
     */
    public short getPart() {
        return getStorage().layerFunction.getCurrentPart();
    }

    /**
     * Retrieves {@link Feature} from cache if exists, otherwise from storage.
     * @implNote <strong> Developer should make sure to call this method knowing that the feature exists</strong>
     */
    public Feature<?, ?> getFeature(String name) {
        if (features != null) {
            for (Feature<?, ?> feature : features) {
                if (feature.getName().equals(name)) return feature;
            }
        }
        if (Objects.nonNull(getStorage())) {
            Feature<?, ?> feature = getStorage().getAttachedFeature(getType(), getId(), name, null);
            feature.setElement(this, false);
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
        if (getStorage() != null) {
            return getStorage().containsAttachedFeature(getType(), getId(), name, null);
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
