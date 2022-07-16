package ai.djl.pytorch.engine;

import ai.djl.Device;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.ndarray.NDResource;

import java.lang.ref.WeakReference;
import java.util.HashMap;

/**
 * A special Singleton NDManager that is direct child of the SystemNDManager.
 * It is not synchronized since access to it is by default single threaded from flink side
 * Has Tracker ability for nested NDArray creations, all other ndarrays are not kept in the reference queue
 * Internally one can create a child of this NDManager if DJL normal NDManager is needed
 * Try to avoid NDManager if it is not going to be closed again
 */
public class LifeCycleNDManager extends PtNDManager {
    private static final transient ThreadLocal<LifeCycleNDManager> instances = ThreadLocal.withInitial(() ->
            new LifeCycleNDManager(PtNDManager.getSystemManager(), PtNDManager.getSystemManager().getDevice())
    );
    private final transient HashMap<String, WeakReference<NDArray>> registrations = new HashMap<>(4000); // Thread Local

    private final transient Scope scope = new Scope();

    private LifeCycleNDManager(NDManager parent, Device device) {
        super(parent, device);
    }

    /**
     * Get NDManager for this Thread
     */
    public static LifeCycleNDManager getInstance() {
        return instances.get();
    }

    public Scope getScope() {
        return scope;
    }

    @Override
    public void attachInternal(String resourceId, AutoCloseable resource) {
//        registrations.putIfAbsent(resourceId, new WeakReference<>((NDArray) resource));
    }

    @Override
    public void tempAttachInternal(NDManager originalManager, String resourceId, NDResource resource) {
        // Pass
    }


    @Override
    public void detachInternal(String resourceId) {
        registrations.remove(resourceId);
    }

    @Override
    public void close() {
        // Not closing explicitely
    }

    /**
     * Cleans the registrations
     */
    public void clean() {
//        if (registrations.size() > 10) {
//            registrations.values().removeIf(val -> {
//                NDArray tmp = val.get();
//                if (tmp == null) return true;
//                if (tmp.getTaskPossession() == 0) {
//                    tmp.close();
//                    return true;
//                }
//                return false;
//            });
//        }
    }

    /**
     * Context for doing ND operations so that input elements will be returned to their original managers after closing
     * Everything extra will be attached to this LifeCycleNDManager
     */
    public class Scope implements AutoCloseable {
        private final transient NDManager[] originalManagers = new NDManager[10];
        private transient NDList[] inputs;

        public Scope start(NDList... inputs) {
            this.inputs = inputs;
            int k = 0;
            for (int i = 0; i < inputs.length; i++) {
                for (int j = 0; j < inputs[i].size(); j++) {
                    originalManagers[k++] = inputs[i].get(j).getManager();
                    inputs[i].get(j).attach(LifeCycleNDManager.this);
                }
            }
            return this;
        }

        @Override
        public void close() throws Exception {
            int k = 0;
            for (int i = 0; i < inputs.length; i++) {
                for (int j = 0; j < inputs[i].size(); j++) {
                    inputs[i].get(j).attach(originalManagers[k++]);
                }
            }
        }
    }

}