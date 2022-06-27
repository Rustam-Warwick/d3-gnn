package ai.djl.pytorch.engine;

import ai.djl.Device;
import ai.djl.ndarray.NDManager;
import ai.djl.ndarray.NDResource;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A special Singleton NDManager that is direct child of the SystemNDManager.
 * It is not synchronized since access to it is by default single threaded from flink side
 * Has Tracker ability for nested NDArray creations, all other ndarrays are not kept in the reference queue
 * Internally one can create a child of this NDManager if DJL normal NDManager is needed
 * Try to avoid NDManager if it is not going to be closed again
 */
public class LifeCycleNDManager  extends PtNDManager {
    public static final AtomicInteger counter = new AtomicInteger(0);
    private static transient LifeCycleNDManager object;
    private static final ThreadLocal<Tracker> TRACKERS = ThreadLocal.withInitial(Tracker::new);
    public LifeCycleNDManager(NDManager parent, Device device) {
        super(parent, device);
    }

    public static LifeCycleNDManager getInstance() {
        if (object == null) object = new LifeCycleNDManager(PtNDManager.getSystemManager(), PtNDManager.getSystemManager().getDevice());
        return object;
    }

    @Override
    public void attachInternal(String resourceId, AutoCloseable resource) {
        int val = counter.incrementAndGet();
        if(val % 10000 == 0) System.out.println(val);
        Tracker tmp = TRACKERS.get();
        if(tmp.isOpen()){
            tmp.attach(resourceId, resource);
        }
    }

    public void attachToTracker(String resourceId, AutoCloseable resource){
        TRACKERS.get().attach(resourceId, resource);
    }

    @Override
    public void tempAttachInternal(NDManager originalManager, String resourceId, NDResource resource) {
        // Pass
    }


    public Tracker startTracker(){
        Tracker tmp = TRACKERS.get();
        tmp.setOpen(true);
        return tmp;
    }

    @Override
    public void detachInternal(String resourceId) {
        Tracker tmp = TRACKERS.get();
        if(tmp.isOpen()){
            tmp.detach(resourceId);
        }
    }

    @Override
    public void close() {
        // Not closing explicitely
    }

    public static class Tracker implements AutoCloseable{
        protected Tracker(){

        }

        private final HashMap<String, AutoCloseable> trackedEntities = new HashMap<String, AutoCloseable>(101);

        private boolean open = false;

        public boolean isOpen() {
            return open;
        }

        public void setOpen(boolean open) {
            this.open = open;
        }

        public void attach(String resourceId, AutoCloseable resource){
            trackedEntities.put(resourceId, resource);
        }

        public void detach(String resourceId){
            trackedEntities.remove(resourceId);
        }

        @Override
        public void close() throws Exception {
            if(trackedEntities.size() > 100) {
                for (AutoCloseable autoCloseable : trackedEntities.values()) {
                    autoCloseable.close();
                }
                trackedEntities.clear();
            }
            open = false;
        }
    }

}