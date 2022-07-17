package ai.djl.pytorch.engine;

import ai.djl.Device;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.ndarray.NDResource;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.concurrent.ConcurrentHashMap;

/**
 * A special Singleton NDManager that is direct child of the SystemNDManager.
 * It is not synchronized since access to it is by default single threaded from flink side
 * Has Tracker ability for nested NDArray creations, all other ndarrays are not kept in the reference queue
 * Internally one can create a child of this NDManager if DJL normal NDManager is needed
 * Try to avoid NDManager if it is not going to be closed again
 */
public class LifeCycleNDManager extends PtNDManager {
    /**
     *
     */
    private static transient LifeCycleNDManager INSTANCE;
    /**
     * Thread with MIN_Priority to clean the NDArrays attached to this Manager after some time of their creation
     */
    private transient final Thread cleanerThread;
    /**
     * NDArray Registrations with a timestamp
     */
    private final transient ConcurrentHashMap<String, Tuple2<NDArray,Long>> registrations = new ConcurrentHashMap<>();
    /**
     * NDArray Delayed list with counter, when counter is zero move back to the registrations if not detached
     */
    private final transient ConcurrentHashMap<String, Tuple2<NDArray, Integer>> delayedList = new ConcurrentHashMap<>();

    private LifeCycleNDManager(NDManager parent, Device device) {
        super(parent, device);
        this.resources = null;
        this.tempResources = null;
        cleanerThread = new Thread(this::clean);
        cleanerThread.setPriority(Thread.MIN_PRIORITY);
        cleanerThread.start();
    }

    /**
     * Get NDManager for this Thread
     */
    public static LifeCycleNDManager getInstance() {
        if(INSTANCE == null){
            INSTANCE = new LifeCycleNDManager(PtNDManager.getSystemManager(), PtNDManager.getSystemManager().defaultDevice());
        }
        return INSTANCE;
    }

    public Scope getScope() {
        return new Scope();
    }



    public void postpone(NDArray resource){
        delayedList.compute(resource.getUid(), (tmpId, tmpRes)->{
           if(tmpRes == null){
               // Never seen before
               registrations.remove(resource.getUid()); // Remove if exists here
               tmpRes = Tuple2.of(resource, 1);
           }else{
               tmpRes.f1++;
           }
           return tmpRes;
        });
    }

    public void prepone(NDArray resource){
        delayedList.compute(resource.getUid(), (tmpId, tmpRes)->{
            if(tmpRes == null) return tmpRes;
            tmpRes.f1--;
            if(tmpRes.f1 == 0){
                if(resource.getManager() == LifeCycleNDManager.this){
                    registrations.put(resource.getUid(), Tuple2.of(resource, System.currentTimeMillis()));
                }
                return null;
            }
            return tmpRes;
        });
    }

    @Override
    public void attachInternal(String resourceId, AutoCloseable resource) {
        if(!delayedList.containsKey(resourceId)){
            // Synchrnoizing post-ponements + No need to add if it is delayed
            registrations.put(resourceId, Tuple2.of((NDArray) resource, System.currentTimeMillis()));
        }
    }

    @Override
    public void tempAttachInternal(NDManager originalManager, String resourceId, NDResource resource) {
        // Pass
    }

    @Override
    public void detachInternal(String resourceId) {
        if(!delayedList.containsKey(resourceId)){
            // Synchronizing Post-ponement stage + if delayed list contains registrations should be empty
            registrations.remove(resourceId);
        }
    }

    @Override
    public void close() {
        // Not closing explicitely

    }

    /**
     * Cleans the registrations
     */
    public void clean(){
        boolean notInterrupted = true;
        while(notInterrupted){
            final long offset = System.currentTimeMillis() - 10000;
            registrations.forEach((key,item)->{
                if (item.f1 < offset){
                    item.f0.close();
                    registrations.remove(key);
                }
            });
            try{
                Thread.sleep(3000);
            }catch (InterruptedException e){
                notInterrupted = false;
            }
        }

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

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        cleanerThread.interrupt();
        for (Tuple2<NDArray, Long> closeable : registrations.values()) {
            closeable.f0.close();
        }
        registrations.clear();
        for (Tuple2<NDArray, Integer> value : delayedList.values()) {
            value.f0.close();
        }
        delayedList.clear();
    }
}