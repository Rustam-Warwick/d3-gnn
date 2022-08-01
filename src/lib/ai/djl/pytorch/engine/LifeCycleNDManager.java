package ai.djl.pytorch.engine;
//
//
//import ai.djl.Device;
//import ai.djl.ndarray.NDArray;
//import ai.djl.ndarray.NDList;
//import ai.djl.ndarray.NDManager;
//import ai.djl.ndarray.NDResource;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.cliffc.high_scale_lib.NonBlockingHashMap;
//import org.cliffc.high_scale_lib.NonBlockingHashMapLong;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.Collections;
//import java.util.Iterator;
//import java.util.LinkedHashMap;
//import java.util.Map;
//
///**
// * A special Singleton NDManager that is direct child of the SystemNDManager.
// * It is not synchronized since access to it is by default single threaded from flink side
// * Has Tracker ability for nested NDArray creations, all other ndarrays are not kept in the reference queue
// * Internally one can create a child of this NDManager if DJL normal NDManager is needed
// * Try to avoid NDManager if it is not going to be closed again
// */
//public class LifeCycleNDManager extends PtNDManager {
//
//    /**
//     * Logged
//     */
//    public static final Logger LOG = LoggerFactory.getLogger(NDManager.class);
//    /**
//     * HashMap for the Thread Pool
//     */
//    private static final transient NonBlockingHashMapLong<Tuple2<Thread, LifeCycleNDManager>> THREADS = new NonBlockingHashMapLong<>();
//
//    /**
//     * Finalizer Thread
//     */
//    static {
//        Thread cleanerThread = new Thread(LifeCycleNDManager::clean);
//        cleanerThread.setPriority(Thread.NORM_PRIORITY);
//        cleanerThread.start();
//    }
//
//    /**
//     * Scopes
//     */
//    protected final transient Scope parentScope = new Scope();
//
//    protected final transient NonBlockingHashMap<String, Tuple2<AutoCloseable, Integer>> detached = new NonBlockingHashMap<>(1 << 6);
//
//    protected final transient Map<String, Tuple2<AutoCloseable, Long>> attached = Collections.synchronizedMap(new LinkedHashMap<>(1 << 3));
//
//    protected long counter = Long.MIN_VALUE + 10000;
//
//    private LifeCycleNDManager(NDManager parent, Device device) {
//        super(parent, device);
//        this.resources = null;
//        this.tempResources = null;
//
//    }
//
//    /**
//     * Get NDManager for this Thread
//     */
//    public static LifeCycleNDManager getInstance() {
//        THREADS.computeIfAbsent(Thread.currentThread().getId(), (a) -> Tuple2.of(Thread.currentThread(), new LifeCycleNDManager(PtNDManager.getSystemManager(), PtNDManager.getSystemManager().defaultDevice())));
//        return THREADS.get(Thread.currentThread().getId()).f1;
//    }
//
//    /**
//     * Cleans the registrations
//     */
//    public static void clean() {
//        boolean notInterrupted = true;
//        while (notInterrupted) {
//            // Cleanup closed threads
//            int count = 0;
//            for (Iterator<Tuple2<Thread, LifeCycleNDManager>> threadLocal = THREADS.values().iterator(); threadLocal.hasNext(); ) {
//                Tuple2<Thread, LifeCycleNDManager> val = threadLocal.next();
//                if (!val.f0.isAlive()) {
//                    // Clean the data structure, thread is no longer needed
//                    try {
//                        for (Tuple2<AutoCloseable, Long> value : val.f1.attached.values()) {
//                            value.f0.close();
//                        }
//                        for (Tuple2<AutoCloseable, Integer> value : val.f1.detached.values()) {
//                            value.f0.close();
//                        }
//                        val.f1.attached.clear();
//                        val.f1.detached.clear();
//                        threadLocal.remove();
//                        for (int i = 0; i < 5; i++) {
//                            System.gc();
//                        }
//                        LOG.info(String.format("All Tensors closed +gc run in Thread: %s", val.f0));
//                    } catch (Exception ignored) {
//                        LOG.error("Exception in trying to close all Tensors");
//                    }
//                } else {
//                    final long intervalStart = val.f1.counter - 2000;
//                    synchronized (val.f1.attached) {
//                        for (Iterator<Tuple2<AutoCloseable, Long>> enumerateValues = val.f1.attached.values().iterator(); enumerateValues.hasNext(); ) {
//                            try {
//                                Tuple2<AutoCloseable, Long> tmp = enumerateValues.next();
//                                if (tmp.f1 >= intervalStart) break;
//                                if (!val.f1.detached.containsKey(((NDArray) tmp.f0).getUid())) {
//                                    tmp.f0.close();
//                                    count++;
//                                    enumerateValues.remove();
//                                }
//                            } catch (Exception ignored) {
//                                LOG.error(ignored.getMessage());
//                            }
//                        }
//                    }
//                }
//            }
//
//            LOG.error("Closed " + count + " tensors");
//            try {
//                Thread.sleep(2000);
//            } catch (InterruptedException e) {
//                LOG.info("Interrupted");
//                notInterrupted = false;
//            }
//        }
//
//    }
//
//    public Scope getScope() {
//        return parentScope;
//    }
//
//    public void postpone(NDArray resource) {
//        detached.compute(resource.getUid(), (key, val) -> {
//            if (val == null) {
//                attached.remove(key);
//                return Tuple2.of(resource, 1);
//            }
//            val.f1++;
//            return val;
//        });
//    }
//
//    public void prepone(NDArray resource) {
//        detached.compute(resource.getUid(), (key, val) -> {
//            if (val == null) return val;
//            if (--val.f1 == 0) {
//                // no longer detached
//                attached.put(key, Tuple2.of(resource, counter)); // Do not increment here!
//                return null;
//            }
//            return val;
//        });
//    }
//
//    @Override
//    public void attachInternal(String resourceId, AutoCloseable resource) {
//        attached.put(resourceId, Tuple2.of(resource, ++counter));
//    }
//
//    @Override
//    public void tempAttachInternal(NDManager originalManager, String resourceId, NDResource resource) {
//        // Pass
//        LOG.error("Trying to temp attach this tensor, should not happen, can cause memory leaks");
//    }
//
//    @Override
//    public void detachInternal(String resourceId) {
//        // No detaching for this NDArray, will be cleaned by the cleaner thread
//        LOG.error("Trying to detach this tensor, not perferred since finalizer has to be created");
//        attached.remove(resourceId);
//        detached.remove(resourceId);
//    }
//
//    @Override
//    public void close() {
//        // No closing logic, handled by the Thread
//    }
//
//    /**
//     * Context for doing ND operations so that input elements will be returned to their original managers after closing
//     * Everything extra will be attached to this LifeCycleNDManager
//     */
//    public class Scope implements AutoCloseable {
//        private final transient NDManager[] originalManagers = new NDManager[10];
//        private transient NDList[] inputs;
//        private transient Scope childScope = null;
//
//        public Scope start(NDList... inputs) {
//            if (this.inputs != null) {
//                if (childScope == null) childScope = new Scope();
//                return childScope.start(inputs); // Nesting of scopes should be supported
//            }
//            this.inputs = inputs;
//            int k = 0;
//            for (int i = 0; i < inputs.length; i++) {
//                for (int j = 0; j < inputs[i].size(); j++) {
//                    originalManagers[k++] = inputs[i].get(j).getManager();
//                    inputs[i].get(j).tempAttach(LifeCycleNDManager.this); // Temp attach not interacting with the maps
//                }
//            }
//            return this;
//        }
//
//        @Override
//        public void close() throws Exception {
//            int k = 0;
//            for (int i = 0; i < inputs.length; i++) {
//                for (int j = 0; j < inputs[i].size(); j++) {
//                    inputs[i].get(j).attach(originalManagers[k++]);
//                }
//            }
//            inputs = null;
//        }
//    }
//}


import ai.djl.Device;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDManager;
import ai.djl.ndarray.NDResource;
import com.github.benmanes.caffeine.cache.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * A special Singleton NDManager that is direct child of the SystemNDManager.
 * It is not synchronized since access to it is by default single threaded from flink side
 * Has Tracker ability for nested NDArray creations, all other ndarrays are not kept in the reference queue
 * Internally one can create a child of this NDManager if DJL normal NDManager is needed
 * Try to avoid NDManager if it is not going to be closed again
 */
public class LifeCycleNDManager extends PtNDManager {

    /**
     * Logged
     */
    public static final Logger LOG = LoggerFactory.getLogger(NDManager.class);
    /**
     * HashMap for the Threads
     */
    private static final transient NonBlockingHashMapLong<Tuple2<Thread, LifeCycleNDManager>> THREADS = new NonBlockingHashMapLong<>();

    static {
        Thread cleanerThread = new Thread(LifeCycleNDManager::clean);
        cleanerThread.setPriority(Thread.NORM_PRIORITY);
        cleanerThread.start();
    }

    /**
     * Scopes
     */
    protected final Scope parentScope = new Scope();
    protected final ManualTicker ticker = new ManualTicker();
    protected final ConcurrentHashMap<AutoCloseable, Integer> detached = new ConcurrentHashMap<>();
    protected long scopedCount = 0;
    protected long closedCount = 0;
    protected final Cache<AutoCloseable, AutoCloseable> attached = Caffeine.newBuilder()
            .evictionListener((RemovalListener<AutoCloseable, AutoCloseable>) (key, value, cause) -> {
                try {
                    if (cause.wasEvicted()) {
                        closedCount++;
                        if (key instanceof PtNDArray) ((PtNDArray) key).closeNotNotify();
                        else key.close();
                    }
                } catch (Exception e) {
                    LOG.error(e.getMessage());
                }
            }).expireAfterWrite(100, TimeUnit.NANOSECONDS)
            .ticker(ticker)
            .scheduler(Scheduler.systemScheduler())
            .build();

    private LifeCycleNDManager(NDManager parent, Device device) {
        super(parent, device);
        this.resources = null;
        this.tempResources = null;
    }

    /**
     * Get NDManager for this Thread
     */
    public static LifeCycleNDManager getInstance() {
        THREADS.computeIfAbsent(Thread.currentThread().getId(), (a) -> Tuple2.of(Thread.currentThread(), new LifeCycleNDManager(PtNDManager.getSystemManager(), PtNDManager.getSystemManager().defaultDevice())));
        return THREADS.get(Thread.currentThread().getId()).f1;
    }

    public static void clean() {
        boolean notInterrupted = true;
        while (notInterrupted) {
            // Cleanup closed threads
            for (Iterator<Tuple2<Thread, LifeCycleNDManager>> threadLocal = THREADS.values().iterator(); threadLocal.hasNext(); ) {
                Tuple2<Thread, LifeCycleNDManager> val = threadLocal.next();
                if (!val.f0.isAlive()) {
                    // Clean the data structure, thread is no longer needed
                    try {
                        for (AutoCloseable value : val.f1.attached.asMap().keySet()) {
                            value.close();
                        }
                        for (AutoCloseable value : val.f1.detached.keySet()) {
                            value.close();
                        }
                        val.f1.attached.asMap().clear();
                        val.f1.detached.clear();
                        threadLocal.remove();
                        System.gc();
                        LOG.info(String.format("All Tensors closed +gc run in Thread: %s", val.f0));
                    } catch (Exception ignored) {
                        LOG.error("Exception in trying to close all Tensors");
                    }
                } else {
                    LOG.error(String.format("Thread:%s, attached:%s detached:%s closed:%s", val.f0, val.f1.attached.asMap().size(), val.f1.detached.size(), val.f1.closedCount));
                }
            }

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                LOG.info("Interrupted");
                notInterrupted = false;
            }


        }

    }

    public Scope getScope() {
        return parentScope;
    }

    public void postpone(NDArray resource) {
        detached.compute(resource, (key, val) -> {
            if (val == null) {
                attached.invalidate(key);
                return 1;
            }
            return val + 1;
        });
    }

    public void prepone(NDArray resource) {
        detached.compute(resource, (key, val) -> {
            if (val == null) return val;
            if (--val == 0) {
                // no longer detached
                attached.put(key, key); // Do not increment here!
                return null;
            }
            return val;
        });
    }

    @Override
    public void attachInternal(String resourceId, AutoCloseable resource) {
        if (!attached.asMap().containsKey(resource) && !detached.containsKey(resource)) {
            if (parentScope.isClosed()) ticker.increment();
            else scopedCount++;
            attached.put(resource, resource);
        }
    }

    @Override
    public void tempAttachInternal(NDManager originalManager, String resourceId, NDResource resource) {
        // Pass
        LOG.error("Trying to temp attach this tensor, should not happen, can cause memory leaks");
    }

    @Override
    public void detachInternal(String resourceId) {
        // No detaching for this NDArray, will be cleaned by the cleaner thread
        LOG.error("detached tensor");
    }

    public void detachInternal(AutoCloseable resource) {
        detached.remove(resource);
        attached.invalidate(resource); // !This might cause eviction is the time is late
    }

    @Override
    public void close() {
        // Not closing explicitely

    }

    /**
     * Logical Clock for releasing the Tensors
     */
    static class ManualTicker implements Ticker {
        private long value = 0;

        public void increment() {
            value++;
        }

        public void increment(long tmp) {
            value += tmp;
        }

        @Override
        public long read() {
            return value;
        }
    }

    /**
     * Context for doing ND operations. All tensos created while we are in this scope are not causing a clock tick
     */
    public class Scope implements AutoCloseable {
        private byte openCount = 0;

        public Scope start() {
            openCount++;
            return this;
        }

        @Override
        public void close() throws Exception {
            openCount--;
            if (isClosed()) {
                // Now the scope is closed update the ticker value back to the scope ticker
                ticker.increment(scopedCount);
                scopedCount = 0;
            }
        }

        public boolean isClosed() {
            return openCount == 0;
        }
    }

}
