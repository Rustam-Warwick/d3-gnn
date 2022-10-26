package ai.djl.pytorch.engine;

import ai.djl.Device;
import ai.djl.ndarray.NDManager;
import ai.djl.ndarray.NDResource;
import com.github.benmanes.caffeine.cache.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * A special Singleton NDManager that is direct child of the SystemNDManager.
 * It is not synchronized since access to it is by default single threaded from flink side
 * Has Tracker ability for nested NDArray creations, all other ndarrays are not kept in the reference queue
 * Internally one can create a child of this NDManager if DJL normal NDManager is needed
 * Try to avoid NDManager if it is not going to be closed again
 */
public class LifeCycleNDManager extends PtNDManager {

    public static final Logger LOG = LoggerFactory.getLogger(NDManager.class);

    private static final NonBlockingHashMapLong<Tuple2<Thread, LifeCycleNDManager>> THREADS = new NonBlockingHashMapLong<>();

    static {
        // Start the Tensor cleaner thread in this JVM globally
        Thread cleanerThread = new Thread(LifeCycleNDManager::clean);
        cleanerThread.setPriority(Thread.NORM_PRIORITY);
        cleanerThread.start();
    }

    // STATIC METHODS

    protected final Scope parentScope = new Scope(); // Scope to delay the tensor removing

    protected final ManualTicker ticker = new ManualTicker(); // Logical timer depending on the data-rate

    protected final Cache<AutoCloseable, AutoCloseable> attached = Caffeine.newBuilder()
            .evictionListener((RemovalListener<AutoCloseable, AutoCloseable>) (key, value, cause) -> {
                try {
                    if (cause.wasEvicted()) {
                        key.close();
                    }
                } catch (Exception e) {
                    LOG.error(e.getMessage());
                }
            }).expireAfterWrite(100, TimeUnit.NANOSECONDS)
            .ticker(ticker)
            .scheduler(Scheduler.systemScheduler())
            .build();
    public int scopedCount = 0; // Count of opened tensors when we are in a scope

    private LifeCycleNDManager(NDManager parent, Device device) {
        super(parent, device);
    }

    /**
     * Get NDManager for this Thread
     */
    public static LifeCycleNDManager getInstance() {
        THREADS.computeIfAbsent(Thread.currentThread().getId(), (a) -> Tuple2.of(Thread.currentThread(), new LifeCycleNDManager(PtNDManager.getSystemManager(), PtNDManager.getSystemManager().defaultDevice())));
        return THREADS.get(Thread.currentThread().getId()).f1;
    }

    /**
     * Analyze Threads using NDArrays and clean them when the thread is stopped
     */
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
                        val.f1.attached.asMap().clear();
                        threadLocal.remove();
                        System.gc();
                        LOG.info(String.format("All Tensors closed +gc run in Thread: %s", val.f0));
                    } catch (Exception ignored) {
                        LOG.error("Exception in trying to close all Tensors");
                    }
                }
            }

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                LOG.info("Interrupted Cleaner Thread ");
                notInterrupted = false;
            }
        }

    }

    /**
     * Get the scope object
     */
    public Scope getScope() {
        return parentScope;
    }

    /**
     * Called when the Tensor is constructed or re-attached
     */
    @Override
    public void attachInternal(String resourceId, AutoCloseable resource) {
        if (!attached.asMap().containsKey(resource)) {
            if (parentScope.isClosed()) ticker.increment();
            else scopedCount++;
            attached.put(resource, resource);
        }
    }

    /**
     * @throws IllegalStateException
     */
    @Override
    public void tempAttachInternal(NDManager originalManager, String resourceId, NDResource resource) {
        // Pass
        throw new IllegalStateException("TempAttaching is disabled for LifeCycleNDManager, please use postpone and prepone for delaying closure");
    }

    /**
     * @throws IllegalStateException
     */
    @Override
    public void detachInternal(String resourceId) {
        // No detaching for this NDArray, will be cleaned by the cleaner thread
        throw new IllegalStateException("For LifeCycleNDManager please use the detachInternal(AutoClosable)");
    }

    /**
     * Custom method for detaching tensors
     */
    public void detachInternal(String resourceId, AutoCloseable resource) {
        attached.invalidate(resource); // !This might cause eviction is the time is late
    }

    /**
     * @implNote Not implemented
     * Closing LifeCycleNDManager is only done with the special Thread
     */
    @Override
    public void close() {
        // Not closing explicitely, delegated to the cleaner and GC
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
        protected byte openCount = 0;

        public Scope start() {
            openCount++;
            return this;
        }

        @Override
        public void close() {
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
