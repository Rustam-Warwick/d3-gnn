package ai.djl.ndarray;

/**
 * Elements whose life-cycle can be controlled should implement this unified interface.
 * Delay -> Delay some common logic for these elements
 * Resume -> Resume some common logic for these elements
 * Destroy -> Destroy some common logic for these elements
 *
 * @implNote The actual logic of those functions differ from class to class, {@see NDManager, NDArray, GraphElement}
 */
public interface LifeCycleControl {

    /**
     * Delays the normal cycle of object pool de-allocation
     */
    default void delay() {
    }

    /**
     * Resume the object to object pool cycle
     */
    default void resume() {
    }

    /**
     * Pool is destroying this object, last actions
     */
    default void destroy() {
    }

    /**
     * Simply resume and then delay
     */
    default void resumeAndDelay() {
        resume();
        delay();
    }

}
