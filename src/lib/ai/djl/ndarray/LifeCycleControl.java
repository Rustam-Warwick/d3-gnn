package ai.djl.ndarray;

/**
 * Elements which are controlled by object pool and can be delayed or resumed to object pool
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
     * Pool is destroying this object last actions
     */
    default void destroy(){

    }

}
