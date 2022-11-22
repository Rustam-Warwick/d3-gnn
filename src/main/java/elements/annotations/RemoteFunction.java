package elements.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Functions decorated such way can execute Rpc messages
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface RemoteFunction {

    /**
     * Should we trigger a new {@link elements.GraphElement} update after this RMI
     */
    boolean triggerUpdate() default true;

    /**
     * Should we trigger {@link storage.BaseStorage} callbacks after this RMI
     */
    boolean triggerCallbacks() default true;

}