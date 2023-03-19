package elements.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Functions decorated such way can execute {@link elements.Rmi} messages
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface RemoteFunction{

    /**
     * Should we trigger a new {@link elements.GraphElement} update after this RMI
     */
    boolean triggerUpdate() default true;
}