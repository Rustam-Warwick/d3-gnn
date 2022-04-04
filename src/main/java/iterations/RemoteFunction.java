package iterations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Functions decorated such way can execute Rpc messages
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface RemoteFunction {

}