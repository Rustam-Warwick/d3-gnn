package types;

import java.io.Serializable;
import java.util.function.Function;

public interface SerialFunction<U,V> extends Function<U,V>, Serializable {
}
