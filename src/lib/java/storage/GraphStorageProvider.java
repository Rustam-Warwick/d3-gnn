package storage;

import java.io.Serializable;
import java.util.function.Supplier;

/**
 * Provider pattern for {@link GraphStorage}, to be submitted on job-startup time to the operator coordinators
 */
public interface GraphStorageProvider extends Supplier<GraphStorage>, Serializable {

    /**
     * Default provider using {@link ListObjectPoolGraphStorage}
     */
    class DefaultGraphStorageProvider implements GraphStorageProvider {
        @Override
        public GraphStorage get() {
            return new ListObjectPoolGraphStorage();
        }
    }
}
