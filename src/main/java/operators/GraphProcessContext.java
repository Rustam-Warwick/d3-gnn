package operators;

import org.apache.flink.api.common.functions.RuntimeContext;
import plugins.Plugin;
import storage.BaseStorage;

public interface GraphProcessContext extends RuntimeContext {

    ThreadLocal<GraphProcessContext> CONTEXT_THREAD_LOCAL = new ThreadLocal<>();

    static GraphProcessContext getContext(){
        return CONTEXT_THREAD_LOCAL.get();
    }

    BaseStorage getStorage();

    Plugin getPlugin(String pluginId);

}
