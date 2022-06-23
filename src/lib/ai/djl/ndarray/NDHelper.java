package ai.djl.ndarray;

import java.io.IOException;
import java.io.InputStream;

public class NDHelper {
    public static final transient ThreadLocal<NDManager> threadNDManager = ThreadLocal.withInitial(NDManager::newBaseManager);

    public static NDArray decodeNumpy(NDManager manager, InputStream is) throws IOException {
        return NDSerializer.decodeNumpy(manager, is);
    }
}
