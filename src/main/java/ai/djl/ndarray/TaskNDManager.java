package ai.djl.ndarray;

/**
 * For Tasks that generate their own tensors this inteface should be used to manager the NDManagers,
 * detached tensors should only be of class @JavaTensor to support auto-closure
 */
public class TaskNDManager {
    private final transient NDManager lifeCycleManager;
    private transient NDManager tempManager;

    public TaskNDManager() {
        this.lifeCycleManager = NDManager.newBaseManager();
        this.tempManager = this.lifeCycleManager.newSubManager();
    }

    public NDManager getTempManager() {
        return this.tempManager;
    }

    public NDManager getLifeCycleManager() {
        return this.lifeCycleManager;
    }

    public void clean() {
        this.tempManager.close();
        this.tempManager = this.lifeCycleManager.newSubManager();

    }

    public void close() {
        this.lifeCycleManager.close();
    }
}
