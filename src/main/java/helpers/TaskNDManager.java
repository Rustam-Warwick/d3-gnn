package helpers;

import ai.djl.ndarray.NDManager;

public class TaskNDManager {
    private transient NDManager lifeCycleManager;
    private transient NDManager tempManager;

    public TaskNDManager(){
        this.lifeCycleManager = NDManager.newBaseManager();
        this.tempManager = this.lifeCycleManager.newSubManager();
    }

    public NDManager getTempManager(){
        return this.tempManager;
    }

    public NDManager getLifeCycleManager(){
        return this.lifeCycleManager;
    }

    public void clean(){
        this.tempManager.close();
        this.tempManager = this.lifeCycleManager.newSubManager();
    }

    public void close(){
        this.lifeCycleManager.close();
    }
}
