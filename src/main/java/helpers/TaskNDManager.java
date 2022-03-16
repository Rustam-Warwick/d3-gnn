package helpers;

import ai.djl.ndarray.NDManager;

public class TaskNDManager {
    public final NDManager taskLifeCycleManager;
    public  NDManager taskSingleProcessManager;

    public TaskNDManager(){
        taskLifeCycleManager = NDManager.newBaseManager();
        taskSingleProcessManager = taskLifeCycleManager.newSubManager();
    }

    public  NDManager getTempManager(){
        return taskSingleProcessManager;
    }

    public  NDManager getGlobalManager(){
        return taskLifeCycleManager;
    }

    public  void clean(){
        taskSingleProcessManager.close();
        taskSingleProcessManager = taskLifeCycleManager.newSubManager();
    }

    public  void close(){
        taskSingleProcessManager.close();
        taskLifeCycleManager.close();
    }

}
