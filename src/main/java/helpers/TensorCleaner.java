package helpers;

import ai.djl.ndarray.NDArray;

import java.lang.ref.Cleaner;

public class TensorCleaner{
    public static transient Cleaner cleaner = Cleaner.create();
    private final NDArray resource;
    public TensorCleaner(NDArray resource){
        this.resource = resource;
    }
    public static void subscribe(NDArray array){
        CleanerRunner a = new CleanerRunner(array);
        cleaner.register(array, a);

    }
    static class CleanerRunner implements Runnable{
        private final NDArray resource;
        public CleanerRunner(NDArray resource){
            this.resource = resource;
        }
        @Override
        public void run() {
            this.resource.close();
        }
    }


}
