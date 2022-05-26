package helpers;

import javax.annotation.Nullable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;

public class PendingWrapperFuture<T> implements RunnableFuture<T> {

    @Nullable
    private RunnableFuture<T> internalFuture;
    private boolean hasBeenCancelled;
    protected PendingWrapperFuture() {
        this.internalFuture = null;
        this.hasBeenCancelled = false;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        hasBeenCancelled = true;
        if(internalFuture !=null)return internalFuture.cancel(mayInterruptIfRunning);
        else return false;
    }

    @Override
    public boolean isCancelled() {
        return internalFuture != null && internalFuture.isCancelled();

    }

    @Override
    public boolean isDone() {
        return internalFuture != null && internalFuture.isDone();
    }

    @Override
    public T get() throws ExecutionException, InterruptedException {
        return internalFuture == null?null: internalFuture.get();
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws ExecutionException, InterruptedException {
        return get();
    }

    @Override
    public void run() {
        if(internalFuture == null) throw new RuntimeException("Not Done Yet");
        else internalFuture.run();
    }

    @Nullable
    public RunnableFuture<T> getInternalFuture() {
        return internalFuture;
    }

    public void setInternalFuture(@Nullable RunnableFuture<T> internalFuture) {
        this.internalFuture = internalFuture;
        if(hasBeenCancelled)internalFuture.cancel(true);
    }
}
