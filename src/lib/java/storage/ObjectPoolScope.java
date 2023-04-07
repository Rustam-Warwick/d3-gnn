package storage;

/**
 * A special {@link AutoCloseable} object that should be opened when you want to access storage with reuse semantics.
 * Reuse semantics depends on the implementation of storage (Some of which might not have any effect whatsoever)
 * However, generally reuse semantics makes use of shared objects to reduce allocation costs
 * In such mode, UDF should not depend on storing the returned objects as they might change value later
 */
public class ObjectPoolScope implements AutoCloseable {

    protected byte openCount;

    protected ObjectPoolScope open() {
        openCount++;
        return this;
    }

    @Override
    public void close() {
        openCount--;
    }

    /**
     * Refresh the pool scope by closing and opening the scope thus resetting all the elements
     */
    public final void refresh() {
        close();
        open();
    }

    public boolean isOpen() {
        return openCount > 0;
    }

}
