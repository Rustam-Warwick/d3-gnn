package aggregators;

public interface BaseAggregator {
    void reduce(Object newElement, int count);
    void bulkReduce(Object ...newElements);
    void replace(Object newElement, Object oldElement);
    void bulkReplace();
    boolean isReady();
    void reset();
}
