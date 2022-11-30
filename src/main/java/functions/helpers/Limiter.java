package functions.helpers;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.util.Preconditions;

public class Limiter<T> implements FilterFunction<T> {
    final long limit;
    long count;

    public Limiter(long limit) {
        Preconditions.checkState(limit > 0);
        this.limit = limit;
    }

    @Override
    public boolean filter(T value) throws Exception {
        return ++count < limit;
    }
}
