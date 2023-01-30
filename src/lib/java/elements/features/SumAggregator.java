package elements.features;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import elements.annotations.RemoteFunction;
import elements.enums.CopyContext;

/**
 * MEAN {@link ai.djl.nn.gnn.AggregatorVariant} aggregator for GNNs
 */
public final class SumAggregator extends Aggregator<CountTensorHolder> {

    public SumAggregator() {
        super();
    }

    public SumAggregator(String id, NDArray value) {
        super(id, new CountTensorHolder(value, 0));
    }

    public SumAggregator(String id, NDArray value, boolean halo) {
        super(id, new CountTensorHolder(value, 0), halo);
    }

    public SumAggregator(String id, NDArray value, boolean halo, short master) {
        super(id, new CountTensorHolder(value, 0), halo, master);
    }

    public SumAggregator(SumAggregator f, CopyContext context) {
        super(f, context);
        if (context == CopyContext.RMI) value = new CountTensorHolder(f.value.val, f.value.count);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SumAggregator copy(CopyContext context) {
        return new SumAggregator(this, context);
    }

    /**
     * {@inheritDoc}
     */
    @RemoteFunction()
    @Override
    public void reduce(NDList newElement, int count) {
        value.val = value.val.add(newElement.get(0));
        value.count += count;
    }

    /**
     * {@inheritDoc}
     */
    @RemoteFunction()
    @Override
    public void replace(NDList newElement, NDList oldElement) {
        value.val = value.val.add((newElement.get(0).sub(oldElement.get(0))));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray getValue() {
        return value.val;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void reset() {
        value.val.subi(value.val);
        value.count = 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int reducedCount() {
        return value.count;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delay() {
        super.delay();
        value.delay();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void resume() {
        super.resume();
        value.resume();
    }

    @Override
    public void destroy() {
        super.destroy();
        value.destroy();
    }
}
