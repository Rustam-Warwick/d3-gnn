package elements.features;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import elements.annotations.RemoteFunction;
import elements.enums.CopyContext;

/**
 * MEAN {@link ai.djl.nn.gnn.AggregatorVariant} aggregator for GNNs
 */
public final class MeanAggregator extends Aggregator<CountTensorHolder> {

    public MeanAggregator() {
        super();
    }

    public MeanAggregator(String id, NDArray value) {
        super(id, new CountTensorHolder(value, 0));
    }

    public MeanAggregator(String id, NDArray value, boolean halo) {
        super(id, new CountTensorHolder(value, 0), halo);
    }

    public MeanAggregator(String id, NDArray value, boolean halo, short master) {
        super(id, new CountTensorHolder(value, 0), halo, master);
    }

    public MeanAggregator(MeanAggregator f, CopyContext context) {
        super(f, context);
        if (context == CopyContext.RMI) value = new CountTensorHolder(f.value.val, f.value.count);

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MeanAggregator copy(CopyContext context) {
        return new MeanAggregator(this, context);
    }

    /**
     * {@inheritDoc}
     */
    @RemoteFunction()
    @Override
    public void reduce(NDList newElement, int count) {
        value.val = value.val.mul(value.count).add(newElement.get(0)).div(++value.count);
    }

    /**
     * {@inheritDoc}
     */
    @RemoteFunction()
    @Override
    public void replace(NDList newElement, NDList oldElement) {
        NDArray increment = newElement.get(0).sub(oldElement.get(0)).div(value.count);
        value.val = value.val.add(increment);
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
    public int getReducedCount() {
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
