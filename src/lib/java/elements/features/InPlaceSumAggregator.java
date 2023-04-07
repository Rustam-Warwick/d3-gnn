package elements.features;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import elements.annotations.RemoteFunction;
import elements.enums.CopyContext;

/**
 * Mean aggregator that does inplace operation hence the updated oldValue is never updated
 *
 * @implNote <strong>Use this if the updated oldValue is never used by the plugins</strong>
 */
public final class InPlaceSumAggregator extends Aggregator<CountTensorHolder> {

    public InPlaceSumAggregator() {
        super();
    }

    public InPlaceSumAggregator(String id, NDArray value) {
        super(id, new CountTensorHolder(value, 0));
    }

    public InPlaceSumAggregator(String id, NDArray value, boolean halo) {
        super(id, new CountTensorHolder(value, 0), halo);
    }

    public InPlaceSumAggregator(String id, NDArray value, boolean halo, short master) {
        super(id, new CountTensorHolder(value, 0), halo, master);
    }

    public InPlaceSumAggregator(InPlaceSumAggregator f, CopyContext context) {
        super(f, context);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InPlaceSumAggregator copy(CopyContext context) {
        return new InPlaceSumAggregator(this, context);
    }

    /**
     * {@inheritDoc}
     */
    @RemoteFunction
    @Override
    public void reduce(NDList newElement, int count) {
        value.val.addi(newElement.get(0));
        value.count += count;
    }

    /**
     * {@inheritDoc}
     */
    @RemoteFunction
    @Override
    public void replace(NDList newElement, NDList oldElement) {
        value.val.addi(newElement.get(0)).subi(oldElement.get(0));
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
