package elements.features;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import elements.GraphElement;
import elements.annotations.RemoteFunction;
import elements.enums.CopyContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * MEAN {@link ai.djl.nn.gnn.AggregatorVariant} aggregator for GNNs
 */
public final class SumAggregator extends Aggregator<Tuple2<NDArray, Integer>> {

    public SumAggregator() {
        super();
    }

    public SumAggregator(String id, NDArray value) {
        super(id, Tuple2.of(value, 0));
    }

    public SumAggregator(String id, NDArray value, boolean halo) {
        super(id, Tuple2.of(value, 0), halo);
    }

    public SumAggregator(String id, NDArray value, boolean halo, short master) {
        super(id, Tuple2.of(value, 0), halo, master);
    }

    public SumAggregator(SumAggregator f, CopyContext context) {
        super(f, context);
        if (context == CopyContext.RMI) value = Tuple2.of(value.f0, value.f1);

    }

    public static NDArray bulkReduce(NDArray newMessages) {
        return newMessages.sum(new int[]{0});
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
     * <p>
     * Delaying tensors if storage needs delay
     * </p>
     */
    @Override
    public void createInternal() {
        super.createInternal();
        if (getGraphRuntimeContext().getStorage().needsTensorDelay()) value.f0.delay();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Delaying tensors if storage need delay
     * </p>
     */
    @Override
    public void updateInternal(GraphElement newElement) {
        super.updateInternal(newElement);
        SumAggregator newAggregator = (SumAggregator) newElement;
        if (getGraphRuntimeContext().getStorage().needsTensorDelay() && newAggregator.value.f0 != value.f0) {
            value.f0.delay();
            newAggregator.value.f0.resume();
        }
    }

    /**
     * {@inheritDoc}
     */
    @RemoteFunction()
    @Override
    public void reduce(NDList newElement, int count) {
        value.f0 = value.f0.add(newElement.get(0));
        value.f1 += count;
    }

    /**
     * {@inheritDoc}
     */
    @RemoteFunction()
    @Override
    public void replace(NDList newElement, NDList oldElement) {
        value.f0 = value.f0.add((newElement.get(0).sub(oldElement.get(0))));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray grad(NDArray aggGradient) {
        return aggGradient.div(value.f1);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray getValue() {
        return value.f0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void reset() {
        value.f0.subi(value.f0);
        value.f1 = 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int reducedCount() {
        return value.f1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delay() {
        super.delay();
        value.f0.delay();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void resume() {
        super.resume();
        value.f0.resume();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TypeInformation<?> getValueTypeInfo() {
        return Types.TUPLE(TypeInformation.of(NDArray.class), Types.INT);
    }
}