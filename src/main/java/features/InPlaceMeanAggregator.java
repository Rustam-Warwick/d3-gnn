package features;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import elements.Feature;
import elements.GraphElement;
import elements.annotations.RemoteFunction;
import elements.enums.CopyContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Mean aggregator that does inplace operation hence the updated oldValue is never updated
 *
 * @implNote <strong>Use this if the updated oldValue is never used by the plugins</strong>
 */
public final class InPlaceMeanAggregator extends Feature<Tuple2<NDArray, Integer>, NDArray> implements Aggregator {

    public InPlaceMeanAggregator() {
        super();
    }

    public InPlaceMeanAggregator(String id, NDArray value) {
        super(id, Tuple2.of(value, 0));
    }

    public InPlaceMeanAggregator(String id, NDArray value, boolean halo) {
        super(id, Tuple2.of(value, 0), halo);
    }

    public InPlaceMeanAggregator(String id, NDArray value, boolean halo, short master) {
        super(id, Tuple2.of(value, 0), halo, master);
    }

    public InPlaceMeanAggregator(InPlaceMeanAggregator f, CopyContext context) {
        super(f, context);
    }

    public static NDArray bulkReduce(NDArray newMessages) {
        return newMessages.sum(new int[]{0});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InPlaceMeanAggregator copy(CopyContext context) {
        return new InPlaceMeanAggregator(this, context);
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
        if (getStorage().needsTensorDelay()) value.f0.delay();
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
        InPlaceMeanAggregator newAggregator = (InPlaceMeanAggregator) newElement;
        if (getStorage().needsTensorDelay() && newAggregator.value.f0 != value.f0) {
            value.f0.delay();
            newAggregator.value.f0.resume();
        }
    }

    /**
     * {@inheritDoc}
     */
    @RemoteFunction
    @Override
    public void reduce(NDList newElement, int count) {
        value.f0.addi(newElement.get(0));
        value.f1 += count;
    }

    /**
     * {@inheritDoc}
     */
    @RemoteFunction
    @Override
    public void replace(NDList newElement, NDList oldElement) {
        value.f0.addi((newElement.get(0).sub(oldElement.get(0))));
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
