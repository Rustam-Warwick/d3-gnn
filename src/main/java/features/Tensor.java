package features;

import ai.djl.ndarray.NDArray;
import elements.Feature;
import elements.GraphElement;
import elements.enums.CopyContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import storage.BaseStorage;

import java.util.function.Consumer;

/**
 * Feature of {@link NDArray} Used to represent embeddings of specific model versions
 */
public class Tensor extends Feature<NDArray, NDArray> {

    public Tensor() {
    }

    public Tensor(String id, NDArray value) {
        super(id, value);
    }

    public Tensor(String id, NDArray value, boolean halo){
        super(id, value, halo);
    }

    public Tensor(String id, NDArray value, boolean halo, short master) {
        super(id, value, halo, master);
    }

    public Tensor(Feature<NDArray, NDArray> f, CopyContext context) {
        super(f, context);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Tensor copy(CopyContext context) {
        return new Tensor(this, context);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Consumer<BaseStorage> createInternal() {
        Consumer<BaseStorage> callback = super.createInternal();
        return ((Consumer<BaseStorage>) storage -> {if(storage.needsTensorDelay()) value.delay();}).andThen(callback);
    }

    /**
     * {@inheritDoc}
     * @return
     */
    @Override
    public Consumer<BaseStorage> updateInternal(GraphElement newElement) {
        Consumer<BaseStorage> callback = super.updateInternal(newElement);
        Tensor newTensor = (Tensor) newElement;
        return callback.andThen(storage -> {
            if(storage.needsTensorDelay() && newTensor.value != value){
                value.delay();
                newTensor.value.resume();
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray getValue() {
        return this.value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean valuesEqual(NDArray v1, NDArray v2) {
        return v1 == v2;
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

    /**
     * {@inheritDoc}
     */
    @Override
    public TypeInformation<?> getValueTypeInfo() {
        return TypeInformation.of(NDArray.class);
    }
}