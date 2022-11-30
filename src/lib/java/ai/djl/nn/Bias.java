package ai.djl.nn;

import ai.djl.Device;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.types.Shape;
import ai.djl.training.ParameterStore;
import ai.djl.util.PairList;
import ai.djl.util.Preconditions;

import java.util.Collections;

public class Bias extends AbstractBlock {
    private final Parameter bias;

    private Shape inputShape;

    public Bias() {
        bias =
                addParameter(
                        Parameter.builder()
                                .setName("bias")
                                .setType(Parameter.Type.BIAS)
                                .build());
    }

    @Override
    protected NDList forwardInternal(ParameterStore parameterStore, NDList inputs, boolean training, PairList<String, Object> params) {
        NDArray input = inputs.singletonOrThrow();
        Device device = input.getDevice();
        NDArray biasArr = parameterStore.getValue(bias, device, training);
        return new NDList(input.add(biasArr));
    }

    @Override
    public Shape[] getOutputShapes(Shape[] inputShapes) {
        return inputShapes;
    }

    @Override
    public PairList<String, Shape> describeInput() {
        return new PairList<>(
                Collections.singletonList("biasInput"), Collections.singletonList(inputShape));
    }

    @Override
    protected void beforeInitialize(Shape... inputShapes) {
        super.beforeInitialize(inputShapes);
        Preconditions.checkArgument(inputShapes.length == 1, "Linear block only support 1 input");
        inputShape = inputShapes[0];
    }

    @Override
    public void prepare(Shape[] inputShapes) {
        bias.setShape(inputShape);
    }

}
