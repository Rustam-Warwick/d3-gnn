package functions.nn;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;

public class MyActivations {
    public static NDList softmax(NDList input){
        NDArray in = input.singletonOrThrow();
        in = in.sub(in.max());
        NDArray numerator = in.exp();
        NDArray denominator = numerator.sum();
        return new NDList(numerator.div(denominator));
    }
}
