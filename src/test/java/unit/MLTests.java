package unit;

import ai.djl.ndarray.BaseNDManager;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDManager;
import ai.djl.ndarray.types.Shape;
import org.junit.Test;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import org.tensorflow.EagerSession;
import org.tensorflow.Operand;
import org.tensorflow.op.Ops;
import org.tensorflow.types.TFloat64;
public class MLTests {

    @Test
    public void testTensorflow(){
        Ops t = Ops.create(EagerSession.create());
        long ms = System.currentTimeMillis();
        for (int i = 0; i < 100000 ; i++) {
            Operand<TFloat64> res = t.math.mul(t.random.randomUniform(t.constant(new int[]{10,10}), TFloat64.class),t.random.randomUniform(t.constant(new int[]{10,10}),TFloat64.class));
        }
        System.out.println("Time taken for TF: " + (System.currentTimeMillis() - ms));
    }

    @Test
    public void testNd4j(){
        long ms = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
            INDArray arr1 = Nd4j.rand(10, 10);
            INDArray arr2 = Nd4j.rand(10, 10);
            arr1.mul(arr2);
        }
        System.out.println("Time taken for ND4j: " + (System.currentTimeMillis() - ms));
    }

    @Test
    public void testPyTorch(){
        long ms = System.currentTimeMillis();
        NDManager m = BaseNDManager.getManager();
        for (int i = 0; i < 100000; i++) {
            NDArray arr1 = m.randomUniform(0, 1, new Shape(10,10));
            NDArray arr2 = m.randomUniform(0, 1, new Shape(10,10));
            arr1.mul(arr2);
        }
        System.out.println("Time taken for PyTorch: " + (System.currentTimeMillis() - ms));

    }


}
