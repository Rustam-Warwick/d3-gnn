package unit;

import ai.djl.ndarray.BaseNDManager;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDManager;
import ai.djl.ndarray.types.Shape;
import org.bytedeco.javacpp.IntPointer;
import org.junit.Test;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import org.tensorflow.*;
import org.tensorflow.ndarray.FloatNdArray;
import org.tensorflow.ndarray.NdArray;
import org.tensorflow.ndarray.NdArrays;
import org.tensorflow.op.Ops;
import org.tensorflow.op.Scope;
import org.tensorflow.op.core.Placeholder;
import org.tensorflow.op.math.Mul;
import org.tensorflow.op.random.RandomUniform;
import org.tensorflow.types.TFloat32;

import java.util.List;

public class MLTests {

    @Test
    public void testTensorflow(){
        long ms = System.currentTimeMillis();
        Graph g = new Graph();
        Ops tf = Ops.create(g);
        Placeholder<TFloat32> input1 = tf.placeholder(TFloat32.class);
        Placeholder<TFloat32> input2 = tf.placeholder(TFloat32.class);
        Mul<TFloat32> res = tf.math.mul(input1, input2);
        Session session = new Session(g);
        for (int i = 0; i < 100000; i++) {
            TFloat32 arr2 = Tensor.of(TFloat32.class,org.tensorflow.ndarray.Shape.of(10,10));
            TFloat32 arr1 = Tensor.of(TFloat32.class,org.tensorflow.ndarray.Shape.of(10,10));
            List<Tensor> result = session.runner().feed(input1, arr1).feed(input2,arr2).run();
            System.out.println(result.size());

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
