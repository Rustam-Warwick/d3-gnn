package benchmarks;

import org.junit.jupiter.api.Disabled;

@Disabled
public class MLTests {

//    @Test
//    public void testTensorflow() {
//        long ms = System.currentTimeMillis();
//        Graph g = new Graph();
//        Session session = new Session(g);
//        Ops tf = Ops.create(g);
//        TFloat32 arr1_float = TFloat32.tensorOf(org.tensorflow.ndarray.Shape.of(10, 10));
//        TFloat32 arr2_float = TFloat32.tensorOf(org.tensorflow.ndarray.Shape.of(10, 10));
//        Placeholder<TFloat32> arr1 = tf.placeholder(TFloat32.class);
//        Placeholder<TFloat32> arr2 = tf.placeholder(TFloat32.class);
//        Add<TFloat32> res = tf.math.aggregate(arr2, arr1);
//        for (int i = 0; i < 100000; i++) {
//            Result a = session.runner().addTarget(res).fetch(res).feed(arr1, arr1_float).feed(arr2, arr2_float).run();
////            arr2.scalars().forEach(item -> System.out.println(item.getObject()));
////            res.asTensor().scalars().forEach(item -> System.out.println(item.getObject()));
//        }
//
//        System.out.println("Time taken for TF: " + (System.currentTimeMillis() - ms));
//    }
//
//    @Test
//    public void testNd4j() {
//        long ms = System.currentTimeMillis();
//        INDArray arr1 = Nd4j.rand(10, 10);
//        INDArray arr2 = Nd4j.rand(10, 10);
//        for (int i = 0; i < 100000; i++) {
//            arr1.aggregate(arr2);
//        }
//        System.out.println("Time taken for ND4j: " + (System.currentTimeMillis() - ms));
//    }
//
//    @Test
//    public void testPyTorch() {
//        long ms = System.currentTimeMillis();
//        NDManager m = BaseNDManager.getManager();
//        m.delay();
//        NDArray arr1 = m.randomUniform(0, 1, new Shape(10, 10));
//        NDArray arr2 = m.randomUniform(0, 1, new Shape(10, 10));
//        for (int i = 0; i < 100000; i++) {
//            arr1.aggregate(arr2);
//        }
//        m.resume();
//        System.out.println("Time taken for PyTorch: " + (System.currentTimeMillis() - ms));
//
//    }


}
