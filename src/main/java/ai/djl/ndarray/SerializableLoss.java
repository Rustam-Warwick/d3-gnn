package ai.djl.ndarray;

import ai.djl.MalformedModelException;
import ai.djl.nn.Parameter;
import ai.djl.pytorch.engine.PtNDArray;
import ai.djl.training.loss.Loss;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Wrapper around DJL Loss class
 */
public class SerializableLoss implements Serializable {
    public Loss internalLoss;

    public SerializableLoss() {
        internalLoss = null;
    }

    public SerializableLoss(Loss internalLoss) {
        this.internalLoss = internalLoss;
    }

    public NDArray evaluate(NDList labels, NDList predictions) {
        return internalLoss.evaluate(labels, predictions);
    }

    public String getName() {
        return internalLoss.getName();
    }


    /**
     * Register all classes possible in DGL otherwise error is throws
     */
    private static void registerAllClasses(Kryo a){
        a.setClassLoader(Thread.currentThread().getContextClassLoader());
        ((Kryo.DefaultInstantiatorStrategy) a.getInstantiatorStrategy()).setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
        a.register(PtNDArray.class, new TensorSerializer());
        a.register(JavaTensor.class, new TensorSerializer());
        a.register(Parameter.class, new ParameterSerializer());
    }

    /**
     * Fallback to Kryo
     */
    private void writeObject(ObjectOutputStream oos) throws IOException {
        Kryo a = new Kryo();
        registerAllClasses(a);
        Output output = new Output(oos);
        a.writeObject(output, this);
        output.flush();
    }

    /**
     * Fallback to a KryoSerializer
     */
    private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException, MalformedModelException, NoSuchFieldException, IllegalAccessException {
        Kryo a = new Kryo();
        registerAllClasses(a);
        ((Kryo.DefaultInstantiatorStrategy) a.getInstantiatorStrategy()).setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
        Input input = new Input(ois);
        SerializableLoss tmp = a.readObject(input, SerializableLoss.class);
        this.internalLoss = tmp.internalLoss;
    }
}
