package functions.nn;

import ai.djl.MalformedModelException;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
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
public class LossWrapper implements Serializable {
    public Loss internalLoss;
    public LossWrapper(){
        internalLoss = null;
    }

    public LossWrapper(Loss internalLoss) {
        this.internalLoss = internalLoss;
    }

    public NDArray evaluate(NDList labels, NDList predictions) {
        return internalLoss.evaluate(labels, predictions);
    }

    public String getName() {
        return internalLoss.getName();
    }

    /**
     * Fallback to Kryo
     */
    private void writeObject(ObjectOutputStream oos) throws IOException {
        Kryo a = new Kryo();
        ((Kryo.DefaultInstantiatorStrategy) a.getInstantiatorStrategy()).setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
        Output output = new Output(oos);
        a.writeObject(output, this);
        output.flush();
    }

    /**
     * Fallback to a KryoSerializer
     */
    private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException, MalformedModelException, NoSuchFieldException, IllegalAccessException {
        Kryo a = new Kryo();
        ((Kryo.DefaultInstantiatorStrategy) a.getInstantiatorStrategy()).setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
        Input input = new Input(ois);
        LossWrapper tmp = a.readObject(input, LossWrapper.class);
        this.internalLoss = tmp.internalLoss;
    }
}
