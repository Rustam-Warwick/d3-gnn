package ai.djl.ndarray;

import ai.djl.nn.Parameter;
import ai.djl.pytorch.engine.PtNDArray;
import ai.djl.pytorch.engine.PtNDManager;
import ai.djl.serializers.NDArrayLZ4Serializer;
import ai.djl.serializers.NDManagerSerializer;
import ai.djl.serializers.ParameterSerializer;
import ai.djl.training.loss.Loss;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.*;

/**
 * Wrapper around DJL Loss class
 */
public class SerializableLoss implements Externalizable {
    public Loss internalLoss;

    public SerializableLoss() {
        internalLoss = null;
    }

    public SerializableLoss(Loss internalLoss) {
        this.internalLoss = internalLoss;
    }

    public static void configureSerializers(Kryo kryo) {
        kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
        ((Kryo.DefaultInstantiatorStrategy) kryo.getInstantiatorStrategy()).setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
        kryo.register(PtNDArray.class, new NDArrayLZ4Serializer());
        kryo.register(PtNDManager.class, new NDManagerSerializer());
        kryo.register(Parameter.class, new ParameterSerializer());
    }

    public NDArray evaluate(NDList labels, NDList predictions) {
        return internalLoss.evaluate(labels, predictions);
    }

    public String getName() {
        return internalLoss.getName();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        Kryo a = new Kryo();
        configureSerializers(a);
        OutputStream tmp = new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                out.write(b);
            }
        };
        Output output = new Output(tmp);
        a.writeObject(output, this);
        output.flush();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        Kryo a = new Kryo();
        configureSerializers(a);
        InputStream tmp = new InputStream() {
            @Override
            public int read() throws IOException {
                return in.read();
            }
        };
        Input input = new Input(tmp);
        SerializableLoss tmpValue = a.readObject(input, SerializableLoss.class);
        this.internalLoss = tmpValue.internalLoss;
    }
}
