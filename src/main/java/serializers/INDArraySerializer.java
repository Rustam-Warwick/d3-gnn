package serializers;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.serde.jackson.shaded.NDArraySerializer;
import org.nd4j.serde.jackson.shaded.NDArrayTextSerializer;
import org.nd4j.shade.jackson.core.JsonGenerator;
import org.nd4j.shade.jackson.core.json.UTF8JsonGenerator;
import org.nd4j.shade.jackson.databind.util.TokenBuffer;
import org.nd4j.shade.jackson.dataformat.xml.ser.ToXmlGenerator;

public class INDArraySerializer extends Serializer<INDArray> {
    NDArrayTextSerializer serializer = new NDArrayTextSerializer();
    @Override
    public void write(Kryo kryo, Output output, INDArray object) {
    }

    @Override
    public INDArray read(Kryo kryo, Input input, Class<INDArray> type) {
        return null;
    }
}
