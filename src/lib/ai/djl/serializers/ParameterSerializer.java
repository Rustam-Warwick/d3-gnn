package ai.djl.serializers;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.Parameter;
import ai.djl.pytorch.engine.PtNDArray;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.lang.reflect.Field;

/**
 * Serializer for model Parameters
 */
public class ParameterSerializer extends Serializer<Parameter> {
    private static Field idField;

    static {
        try {
            Field id = Parameter.class.getDeclaredField("id");
            id.setAccessible(true);
            idField = id;
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Kryo kryo, Output output, Parameter object) {
        if (!object.isInitialized()) {
            throw new RuntimeException("Initialize the Parameter before sending it");
        }
        output.writeString(object.getName());
        output.writeString(object.getId());
        kryo.writeObject(output, object.getArray());
    }

    @Override
    public Parameter read(Kryo kryo, Input input, Class<Parameter> type) {
        try {
            String name = input.readString();
            String id = input.readString();
            NDArray array = kryo.readObject(input, PtNDArray.class);
            Shape shape = array.getShape();
            Parameter a = Parameter.builder().setName(name).optArray(array).optShape(shape).setType(Parameter.Type.OTHER).build();
            idField.set(a, id);
            return a;

        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }
}
