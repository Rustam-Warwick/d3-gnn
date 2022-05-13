package ai.djl.ndarray;

import ai.djl.ndarray.types.Shape;
import ai.djl.nn.Parameter;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.IOException;
import java.lang.reflect.Field;

public class ParameterSerializer extends Serializer<Parameter> {
    private static final NDManager manager = NDManager.newBaseManager();
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
            output.writeChar('N');
            return;
        }
        output.writeChar('P');
        output.writeString(object.getName());
        output.writeString(object.getId());
        output.write(object.getArray().encode());
    }

    @Override
    public Parameter read(Kryo kryo, Input input, Class<Parameter> type) {
        char magic = input.readChar();
        if (magic == 'N') {
            return null;
        } else if (magic != 'P') {
            return null;
        }

        try {
            String name = input.readString();
            String id = input.readString();
            NDArray array = manager.decode(input);
            Shape shape = array.getShape();
            Parameter a = Parameter.builder().setName(name).optArray(array).optShape(shape).setType(Parameter.Type.OTHER).build();
            idField.set(a, id);
            return a;

        } catch (IOException | IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }
}
