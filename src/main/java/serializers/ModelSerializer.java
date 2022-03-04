//package serializers;
//
//import ai.djl.BaseModel;
//import ai.djl.Model;
//import ai.djl.ndarray.types.Shape;
//import ai.djl.pytorch.engine.PtModel;
//import ai.djl.repository.zoo.Criteria;
//import ai.djl.repository.zoo.ZooModel;
//import ai.djl.util.Pair;
//import ai.djl.util.PairList;
//import ai.djl.util.Utils;
//import com.esotericsoftware.kryo.Kryo;
//import com.esotericsoftware.kryo.io.Input;
//import com.esotericsoftware.kryo.io.Output;
//import org.apache.flink.api.java.typeutils.runtime.kryo.JavaSerializer;
//
//import java.io.BufferedOutputStream;
//import java.io.DataOutputStream;
//import java.io.IOException;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.util.Locale;
//import java.util.Map;
//
//public class ModelSerializer extends JavaSerializer<BaseModel> {
//    @Override
//    public void write(Kryo kryo, Output output, BaseModel o) {
//        if (o.getBlock() == null || !o.getBlock().isInitialized()) {
//            throw new IllegalStateException("Model has not be trained or loaded yet.");
//        }
//            output.writeString("DJL@");
//            output.writeInt(1);
//            output.writeString(o.getName());
//            output.writeString(o.getDataType().name());
//            PairList<String, Shape> inputData = o.getBlock().describeInput();
//            output.writeInt(inputData.size());
//            for (Pair<String, Shape> desc : inputData) {
//                String name = desc.getKey();
//                if (name == null) {
//                    output.writeString("");
//                } else {
//                    output.writeString(name);
//                }
//                output.write(desc.getValue().getEncoded());
//            }
//
////            output.writeInt(o..size());
////            for (Map.Entry<String, String> entry : properties.entrySet()) {
////                dos.writeUTF(entry.getKey());
////                dos.writeUTF(entry.getValue());
////            }
//            DataOutputStream tmp = new DataOutputStream(output);
//        try {
//            o.getBlock().saveParameters(tmp);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    @Override
//    public BaseModel read(Kryo kryo, Input input, Class aClass) {
//        return super.read(kryo, input, aClass);
//    }
//}
