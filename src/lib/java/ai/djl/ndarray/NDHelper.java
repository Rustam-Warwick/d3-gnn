package ai.djl.ndarray;

import ai.djl.Model;
import ai.djl.nn.Parameter;
import ai.djl.pytorch.engine.PtNDArray;
import ai.djl.pytorch.engine.PtNDManager;
import ai.djl.serializers.NDArrayLZ4Serializer;
import ai.djl.serializers.NDManagerSerializer;
import ai.djl.serializers.ParameterSerializer;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.*;

public class NDHelper {
    public static Void VOID;

    static {
        try {
            Constructor<Void> c = Void.class.getDeclaredConstructor();
            c.setAccessible(true);
            VOID = c.newInstance();
            c.setAccessible(false);
        } catch (NoSuchMethodException | InvocationTargetException | InstantiationException |
                 IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    /**
     * Decode numpy to NDArray
     */
    public static NDArray decodeNumpy(NDManager manager, InputStream is) throws IOException {
        return NDSerializer.decodeNumpy(manager, is);
    }

    /**
     * Add all the necessary serializers for Tensor related stuff
     */
    public static ExecutionConfig addSerializers(ExecutionConfig config) {
        config.addDefaultKryoSerializer(NDArray.class, NDArrayLZ4Serializer.class);
        config.addDefaultKryoSerializer(PtNDArray.class, NDArrayLZ4Serializer.class);
        config.addDefaultKryoSerializer(PtNDManager.class, NDManagerSerializer.class);
        config.addDefaultKryoSerializer(NDManager.class, NDManagerSerializer.class);
        config.addDefaultKryoSerializer(BaseNDManager.class, NDManagerSerializer.class);
        config.addDefaultKryoSerializer(Parameter.class, ParameterSerializer.class);
        return config;
    }

    /**
     * Reflectively copy fields from one Object to another
     *
     * @implNote Should be of the same type
     */
    public static void copyFields(Object from, Object to) {
        assert Objects.equals(from.getClass(), to.getClass());
        List<Field> fields = TypeExtractor.getAllDeclaredFields(from.getClass(), false);
        for (Field field : fields) {
            try {
                field.setAccessible(true);
                Object value = field.get(from);
                field.set(to, value);
                field.setAccessible(false);
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Load a model from the given path
     */
    public static void loadModel(Path modelPath, Model model) {
        File folder = new File(String.valueOf(modelPath));
        FilenameFilter onlyNumpy = (dir, name) -> name.toLowerCase().endsWith(".npy");
        List<File> numpyParameterFiles = new ArrayList<>();
        Collections.addAll(numpyParameterFiles, folder.listFiles(onlyNumpy));
        numpyParameterFiles.sort(Comparator.comparing(File::toString));
        model.getBlock().getParameters().forEach(param -> {
            try {
                InputStream in = new FileInputStream(numpyParameterFiles.remove(0));
                NDArray tmp = NDHelper.decodeNumpy(model.getNDManager(), in);
                param.getValue().setArray(tmp);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

}
