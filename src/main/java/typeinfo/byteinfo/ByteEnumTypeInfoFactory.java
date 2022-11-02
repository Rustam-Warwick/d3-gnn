package typeinfo.byteinfo;

import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractionUtils;

import java.lang.reflect.Type;
import java.util.Map;

public class ByteEnumTypeInfoFactory<T extends Enum<T>> extends TypeInfoFactory<T> {
    @Override
    public TypeInformation<T> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
        return new ByteEnumTypeInfo<T>(TypeExtractionUtils.typeToClass(t));
    }
}
