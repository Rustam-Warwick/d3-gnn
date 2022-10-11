package typeinfo;

import ai.djl.ndarray.NDArray;
import ai.djl.pytorch.engine.LifeCycleNDManager;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Map;

public class GradientCollectorTypeInfo<T> extends MapTypeInfo<T, NDArray> {
    public GradientCollectorTypeInfo(TypeInformation<T> keyTypeInfo) {
        super(keyTypeInfo, TypeInformation.of(NDArray.class));
    }

    public GradientCollectorTypeInfo(Class<T> keyClass){
        super(keyClass, NDArray.class);
    }

    @Override
    public TypeSerializer<Map<T, NDArray>> createSerializer(ExecutionConfig config) {
        return new GradientCollectorTypeSerializer<>(super.createSerializer(config));
    }

    public static class GradientCollectorTypeSerializer<T> extends TypeSerializer<Map<T, NDArray>>{
        private final TypeSerializer<Map<T, NDArray>> parentSerializer;

        public GradientCollectorTypeSerializer(TypeSerializer<Map<T, NDArray>> parentSerializer) {
            this.parentSerializer = parentSerializer;
        }

        @Override
        public boolean isImmutableType() {
            return parentSerializer.isImmutableType();
        }

        @Override
        public TypeSerializer<Map<T, NDArray>> duplicate() {
            return new GradientCollectorTypeSerializer<>(parentSerializer.duplicate());
        }

        @Override
        public Map<T, NDArray> createInstance() {
            return parentSerializer.createInstance();
        }

        @Override
        public Map<T, NDArray> copy(Map<T, NDArray> from) {
            try (LifeCycleNDManager.Scope ignored = LifeCycleNDManager.getInstance().getScope().start()) {
                int tmp = LifeCycleNDManager.getInstance().scopedCount;
                Map<T, NDArray> val = parentSerializer.copy(from);
                LifeCycleNDManager.getInstance().scopedCount = tmp;
                return val;
            }
        }

        @Override
        public Map<T, NDArray> copy(Map<T, NDArray> from, Map<T, NDArray> reuse) {
            try (LifeCycleNDManager.Scope ignored = LifeCycleNDManager.getInstance().getScope().start()) {
                int tmp = LifeCycleNDManager.getInstance().scopedCount;
                Map<T, NDArray> val =  parentSerializer.copy(from, reuse);
                LifeCycleNDManager.getInstance().scopedCount = tmp;
                return val;
            }
        }

        @Override
        public int getLength() {
            return parentSerializer.getLength();
        }

        @Override
        public void serialize(Map<T, NDArray> record, DataOutputView target) throws IOException {
            parentSerializer.serialize(record, target);
        }

        @Override
        public Map<T, NDArray> deserialize(DataInputView source) throws IOException {
            try (LifeCycleNDManager.Scope ignored = LifeCycleNDManager.getInstance().getScope().start()) {
                int tmp = LifeCycleNDManager.getInstance().scopedCount;
                Map<T, NDArray> val =  parentSerializer.deserialize(source);
                LifeCycleNDManager.getInstance().scopedCount = tmp;
                return val;
            }
        }

        @Override
        public Map<T, NDArray> deserialize(Map<T, NDArray> reuse, DataInputView source) throws IOException {
            try (LifeCycleNDManager.Scope ignored = LifeCycleNDManager.getInstance().getScope().start()) {
                int tmp = LifeCycleNDManager.getInstance().scopedCount;
                Map<T, NDArray> val = parentSerializer.deserialize(reuse, source);
                LifeCycleNDManager.getInstance().scopedCount = tmp;
                return val;
            }
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            parentSerializer.copy(source, target);
        }

        @Override
        public boolean equals(Object obj) {
            return parentSerializer.equals(obj);
        }

        @Override
        public int hashCode() {
            return parentSerializer.hashCode();
        }

        @Override
        public TypeSerializerSnapshot<Map<T, NDArray>> snapshotConfiguration() {
            return parentSerializer.snapshotConfiguration();
        }
    }
}
