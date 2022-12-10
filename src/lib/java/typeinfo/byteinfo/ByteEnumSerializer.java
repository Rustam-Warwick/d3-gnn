package typeinfo.byteinfo;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.Array;
import java.util.*;

import static org.apache.flink.util.Preconditions.*;

@Internal
public final class ByteEnumSerializer<T extends Enum<T>> extends TypeSerializer<T> {

    private static final long serialVersionUID = 1L;

    private final Class<T> enumClass;

    /**
     * Maintain our own map of enum value to their ordinal, instead of directly using {@link
     * Enum#ordinal()}. This allows us to maintain backwards compatibility for previous serialized
     * data in the case that the order of enum constants was changed or new constants were added.
     *
     * <p>On a fresh startTermination with no reconfiguration, the ordinals would simply be identical to the
     * enum constants actual ordinals. Ordinals may change after reconfiguration.
     */
    private Map<T, Integer> valueToOrdinal;

    /**
     * Array of enum constants with their indexes identical to their ordinals in the {@link
     * #valueToOrdinal} map. Serves as a bidirectional map to have fast access from ordinal to
     * value. May be reordered after reconfiguration.
     */
    private T[] values;

    public ByteEnumSerializer(Class<T> enumClass) {
        this(enumClass, enumClass.getEnumConstants());
    }

    private ByteEnumSerializer(Class<T> enumClass, T[] enumValues) {
        this.enumClass = checkNotNull(enumClass);
        this.values = checkNotNull(enumValues);
        checkArgument(Enum.class.isAssignableFrom(enumClass), "not an enum");

        checkArgument(this.values.length > 0, "cannot use an empty enum");

        this.valueToOrdinal = new EnumMap<>(this.enumClass);
        int i = 0;
        for (T value : values) {
            this.valueToOrdinal.put(value, i++);
        }
    }

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public ByteEnumSerializer<T> duplicate() {
        return this;
    }

    @Override
    public T createInstance() {
        checkState(values != null);
        return values[0];
    }

    @Override
    public T copy(T from) {
        return from;
    }

    @Override
    public T copy(T from, T reuse) {
        return from;
    }

    @Override
    public int getLength() {
        return 1;
    }

    @Override
    public void serialize(T record, DataOutputView target) throws IOException {
        // use our own maintained ordinals instead of the actual enum ordinal
        target.writeByte(valueToOrdinal.get(record));
    }

    @Override
    public T deserialize(DataInputView source) throws IOException {
        return values[source.readByte()];
    }

    @Override
    public T deserialize(T reuse, DataInputView source) throws IOException {
        return values[source.readByte()];
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        target.write(source, 1);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ByteEnumSerializer) {
            ByteEnumSerializer<?> other = (ByteEnumSerializer<?>) obj;

            return other.enumClass == this.enumClass && Arrays.equals(values, other.values);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return enumClass.hashCode();
    }

    // --------------------------------------------------------------------------------------------

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        // may be null if this serializer was onDeserialized from an older version
        if (this.values == null) {
            this.values = enumClass.getEnumConstants();

            this.valueToOrdinal = new EnumMap<>(this.enumClass);
            int i = 0;
            for (T value : values) {
                this.valueToOrdinal.put(value, i++);
            }
        }
    }

    // --------------------------------------------------------------------------------------------
    // Serializer configuration snapshotting & compatibility
    // --------------------------------------------------------------------------------------------

    @Override
    public ByteEnumSerializerSnapshot<T> snapshotConfiguration() {
        return new ByteEnumSerializerSnapshot(enumClass, values);
    }

    @VisibleForTesting
    T[] getValues() {
        return values;
    }

    // --------------------------------------------------------------------------------------------
    // Test utilities
    // --------------------------------------------------------------------------------------------

    @VisibleForTesting
    Map<T, Integer> getValueToOrdinal() {
        return valueToOrdinal;
    }

    @Override
    public String toString() {
        return "EnumSerializer{"
                + "enumClass="
                + enumClass
                + ", values="
                + Arrays.toString(values)
                + '}';
    }

    /**
     * {@link TypeSerializerSnapshot} for {@link org.apache.flink.api.common.typeutils.base.EnumSerializer}.
     */
    public static final class ByteEnumSerializerSnapshot<T extends Enum<T>>
            implements TypeSerializerSnapshot<T> {
        private static final int CURRENT_VERSION = 3;

        private T[] previousEnums;
        private Class<T> enumClass;

        @SuppressWarnings("unused")
        public ByteEnumSerializerSnapshot() {
            // this constructor is used when restoring from a checkpoint/savepoint.
        }

        ByteEnumSerializerSnapshot(Class<T> enumClass, T[] enums) {
            this.enumClass = checkNotNull(enumClass);
            this.previousEnums = checkNotNull(enums);
        }

        @Override
        public int getCurrentVersion() {
            return CURRENT_VERSION;
        }

        @Override
        public void writeSnapshot(DataOutputView out) throws IOException {
            checkState(enumClass != null, "Enum class can not be null.");
            out.writeUTF(enumClass.getName());
            out.writeInt(previousEnums.length);
            for (T enumConstant : previousEnums) {
                out.writeUTF(enumConstant.name());
            }
        }

        @Override
        public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
                throws IOException {
            enumClass = InstantiationUtil.resolveClassByName(in, userCodeClassLoader);

            int numEnumConstants = in.readInt();

            @SuppressWarnings("unchecked")
            T[] previousEnums = (T[]) Array.newInstance(enumClass, numEnumConstants);
            for (int i = 0; i < numEnumConstants; i++) {
                String enumName = in.readUTF();
                try {
                    previousEnums[i] = Enum.valueOf(enumClass, enumName);
                } catch (IllegalArgumentException e) {
                    throw new IllegalStateException(
                            "Could not create a restore serializer for enum "
                                    + enumClass
                                    + ". Probably because an enum value was removed.");
                }
            }

            this.previousEnums = previousEnums;
        }

        @Override
        public TypeSerializer<T> restoreSerializer() {
            checkState(enumClass != null, "Enum class can not be null.");

            return new ByteEnumSerializer<>(enumClass, previousEnums);
        }

        @Override
        public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(
                TypeSerializer<T> newSerializer) {
            if (!(newSerializer instanceof ByteEnumSerializer)) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }

            ByteEnumSerializer<T> newEnumSerializer = (ByteEnumSerializer) newSerializer;
            if (!enumClass.equals(newEnumSerializer.enumClass)) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }

            T[] currentEnums = enumClass.getEnumConstants();

            if (Arrays.equals(previousEnums, currentEnums)) {
                return TypeSerializerSchemaCompatibility.compatibleAsIs();
            }

            Set<T> reconfiguredEnumSet = new LinkedHashSet<>(Arrays.asList(previousEnums));
            reconfiguredEnumSet.addAll(Arrays.asList(currentEnums));

            @SuppressWarnings("unchecked")
            T[] reconfiguredEnums =
                    reconfiguredEnumSet.toArray(
                            (T[]) Array.newInstance(enumClass, reconfiguredEnumSet.size()));

            ByteEnumSerializer<T> reconfiguredSerializer =
                    new ByteEnumSerializer<>(enumClass, reconfiguredEnums);
            return TypeSerializerSchemaCompatibility.compatibleWithReconfiguredSerializer(
                    reconfiguredSerializer);
        }
    }
}

