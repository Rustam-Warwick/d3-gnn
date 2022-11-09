/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package typeinfo.graphopinfo;

import elements.GraphElement;
import elements.GraphOp;
import elements.enums.Op;
import elements.enums.MessageCommunication;
import operators.events.BaseOperatorEvent;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;

/**
 * Efficient Serialization of GraphOp taking into account the max enum sizes
 *
 * @implNote Ops Enum size should be <= 8
 * Assuming that the GraphOp class is not evolving a.k.a final
 */
public final class GraphOpSerializer extends TypeSerializer<GraphOp> {
    private static final Op[] OPS = Op.values();
    private static final MessageCommunication[] MESSAGE_COMMUNICATIONS = MessageCommunication.values();
    private final TypeSerializer[] fieldSerializers;
    private final ExecutionConfig config;
    private TypeSerializer<GraphElement> graphElementTypeSerializer;
    private TypeSerializer<BaseOperatorEvent> operatorEventTypeSerializer;
    private TypeSerializer<Short> partIdTypeSerializer;
    private TypeSerializer<Long> timestampTypeSerializer;

    public GraphOpSerializer(TypeSerializer<?>[] fieldSerializers, ExecutionConfig config) {
        this.config = config;
        Field[] fields = GraphOp.class.getFields();
        this.fieldSerializers = fieldSerializers;
        for (int i = 0; i < fields.length; i++) {
            if (fields[i].getName().equals("partId"))
                partIdTypeSerializer = (TypeSerializer<Short>) fieldSerializers[i];
            if (fields[i].getName().equals("element"))
                graphElementTypeSerializer = (TypeSerializer<GraphElement>) fieldSerializers[i];
            if (fields[i].getName().equals("operatorEvent"))
                operatorEventTypeSerializer = (TypeSerializer<BaseOperatorEvent>) fieldSerializers[i];
            if (fields[i].getName().equals("ts")) timestampTypeSerializer = (TypeSerializer<Long>) fieldSerializers[i];
        }

    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<GraphOp> duplicate() {
        TypeSerializer<Object>[] duplicateFieldSerializers = duplicateSerializers(fieldSerializers);
        return new GraphOpSerializer(duplicateFieldSerializers, config);
    }

    @SuppressWarnings("unchecked")
    private TypeSerializer<Object>[] duplicateSerializers(TypeSerializer<?>[] serializers) {
        boolean stateful = false;
        TypeSerializer<?>[] duplicateSerializers = new TypeSerializer[serializers.length];

        for (int i = 0; i < serializers.length; i++) {
            duplicateSerializers[i] = serializers[i].duplicate();
            if (duplicateSerializers[i] != serializers[i]) {
                // at least one of them is stateful
                stateful = true;
            }
        }

        if (!stateful) {
            // as a small memory optimization, we can share the same object between instances
            duplicateSerializers = serializers;
        }
        return (TypeSerializer<Object>[]) duplicateSerializers;
    }

    @Override
    public GraphOp createInstance() {
        return new GraphOp();
    }

    @Override
    public GraphOp copy(GraphOp from) {
        GraphOp copy = from.copy();
        if (copy.element != null) graphElementTypeSerializer.copy(copy.element);
        if (copy.operatorEvent != null) operatorEventTypeSerializer.copy(copy.operatorEvent);
        return copy;
    }

    @Override
    public GraphOp copy(GraphOp from, GraphOp reuse) {
        if (reuse == null) return copy(from);
        reuse.setPartId(from.partId);
        reuse.setTimestamp(from.ts);
        reuse.setOp(from.op);
        reuse.setMessageCommunication(from.messageCommunication);
        if (from.element == null) reuse.element = null;
        else reuse.element = graphElementTypeSerializer.copy(from.element, reuse.element);
        if (from.operatorEvent == null) reuse.operatorEvent = null;
        else reuse.operatorEvent = operatorEventTypeSerializer.copy(from.operatorEvent, reuse.operatorEvent);
        return reuse;
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(GraphOp record, DataOutputView target) throws IOException {
        byte flag = 0x00;
        flag |= ((byte) record.op.ordinal()) << 5;
        flag |= ((byte) record.messageCommunication.ordinal()) << 4;
        if (record.partId != -1) flag |= 1 << 3;
        if (record.element != null) flag |= 1 << 2;
        if (record.ts != null) flag |= 1 << 1;
        if (record.operatorEvent != null) flag |= 1;
        target.write(flag);
        if (record.partId != -1) partIdTypeSerializer.serialize(record.partId, target);
        if (record.element != null) graphElementTypeSerializer.serialize(record.element, target);
        if (record.ts != null) timestampTypeSerializer.serialize(record.ts, target);
        if (record.operatorEvent != null) operatorEventTypeSerializer.serialize(record.operatorEvent, target);
    }

    @Override
    public GraphOp deserialize(DataInputView source) throws IOException {
        byte flag = source.readByte();
        int OpOrdinal = (flag & 0xe0) >> 5;
        int messageCommunicationOrdinal = (flag & 0x10) >> 4;
        boolean hasPartId = (flag & 0x08) >> 3 == 1;
        boolean hasElement = (flag & 0x04) >> 2 == 1;
        boolean hasTs = (flag & 0x02) >> 1 == 1;
        boolean hasOpEvent = (flag & 0x01) == 1;
        short partId = -1;
        GraphElement el = null;
        Long ts = null;
        BaseOperatorEvent event = null;
        if (hasPartId) partId = partIdTypeSerializer.deserialize(source);
        if (hasElement) el = graphElementTypeSerializer.deserialize(source);
        if (hasTs) ts = timestampTypeSerializer.deserialize(source);
        if (hasOpEvent) event = operatorEventTypeSerializer.deserialize(source);
        return new GraphOp(OPS[OpOrdinal], partId, el, event, MESSAGE_COMMUNICATIONS[messageCommunicationOrdinal], ts);

    }

    @Override
    public GraphOp deserialize(GraphOp reuse, DataInputView source) throws IOException {
        if (reuse == null) return deserialize(source);
        byte flag = source.readByte();
        int OpOrdinal = (flag & 0xe0) >> 5;
        int messageCommunicationOrdinal = (flag & 0x10) >> 4;
        boolean hasPartId = (flag & 0x08) >> 3 == 1;
        boolean hasElement = (flag & 0x04) >> 2 == 1;
        boolean hasTs = (flag & 0x02) >> 1 == 1;
        boolean hasOpEvent = (flag & 0x01) == 1;
        Short partId = null;
        GraphElement el = null;
        Long ts = null;
        BaseOperatorEvent event = null;
        if (hasPartId) partId = partIdTypeSerializer.deserialize(source);
        if (hasElement) el = graphElementTypeSerializer.deserialize(source);
        if (hasTs) ts = timestampTypeSerializer.deserialize(source);
        if (hasOpEvent) event = operatorEventTypeSerializer.deserialize(source);
        reuse.setOperatorEvent(event);
        reuse.setOp(OPS[OpOrdinal]);
        reuse.setElement(el);
        reuse.setTimestamp(ts);
        reuse.setMessageCommunication(MESSAGE_COMMUNICATIONS[messageCommunicationOrdinal]);
        reuse.setPartId(partId);
        return reuse;
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        byte flag = source.readByte();
        boolean hasPartId = (flag & 0x08) >> 3 == 1;
        boolean hasElement = (flag & 0x04) >> 2 == 1;
        boolean hasTs = (flag & 0x02) >> 1 == 1;
        boolean hasOpEvent = (flag & 0x01) == 1;
        target.write(flag);
        if (hasPartId) partIdTypeSerializer.copy(source, target);
        if (hasElement) graphElementTypeSerializer.copy(source, target);
        if (hasTs) timestampTypeSerializer.copy(source, target);
        if (hasOpEvent) operatorEventTypeSerializer.copy(source, target);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof GraphOpSerializer) {
            GraphOpSerializer other = (GraphOpSerializer) obj;
            return Arrays.equals(fieldSerializers, other.fieldSerializers);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return 31
                * (31 * Arrays.hashCode(fieldSerializers));
    }

    @Override
    public TypeSerializerSnapshot<GraphOp> snapshotConfiguration() {
        return null;
    }
}
