package org.apache.flink.runtime.state.graph;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.PartNumber;
import org.apache.flink.runtime.state.internal.InternalKvState;

public class BaseGraphState<NS, ST> implements InternalKvState<PartNumber,NS,ST> {

    protected final transient TypeSerializer<PartNumber> keySerializer;

    protected final transient TypeSerializer<NS> nameSpaceSerializer;

    public BaseGraphState(GraphKeyedStateBackend<PartNumber> keyedStateBackend, TypeSerializer<NS> nameSpaceSerializer) {
        this.keySerializer = keyedStateBackend.getKeySerializer();
        this.nameSpaceSerializer = nameSpaceSerializer;
    }

    @Override
    public TypeSerializer<PartNumber> getKeySerializer() {
        return null;
    }

    @Override
    public TypeSerializer<NS> getNamespaceSerializer() {
        return null;
    }

    @Override
    public TypeSerializer<ST> getValueSerializer() {
        return null;
    }

    @Override
    public void setCurrentNamespace(NS namespace) {

    }

    @Override
    public byte[] getSerializedValue(byte[] serializedKeyAndNamespace, TypeSerializer<PartNumber> safeKeySerializer, TypeSerializer<NS> safeNamespaceSerializer, TypeSerializer<ST> safeValueSerializer) throws Exception {
        return new byte[0];
    }

    @Override
    public StateIncrementalVisitor<PartNumber, NS, ST> getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
        return null;
    }

    @Override
    public void clear() {

    }
}
