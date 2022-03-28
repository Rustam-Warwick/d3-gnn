package functions;

import elements.*;
import iterations.IterationState;
import iterations.Rpc;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import storage.HashMapStorage;

import java.util.Objects;

public class GraphProcessFn extends HashMapStorage {
    @Override
    public void process(GraphOp value) {
        switch (value.op) {
            case COMMIT:
                GraphElement thisElement = this.getElement(value.element);
                if (Objects.isNull(thisElement)) {
                    if (!this.isLast() && (value.element.elementType() == ElementType.EDGE || value.element.elementType() == ElementType.VERTEX)) {
                        this.message(new GraphOp(Op.COMMIT, this.currentKey, value.element.copy(), IterationState.FORWARD));
                    }
                    value.element.setStorage(this);
                    value.element.createElement();
                } else {
                    thisElement.externalUpdate(value.element);
                }
                break;
            case SYNC:
                GraphElement el = this.getElement(value.element);
                if (Objects.isNull(el)) {
                    if (!this.isLast() && (value.element.elementType() == ElementType.EDGE || value.element.elementType() == ElementType.VERTEX)) {
                        this.message(new GraphOp(Op.COMMIT, this.currentKey, value.element.copy(), IterationState.FORWARD));
                    }
                    el = value.element.copy();
                    el.setStorage(this);
                    el.createElement();
                    if (el.state() == ReplicaState.MASTER) el.syncElement(value.element);
                } else {
                    el.syncElement(value.element);
                }
                break;
            case RPC:
                GraphElement rpcElement = this.getElement(value.element.getId(), value.element.elementType());
                Rpc.execute(rpcElement, (Rpc) value.element);
                break;
        }
    }

}
