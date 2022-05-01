package iterations;

import elements.ElementType;
import elements.GraphOp;
import elements.Op;
import storage.BaseStorage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class RemoteInvoke {
    String elementId = null;
    ElementType elementType = null;
    String methodName = null;
    MessageDirection messageDirection = null;
    List<Short> destinations = new ArrayList<>();
    Object[] args = null;
    Boolean hasUpdate = null;
    int ts = Integer.MIN_VALUE;

    public RemoteInvoke toElement(String elementId, ElementType elementType) {
        this.elementId = elementId;
        this.elementType = elementType;
        return this;
    }

    public RemoteInvoke method(String methodName) {
        this.methodName = methodName;
        return this;
    }

    public RemoteInvoke where(MessageDirection messageDirection) {
        this.messageDirection = messageDirection;
        return this;
    }

    public RemoteInvoke withArgs(Object... args) {
        this.args = args;
        return this;
    }

    public RemoteInvoke addDestination(Short dest) {
        destinations.add(dest);
        return this;
    }

    public RemoteInvoke addDestinations(List<Short> dests) {
        this.destinations.addAll(dests);
        return this;
    }

    public RemoteInvoke hasUpdate() {
        this.hasUpdate = true;
        return this;
    }

    public RemoteInvoke noUpdate() {
        this.hasUpdate = false;
        return this;
    }

    public RemoteInvoke withTimestamp(int ts) {
        this.ts = ts;
        return this;
    }

    public boolean verify() {
        return Objects.nonNull(elementId) && Objects.nonNull(elementType) && Objects.nonNull(methodName) && Objects.nonNull(messageDirection) && Objects.nonNull(destinations) && Objects.nonNull(args) && Objects.nonNull(hasUpdate);
    }

    private List<GraphOp> build() {
        if (!verify()) {
            System.out.println("Error occured in builder");
            return Collections.emptyList();
        }
        Rmi message = new Rmi(elementId, methodName, args, elementType, hasUpdate);
        message.setTimestamp(this.ts);
        return destinations.stream().map(item -> (
                new GraphOp(Op.RMI, item, message, messageDirection, this.ts)
        )).collect(Collectors.toList());
    }

    public void buildAndRun(BaseStorage storage) {
        List<GraphOp> graphOps = build();
        for (GraphOp a : graphOps) {
            if (a.part_id == storage.layerFunction.getCurrentPart() && a.direction == MessageDirection.ITERATE) {
                Rmi.execute(storage.getElement(elementId, elementType), (Rmi) a.element);
            } else {
                storage.layerFunction.message(a);
            }
        }
    }
}
