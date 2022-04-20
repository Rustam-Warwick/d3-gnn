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
    IterationType iterationType = null;
    List<Short> destinations = new ArrayList<>();
    Object[] args = null;
    Boolean hasUpdate = null;

    public RemoteInvoke toElement(String elementId, ElementType elementType) {
        this.elementId = elementId;
        this.elementType = elementType;
        return this;
    }

    public RemoteInvoke method(String methodName) {
        this.methodName = methodName;
        return this;
    }

    public RemoteInvoke where(IterationType iterationType) {
        this.iterationType = iterationType;
        return this;
    }

    public RemoteInvoke withArgs(Object... args) {
        this.args = args;
        return this;
    }

    public RemoteInvoke toDestination(Short dest) {
        destinations.add(dest);
        return this;
    }

    public RemoteInvoke toDestinations(List<Short> dests) {
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

    public boolean verify() {
        return Objects.nonNull(elementId) && Objects.nonNull(elementType) && Objects.nonNull(methodName) && Objects.nonNull(iterationType) && Objects.nonNull(destinations) && Objects.nonNull(args) && Objects.nonNull(hasUpdate);
    }

    private List<GraphOp> build() {
        if (!verify()) {
            System.out.println("Error occured in builder");
            return Collections.emptyList();
        }
        Rmi message = new Rmi(elementId, methodName, args, elementType, hasUpdate);
        return destinations.stream().map(item -> (
                new GraphOp(Op.RMI, item, message, iterationType)
        )).collect(Collectors.toList());
    }

    public void buildAndRun(BaseStorage storage) {
        List<GraphOp> graphOps = build();
        for (GraphOp a : graphOps) {
            if (a.part_id == storage.layerFunction.getCurrentPart()) {
                Rmi.execute(storage.getElement(elementId, elementType), (Rmi) a.element);
            } else {
                storage.layerFunction.message(a);
            }
        }
    }
}
