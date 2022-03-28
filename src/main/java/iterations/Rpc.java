package iterations;

import elements.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class Rpc extends GraphElement {
    public Object[] args;
    public ElementType elemType;
    public boolean hasUpdate = true;
    public String methodName;

    public Rpc() {
        super();
    }

    public Rpc(String id, String methodName, Object[] args, ElementType elemType) {
        super(id);
        this.args = args;
        this.methodName = methodName;
        this.elemType = elemType;
    }

    public Rpc(String id, String methodName, Object[] args, ElementType elemType, boolean hasUpdate) {
        super(id);
        this.args = args;
        this.methodName = methodName;
        this.elemType = elemType;
        this.hasUpdate = hasUpdate;
    }

    public static void execute(GraphElement element, Rpc message) {
        try {
            if (message.hasUpdate) {
                GraphElement deepCopyElement = element.deepCopy();
                Method method = Arrays.stream(deepCopyElement.getClass().getMethods()).filter(item -> item.isAnnotationPresent(RemoteFunction.class) && item.getName().equals(message.methodName)).findFirst().get();
                method.invoke(deepCopyElement, message.args);
                element.externalUpdate(deepCopyElement);

            } else {
                Method method = Arrays.stream(element.getClass().getMethods()).filter(item -> item.isAnnotationPresent(RemoteFunction.class) && item.getName().equals(message.methodName)).findFirst().get();
                method.invoke(element, message.args);
            }

        } catch (InvocationTargetException | IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    /**
     * Send Update RPC Message to master of this element
     *
     * @param el
     * @param methodName
     * @param args
     */
    public static void call(GraphElement el, String methodName, Object... args) {
        Rpc rpc = new Rpc(el.getId(), methodName, args, el.elementType());
        rpc.setStorage(el.storage);
        if (el.state() == ReplicaState.MASTER) {
            Rpc.execute(el, rpc);
        } else {
            rpc.storage.message(new GraphOp(Op.RPC, el.masterPart(), rpc, IterationState.ITERATE));
        }
    }

    /**
     * Send Procedure RPC Message to master of this element
     *
     * @param el
     * @param methodName
     * @param args
     */
    public static void callProcedure(GraphElement el, String methodName, Object... args) {
        Rpc rpc = new Rpc(el.getId(), methodName, args, el.elementType(), false);
        rpc.setStorage(el.storage);
        if (el.state() == ReplicaState.MASTER) {
            Rpc.execute(el, rpc);
        } else {
            rpc.storage.message(new GraphOp(Op.RPC, el.masterPart(), rpc, IterationState.ITERATE));
        }
    }

    public static void callProcedure(GraphElement el, String methodName, IterationState iterationState, RemoteDestination remoteDestination, Object... args) {
        Rpc rpc = new Rpc(el.getId(), methodName, args, el.elementType(), false);
        rpc.setStorage(el.storage);
        ArrayList<Short> destinations = new ArrayList<>();
        switch (remoteDestination) {
            case SELF:
                destinations.add(el.getPartId());
                break;
            case MASTER:
                destinations.add(el.masterPart());
                break;
            case REPLICAS:
                destinations.addAll(el.replicaParts());
                break;
            case ALL:
                destinations.addAll(el.replicaParts());
                destinations.add(el.masterPart());
        }
        callProcedure(el, methodName, iterationState, destinations, args);
    }

    public static void callProcedure(GraphElement el, String methodName, IterationState iterationState, List<Short> destinations, Object... args) {
        if (Objects.isNull(destinations)) return;
        Rpc rpc = new Rpc(el.getId(), methodName, args, el.elementType(), false);
        rpc.setStorage(el.storage);
        for (Short part : destinations) {
            if (part == el.getPartId() && iterationState == IterationState.ITERATE) {
                Rpc.execute(el, rpc);
            } else {
                rpc.storage.message(new GraphOp(Op.RPC, part, rpc, iterationState));
            }
        }
    }

    @Override
    public ElementType elementType() {
        return this.elemType;
    }
}
