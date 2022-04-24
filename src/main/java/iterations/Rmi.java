package iterations;

import elements.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class Rmi extends GraphElement {
    public Object[] args;
    public ElementType elemType;
    public boolean hasUpdate = true;
    public String methodName;

    public Rmi() {
        super();
    }

    public Rmi(String id, String methodName, Object[] args, ElementType elemType) {
        super(id);
        this.args = args;
        this.methodName = methodName;
        this.elemType = elemType;
    }

    public Rmi(String id, String methodName, Object[] args, ElementType elemType, boolean hasUpdate) {
        super(id);
        this.args = args;
        this.methodName = methodName;
        this.elemType = elemType;
        this.hasUpdate = hasUpdate;
    }

    public static void execute(GraphElement element, Rmi message) {
        try {
            if (message.hasUpdate) {
                GraphElement deepCopyElement = element.deepCopy();
                Method method = Arrays.stream(deepCopyElement.getClass().getMethods()).filter(item -> item.isAnnotationPresent(RemoteFunction.class) && item.getName().equals(message.methodName)).findFirst().get();
                method.invoke(deepCopyElement, message.args);
                element.update(deepCopyElement);

            } else {
                Method method = Arrays.stream(element.getClass().getMethods()).filter(item -> item.isAnnotationPresent(RemoteFunction.class) && item.getName().equals(message.methodName)).findFirst().get();
                method.invoke(element, message.args);
            }

        } catch (InvocationTargetException | IllegalAccessException e) {
            e.printStackTrace();
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
        Rmi rmi = new Rmi(el.getId(), methodName, args, el.elementType(), false);
        rmi.setStorage(el.storage);
        if (el.state() == ReplicaState.MASTER) {
            Rmi.execute(el, rmi);
        } else {
            rmi.storage.layerFunction.message(new GraphOp(Op.RMI, el.masterPart(), rmi, IterationType.ITERATE));
        }
    }

    public static void callProcedure(GraphElement el, String methodName, IterationType iterationType, RemoteDestination remoteDestination, Object... args) {
        Rmi rmi = new Rmi(el.getId(), methodName, args, el.elementType(), false);
        rmi.setStorage(el.storage);
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
        callProcedure(el, methodName, iterationType, destinations, args);
    }

    public static void callProcedure(GraphElement el, String methodName, IterationType iterationType, List<Short> destinations, Object... args) {
        if (Objects.isNull(destinations)) return;
        Rmi rmi = new Rmi(el.getId(), methodName, args, el.elementType(), false);
        rmi.setStorage(el.storage);
        for (Short part : destinations) {
            if (part == el.getPartId() && iterationType == IterationType.ITERATE) {
                Rmi.execute(el, rmi);
            } else {
                rmi.storage.layerFunction.message(new GraphOp(Op.RMI, part, rmi, iterationType));
            }
        }
    }

    @Override
    public ElementType elementType() {
        return this.elemType;
    }
}
