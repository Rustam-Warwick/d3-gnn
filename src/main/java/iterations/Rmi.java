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
                deepCopyElement.setTimestamp(message.getTimestamp()); // Replace element timestamp with model timestamp
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
