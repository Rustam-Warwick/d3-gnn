package elements.iterations;

import elements.ElementType;
import elements.GraphElement;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;

public class Rmi extends GraphElement {
    public Object[] args;
    public ElementType elemType;
    public Boolean hasUpdate = true;
    public String methodName;

    public Rmi() {
        super();
    }

    public Rmi(String id, String methodName, Object[] args, ElementType elemType, boolean hasUpdate, Long ts) {
        super(id);
        this.args = args;
        this.methodName = methodName;
        this.elemType = elemType;
        this.hasUpdate = hasUpdate;
        this.ts = ts;
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

    @Override
    public ElementType elementType() {
        return this.elemType;
    }
}
