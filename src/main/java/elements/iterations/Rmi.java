package elements.iterations;

import elements.ElementType;
import elements.GraphElement;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;

public class Rmi extends GraphElement {
    public static transient ConcurrentHashMap<Class<?>, ConcurrentHashMap<String, Method>> classRemoteMethods = new ConcurrentHashMap<>(15);

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

    public static void cacheClassIfNotExists(Class<?> clazz) {
        if (!classRemoteMethods.containsKey(clazz)) {
            ConcurrentHashMap<String, Method> thisClassMethods = new ConcurrentHashMap<>(10);
            Method[] methods = clazz.getMethods();
            for (Method method : methods) {
                if (method.isAnnotationPresent(RemoteFunction.class)) thisClassMethods.put(method.getName(), method);
            }
            classRemoteMethods.put(clazz, thisClassMethods);
        }
    }

    public static void execute(GraphElement element, Rmi message) {
        try {
            cacheClassIfNotExists(element.getClass()); // Add cache to the concurrent map
            if (message.hasUpdate) {
                GraphElement deepCopyElement = element.deepCopy();
                deepCopyElement.setTimestamp(message.getTimestamp()); // Replace element timestamp with model timestamp
                Method method = classRemoteMethods.get(element.getClass()).get(message.methodName);
                method.invoke(deepCopyElement, message.args);
                element.update(deepCopyElement);

            } else {
                Method method = classRemoteMethods.get(element.getClass()).get(message.methodName);
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
