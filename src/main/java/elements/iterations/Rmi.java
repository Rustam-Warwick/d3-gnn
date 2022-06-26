package elements.iterations;

import elements.ElementType;
import elements.GraphElement;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;

public class Rmi extends GraphElement {
    public static transient ConcurrentHashMap<Class<?>, ConcurrentHashMap<String, MethodHandle>> classRemoteMethods = new ConcurrentHashMap<>(15);
    public static transient MethodHandles.Lookup globalLookup = MethodHandles.lookup();
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
            ConcurrentHashMap<String, MethodHandle> thisClassMethods = new ConcurrentHashMap<>(5);
            Method[] methods = clazz.getMethods();
            for (Method method : methods) {
                if (method.isAnnotationPresent(RemoteFunction.class)) {
                    try {
                        MethodType mt = MethodType.methodType(method.getReturnType(), method.getParameterTypes());
                        MethodHandle mtHandle = globalLookup.findVirtual(clazz, method.getName(), mt);
                        thisClassMethods.put(method.getName(), mtHandle);
                    } catch (NoSuchMethodException | IllegalAccessException e) {
                        e.printStackTrace();
                    }
                }
            }
            classRemoteMethods.put(clazz, thisClassMethods);
        }
    }

    public static Object[] generateVarargs(Object[] args, GraphElement el) {
        Object[] newArgs = new Object[args.length + 1];
        newArgs[0] = el;
        System.arraycopy(args, 0, newArgs, 1, args.length);
        return newArgs;
    }

    public static void execute(GraphElement element, Rmi message) {
        try {
            cacheClassIfNotExists(element.getClass()); // Cache MethodHandles of all elements of the given class
            if (message.hasUpdate) {
                GraphElement deepCopyElement = element.deepCopy(); // Creates a full copy of the element
                deepCopyElement.setTimestamp(message.getTimestamp()); // Replace element timestamp with model timestamp
                MethodHandle method = classRemoteMethods.get(element.getClass()).get(message.methodName);
                Object[] args = generateVarargs(message.args, deepCopyElement);
                method.invokeWithArguments(args);
                element.update(deepCopyElement);

            } else {
                MethodHandle method = classRemoteMethods.get(element.getClass()).get(message.methodName);
                Object[] args = generateVarargs(message.args, element);
                method.invokeWithArguments(args);
            }

        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    @Override
    public ElementType elementType() {
        return this.elemType;
    }
}
