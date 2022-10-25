package elements.iterations;

import ai.djl.ndarray.ObjectPoolControl;
import elements.ElementType;
import elements.GraphElement;
import operators.BaseWrapperOperator;
import org.apache.flink.util.ExceptionUtils;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

public class Rmi extends GraphElement {
    public static ConcurrentHashMap<Class<?>, ConcurrentHashMap<String, MethodHandle>> classRemoteMethods = new ConcurrentHashMap<>(1 << 4);
    public static MethodHandles.Lookup globalLookup = MethodHandles.lookup();
    public Object[] args;
    public ElementType elemType;
    public Boolean hasUpdate = true;
    public String methodName;

    public String id;

    public Rmi() {
        super();
    }

    @Override
    public GraphElement copy() {
        throw new IllegalStateException("RMI Not copied");
    }

    @Override
    public GraphElement deepCopy() {
        throw new IllegalStateException("RMI Not copied");
    }

    public Rmi(String id, String methodName, Object[] args, ElementType elemType, boolean hasUpdate, Long ts) {
        this.id = id;
        this.args = args;
        this.methodName = methodName;
        this.elemType = elemType;
        this.hasUpdate = hasUpdate;
    }

    public static void cacheClassIfNotExists(Class<?> clazz) {
        if (!classRemoteMethods.containsKey(clazz)) {
            ConcurrentHashMap<String, MethodHandle> thisClassMethods = new ConcurrentHashMap<>(1 << 3);
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
            BaseWrapperOperator.LOG.error(ExceptionUtils.stringifyException(e));
            BaseWrapperOperator.LOG.error(message.toString());
        }
    }

    @Override
    public void delay() {
        super.delay();
        for (Object arg : args) {
            if(arg instanceof ObjectPoolControl) ((ObjectPoolControl) arg).delay();
        }
    }
    @Override
    public void resume() {
        super.resume();
        for (Object arg : args) {
            if(arg instanceof ObjectPoolControl) ((ObjectPoolControl) arg).resume();
        }
    }

    @Override
    public ElementType elementType() {
        return this.elemType;
    }

    @Override
    public String getId() {
        return id;
    }
    @Override
    public String toString() {
        return "Rmi{" +
                ", args=" + Arrays.toString(args) +
                ", elemType=" + elemType +
                ", hasUpdate=" + hasUpdate +
                ", methodName='" + methodName + '\'' +
                '}';
    }
}
