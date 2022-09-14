package elements.iterations;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
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
import java.util.function.Consumer;

public class Rmi extends GraphElement {
    public static transient ConcurrentHashMap<Class<?>, ConcurrentHashMap<String, MethodHandle>> classRemoteMethods = new ConcurrentHashMap<>(1 << 4);
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
            BaseWrapperOperator.LOG.error(ExceptionUtils.stringifyException(e));
        }
    }

    @Override
    public void applyForNDArrays(Consumer<NDArray> operation) {
        super.applyForNDArrays(operation);
        for (Object arg : args) {
            if (arg instanceof NDArray) {
                operation.accept((NDArray) arg);
            }else if(arg instanceof NDList){
                NDList tmp = (NDList) arg;
                for (NDArray ndArray : tmp) {
                    operation.accept(ndArray);
                }
            }
        }
    }

    @Override
    public String toString() {
        return "Rmi{" +
                "id='" + id + '\'' +
                ", args=" + Arrays.toString(args) +
                ", elemType=" + elemType +
                ", hasUpdate=" + hasUpdate +
                ", methodName='" + methodName + '\'' +
                '}';
    }

    @Override
    public ElementType elementType() {
        return this.elemType;
    }
}
