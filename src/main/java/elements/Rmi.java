package elements;

import ai.djl.ndarray.ObjectPoolControl;
import com.esotericsoftware.reflectasm.MethodAccess;
import elements.annotations.RemoteFunction;
import elements.enums.CopyContext;
import elements.enums.ElementType;
import elements.enums.MessageDirection;
import elements.enums.Op;
import operators.BaseWrapperOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.ExceptionUtils;
import storage.BaseStorage;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Rmi is a GraphElement type to execute some remote functions in other GraphElements such as {@link features.Tensor}
 */
public class Rmi extends GraphElement {
    public static ConcurrentHashMap<Class<?>, Tuple2<MethodAccess, HashMap<String, Integer>>> classRemoteMethods = new ConcurrentHashMap<>(1 << 4);

    public Object[] args;

    public ElementType elemType;

    public String id;

    public boolean hasUpdate = true;

    public String methodName;

    public Rmi() {
        super();
    }

    public Rmi(String id, String methodName, ElementType elemType, Object[] args, boolean hasUpdate) {
        this.id = id;
        this.args = args;
        this.methodName = methodName;
        this.elemType = elemType;
        this.hasUpdate = hasUpdate;
    }

    public static void cacheClassIfNotExists(Class<?> clazz) {
        if (!classRemoteMethods.containsKey(clazz)) {
            MethodAccess tmp = MethodAccess.get(clazz);
            HashMap<String, Integer> classMethodIds = new HashMap<>(1 << 3);
            Method[] methods = clazz.getMethods();
            for (Method method : methods) {
                if (method.isAnnotationPresent(RemoteFunction.class)) {
                    classMethodIds.put(method.getName(), tmp.getIndex(method.getName()));
                }
            }
            classRemoteMethods.put(clazz, Tuple2.of(tmp, classMethodIds));
        }
    }

    public static void execute(GraphElement element, Rmi message) {
        try {
            cacheClassIfNotExists(element.getClass()); // Cache MethodHandles of all elements of the given class
            if (message.hasUpdate) {
                GraphElement deepCopyElement = element.copy(CopyContext.RMI); // Creates an RMI copy of the element
                Tuple2<MethodAccess, HashMap<String, Integer>> tmp = classRemoteMethods.get(element.getClass());
                tmp.f0.invoke(deepCopyElement, tmp.f1.get(message.methodName), message.args);
                element.update(deepCopyElement);
            } else {
                Tuple2<MethodAccess, HashMap<String, Integer>> tmp = classRemoteMethods.get(element.getClass());
                tmp.f0.invoke(element, tmp.f1.get(message.methodName), message.args);
            }
        } catch (Throwable e) {
            BaseWrapperOperator.LOG.error(ExceptionUtils.stringifyException(e));
            BaseWrapperOperator.LOG.error(message.toString());
        }
    }

    public static void buildAndRun(Rmi rmi, BaseStorage storage, List<Short> destinations, MessageDirection messageDirection) {
        for (Short destination : destinations) {
            if (destination.equals(storage.layerFunction.getCurrentPart()) && messageDirection == MessageDirection.ITERATE) {
                Rmi.execute(storage.getElement(rmi.getId(), rmi.elementType()), rmi);
            } else {
                storage.layerFunction.message(new GraphOp(Op.RMI, destination, rmi), messageDirection);
            }
        }
    }

    public static void buildAndRun(Rmi rmi, BaseStorage storage, short destination, MessageDirection messageDirection) {
        if (destination == storage.layerFunction.getCurrentPart() && messageDirection == MessageDirection.ITERATE) {
            Rmi.execute(storage.getElement(rmi.getId(), rmi.elementType()), rmi);
        } else {
            storage.layerFunction.message(new GraphOp(Op.RMI, destination, rmi), messageDirection);
        }
    }

    @Override
    public GraphElement copy(CopyContext context) {
        throw new IllegalStateException("RMI Not copied");
    }

    @Override
    public void delay() {
        super.delay();
        for (Object arg : args) {
            if (arg instanceof ObjectPoolControl) ((ObjectPoolControl) arg).delay();
        }
    }

    @Override
    public void resume() {
        super.resume();
        for (Object arg : args) {
            if (arg instanceof ObjectPoolControl) ((ObjectPoolControl) arg).resume();
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
