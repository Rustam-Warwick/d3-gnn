package elements;

import ai.djl.ndarray.LifeCycleControl;
import com.esotericsoftware.reflectasm.MethodAccess;
import elements.annotations.RemoteFunction;
import elements.enums.CopyContext;
import elements.enums.ElementType;
import elements.enums.MessageDirection;
import elements.enums.Op;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A special element for executing methods in other {@link GraphElement}
 */
public class Rmi extends GraphElement {

    /**
     * Log
     */
    private static final Logger LOG = LoggerFactory.getLogger(Rmi.class);
    /**
     * Cached remote methods of a specific class
     */
    public static ConcurrentHashMap<Class<?>, Tuple2<MethodAccess, HashMap<String, Integer>>> classRemoteMethods = new ConcurrentHashMap<>(1 << 4);
    /**
     * Method Arguments list
     */
    public Object[] args;

    /**
     * Type of element where this message is directed to
     */
    public ElementType elemType;

    /**
     * Id of element where this message is directed to
     */
    public String id;

    /**
     * Should RMI be followed with an update query
     */
    public boolean hasUpdate = true;

    /**
     * Name of the remote method to be called
     */
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

    public Rmi(Rmi element, CopyContext context) {
        super(element, context);
        args = element.args;
        elemType = element.elemType;
        id = element.id;
        hasUpdate = element.hasUpdate;
        methodName = element.methodName;
    }

    /**
     * Helper for caching all {@link RemoteFunction} of the given class with the {@link MethodAccess}
     */
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

    /**
     * Execute the RMI on {@link GraphElement}
     */
    public static void execute(GraphElement element, String methodName, boolean hasUpdate, Object... args) {
        try {
            cacheClassIfNotExists(element.getClass()); // Cache MethodHandles of all elements of the given class
            if (hasUpdate) {
                GraphElement deepCopyElement = element.copy(CopyContext.RMI); // Creates an RMI copy of the element
                Tuple2<MethodAccess, HashMap<String, Integer>> tmp = classRemoteMethods.get(element.getClass());
                tmp.f0.invoke(deepCopyElement, tmp.f1.get(methodName), args);
                getStorage().runCallback(element.update(deepCopyElement).f0);
            } else {
                Tuple2<MethodAccess, HashMap<String, Integer>> tmp = classRemoteMethods.get(element.getClass());
                tmp.f0.invoke(element, tmp.f1.get(methodName), args);
            }
        } catch (Throwable e) {
            LOG.error(ExceptionUtils.stringifyException(e));
        }
    }


    /**
     * Executes if intended for this part or sends a {@link GraphOp}
     */
    public static void buildAndRun(String id, ElementType elemType, String methodName, boolean hasUpdate, short destination, MessageDirection messageDirection, Object... args) {
        if (destination == getStorage().layerFunction.getCurrentPart() && messageDirection == MessageDirection.ITERATE) {
            Rmi.execute(getStorage().getElement(id, elemType), methodName, hasUpdate, args);
        } else {
            getStorage().layerFunction.message(new GraphOp(Op.RMI, destination, new Rmi(id, methodName, elemType, args, hasUpdate)), messageDirection);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Rmi copy(CopyContext context) {
        return new Rmi(this, context);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delay() {
        super.delay();
        for (Object arg : args) {
            if (arg instanceof LifeCycleControl) ((LifeCycleControl) arg).delay();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void resume() {
        super.resume();
        for (Object arg : args) {
            if (arg instanceof LifeCycleControl) ((LifeCycleControl) arg).resume();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ElementType getType() {
        return this.elemType;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getId() {
        return id;
    }

    /**
     * {@inheritDoc}
     */
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
