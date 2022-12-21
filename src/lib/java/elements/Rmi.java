package elements;

import ai.djl.ndarray.LifeCycleControl;
import com.esotericsoftware.reflectasm.MethodAccess;
import elements.annotations.RemoteFunction;
import elements.enums.CopyContext;
import elements.enums.ElementType;
import elements.enums.Op;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.graph.OutputTags;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.OutputTag;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * A special element for executing methods in other {@link GraphElement}
 */
public class Rmi extends GraphElement {

    /**
     * Cached remote methods of a specific class
     * <strong>
     * Class -> (Method Access, MethodName -> (Method Index, HasUpdate, hasCallback))
     * </strong>
     */
    public static final Map<Class<?>, Tuple2<MethodAccess, HashMap<String, Tuple2<Integer, Boolean>>>> classRemoteMethods = new NonBlockingHashMap<>(1 << 4);
    /**
     * Log
     */
    private static final Logger LOG = LoggerFactory.getLogger(Rmi.class);
    /**
     * Method Arguments list
     */
    public Object[] args;

    /**
     * Type of element where this output is directed to
     */
    public ElementType elemType;

    /**
     * ID of element where this output is directed to
     */
    public Object id;

    /**
     * Name of the remote method to be called
     */
    public String methodName;

    public Rmi() {
        super();
    }

    public Rmi(Object id, String methodName, ElementType elemType, Object[] args) {
        this.id = id;
        this.args = args;
        this.methodName = methodName;
        this.elemType = elemType;
    }

    public Rmi(Rmi element, CopyContext context) {
        super(element, context);
        args = element.args;
        elemType = element.elemType;
        id = element.id;
        methodName = element.methodName;
    }

    /**
     * Helper for caching all {@link RemoteFunction} of the given class with the {@link MethodAccess}
     */
    private static Tuple2<MethodAccess, HashMap<String, Tuple2<Integer, Boolean>>> getClassRemoteMethods(Class<?> clazz) {
        MethodAccess tmp = MethodAccess.get(clazz);
        HashMap<String, Tuple2<Integer, Boolean>> classMethodIds = new HashMap<>(1 << 3);
        Method[] methods = clazz.getMethods();
        for (Method method : methods) {
            if (method.isAnnotationPresent(RemoteFunction.class)) {
                boolean isUpdateMethod = method.getAnnotation(RemoteFunction.class).triggerUpdate();
                classMethodIds.put(method.getName(), Tuple2.of(tmp.getIndex(method.getName()), isUpdateMethod));
            }
        }
        return Tuple2.of(tmp, classMethodIds);
    }

    /**
     * Execute the RMI on {@link GraphElement}
     */
    public static void execute(GraphElement element, String methodName, Object... args) {
        try {
            classRemoteMethods.computeIfAbsent(element.getClass(), Rmi::getClassRemoteMethods); // Cache MethodHandles of all elements of the given class
            Tuple2<MethodAccess, HashMap<String, Tuple2<Integer, Boolean>>> classMethods = classRemoteMethods.get(element.getClass());
            Tuple2<Integer, Boolean> method = classMethods.f1.get(methodName);
            if (method.f1) {
                GraphElement deepCopyElement = element.copy(CopyContext.RMI); // Creates an RMI copy of the element
                classMethods.f0.invoke(deepCopyElement, method.f0, args);
                element.update(deepCopyElement);
            } else {
                classMethods.f0.invoke(element, method.f0, args);
            }
        } catch (Throwable e) {
            LOG.error(ExceptionUtils.stringifyException(e));
        }
    }

    /**
     * Executes if intended for this part or sends a {@link GraphOp}
     */
    public static void buildAndRun(Object id, ElementType elemType, String methodName, short destination, OutputTag<GraphOp> messageDirection, Object... args) {
        if (destination == getGraphRuntimeContext().getCurrentPart() && messageDirection == OutputTags.ITERATE_OUTPUT_TAG) {
            execute(getGraphRuntimeContext().getStorage().getElement(id, elemType), methodName, args);
        } else {
            getGraphRuntimeContext().output(new GraphOp(Op.RMI, destination, new Rmi(id, methodName, elemType, args)), messageDirection);
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
        for (Object arg : args) {
            if (arg instanceof LifeCycleControl) ((LifeCycleControl) arg).delay();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void resume() {
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
    public Object getId() {
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
                ", methodName='" + methodName + '\'' +
                '}';
    }
}
