package iterations;

import elements.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;

public class Rpc extends GraphElement {
    public Object[] args;
    public ElementType elemType;
    public boolean hasUpdate = true;
    public String methodName;
    public Rpc() {
        super();
    }

    public Rpc(String id, String methodName, Object[] args, ElementType elemType) {
        super(id);
        this.args = args;
        this.methodName = methodName;
        this.elemType = elemType;
    }
    public Rpc(String id, String methodName, Object[] args, ElementType elemType, boolean hasUpdate) {
        super(id);
        this.args = args;
        this.methodName = methodName;
        this.elemType = elemType;
        this.hasUpdate = hasUpdate;
    }

    public static void execute(GraphElement element, Rpc message){
        try {
            if(message.hasUpdate){
                GraphElement deepCopyElement = element.deepCopy();
                Method method = Arrays.stream(deepCopyElement.getClass().getMethods()).filter(item-> item.getName().equals(message.methodName)).findFirst().get();
                method.invoke(deepCopyElement, message.args);
                element.externalUpdate(deepCopyElement);

            }else{
                Method method = element.getClass().getMethod(message.methodName);
                method.invoke(element, message.args);
            }

        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    public static void call(GraphElement el, String methodName, Object ...args){
        Rpc rpc = new Rpc(el.getId(), methodName, args, el.elementType());
        rpc.setStorage(el.storage);
        if(el.state() == ReplicaState.MASTER){
            Rpc.execute(el, rpc);
        }
        else{
            rpc.storage.message(new GraphOp(Op.RPC,el.masterPart(), rpc, IterationState.ITERATE));
        }
    }

    public static void callProcedure(GraphElement el, String methodName, Object ...args){
        Rpc rpc = new Rpc(el.getId(), methodName, args, el.elementType(),false);
        rpc.setStorage(el.storage);
        if(el.state() == ReplicaState.MASTER){
            Rpc.execute(el, rpc);
        }
        else{
            rpc.storage.message(new GraphOp(Op.RPC,el.masterPart(), rpc, IterationState.ITERATE));
        }
    }

    @Override
    public ElementType elementType() {
        return this.elemType;
    }
}
