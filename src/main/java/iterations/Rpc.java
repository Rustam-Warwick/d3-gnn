package iterations;

import elements.GraphElement;
import elements.GraphOp;
import elements.Op;

public class Rpc extends GraphElement {
    public Object[] args;
    public String methodName;
    public Rpc() {

    }

    public Rpc(String id, String methodName, Object[] args) {
        super(id);
        this.args = args;
        this.methodName = methodName;
    }

    public static void call(GraphElement el, String methodName, Object ...args){
        Rpc rpc = new Rpc(el.getId(), methodName, args);
        rpc.setStorage(el.storage);
        rpc.storage.message(new GraphOp(Op.RPC,el.masterPart(), rpc, ));
    }





}
