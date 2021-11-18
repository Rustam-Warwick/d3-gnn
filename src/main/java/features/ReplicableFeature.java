package features;

import types.GraphElement;
import types.GraphQuery;
import types.ReplicableGraphElement;
import types.SerialFunction;

import java.util.Arrays;
import java.util.Objects;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

abstract public class ReplicableFeature<T> extends Feature<T>{
    public volatile AtomicInteger lastModified = new AtomicInteger(0);
    public volatile T value;
    public transient CompletableFuture<T> fuzzyValue;
    public ReplicableGraphElement.STATE state;

    /**
     * Wraps UPDATE in GraphQuery
     * @param a
     * @return
     */
    public static GraphQuery prepareMessage(Update<?> a){
        return new GraphQuery(a).changeOperation(GraphQuery.OPERATORS.SYNC);
    }
    // CONSTRUCTORS
    public ReplicableFeature() {
        super();
        this.state = ReplicableGraphElement.STATE.NONE;
        this.value = null;
        this.fuzzyValue = null;
    }

    public ReplicableFeature(String fieldName) {
        super(fieldName);
        this.state = ReplicableGraphElement.STATE.NONE;
        this.value = null;
        this.fuzzyValue = null;
    }

    @Deprecated
    public ReplicableFeature(String fieldName, GraphElement element) {
        super(fieldName, element);
        this.value = null;
    }

    public ReplicableFeature(String fieldName, GraphElement element, T value){
        super(fieldName,element);
        this.value = value;
        this.fuzzyValue = new CompletableFuture<>();
        this.fuzzyValue.complete(this.value);// Complete if Master
        this.editHandler(null); // Sync messages with the master node if this is replica
    }


    /**
     * All functions should direct their lambda to this guy
     * Sending lambda over to master for editing
     * @param fn
     */
    public void editHandler(SerialFunction<ReplicableFeature<T>,Boolean> fn){
        if(this.state == ReplicableGraphElement.STATE.MASTER){
            // Need to execute and send the updates to replicas
            this.handleMasterRPC(fn);

        }
        else if(this.state== ReplicableGraphElement.STATE.REPLICA){
            // Need to get the fn to master and wait for message
            // @todo Do we need to wrap into .whenComplete of this.fuzzyValue???
            Update<T> newUpdate = new Update<T>(this,fn);
            newUpdate.setState(this.state);
            ReplicableGraphElement tmp = (ReplicableGraphElement) this.element;
            tmp.sendMessageToMaster(ReplicableFeature.prepareMessage(newUpdate));
            this.incompleteFuzzy();
        }
    }

    /**
     * Makes fuzzy value incomplete. Only used in REPLICA Features
     */
    public void incompleteFuzzy(){
        if(this.state== ReplicableGraphElement.STATE.MASTER)return; // Master cannot be fuzzy
        if(this.fuzzyValue==null || this.fuzzyValue.isDone()){
            // already done so need a new fuzzyValue
            this.fuzzyValue = new CompletableFuture<>();
            this.fuzzyValue.complete(this.value);
        }
    }
    public void completeFuzzy(){
        if(!this.fuzzyValue.isDone()){
            this.fuzzyValue.complete(this.value);
        }
    }


    /**
     * Returns an UPDATE Query that sends the state of current Feature
     * @return
     */
    public Update<T> prepareStateSync(){
        Update<T> newUpdate = new Update<T>(this,null);
        newUpdate.setLastModified(this.lastModified.get());
        newUpdate.setValue(this.value);
        newUpdate.setState(this.state);
        return newUpdate;
    }

    /**
     * Handle RPC(Remote Procedure Call) on the master Feature
     * @param fn
     */
    public void handleMasterRPC(SerialFunction<ReplicableFeature<T>,Boolean> fn) {
        if(Objects.isNull(fn))return;
        Boolean changed = fn.apply(this);
        if (changed) {
            // 1. Increment lastUpdateed
            this.lastModified.incrementAndGet();
            // 2. Compose state message and update the replica
            ((ReplicableGraphElement) element).sendMessageToReplicas(ReplicableFeature.prepareMessage(this.prepareStateSync()));
        }
    }

    /**
     * Handle new message coming from Master to Replica
     * @param msg
     */
    public void handleMasterMessage(Update<T> msg){
        if(msg.lastModified >= this.lastModified.get()){
            // Can be merged
            this.value = msg.value;
            this.lastModified.set(msg.lastModified);
            this.completeFuzzy();
        }
    }


    /**
     * External Update came to this feature, handle it
     * @param msg
     */
    @Override
    public void updateMessage(Update<T> msg) {
        if(this.state== ReplicableGraphElement.STATE.MASTER){
            if(Objects.isNull(msg.updateFn)){
                // There is no payload RPC, makes sense only if external update or sync request
                if(msg.state== ReplicableGraphElement.STATE.NONE){
                    // External message needs to be commited (Excenal Update)
                    // @todo add State replacer
                }else{
                    // This guy wants to get latest state of mine (Sync Request)
                    ((ReplicableGraphElement)element).sendMessage(ReplicableFeature.prepareMessage(this.prepareStateSync()),msg.partId);

                }
            }else{
                // There is a payload lambda RPC execute it!
                this.handleMasterRPC((SerialFunction<ReplicableFeature<T>, Boolean>) msg.updateFn);
            }
        }else if(this.state == ReplicableGraphElement.STATE.REPLICA){
            if(msg.state== ReplicableGraphElement.STATE.MASTER){
                // Replace the state if modifyCounter is less than incoming
                this.handleMasterMessage(msg);

            }else if(msg.state == ReplicableGraphElement.STATE.NONE){
                // Needs to be redirected to master. Updates are done in master. Why it is here? D
            }
        }
    }

    public void startTimer(int period, String... ids) {
        if (this.attachedId == null || !Arrays.asList(ids).contains(this.attachedId))  return;
        Timer a = new Timer();
        ReplicableFeature<T> as = this;
        a.schedule(new TimerTask() {
            @Override
            public void run() {
                System.out.format("Part %s Id:%s  Completed:%s Values:%s ReplicationState:%s Updated last:%s \n", as.partId,as.attachedId, as.fuzzyValue.isDone(), as.value, as.state, as.lastModified);
            }
        }, 0, period);
    }

    @Override
    public void setValue(T value) {
        this.value = value;
    }

    @Override
    public CompletableFuture<T> getValue() {
        return this.fuzzyValue;
    }

    @Override
    public void setElement(GraphElement element) {
        super.setElement(element);
        this.state = ((ReplicableGraphElement)element).getState();
    }
}
