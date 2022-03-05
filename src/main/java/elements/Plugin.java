package elements;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Plugin extends ReplicableGraphElement{
    public List<Short> replicaList=null;
    public Plugin(){
        super(null, false, (short) 1);
    }
    public Plugin(String id) {
        super(id, false, (short) 0);
    }

    @Override
    public Boolean createElement() {
        return false;
    }

    @Override
    public Tuple2<Boolean, GraphElement> externalUpdate(GraphElement newElement) {
        return null;
    }

    @Override
    public Tuple2<Boolean, GraphElement> syncElement(GraphElement newElement) {
        return null;
    }

    @Override
    public List<Short> replicaParts() {
        if(Objects.isNull(this.replicaList)){
            this.replicaList = new ArrayList<>();
            for(short i=1;i<this.storage.parallelism;i++){
                replicaList.add(i);
            }
        }

        return this.replicaList;
    }

    @Override
    public ElementType elementType() {
        return ElementType.PLUGIN;
    }

    public void addElementCallback(GraphElement element){

    }
    public void updateElementCallback(GraphElement newElement, GraphElement oldElement){

    }
    public void open(){

    }

}
