package plugins;

import elements.Feature;
import elements.GraphElement;
import elements.Plugin;
import elements.ReplicaState;
import org.apache.flink.metrics.Gauge;

public class ReportReplicationFactor extends Plugin {
    public transient int replicationFactor = 0;
    public int numberOfMasterVertices = 0;
    public int totalReplicaParts = 0;
    public ReportReplicationFactor(){
        super("reporter/RF");
    }
    public ReportReplicationFactor(String id){
        super(id);
    }

    @Override
    public void open() {
        super.open();
        ReportReplicationFactor el = this;
        this.storage.getRuntimeContext().getMetricGroup().gauge("Replication Factor", new Gauge<Integer>(){
            @Override
            public Integer getValue() {
                return el.replicationFactor;
            }
        });
    }

    @Override
    public void addElementCallback(GraphElement element) {
        super.addElementCallback(element);
        switch (element.elementType()){
            case VERTEX:{
                if(element.state() == ReplicaState.MASTER){
                    numberOfMasterVertices++;
                }
            }
        }
    }

    @Override
    public void updateElementCallback(GraphElement newElement, GraphElement oldElement) {
        super.updateElementCallback(newElement, oldElement);
        switch (newElement.elementType()){
            case FEATURE:{
                Feature newFeature = (Feature) newElement;
                if(newFeature.getFieldName().equals("parts")){
                    totalReplicaParts ++;
                    replicationFactor = (int)((float) totalReplicaParts / numberOfMasterVertices * 1000);
                }
            }
        }
    }
}
