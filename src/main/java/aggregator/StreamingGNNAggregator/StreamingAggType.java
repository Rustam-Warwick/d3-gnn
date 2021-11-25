package aggregator.StreamingGNNAggregator;

import vertex.BaseVertex;

public class StreamingAggType<VT extends BaseVertex> {
    public VT source;
    public VT destination;

    public StreamingAggType(){
        this.source = null;
        this.destination = null;
    }
    public StreamingAggType(VT source){
        this.source = source;
    }
    public StreamingAggType(VT source, VT destination){
        this.source = source;
        this.destination = destination;
    }

    public void setDestination(VT destination) {
        this.destination = destination;
    }

    public void setSource(VT source) {
        this.source = source;
    }
}
