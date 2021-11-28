package part;

import aggregator.BaseAggregator;
import aggregator.StreamingGNNAggregator.StreamingAggType;
import aggregator.StreamingGNNAggregator.StreamingGNNAggregator;
import edge.BaseEdge;
import features.Feature;
import storage.GraphStorage;
import storage.HashMapGraphStorage;
import types.GraphQuery;
import vertex.BaseVertex;

import java.util.ArrayList;


public class GNNPart<VT extends BaseVertex> extends  BasePart<VT> {

    public GNNPart() {
        super();
    }
    public GNNPart(short L,short maxL){
        super();
        this.L = L;
        this.maxL = maxL;
    }
    @Override
    public GraphStorage<VT> newStorage(){
        return new HashMapGraphStorage<>();
    }

    @Override
    public BaseAggregator<VT>[] newAggregators() {

        return new BaseAggregator[]{new StreamingGNNAggregator(this.L,this.maxL)};

    }

    @Override
    public void dispatch(GraphQuery query){
        try {
            boolean isVertex = query.element instanceof BaseVertex;
            boolean isEdge = query.element instanceof BaseEdge;
            boolean isFeature = query.element instanceof Feature.Update;
            switch (query.op) {
                case ADD : {
                    if (isEdge) {
                        this.collect(query,true); // Send to next layer to add it as well
                        BaseEdge<VT> tmp = (BaseEdge<VT>) query.element;
                        getStorage().addEdge(tmp);
                    }
                    break;
                }
                case REMOVE : System.out.println("Remove Operation");
                case SYNC : {
                    if(isFeature) {
                        getStorage().updateFeature((Feature.Update<?>) query.element);
                    }
                    break;
                }
                case AGG : {

                    break;
                }
                default : System.out.println("Undefined Operation");
        }
        aggFunctions.forEach((fn) -> {
                if (fn.shouldTrigger(query)) fn.dispatch(query);
            });



    }
    catch (Exception e){
        System.out.println(e);
    }


    }
}
