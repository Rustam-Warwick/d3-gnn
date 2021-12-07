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


public class GNNPart  extends  BasePart {

    public GNNPart() {
        super();
    }


    @Override
    public GraphStorage newStorage(){
        return new HashMapGraphStorage(this);
    }
    @Override
    public BaseAggregator[] newAggregators() {
        return new BaseAggregator[]{new StreamingGNNAggregator()};
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
                        BaseEdge<BaseVertex> tmp = (BaseEdge) query.element;
                        getStorage().addEdge(tmp);
                    }
                    break;
                }
                case REMOVE : System.out.println("Remove Operation");
                case UPDATE:{
                    if(isFeature) {
                        getStorage().updateFeature((Feature.Update<?>) query.element);
                    }
                    break;
                }
                case SYNC : {
                    if(isFeature) {
                        getStorage().updateFeature((Feature.Update<?>) query.element);
                    }
                    break;
                }
                case AGG : {
                    // Handled below in aggFunctions
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
