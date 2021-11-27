package part;

import aggregator.BaseAggregator;
import edge.BaseEdge;
import features.Feature;
import storage.GraphStorage;
import storage.HashMapGraphStorage;
import types.GraphQuery;
import vertex.BaseVertex;


public class L1Part<VT extends BaseVertex> extends  BasePart<VT> {
    public L1Part() {
        super();
    }

    @Override
    public GraphStorage<VT> newStorage(){
        return new HashMapGraphStorage<>();
    }

    @Override
    public BaseAggregator<VT>[] newAggregators() {
        return new BaseAggregator[0];
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
                    aggFunctions.forEach((fn) -> {
                        if (fn.shouldTrigger(query)) fn.dispatch(query);
                    });
                    break;
                }
                default : System.out.println("Undefined Operation");
        }



    }
    catch (Exception e){
        System.out.println(e);
    }


    }
}
