package part;

import aggregator.BaseAggregator;
import aggregator.GNNAggregator.MyFirstGNNAggregator;
import edge.BaseEdge;
import features.Feature;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import storage.GraphStorage;
import storage.HashMapGraphStorage;
import types.GraphQuery;
import vertex.BaseVertex;


public class SimplePart<VT extends BaseVertex> extends  BasePart<VT> {
    public SimplePart() {
        super();
    }



    @Override
    public GraphStorage<VT> newStorage(){
        return new HashMapGraphStorage<>();
    }

    @Override
    public BaseAggregator<VT>[] newAggregators() {
//       return new BaseAggregator[]{new MyFirstGNNAggregator<VT>()};
        return new BaseAggregator[0];
    }

    @Override
    public void dispatch(GraphQuery query){
        try {
            boolean isVertex = query.element instanceof BaseVertex;
            boolean isEdge = query.element instanceof BaseEdge;
            boolean isFeature = query.element instanceof Feature.Update;
            switch (query.op) {
                case ADD -> {
                    if (isEdge) {
                        BaseEdge<VT> tmp = (BaseEdge<VT>) query.element;
                        getStorage().addEdge(tmp);
                    }
                }
                case REMOVE -> System.out.println("Remove Operation");
                case SYNC -> {
                    if(isFeature) {
                        getStorage().updateFeature((Feature.Update<?>) query.element);
                    }
                }
                case AGG -> {

                }
                default -> System.out.println("Undefined Operation");
        }

        aggFunctions.forEach((fn) -> {
            if (fn.shouldTrigger(query)) fn.dispatch(query);
        });

    }
    catch (Exception e){
        System.out.println(e.getMessage());
    }


    }
}
