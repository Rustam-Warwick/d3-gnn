package plugins.edge_classification;

import ai.djl.ndarray.SerializableLoss;
import ai.djl.translate.StackBatchifier;
import elements.*;
import operators.events.StartTraining;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.HashSet;

public class EdgeClassificationTrainingPlugin extends Plugin {

    public final String modelName;

    public final int MAX_BATCH_SIZE; // Overall batch size across output layers

    public transient int LOCAL_BATCH_SIZE; // Estimate batch size for this operator

    public final SerializableLoss loss;

    public transient StackBatchifier batchifier; // Helper for batching data

    public int BATCH_COUNT = 0;


    public EdgeClassificationTrainingPlugin(String modelName, SerializableLoss loss, int MAX_BATCH_SIZE) {
        super(String.format("%s-trainer", modelName));
        this.MAX_BATCH_SIZE = MAX_BATCH_SIZE;
        this.loss = loss;
        this.modelName = modelName;
    }

    public EdgeClassificationTrainingPlugin(String modelName, SerializableLoss loss) {
        this(modelName, loss, 1024);
    }

    @Override
    public void open() throws Exception {
        super.open();
        LOCAL_BATCH_SIZE = MAX_BATCH_SIZE / storage.layerFunction.getRuntimeContext().getNumberOfParallelSubtasks();
        batchifier = new StackBatchifier();
        storage.layerFunction.runForAllLocalParts(() -> {
            setFeature("trainSrcVertices", new Feature<>(new HashMap<String, HashMap<String, Byte>>(LOCAL_BATCH_SIZE), true, null)); // Ready for training
            setFeature("trainDestVertices", new Feature<>(new HashMap<String, HashMap<String, Byte>>(LOCAL_BATCH_SIZE), true, null)); // Pending for Features label is here
            setFeature("readyTrainingEdges", new Feature<>(new HashSet<String>(LOCAL_BATCH_SIZE), true, null)); // Pending for Features label is here
        });
    }

    /**
     * Add value to the batch. If filled send event to the coordinator
     */
    public void incrementBatchCount() {
        BATCH_COUNT++;
        if (BATCH_COUNT % LOCAL_BATCH_SIZE == 0) {
            storage.layerFunction.operatorEventMessage(new StartTraining());
        }
    }

    public void mergeTrainingDataState(@Nonnull Edge e) {
        // 1. Add edge to the waiting list
        Feature<HashSet<String>, HashSet<String>> readyEdges = (Feature<HashSet<String>, HashSet<String>>) getFeature("readyTrainingEdges");
        Feature<HashMap<String, HashMap<String, Byte>>, HashMap<String, HashMap<String, Byte>>> srcVertexState = (Feature<HashMap<String, HashMap<String, Byte>>, HashMap<String, HashMap<String, Byte>>>) getFeature("trainSrcVertices");
        Feature<HashMap<String, HashMap<String, Byte>>, HashMap<String, HashMap<String, Byte>>> destVertexState = (Feature<HashMap<String, HashMap<String, Byte>>, HashMap<String, HashMap<String, Byte>>>) getFeature("trainDestVertices");
        if(e.src.containsFeature("feature") && e.dest.containsFeature("feature")){
            // All dependecies are here,
            readyEdges.getValue().add(e.getId());
            storage.updateFeature(readyEdges);
            incrementBatchCount();
            return;
        }
        else if(e.src.containsFeature("feature") || e.dest.containsFeature("feature")){
            // Either sides are not here yet
            srcVertexState.getValue().compute(e.src.getId(), (src, value)->{
                if(value == null) value = new HashMap<>(5);
                value.put(e.dest.getId(), (byte) 1);
                return value;
            });

            destVertexState.getValue().compute(e.dest.getId(), (dest, value)->{
                if(value == null) value = new HashMap<>(5);
                value.put(e.src.getId(), (byte) 1);
                return value;
            });
        }
        else{
            srcVertexState.getValue().compute(e.src.getId(), (src, value)->{
                if(value == null) value = new HashMap<>(5);
                value.put(e.dest.getId(), (byte) 0);
                return value;
            });

            destVertexState.getValue().compute(e.dest.getId(), (dest, value)->{
                if(value == null) value = new HashMap<>(5);
                value.put(e.src.getId(), (byte) 0);
                return value;
            });
        }

        storage.updateFeature(srcVertexState);
        storage.updateFeature(destVertexState);
    }

    public void mergeTrainingDataState(@Nonnull Vertex v){
        Feature<HashSet<String>, HashSet<String>> readyEdges = (Feature<HashSet<String>, HashSet<String>>) getFeature("readyTrainingEdges");
        Feature<HashMap<String, HashMap<String, Byte>>, HashMap<String, HashMap<String, Byte>>> srcVertexState = (Feature<HashMap<String, HashMap<String, Byte>>, HashMap<String, HashMap<String, Byte>>>) getFeature("trainSrcVertices");
        Feature<HashMap<String, HashMap<String, Byte>>, HashMap<String, HashMap<String, Byte>>> destVertexState = (Feature<HashMap<String, HashMap<String, Byte>>, HashMap<String, HashMap<String, Byte>>>) getFeature("trainDestVertices");
        srcVertexState.getValue().computeIfPresent(v.getId(), (src, value)->{
            value.replaceAll((dest, state)->{
                state++;
                if(state == 2){
                    // Both are here
                    incrementBatchCount();
                    destVertexState.getValue().get(dest).remove(src);
                    readyEdges.getValue().add(src+":"+dest);
                    return null;
                }

                return state;
            });

            if(value.isEmpty()) return null;
            return value;
        });

        destVertexState.getValue().computeIfPresent(v.getId(), (dest, value)->{
            value.replaceAll((src, state)->{
                state++;
                if(state == 2){
                    // Both are here
                    incrementBatchCount();
                    srcVertexState.getValue().get(src).remove(dest);
                    readyEdges.getValue().add(src+":"+dest);
                    return null;
                }

                return state;
            });

            if(value.isEmpty()) return null;
            return value;
        });
        storage.updateFeature(readyEdges);
        storage.updateFeature(srcVertexState);
        storage.updateFeature(destVertexState);
    }

    @Override
    public void addElementCallback(GraphElement element) {
        super.addElementCallback(element);
        if(element.elementType() == ElementType.FEATURE){
            Feature<?,?> feature = (Feature<?, ?>) element;
            if(feature.attachedTo != null && feature.attachedTo.f0 == ElementType.EDGE && "trainLabel".equals(feature.getName())){
                // Training label arrived
                mergeTrainingDataState((Edge) feature.getElement());
            }
            else if(feature.attachedTo != null && feature.attachedTo.f0 == ElementType.VERTEX && "feature".equals(feature.getName())){
                mergeTrainingDataState((Vertex) feature.getElement());
            }
        }
    }


}
