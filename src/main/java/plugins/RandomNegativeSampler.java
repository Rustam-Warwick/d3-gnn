package plugins;

import elements.*;
import elements.iterations.MessageDirection;

import java.util.List;

public class RandomNegativeSampler extends Plugin {
    public final double p;
    public int i = 0;
    public int j = 0;

    public RandomNegativeSampler() {
        this(0.002);
    }

    public RandomNegativeSampler(double p) {
        super("negative-sampler");
        this.p = p;
    }

    public static boolean hasIntersection(List<Short> one, List<Short> two) {
        for (short el : one) {
            if (two.contains(el)) return true;
        }
        return false;
    }

    @Override
    public void open() throws Exception {
        super.open();
    }

    @Override
    public void addElementCallback(GraphElement element) {
        super.addElementCallback(element);
        if (element.elementType() == ElementType.EDGE) {
            double valueRandom = Math.random();
            if (valueRandom < p) {
                forwardNegativeSample();
            }
        }
    }

    /**
     * Traversing the Vertices iterator to produce random negative samples
     * Samples are selected as 2 master-vertices which do not have common replica parts and no edges
     * Reasoning is that if common edges existed probably partitioner put it in the same part
     */
    public void forwardNegativeSample() {
        int intI = 0;
        for (Vertex src : storage.getVertices()) {
            if (intI < i) {
                // Make sure we start where we stop
                intI++;
                continue;
            }
            if (src.state() == ReplicaState.MASTER) {
                int intJ = 0;
                for (Vertex dest : storage.getVertices()) {
                    if (intJ < j) {
                        intJ++;
                        continue;
                    }
                    if (dest.state() == ReplicaState.MASTER && !hasIntersection(src.replicaParts(), dest.replicaParts()) && storage.getEdge(src.getId() + ":" + dest.getId()) == null) {
                        Edge tmp = new Edge(src.copy(), dest.copy());
                        tmp.setTimestamp(storage.layerFunction.currentTimestamp());
                        tmp.setFeature("label", new Feature<Integer, Integer>(0));
                        storage.layerFunction.message(new GraphOp(Op.COMMIT, getPartId(), tmp, tmp.getTimestamp()), MessageDirection.FORWARD);
                        i = intI;
                        j = intJ + 1;
                        return;
                    }
                    intJ++;
                }
                j = 0;
            }
            intI++;
        }
        i = 0;
        j = 0;
    }

}
