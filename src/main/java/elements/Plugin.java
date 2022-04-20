package elements;

import org.apache.flink.streaming.api.watermark.Watermark;
import scala.Tuple2;

import java.util.List;

/**
 * Plugin is a unique Graph element that is attached to storage, so it is not in the life cycle of logical keys
 */
public class Plugin extends ReplicableGraphElement {
    public Plugin() {
        super(null, false, (short) 0);
    }

    public Plugin(String id) {
        super(id, false, (short) 0);
    }

    @Override
    public Boolean createElement() {
        return false;
    }

    @Override
    public Tuple2<Boolean, GraphElement> update(GraphElement newElement) {
        return null;
    }

    @Override
    public Tuple2<Boolean, GraphElement> sync(GraphElement newElement) {
        return null;
    }

    @Override
    public List<Short> replicaParts() {
        return this.storage.layerFunction.getReplicaMasterParts();
    }

    @Override
    public ElementType elementType() {
        return ElementType.PLUGIN;
    }

    public void addElementCallback(GraphElement element) {

    }

    public void updateElementCallback(GraphElement newElement, GraphElement oldElement) {

    }

    public void deleteElementCallback(GraphElement deletedElement) {

    }

    public void onWatermark(Watermark w) {

    }

    public void close() {

    }

    public void open() {

    }

    public void add() {

    }

}
