package elements;

import elements.enums.CopyContext;
import elements.enums.ElementType;
import elements.enums.ReplicaState;

/**
 * Simple Wrapper element used for sending SYNC_REQUEST {@link GraphOp}
 */
public final class SyncElement extends GraphElement {

    /**
     * ID of element to go to
     */
    public String elementId;

    /**
     * Type of element to go to
     */
    public ElementType elementType;

    /**
     * Part where this element is being sent from
     */
    public short partId;

    public SyncElement() {

    }

    public SyncElement(GraphElement toSync) {
        elementId = toSync.getId();
        elementType = toSync.getType();
        partId = super.getPart();
    }

    public SyncElement(SyncElement element, CopyContext context) {
        super(element, context);
        elementId = element.elementId;
        elementType = element.elementType;
        partId = element.partId;
    }

    @Override
    public SyncElement copy(CopyContext context) {
        return new SyncElement(this, context);
    }

    @Override
    public String getId() {
        return elementId;
    }

    @Override
    public ElementType getType() {
        return elementType;
    }

    @Override
    public ReplicaState state() {
        return ReplicaState.REPLICA;
    }

    @Override
    public short getPart() {
        return partId;
    }
}
