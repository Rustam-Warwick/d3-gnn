package elements;

import elements.enums.CopyContext;
import elements.enums.ElementType;
import elements.enums.ReplicaState;

/**
 * Simple Wrapper element used for sending SYNC_REQUEST {@link GraphOp}
 */
public final class SyncRequest extends GraphElement {

    /**
     * ID of element to go to
     */
    public Object elementId;

    /**
     * Type of element to go to
     */
    public ElementType elementType;

    /**
     * Part where this element is being sent from to then return SYNC messages
     */
    public short partId;

    public SyncRequest() {

    }

    public SyncRequest(GraphElement toSync) {
        elementId = toSync.getId();
        elementType = toSync.getType();
        partId = toSync.getPart();
    }

    public SyncRequest(SyncRequest element, CopyContext context) {
        super(element, context);
        elementId = element.elementId;
        elementType = element.elementType;
        partId = element.partId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SyncRequest copy(CopyContext context) {
        return new SyncRequest(this, context);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object getId() {
        return elementId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ElementType getType() {
        return elementType;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ReplicaState state() {
        return ReplicaState.REPLICA;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onDeserialized() {}

    /**
     * {@inheritDoc}
     */
    @Override
    public void resume() {}

    /**
     * {@inheritDoc}
     */
    @Override
    public void delay() {}

    /**
     * {@inheritDoc}
     */
    @Override
    public short getPart() {
        return partId;
    }
}
