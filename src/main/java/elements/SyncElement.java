package elements;

import elements.enums.CopyContext;
import elements.enums.ElementType;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Simple Wrapper element used for sending SYNC messages
 * @implNote Regular RMI is not well suited since we also need to have info about part_id where this message was sent
 */
public final class SyncElement extends GraphElement {

    public Tuple2<String, ElementType> identity;

    public SyncElement() {

    }

    public SyncElement(String id, ElementType type) {
        identity = Tuple2.of(id, type);
    }

    public SyncElement(SyncElement element, CopyContext context) {
        super(element, context);
        identity = element.identity;
    }

    @Override
    public SyncElement copy(CopyContext context) {
        return new SyncElement(this, context);
    }

    @Override
    public String getId() {
        return identity.f0;
    }

    @Override
    public ElementType elementType() {
        return identity.f1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        SyncElement that = (SyncElement) o;
        return identity.equals(that.identity);
    }

    @Override
    public int hashCode() {
        return identity.hashCode();
    }
}
