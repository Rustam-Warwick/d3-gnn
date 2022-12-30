package org.apache.flink.runtime.state;

import java.io.Serializable;
import java.util.Objects;

/**
 * A class Representing PartNumber, wrapper used to distinguish keys requiring murmurHash from this one
 */
public class PartNumber implements Serializable {
    public short partId;

    public PartNumber() {
    }

    public PartNumber(short partId) {
        this.partId = partId;
    }

    public static PartNumber of(short number) {
        return new PartNumber(number);
    }

    public short getPartId() {
        return partId;
    }

    public void setPartId(short partId) {
        this.partId = partId;
    }

    @Override
    public String toString() {
        return "PartNumber{" +
                "partId=" + partId +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PartNumber that = (PartNumber) o;
        return Objects.equals(partId, that.partId);
    }

    @Override
    public int hashCode() {
        return partId;
    }
}
