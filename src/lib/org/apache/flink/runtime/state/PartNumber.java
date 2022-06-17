package org.apache.flink.runtime.state;

/**
 * A class Representing PartNumber, wrapper used to distinguish keys requiring murmurHash from this one
 */
public class PartNumber{
    public short partId;

    public PartNumber(short partId) {
        this.partId = partId;
    }

    public static PartNumber of(short number){
        return new PartNumber(number);
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

        return partId == that.partId;
    }

    @Override
    public int hashCode() {
        return partId;
    }
}
