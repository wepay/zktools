package com.wepay.zktools.clustermgr;

import java.util.Objects;

/**
 * This class holds a partition information.
 */
public class PartitionInfo {

    /**
     * Partition id
     */
    public final int partitionId;
    /**
     * A sequence number incremented when the assignment changes
     */
    public final int generation;

    public PartitionInfo(int partitionId, int generation) {
        this.partitionId = partitionId;
        this.generation = generation;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PartitionInfo that = (PartitionInfo) o;
        return partitionId == that.partitionId
                && generation == that.generation;
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionId, generation);
    }

    @Override
    public String toString() {
        return "PartitionInfo{"
                + "partitionId=" + partitionId
                + ", generation=" + generation
                + '}';
    }
}
