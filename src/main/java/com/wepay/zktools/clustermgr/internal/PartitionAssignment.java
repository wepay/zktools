package com.wepay.zktools.clustermgr.internal;

import com.wepay.zktools.clustermgr.PartitionInfo;
import com.wepay.zktools.util.Logging;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class PartitionAssignment {

    private static final Logger logger = Logging.getLogger(PartitionAssignment.class);

    /**
     * Cversion is the number of changes to children of this ZNode
     */
    public final int cversion;
    public final int numPartitions;
    public final int numEndpoints;

    private final Map<Integer, List<PartitionInfo>> assignment;

    public PartitionAssignment() {
        this(-1, 0, Collections.emptyMap());
    }

    public PartitionAssignment(int cversion, int numPartitions, Map<Integer, List<PartitionInfo>> assignment) {
        this.cversion = cversion;
        this.numPartitions = numPartitions;
        this.numEndpoints = assignment.size();
        this.assignment = Collections.unmodifiableMap(new HashMap<>(assignment));
    }

    public Set<Integer> serverIds() {
        return assignment.keySet();
    }

    public List<PartitionInfo> partitionsFor(Integer serverId) {
        List<PartitionInfo> partitions = assignment.get(serverId);
        return partitions != null ? partitions : Collections.emptyList();
    }

    public static boolean isComplete(int numPartitions, Map<Integer, List<PartitionInfo>> assignment) {
        HashSet<Integer> partitions = new HashSet<>();

        for (List<PartitionInfo> list : assignment.values()) {
            for (PartitionInfo p : list) {
                if (p.partitionId < numPartitions) {
                    partitions.add(p.partitionId);
                } else {
                    throw new IllegalArgumentException(
                        "partition id out of range: partitionId=" + p.partitionId + " numPartitions=" + numPartitions
                    );
                }
            }
        }

        return partitions.size() == numPartitions;
    }

    @Override
    public int hashCode() {
        return Objects.hash(cversion, numPartitions, numEndpoints, assignment);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof PartitionAssignment) {
            PartitionAssignment other = (PartitionAssignment) obj;

            return cversion == other.cversion
                && numPartitions == other.numPartitions
                && numEndpoints == other.numEndpoints
                && assignment.equals(other.assignment);
        } else {
            return false;
        }
    }

}
