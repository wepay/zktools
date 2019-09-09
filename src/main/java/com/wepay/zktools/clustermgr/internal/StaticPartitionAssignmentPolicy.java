package com.wepay.zktools.clustermgr.internal;

import com.wepay.zktools.clustermgr.PartitionInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An implementation of partition assignment policy.
 * This policy assumes that a server has a list of preferred partitions.
 * And the policy simply endorses it.
 * So, it is possible a partition is served by no server, one server, or multiple servers.
 * <P>
 * The preferred partition lists come from {@link ServerDescriptor}s which are created from
 * {@link com.wepay.zktools.clustermgr.ManagedServer} (see {@link ClusterManagerImpl}).
 * The {@code generation} in the partition info is always set to zero.
 * </P>
 */
public class StaticPartitionAssignmentPolicy implements PartitionAssignmentPolicy {

    @Override
    public PartitionAssignment update(int cversion,
                                      PartitionAssignment oldAssignment,
                                      final int numPartitions,
                                      Map<Integer, ServerDescriptor> serverDescriptors) {

        if (numPartitions < oldAssignment.numPartitions) {
            throw new IllegalStateException("decreasing the number of partitions is not allowed");
        }

        Map<Integer, List<PartitionInfo>> newAssignment = new HashMap<>();

        for (ServerDescriptor serverDescriptor : serverDescriptors.values()) {
            List<PartitionInfo> partitions = new ArrayList<>(serverDescriptor.partitions.size());
            for (int partitionId : serverDescriptor.partitions) {
                if (partitionId < numPartitions) {
                    partitions.add(new PartitionInfo(partitionId, 0));
                } else {
                    throw new IllegalArgumentException(
                        "partition id out of range: partitionId=" + partitionId + " numPartitions=" + numPartitions
                    );
                }
            }

            newAssignment.put(serverDescriptor.serverId, partitions);
        }

        return new PartitionAssignment(cversion, numPartitions, newAssignment);
    }

}
