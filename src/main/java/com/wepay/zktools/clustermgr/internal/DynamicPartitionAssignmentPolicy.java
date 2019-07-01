package com.wepay.zktools.clustermgr.internal;

import com.wepay.zktools.clustermgr.PartitionInfo;
import com.wepay.zktools.util.Logging;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * An implementation of partition assignment policy. Each partition is assigned to exactly one server.
 * It balances the number of partitions assigned to server.
 * It also tries to minimize the partition migration at the same time.
 */
public class DynamicPartitionAssignmentPolicy implements PartitionAssignmentPolicy {

    private static final Logger logger = Logging.getLogger(DynamicPartitionAssignmentPolicy.class);

    @Override
    public PartitionAssignment update(int cversion,
                                      PartitionAssignment oldAssignment,
                                      final int numPartitions,
                                      Map<Integer, ServerDescriptor> serverDescriptors) {

        if (numPartitions < oldAssignment.numPartitions)
            throw new IllegalStateException("decreasing the number of partitions is not allowed");

        int numServers = serverDescriptors.size();

        if (numServers > 0) {
            ArrayList<PartitionInfo> unassigned = new ArrayList<>();

            // Add new partitions to the unassigned set
            for (int id = oldAssignment.numPartitions; id < numPartitions; id++) {
                unassigned.add(new PartitionInfo(id, 0));
            }
            // Unassign partitions from lost servers
            for (Integer serverId : oldAssignment.serverIds()) {
                if (!serverDescriptors.containsKey(serverId)) {
                    unassigned.addAll(oldAssignment.partitionsFor(serverId));
                }
            }

            HashMap<Integer, List<PartitionInfo>> newAssignment = new HashMap<>();

            int numPartitionsToAssign = numPartitions;
            int minNumPartitionsPerServer = numPartitionsToAssign / numServers;
            int remainingPartitions = numPartitionsToAssign % numServers;

            // Inherit the currently assigned partitions as many as possible
            for (Integer serverId : serverDescriptors.keySet()) {
                List<PartitionInfo> partitions = new ArrayList<>(minNumPartitionsPerServer + (remainingPartitions > 0 ? 1 : 0));
                newAssignment.put(serverId, partitions);
                for (PartitionInfo info : oldAssignment.partitionsFor(serverId)) {
                    // Move partition infos to new list of partitions up to maxNumPartitionsPerServer
                    // Excess partitions will be unassigned
                    if (partitions.size() < minNumPartitionsPerServer) {
                        partitions.add(info);
                        numPartitionsToAssign--;
                    } else if (partitions.size() == minNumPartitionsPerServer && remainingPartitions > 0) {
                        partitions.add(info);
                        numPartitionsToAssign--;
                        remainingPartitions--;
                    } else {
                        unassigned.add(info);
                    }
                }
            }

            // Assign unassigned partitions. Partition infos are recreated with new generation numbers.
            Iterator<PartitionInfo> iterator = unassigned.iterator();
            for (Integer serverId : serverDescriptors.keySet()) {
                List<PartitionInfo> partitions = newAssignment.get(serverId);
                while (iterator.hasNext()) {
                    if (partitions.size() < minNumPartitionsPerServer) {
                        PartitionInfo oldInfo = iterator.next();
                        partitions.add(new PartitionInfo(oldInfo.partitionId, oldInfo.generation + 1));
                        numPartitionsToAssign--;
                    } else if (partitions.size() == minNumPartitionsPerServer && remainingPartitions > 0) {
                        PartitionInfo oldInfo = iterator.next();
                        partitions.add(new PartitionInfo(oldInfo.partitionId, oldInfo.generation + 1));
                        numPartitionsToAssign--;
                        remainingPartitions--;
                    } else {
                        break;
                    }
                }
            }

            // Randomize the partition order so we don't favor particular partitions in later reassignments
            for (List<PartitionInfo> partitions : newAssignment.values()) {
                Collections.shuffle(partitions);
            }

            if (PartitionAssignment.isComplete(numPartitions, newAssignment)) {
                return new PartitionAssignment(cversion, numPartitions, newAssignment);
            } else {
                logger.error("incomplete partition assignment");
                throw new IllegalStateException("incomplete partition assignment");
            }
        } else {
            return new PartitionAssignment(cversion, numPartitions, Collections.emptyMap());
        }
    }

}
