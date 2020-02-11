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
            // store partitionInfo of all the partitions.
            Map<Integer, PartitionInfo> unassignedPartitionInfoMap = new HashMap<>();
            Map<Integer, Integer> partitionToExistingServerMap = new HashMap<>();

            // Add new partitions to the unassigned set
            for (int id = oldAssignment.numPartitions; id < numPartitions; id++) {
                PartitionInfo partitionInfo = new PartitionInfo(id, 0);
                unassignedPartitionInfoMap.put(id, partitionInfo);
                partitionToExistingServerMap.put(id, -1);
            }

            // reassign partitions of all servers
            for (Integer serverId : oldAssignment.serverIds()) {
                // store partition assignment of all old partitions.
                oldAssignment.partitionsFor(serverId).forEach(partitionInfo -> {
                    unassignedPartitionInfoMap.put(partitionInfo.partitionId, partitionInfo);
                    partitionToExistingServerMap.put(partitionInfo.partitionId, serverId);
                });
            }

            HashMap<Integer, List<PartitionInfo>> newAssignment = new HashMap<>();
            int numPartitionsToAssign = numPartitions;
            int minNumPartitionsPerServer = numPartitionsToAssign / numServers;
            int remainingPartitions = numPartitionsToAssign % numServers;

            // Inherit the currently assigned preferred partitions as many as possible
            for (Map.Entry<Integer, ServerDescriptor> serverEntry : serverDescriptors.entrySet()) {
                List<PartitionInfo> partitions = new ArrayList<>(minNumPartitionsPerServer + (remainingPartitions > 0 ? 1 : 0));
                newAssignment.put(serverEntry.getKey(), partitions);

                // Handle preferred partitions.
                // Recreate partition info with the new generation number if the partition is being moved to a new
                // server.
                for (Integer partitionId : serverEntry.getValue().partitions) {
                    PartitionInfo oldInfo = unassignedPartitionInfoMap.get(partitionId);
                    if (oldInfo != null) {
                        Integer oldServerId = partitionToExistingServerMap.get(partitionId);
                        if (partitions.size() < minNumPartitionsPerServer) {
                            if (serverEntry.getKey().equals(oldServerId)) {
                                partitions.add(oldInfo);
                            } else {
                                partitions.add(new PartitionInfo(partitionId, oldInfo.generation + 1));
                            }
                            numPartitionsToAssign--;
                            unassignedPartitionInfoMap.remove(partitionId);
                        } else if (partitions.size() == minNumPartitionsPerServer && remainingPartitions > 0) {
                            if (serverEntry.getKey().equals(oldServerId)) {
                                partitions.add(oldInfo);
                            } else {
                                partitions.add(new PartitionInfo(partitionId, oldInfo.generation + 1));
                            }
                            numPartitionsToAssign--;
                            unassignedPartitionInfoMap.remove(partitionId);
                            remainingPartitions--;
                        }
                    }
                }
            }

            // Inherit the currently assigned partitions as many as possible
            for (Integer serverId : serverDescriptors.keySet()) {
                List<PartitionInfo> partitions = newAssignment.get(serverId);
                // Handle rest of the partitions.
                for (PartitionInfo info : oldAssignment.partitionsFor(serverId)) {
                    Integer partitionId = info.partitionId;
                    if (unassignedPartitionInfoMap.get(partitionId) != null) {
                        // Move partition infos to new list of partitions up to maxNumPartitionsPerServer
                        if (partitions.size() < minNumPartitionsPerServer) {
                            partitions.add(info);
                            numPartitionsToAssign--;
                            unassignedPartitionInfoMap.remove(partitionId);
                        } else if (partitions.size() == minNumPartitionsPerServer && remainingPartitions > 0) {
                            partitions.add(info);
                            numPartitionsToAssign--;
                            unassignedPartitionInfoMap.remove(partitionId);
                            remainingPartitions--;
                        } else {
                            break;
                        }
                    }
                }
            }

            // Assign unassigned partitions. Partition infos are recreated with new generation numbers.
            Iterator<PartitionInfo> iterator = unassignedPartitionInfoMap.values().iterator();
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
