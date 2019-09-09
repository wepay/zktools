package com.wepay.zktools.clustermgr.internal;

import com.wepay.zktools.clustermgr.PartitionInfo;
import com.wepay.zktools.zookeeper.serializer.SerializerHelper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * Serializer for PartitionAssignment
 */
public class PartitionAssignmentSerializer extends SerializerHelper<PartitionAssignment> {

    private static final byte VERSION = 3;

    @Override
    public void serialize(PartitionAssignment assignments, DataOutput out) throws IOException {
        Set<Integer> serverIds = assignments.serverIds();

        out.writeByte(VERSION);
        out.writeInt(assignments.cversion);
        out.writeInt(assignments.numPartitions);
        out.writeInt(serverIds.size());
        for (Integer serverId : serverIds) {
            out.writeInt(serverId);
            List<PartitionInfo> partitions = assignments.partitionsFor(serverId);
            out.writeInt(partitions.size());
            for (PartitionInfo p : partitions) {
                out.writeInt(p.partitionId);
                out.writeInt(p.generation);
            }
        }
    }

    @Override
    public PartitionAssignment deserialize(DataInput in) throws IOException {
        byte version = in.readByte();

        if (version != VERSION) {
            throw new IOException("version mismatch: " + version);
        }

        int cversion = in.readInt();
        int numPartitions = in.readInt();
        int numNodes = in.readInt();
        HashMap<Integer, List<PartitionInfo>> assignment = new HashMap<>(numNodes);

        for (int n = 0; n < numNodes; n++) {
            Integer serverId = in.readInt();
            int size = in.readInt();
            List<PartitionInfo> partitions = new ArrayList<>(size);
            assignment.put(serverId, partitions);
            for (int p = 0; p < size; p++) {
                partitions.add(new PartitionInfo(in.readInt(), in.readInt()));
            }
        }

        return new PartitionAssignment(cversion, numPartitions, assignment);
    }

}
