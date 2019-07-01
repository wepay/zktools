package com.wepay.zktools.clustermgr.internal;

import com.wepay.zktools.zookeeper.serializer.SerializerHelper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Serializer for ClusterParams
 */
public class ClusterParamsSerializer extends SerializerHelper<ClusterParams> {

    private static final int MAGIC_NUMBER = "Cluster Manager".hashCode();
    private static final byte VERSION = 1;

    @Override
    public void serialize(ClusterParams clusterParams, DataOutput out) throws IOException {
        out.writeInt(MAGIC_NUMBER);
        out.writeByte(VERSION);
        out.writeUTF(clusterParams.name);
        out.writeInt(clusterParams.numPartitions);
    }

    @Override
    public ClusterParams deserialize(DataInput in) throws IOException {
        if (in.readInt() != MAGIC_NUMBER)
            throw new IOException("magic number mismatch");

        byte version = in.readByte();
        if (version != VERSION)
            throw new IOException("version mismatch: " + version);

        String name = in.readUTF();
        int numPartitions = in.readInt();
        return new ClusterParams(name, numPartitions);
    }

}
