package com.wepay.zktools.clustermgr.internal;

import com.wepay.zktools.clustermgr.Endpoint;
import com.wepay.zktools.zookeeper.serializer.SerializerHelper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Serializer for ServerDescriptor
 */
public class ServerDescriptorSerializer extends SerializerHelper<ServerDescriptor> {

    private static final byte VERSION = 1;

    @Override
    public void serialize(ServerDescriptor serverDescriptor, DataOutput out) throws IOException {
        out.writeByte(VERSION);
        out.writeInt(serverDescriptor.serverId);
        out.writeUTF(serverDescriptor.endpoint.host);
        out.writeInt(serverDescriptor.endpoint.port);
        out.writeInt(serverDescriptor.partitions.size());
        for (Integer partitionId : serverDescriptor.partitions) {
            out.writeInt(partitionId);
        }
    }

    @Override
    public ServerDescriptor deserialize(DataInput in) throws IOException {
        byte version = in.readByte();
        if (version != VERSION)
            throw new IOException("version mismatch: " + version);

        int serverId = in.readInt();
        String host = in.readUTF();
        int port = in.readInt();
        int size = in.readInt();
        ArrayList<Integer> partitions = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            partitions.add(in.readInt());
        }
        return new ServerDescriptor(serverId, new Endpoint(host, port), partitions);
    }

}
