package com.wepay.zktools.clustermgr.internal;

import com.wepay.zktools.clustermgr.Endpoint;

import java.util.List;

/**
 * Server descriptor holds information regarding a server (server id, endpoint, and partitions).
 */
public class ServerDescriptor {

    public final int serverId;
    public final Endpoint endpoint;

    /**
     * The list of partitions that a server prefers. This is meant for stateful
     * services where service might wish to declare that it can only support
     * certain partitions due to the state that it has on local disk.
     */
    public final List<Integer> partitions;

    public ServerDescriptor(int serverId, Endpoint endpoint, List<Integer> partitions) {
        this.serverId = serverId;
        this.endpoint = endpoint;
        this.partitions = partitions;
    }

    @Override
    public String toString() {
        return "[serverId=" + serverId + " endpoint=" + endpoint + " partitions=" + partitions + "]";
    }
}
