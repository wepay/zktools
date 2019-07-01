package com.wepay.zktools.test;

import com.wepay.zktools.clustermgr.ClusterManagerException;
import com.wepay.zktools.clustermgr.Endpoint;
import com.wepay.zktools.clustermgr.ManagedServer;
import com.wepay.zktools.clustermgr.PartitionInfo;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

@SuppressFBWarnings("URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
public class MockManagedServer implements ManagedServer {

    public String clusterName = null;
    public int serverId;
    public final Set<Integer> partitions = new ConcurrentSkipListSet<>();

    private final Endpoint endpoint;
    private final List<Integer> preferredPartitions;

    public MockManagedServer(String host, int port) {
        this.endpoint = new Endpoint(host, port);
        this.preferredPartitions = Collections.emptyList();
    }

    public MockManagedServer(String host, int port, List<Integer> preferredPartitions) {
        this.endpoint = new Endpoint(host, port);
        this.preferredPartitions = preferredPartitions;
    }

    @Override
    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    @Override
    public void setServerId(int serverId) {
        this.serverId = serverId;
    }

    @Override
    public Endpoint endpoint() throws ClusterManagerException {
        return endpoint;
    }

    @Override
    public void setPartitions(List<PartitionInfo> partitionInfos) {
        synchronized (this) {
            HashSet<Integer> remaining = new HashSet<>(partitions);

            for (PartitionInfo info : partitionInfos) {
                if (!remaining.remove(info.partitionId)) {
                    // Add if the partition is new
                    if (!remaining.remove(info.partitionId)) {
                        partitions.add(info.partitionId);
                    }
                }
            }
            // Remove extraneous partitions
            for (Integer partitionId : remaining) {
                partitions.remove(partitionId);
            }
        }
    }

    @Override
    public List<Integer> getPreferredPartitions() {
        return preferredPartitions;
    }

}
