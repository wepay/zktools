package com.wepay.zktools.test;

import com.wepay.zktools.clustermgr.Endpoint;
import com.wepay.zktools.clustermgr.ManagedClient;
import com.wepay.zktools.clustermgr.PartitionInfo;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@SuppressFBWarnings("URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
public class MockManagedClient implements ManagedClient {

    public String clusterName = null;
    public int clientId;
    public int numPartitions;
    public final Map<Integer, Endpoint> partitions = new ConcurrentHashMap<>();

    @Override
    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    @Override
    public void setClientId(int clientId) {
        this.clientId = clientId;
    }

    @Override
    public void setNumPartitions(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    @Override
    public void setEndpoints(Map<Endpoint, List<PartitionInfo>> endpoints) {
        for (Map.Entry<Endpoint, List<PartitionInfo>> entry : endpoints.entrySet()) {
            Endpoint endpoint = entry.getKey();
            for (PartitionInfo info : entry.getValue()) {
                partitions.put(info.partitionId, endpoint);
            }
        }
    }

    @Override
    public void removeServer(Endpoint endpoint) {
        for (Map.Entry<Integer, Endpoint> entry : partitions.entrySet()) {
            if (endpoint.equals(entry.getValue())) {
                partitions.remove(entry.getKey());
            }
        }
    }

}
