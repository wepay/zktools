package com.wepay.zktools.clustermgr;

import java.util.List;
import java.util.Map;

/**
 * An interface definition for a client to be managed.
 */
public interface ManagedClient {

    void setClusterName(String name);

    void setClientId(int clientId);

    void setNumPartitions(int numPartitions);

    void removeServer(Endpoint endpoint);

    void setEndpoints(Map<Endpoint, List<PartitionInfo>> endpoints);

}
