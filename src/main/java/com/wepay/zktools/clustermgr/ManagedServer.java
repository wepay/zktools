package com.wepay.zktools.clustermgr;

import java.util.Collections;
import java.util.List;

/**
 * An interface definition for a server to be managed.
 */
public interface ManagedServer {

    void setClusterName(String name);

    void setServerId(int serverId);

    Endpoint endpoint() throws ClusterManagerException;

    void setPartitions(List<PartitionInfo> partitionInfos);

    default List<Integer> getPreferredPartitions() {
        return Collections.emptyList();
    }
}
