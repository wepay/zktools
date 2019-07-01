package com.wepay.zktools.clustermgr;

import com.wepay.zktools.clustermgr.internal.ClusterManagerImpl;
import com.wepay.zktools.clustermgr.internal.PartitionAssignment;
import com.wepay.zktools.clustermgr.internal.PartitionAssignmentPolicy;
import com.wepay.zktools.clustermgr.internal.ServerDescriptor;
import com.wepay.zktools.zookeeper.ZNode;
import com.wepay.zktools.zookeeper.ZooKeeperClient;

import java.util.Set;

/**
 * A simple cluster manager.
 */
public interface ClusterManager {

    static ClusterManager create(ZooKeeperClient zkClient, ZNode root, PartitionAssignmentPolicy partitionAssignmentPolicy)
        throws ClusterManagerException {
        return new ClusterManagerImpl(zkClient, root, partitionAssignmentPolicy);
    }

    /**
     * Closes this cluster manager
     */
    void close();

    /**
     * Returns the name of this cluster.
     * @return the name of the cluster
     */
    String clusterName();

    /**
     * Returns the number of partitions.
     * @return the number of partitions
     */
    int numPartitions();

    /**
     * Returns the server descriptors from ZooKeeper.
     * @return the server descriptors
     */
    Set<ServerDescriptor> serverDescriptors() throws ClusterManagerException;

    /**
     * Returns the partition assignment from ZooKeeper
     * @return the partition assignment
     */
    PartitionAssignment partitionAssignment() throws ClusterManagerException;

    /**
     * Register the specified client to be managed by the cluster manager.
     * @param client a client to be managed
     * @throws ClusterManagerException
     */
    void manage(ManagedClient client) throws ClusterManagerException;

    /**
     * Register the specified server to be managed by the cluster manager.
     * @param server a server to be managed
     * @throws ClusterManagerException
     */
    void manage(ManagedServer server) throws ClusterManagerException;

    /**
     * Unregister the specified client from the cluster manager.
     * @param client a client to be unmanaged
     * @throws ClusterManagerException
     */
    void unmanage(ManagedClient client) throws ClusterManagerException;

    /**
     * Unregister the specified server from the cluster manager.
     * @param server a server to be unmanaged
     * @throws ClusterManagerException
     */
    void unmanage(ManagedServer server) throws ClusterManagerException;

}
