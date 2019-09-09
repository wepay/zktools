package com.wepay.zktools.clustermgr.internal;

import com.wepay.zktools.clustermgr.ClusterManager;
import com.wepay.zktools.clustermgr.ClusterManagerException;
import com.wepay.zktools.clustermgr.Endpoint;
import com.wepay.zktools.clustermgr.ManagedClient;
import com.wepay.zktools.clustermgr.ManagedServer;
import com.wepay.zktools.clustermgr.PartitionInfo;
import com.wepay.zktools.util.Logging;
import com.wepay.zktools.util.State;
import com.wepay.zktools.zookeeper.NodeData;
import com.wepay.zktools.zookeeper.Serializer;
import com.wepay.zktools.zookeeper.WatcherHandle;
import com.wepay.zktools.zookeeper.ZNode;
import com.wepay.zktools.zookeeper.ZooKeeperClient;
import com.wepay.zktools.zookeeper.ZooKeeperClientException;
import com.wepay.zktools.zookeeper.ZooKeeperSession;
import com.wepay.zktools.zookeeper.serializer.IntegerSerializer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An implementation of {@link ClusterManager}
 */
public class ClusterManagerImpl implements ClusterManager {

    private static final Logger logger = Logging.getLogger(ClusterManagerImpl.class);

    private static final String serverNodePrefix = "s_";
    private static final ServerDescriptorSerializer serverDescriptorSerializer = new ServerDescriptorSerializer();
    private static final PartitionAssignmentSerializer partitionAssignmentSerializer = new PartitionAssignmentSerializer();
    private static final IntegerSerializer intSerializer = new IntegerSerializer();

    private final ZooKeeperClient zkClient;
    private final ZNode clientIdZNode;
    private final ZNode serverIdZNode;
    private final ZNode serverDir;
    private final PartitionAssignmentPolicy partitionAssignmentPolicy;

    public final State<Cluster> clusterState;

    private final ClusterParams clusterParams;
    private final LinkedList<ManagedClient> managedClients = new LinkedList<>();
    private final Object serverManagementLock = new Object();
    private final IdentityHashMap<ManagedServer, ManagedServerInfo> managedServers = new IdentityHashMap<>();

    private WatcherHandle connectionWatcherHandle = null;
    private WatcherHandle serverWatcherHandle = null;
    private WatcherHandle assignmentWatcherHandle = null;

    private volatile boolean running = true;

    public ClusterManagerImpl(ZooKeeperClient zkClient, ZNode rootZNode,
                              PartitionAssignmentPolicy partitionAssignmentPolicy) throws ClusterManagerException {
        ZNode idsZNode;
        try {
            this.serverDir = new ZNode(rootZNode, "serverDescriptors");
            idsZNode = new ZNode(rootZNode, "ids");
            this.clientIdZNode = new ZNode(idsZNode, "client");
            this.serverIdZNode = new ZNode(idsZNode, "server");
        } catch (IllegalArgumentException ex) {
            throw new ClusterManagerException("failed to start cluster manager", ex);
        }

        this.clusterState = new State<>(new Cluster(Collections.emptyMap(), new PartitionAssignment()));
        this.partitionAssignmentPolicy = partitionAssignmentPolicy;

        this.zkClient = zkClient;

        try {
            ZooKeeperSession s = zkClient.session();
            if (s.exists(rootZNode) != null) {
                NodeData<ClusterParams> clusterParams = s.getData(rootZNode, new ClusterParamsSerializer());

                if (clusterParams.value == null) {
                    logger.error("failed to start cluster manager: cluster parameters missing");
                    throw new ClusterManagerException("failed to start cluster manager: cluster parameters missing");
                }

                this.clusterParams = clusterParams.value;

            } else {
                logger.error("failed to create cluster manager znodes: root node not found");
                throw new ClusterManagerException("failed to start cluster manager: root node not found");
            }
        } catch (Exception ex) {
            logger.error("failed to create cluster manager znodes", ex);
            throw new ClusterManagerException("failed to start cluster manager", ex);
        }

        // Start watching the internal connection
        this.connectionWatcherHandle = zkClient.onConnected(this::onConnected);

        // Start watching partition assignments and server descriptors
        this.serverWatcherHandle =
            zkClient.watch(serverDir, this::updateServers, partitionAssignmentSerializer, serverDescriptorSerializer);
    }

    public static void createZNodes(ZooKeeperClient zkClient, ZNode rootZNode) throws ClusterManagerException {
        try {
            createNode(zkClient, new ZNode(rootZNode, "serverDescriptors"), new PartitionAssignment(), partitionAssignmentSerializer);
            ZNode idsZNode = new ZNode(rootZNode, "ids");
            createNode(zkClient, idsZNode);
            createNode(zkClient, new ZNode(idsZNode, "client"), 0, intSerializer);
            createNode(zkClient, new ZNode(idsZNode, "server"), 0, intSerializer);
        } catch (Exception ex) {
            logger.error("failed to create cluster manager znodes", ex);
            throw new ClusterManagerException("failed to create cluster manager znodes", ex);
        }
    }

    @Override
    public void close() {
        running = false;
        closeHandles(connectionWatcherHandle, serverWatcherHandle, assignmentWatcherHandle);
    }

    @Override
    public String clusterName() {
        return clusterParams.name;
    }

    @Override
    public int numPartitions() {
        return clusterParams.numPartitions;
    }

    @Override
    public Set<ServerDescriptor> serverDescriptors() throws ClusterManagerException {
        Set<ServerDescriptor> serverDescriptors = new HashSet<>();

        try {
            Set<ZNode> serverDescriptorZNodes = zkClient.getChildren(serverDir);

            for (ZNode serverDescriptorZNode : serverDescriptorZNodes) {
                NodeData<ServerDescriptor> serverDescriptorNodeData = zkClient.getData(serverDescriptorZNode, new ServerDescriptorSerializer());
                if (serverDescriptorNodeData != null) {
                    serverDescriptors.add(serverDescriptorNodeData.value);
                } else {
                    logger.warn("found an empty server descriptor at: {}", serverDescriptorZNode.path);
                }
            }
        } catch (Exception ex) {
            throw new ClusterManagerException("unable to fetch server descriptors from ZooKeeper", ex);
        }
        return serverDescriptors;
    }

    @Override
    public PartitionAssignment partitionAssignment() throws ClusterManagerException {
        try {
            NodeData<PartitionAssignment> partitionAssignmentNodeData =
                zkClient.getData(serverDir, new PartitionAssignmentSerializer());
            if (partitionAssignmentNodeData != null) {
                return partitionAssignmentNodeData.value;
            } else {
                throw new ClusterManagerException("found partition assignment node, but it was empty");
            }
        } catch (Exception ex) {
            throw new ClusterManagerException("unable to fetch server descriptors from ZooKeeper", ex);
        }
    }

    @Override
    public void manage(ManagedClient client) throws ClusterManagerException {
        client.setClusterName(clusterParams.name);
        client.setClientId(nextId(clientIdZNode));
        client.setNumPartitions(clusterParams.numPartitions);

        // Set endpoints.
        synchronized (clusterState) {
            synchronized (managedClients) {
                if (running) {
                    if (managedClients.contains(client))
                        throw new ClusterManagerException("client already managed");

                    try {
                        Cluster currentCluster = clusterState.get();
                        if (setEndpoints(client, currentCluster.partitionAssignment, currentCluster.serverMembership)) {
                            managedClients.add(client);
                        } else {
                            logger.error("unable to manage client: cluster state is inconsistent");
                            throw new ClusterManagerException("unable to manage client: cluster state is inconsistent");
                        }

                    } catch (Exception ex) {
                        logger.error("unable to manage client: cluster state is inconsistent", ex);
                        throw new ClusterManagerException("unable to manage client", ex);
                    }
                } else {
                    throw new ClusterManagerException("already closed");
                }
            }
        }
    }

    @Override
    public void manage(ManagedServer server) throws ClusterManagerException {
        synchronized (serverManagementLock) {
            synchronized (managedServers) {
                if (running) {
                    if (managedServers.containsKey(server)) {
                        throw new ClusterManagerException("server already managed");
                    }
                } else {
                    throw new ClusterManagerException("already closed");
                }
            }

            int serverId = nextId(serverIdZNode);
            ServerDescriptor descriptor =
                new ServerDescriptor(serverId, server.endpoint(), server.getPreferredPartitions());

            try {
                synchronized (managedServers) {
                    ZNode znode = createServerZNode(descriptor);
                    managedServers.put(server, new ManagedServerInfo(descriptor.serverId, znode));
                }
            } catch (Exception ex) {
                throw new ClusterManagerException("unable to manage server", ex);
            }

            server.setClusterName(clusterParams.name);
            server.setServerId(serverId);
        }
    }

    @Override
    public void unmanage(ManagedClient client) throws ClusterManagerException {
        synchronized (managedClients) {
            if (!managedClients.remove(client)) {
                throw new ClusterManagerException("no such managed client");
            }
        }
    }

    @Override
    public void unmanage(ManagedServer server) throws ClusterManagerException {
        synchronized (serverManagementLock) {
            ManagedServerInfo managedServerInfo;

            synchronized (managedServers) {
                managedServerInfo = managedServers.remove(server);
            }

            if (managedServerInfo == null) {
                throw new ClusterManagerException("no such managed server");
            }

            try {
                zkClient.delete(managedServerInfo.znode);

            } catch (KeeperException.NoNodeException ex) {
                // Ignore
            } catch (Exception ex) {
                throw new ClusterManagerException("unable to unmanage server", ex);
            }
        }
    }

    private static void closeHandles(WatcherHandle... handles) {
        for (WatcherHandle handle : handles) {
            if (handle != null)
                handle.close();
        }
    }

    private void onConnected(ZooKeeperSession s) {
        synchronized (managedServers) {
            if (running) {
                try {
                    for (ManagedServer server : new HashSet<>(managedServers.keySet())) {
                        ManagedServerInfo managedServerInfo = managedServers.get(server);
                        if (s.exists(managedServerInfo.znode) == null) {
                            // The znode is gone, recreate it
                            ServerDescriptor descriptor =
                                new ServerDescriptor(managedServerInfo.serverId, server.endpoint(), server.getPreferredPartitions());

                            createServerZNode(s, descriptor);
                        }
                    }
                } catch (Exception ex) {
                    // Ignore
                }
            }
        }
    }

    private boolean isCoordinator(Map<Integer, ServerDescriptor> membership) {
        Integer minServerId = Collections.min(membership.keySet());

        for (ManagedServerInfo managedServerInfo : managedServers.values()) {
            if (minServerId.equals(managedServerInfo.serverId))
                return true;
        }

        return false;
    }

    private void updateServers(NodeData<PartitionAssignment> nodeData, Map<ZNode, NodeData<ServerDescriptor>> serverNodeData) {
        final int version = nodeData.stat.getVersion();
        final int cversion = nodeData.stat.getCversion();

        // Get the current assignment in ZK
        // We should not use the cached assignment to increment the generation numbers correctly
        PartitionAssignment assignment = nodeData.value;

        // Create new membership map
        HashMap<Integer, ServerDescriptor> membership = new HashMap<>();
        for (NodeData<ServerDescriptor> data : serverNodeData.values()) {
            membership.put(data.value.serverId, data.value);
        }

        if (assignment != null && assignment.cversion == cversion) {
            // We got a consistent state (membership and partition assignment)
            updateCluster(membership, assignment);

        } else if (!membership.isEmpty()) {
            // Update the partition assignment
            while (true) {
                try {
                    ZooKeeperSession s = zkClient.session();

                    synchronized (managedServers) {
                        if (managedServers.size() > 0) {
                            if (isCoordinator(membership)) {
                                PartitionAssignment newAssignment =
                                    partitionAssignmentPolicy.update(cversion, assignment, clusterParams.numPartitions, membership);

                                // Save the new assignment in ZK. The version check will detect concurrent update.
                                s.setData(serverDir, newAssignment, partitionAssignmentSerializer, version);
                            }
                        }
                        return;
                    }
                } catch (KeeperException.BadVersionException ex) {
                    // Ignore The cluster has changed. There is no point updating assignment.
                    return;
                } catch (KeeperException.ConnectionLossException | KeeperException.SessionExpiredException ex) {
                    // Ignore and Retry
                    logger.warn("failed to update partition assignment, retrying...", ex);
                } catch (Throwable ex) {
                    // The assignment will stay stale. This is fatal. Do not retry.
                    logger.error("failed to update partition assignment", ex);
                    return;
                }
            }
        } else {
            // There is no server. The assignment will stay stale. Force clients to remove all servers from their view.
            removeAllServers();
        }

    }

    private void updateCluster(HashMap<Integer, ServerDescriptor> membership, PartitionAssignment assignment) {
        synchronized (clusterState) {
            // Update the server membership
            Cluster oldCluster = clusterState.get();

            // Update servers
            synchronized (managedServers) {
                for (Map.Entry<ManagedServer, ManagedServerInfo> entry : managedServers.entrySet()) {
                    ManagedServer server = entry.getKey();
                    ManagedServerInfo managedServerInfo = entry.getValue();

                    if (assignment != null) {
                        server.setPartitions(assignment.partitionsFor(managedServerInfo.serverId));
                    } else {
                        server.setPartitions(Collections.emptyList());
                    }
                }
            }

            // Update clients
            synchronized (managedClients) {
                // Remove dead servers from clients' view of the cluster
                for (Map.Entry<Integer, ServerDescriptor> entry : oldCluster.serverMembership.entrySet()) {
                    if (!membership.containsKey(entry.getKey())) {
                        for (ManagedClient client : managedClients) {
                            client.removeServer(entry.getValue().endpoint);
                        }
                    }
                }

                for (ManagedClient client : managedClients) {
                    if (!setEndpoints(client, assignment, membership)) {
                        logger.error("partition assignment is inconsistent with server data");
                    }
                }
            }

            clusterState.set(new Cluster(membership, assignment));
        }
    }

    private void removeAllServers() {
        synchronized (clusterState) {
            // Update the server membership
            Cluster oldCluster = clusterState.get();

            synchronized (managedClients) {
                // Remove all servers from clients' view of the cluster
                for (Map.Entry<Integer, ServerDescriptor> entry : oldCluster.serverMembership.entrySet()) {
                    for (ManagedClient client : managedClients) {
                        client.removeServer(entry.getValue().endpoint);
                    }
                }
            }

            clusterState.set(new Cluster(Collections.emptyMap(), new PartitionAssignment()));
        }
    }

    private boolean setEndpoints(ManagedClient client, PartitionAssignment assignment, Map<Integer, ServerDescriptor> serverDescriptors) {
        Map<Endpoint, List<PartitionInfo>> endpoints = new HashMap<>();

        if (assignment != null) {
            for (Integer serverId : assignment.serverIds()) {
                ServerDescriptor serverDescriptor = serverDescriptors.get(serverId);

                if (serverDescriptor != null) {
                    endpoints.put(serverDescriptor.endpoint, Collections.unmodifiableList(assignment.partitionsFor(serverId)));
                } else {
                    // The partition assignment is inconsistent with server data.
                    return false;
                }
            }
        }
        client.setEndpoints(endpoints);

        return true;
    }

    private static void createNode(ZooKeeperClient zkClient, ZNode znode) throws KeeperException, ZooKeeperClientException {
        try {
            zkClient.create(znode, CreateMode.PERSISTENT);

        } catch (KeeperException.NodeExistsException ex) {
            // Ignore
        }
    }

    private static <T> void createNode(ZooKeeperClient zkClient, ZNode znode, T data, Serializer<T> serializer) throws KeeperException, ZooKeeperClientException {
        try {
            zkClient.create(znode, data, serializer, CreateMode.PERSISTENT);

        } catch (KeeperException.NodeExistsException ex) {
            // Ignore the exception when the znode already exists
        }
    }

    private ZNode createServerZNode(ZooKeeperSession s, ServerDescriptor descriptor) throws Exception {
        return s.create(new ZNode(serverDir, serverNodePrefix), descriptor, serverDescriptorSerializer, CreateMode.EPHEMERAL_SEQUENTIAL);
    }

    private ZNode createServerZNode(ServerDescriptor descriptor) throws Exception {
        return zkClient.create(new ZNode(serverDir, serverNodePrefix), descriptor, serverDescriptorSerializer, CreateMode.EPHEMERAL_SEQUENTIAL);
    }

    private int nextId(ZNode idZNode) throws ClusterManagerException {
        while (true) {
            try {
                ZooKeeperSession s = zkClient.session();

                NodeData<Integer> data = s.getData(idZNode, intSerializer);
                int nextId = data.value + 1;

                // Set the new transactionId. The version is checked to detect a race condition.
                s.setData(idZNode, nextId, intSerializer, data.stat.getVersion());

                return nextId;

            } catch (KeeperException.ConnectionLossException | KeeperException.SessionExpiredException | KeeperException.BadVersionException ex) {
               // Ignore and Retry (BadVersionException means a race condition)
                logger.warn("failed to get new id, retrying...", ex);
            } catch (Exception ex) {
                throw new ClusterManagerException("unable to get new id", ex);
            }
        }
    }

    public static class Cluster {

        public final Map<Integer, ServerDescriptor> serverMembership;
        public final PartitionAssignment partitionAssignment;

        Cluster(Map<Integer, ServerDescriptor> serverMembership, PartitionAssignment partitionAssignment) {
            this.serverMembership = Collections.unmodifiableMap(serverMembership);
            this.partitionAssignment = partitionAssignment;
        }

    }

}
