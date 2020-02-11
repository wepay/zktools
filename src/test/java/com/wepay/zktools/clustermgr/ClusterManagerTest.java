package com.wepay.zktools.clustermgr;

import com.wepay.zktools.clustermgr.internal.ClusterManagerImpl;
import com.wepay.zktools.clustermgr.internal.ClusterManagerImpl.Cluster;
import com.wepay.zktools.clustermgr.internal.ClusterParams;
import com.wepay.zktools.clustermgr.internal.ClusterParamsSerializer;
import com.wepay.zktools.clustermgr.internal.DynamicPartitionAssignmentPolicy;
import com.wepay.zktools.clustermgr.internal.PartitionAssignment;
import com.wepay.zktools.clustermgr.internal.ServerDescriptor;
import com.wepay.zktools.test.MockManagedClient;
import com.wepay.zktools.test.MockManagedServer;
import com.wepay.zktools.test.ZKTestUtils;
import com.wepay.zktools.test.util.ZooKeeperServerRunner;
import com.wepay.zktools.util.StateChangeFuture;
import com.wepay.zktools.zookeeper.ZNode;
import com.wepay.zktools.zookeeper.ZooKeeperClient;
import com.wepay.zktools.zookeeper.internal.ZooKeeperClientImpl;
import com.wepay.zktools.zookeeper.serializer.IntegerSerializer;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ClusterManagerTest extends ZKTestUtils {

    private final ZNode root = new ZNode("/cluster");

    @Test
    public void testPartitionAssignment() throws Exception {
        testPartitionAssignment(1);
        testPartitionAssignment(2);
        testPartitionAssignment(3);
        testPartitionAssignment(10);
    }

    @Test
    public void testPartitionAssignmentWithNoPartition() throws Exception {
        testPartitionAssignment(0);
    }

    private void testPartitionAssignment(final int numPartitions) throws Exception {
        MockManagedServer server1 = new MockManagedServer("host1", 9001);
        MockManagedServer server2 = new MockManagedServer("host2", 9002);
        MockManagedClient client1 = new MockManagedClient();
        MockManagedClient client2 = new MockManagedClient();
        StateChangeFuture<ClusterManagerImpl.Cluster> clusterStateChangeFuture;
        Map<Integer, Endpoint> expectedPartitions = new HashMap<>();

        ZooKeeperServerRunner zooKeeperServerRunner = new ZooKeeperServerRunner(0);
        try {
            String connectString = zooKeeperServerRunner.start();
            ZooKeeperClient zkClient = new ZooKeeperClientImpl(connectString, 30000);

            // Cluster setup
            ZNode clusterZNode = new ZNode("/cluster");
            zkClient.create(clusterZNode, CreateMode.PERSISTENT);
            zkClient.setData(clusterZNode, new ClusterParams("test cluster", numPartitions), new ClusterParamsSerializer());
            ClusterManagerImpl.createZNodes(zkClient, root);

            // ClusterManager
            ClusterManagerImpl cluster = new ClusterManagerImpl(zkClient, root, new DynamicPartitionAssignmentPolicy());

            // The first client
            cluster.manage(client1);
            assertEquals(numPartitions, client1.numPartitions);
            assertTrue(client1.partitions.isEmpty());

            // The first server
            clusterStateChangeFuture = cluster.clusterState.watch();
            cluster.manage(server1);
            clusterStateChangeFuture.get();

            // Check the partition assignment
            computeExpectedPartitions(expectedPartitions, server1);
            assertEquals(numPartitions, expectedPartitions.size());
            assertEquals(expectedPartitions, client1.partitions);

            // The second client
            cluster.manage(client2);
            assertEquals(expectedPartitions, client2.partitions);

            // The second server
            clusterStateChangeFuture = cluster.clusterState.watch();
            cluster.manage(server2);
            clusterStateChangeFuture.get();

            // Check the partition assignment
            int minNumPartitionPerServer = numPartitions / 2;
            int maxNumPartitionPerServer = numPartitions / 2 + (numPartitions % 2);

            assertTrue(server1.partitions.size() >= minNumPartitionPerServer && server1.partitions.size() <= maxNumPartitionPerServer);
            assertTrue(server2.partitions.size() >= minNumPartitionPerServer && server2.partitions.size() <= maxNumPartitionPerServer);
            assertEquals(numPartitions, server1.partitions.size() + server2.partitions.size());

            computeExpectedPartitions(expectedPartitions, server1, server2);
            assertEquals(numPartitions, expectedPartitions.size());
            assertEquals(numPartitions, client1.partitions.size());
            assertEquals(numPartitions, client2.partitions.size());

            assertEquals(expectedPartitions, client1.partitions);
            assertEquals(expectedPartitions, client2.partitions);

            // Remove the first server
            clusterStateChangeFuture = cluster.clusterState.watch();
            cluster.unmanage(server1);
            clusterStateChangeFuture.get();

            // Check the partition assignment
            assertEquals(numPartitions, server2.partitions.size());

            computeExpectedPartitions(expectedPartitions, server2);
            assertEquals(numPartitions, expectedPartitions.size());

            assertEquals(expectedPartitions, client1.partitions);
            assertEquals(expectedPartitions, client2.partitions);

            // Remove the second server. There will be no server left.
            clusterStateChangeFuture = cluster.clusterState.watch();
            cluster.unmanage(server2);
            clusterStateChangeFuture.get();

            // Check the partition assignment. There shouldn't be any assignment in clients.
            assertTrue(client1.partitions.isEmpty());
            assertTrue(client2.partitions.isEmpty());

            // Bring back the first server
            clusterStateChangeFuture = cluster.clusterState.watch();
            cluster.manage(server1);
            clusterStateChangeFuture.get();

            // Check the partition assignment
            computeExpectedPartitions(expectedPartitions, server1);
            assertEquals(numPartitions, expectedPartitions.size());
            assertEquals(expectedPartitions, client1.partitions);
            assertEquals(expectedPartitions, client2.partitions);

        } finally {
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.clear();
        }
    }

    @Test
    public void testServerDescriptorsAndPartitionAssignment() throws Exception {
        int numPartitions = 2;
        MockManagedServer server1 = new MockManagedServer("host1", 9001);
        MockManagedServer server2 = new MockManagedServer("host2", 9002);
        StateChangeFuture<ClusterManagerImpl.Cluster> clusterStateChangeFuture;

        ZooKeeperServerRunner zooKeeperServerRunner = new ZooKeeperServerRunner(0);
        try {
            String connectString = zooKeeperServerRunner.start();
            ZooKeeperClient zkClient = new ZooKeeperClientImpl(connectString, 30000);

            // Cluster setup
            ZNode clusterZNode = new ZNode("/cluster");
            zkClient.create(clusterZNode, CreateMode.PERSISTENT);
            zkClient.setData(clusterZNode, new ClusterParams("test cluster", numPartitions), new ClusterParamsSerializer());
            ClusterManagerImpl.createZNodes(zkClient, root);

            // ClusterManager
            ClusterManagerImpl cluster = new ClusterManagerImpl(zkClient, root, new DynamicPartitionAssignmentPolicy());

            // The first server
            clusterStateChangeFuture = cluster.clusterState.watch();
            cluster.manage(server1);
            clusterStateChangeFuture.get();

            // Check the server descriptors
            List<ServerDescriptor> serverDescriptors = new ArrayList<>(cluster.serverDescriptors());
            assertEquals(1, serverDescriptors.size());
            ServerDescriptor serverDescriptor = serverDescriptors.get(0);
            assertEquals(1, serverDescriptor.serverId);
            assertEquals(server1.endpoint(), serverDescriptor.endpoint);
            assertEquals(0, serverDescriptor.partitions.size());

            // Check the partition assignments
            PartitionAssignment partitionAssignment = cluster.partitionAssignment();
            assertEquals(1, partitionAssignment.cversion);
            assertEquals(2, partitionAssignment.numPartitions);
            assertEquals(1, partitionAssignment.numEndpoints);
            assertEquals(1, partitionAssignment.serverIds().size());
            assertEquals(1, partitionAssignment.serverIds().iterator().next().intValue());
            List<PartitionInfo> partitions = partitionAssignment.partitionsFor(1);
            partitions.sort(Comparator.comparingInt(p -> p.partitionId));
            assertEquals(2, partitions.size());
            assertEquals(new PartitionInfo(0, 1), partitions.get(0));
            assertEquals(new PartitionInfo(1, 1), partitions.get(1));

            // The second server
            clusterStateChangeFuture = cluster.clusterState.watch();
            cluster.manage(server2);
            clusterStateChangeFuture.get();

            // Check the server descriptors
            serverDescriptors = new ArrayList<>(cluster.serverDescriptors());
            assertEquals(2, serverDescriptors.size());
            serverDescriptors.sort(Comparator.comparingInt(sd -> sd.serverId));
            assertEquals(1, serverDescriptors.get(0).serverId);
            assertEquals(server1.endpoint(), serverDescriptors.get(0).endpoint);
            assertEquals(0, serverDescriptors.get(0).partitions.size());
            assertEquals(2, serverDescriptors.get(1).serverId);
            assertEquals(server2.endpoint(), serverDescriptors.get(1).endpoint);
            assertEquals(0, serverDescriptors.get(1).partitions.size());

            // Check the partition assignments
            partitionAssignment = cluster.partitionAssignment();
            assertEquals(2, partitionAssignment.cversion);
            assertEquals(2, partitionAssignment.numPartitions);
            assertEquals(2, partitionAssignment.numEndpoints);
            assertEquals(2, partitionAssignment.serverIds().size());
            List<Integer> serverIds = new ArrayList<>(partitionAssignment.serverIds());
            serverIds.sort(Integer::compare);
            assertEquals(1, serverIds.get(0).intValue());
            assertEquals(2, serverIds.get(1).intValue());

            // Check server partition info
            List<PartitionInfo> partitions1 = partitionAssignment.partitionsFor(1);
            List<PartitionInfo> partitions2 = partitionAssignment.partitionsFor(2);
            assertEquals(1, partitions1.size());
            assertEquals(1, partitions2.size());
            PartitionInfo partitionInfo1 = partitions1.get(0);
            PartitionInfo partitionInfo2 = partitions2.get(0);
            if (partitionInfo1.partitionId == 0) {
                assertEquals(new PartitionInfo(0, 1), partitionInfo1);
                assertEquals(new PartitionInfo(1, 2), partitionInfo2);
            } else {
                assertEquals(new PartitionInfo(1, 1), partitionInfo1);
                assertEquals(new PartitionInfo(0, 2), partitionInfo2);
            }
        } finally {
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.clear();
        }
    }

    @Test
    public void testReManageSameServer() throws Exception {
        int numPartitions = 2;
        MockManagedServer server1 = new MockManagedServer("host1", 9001);
        StateChangeFuture<ClusterManagerImpl.Cluster> clusterStateChangeFuture;

        ZooKeeperServerRunner zooKeeperServerRunner = new ZooKeeperServerRunner(0);
        try {
            String connectString = zooKeeperServerRunner.start();
            ZooKeeperClient zkClient = new ZooKeeperClientImpl(connectString, 30000);
            Cluster clusterState = null;

            // Cluster setup
            ZNode clusterZNode = new ZNode("/cluster");
            zkClient.create(clusterZNode, CreateMode.PERSISTENT);
            zkClient.setData(clusterZNode, new ClusterParams("test cluster", numPartitions), new ClusterParamsSerializer());
            ClusterManagerImpl.createZNodes(zkClient, root);

            // ClusterManager
            ClusterManagerImpl cluster = new ClusterManagerImpl(zkClient, root, new DynamicPartitionAssignmentPolicy());

            // Manage server 1
            clusterStateChangeFuture = cluster.clusterState.watch();
            cluster.manage(server1);
            clusterStateChangeFuture.get();

            // Check the server descriptors
            List<ServerDescriptor> serverDescriptors = new ArrayList<>(cluster.serverDescriptors());
            assertEquals(1, serverDescriptors.size());
            ServerDescriptor serverDescriptor = serverDescriptors.get(0);
            assertEquals(1, serverDescriptor.serverId);
            assertEquals(server1.endpoint(), serverDescriptor.endpoint);
            assertEquals(0, serverDescriptor.partitions.size());

            // Check the partition assignments
            clusterState = cluster.clusterState.get();
            PartitionAssignment partitionAssignment = cluster.partitionAssignment();
            assertEquals(1, clusterState.version);
            assertEquals(1, partitionAssignment.cversion);
            assertEquals(2, partitionAssignment.numPartitions);
            assertEquals(1, partitionAssignment.numEndpoints);
            assertEquals(1, partitionAssignment.serverIds().size());
            assertEquals(1, partitionAssignment.serverIds().iterator().next().intValue());
            List<PartitionInfo> partitions = partitionAssignment.partitionsFor(1);
            partitions.sort(Comparator.comparingInt(p -> p.partitionId));
            assertEquals(2, partitions.size());
            assertEquals(new PartitionInfo(0, 1), partitions.get(0));
            assertEquals(new PartitionInfo(1, 1), partitions.get(1));

            // Re-manage server 1
            clusterStateChangeFuture = cluster.clusterState.watch();
            cluster.manage(server1);
            clusterStateChangeFuture.get();

            // Check the server descriptors
            serverDescriptors = new ArrayList<>(cluster.serverDescriptors());
            assertEquals(1, serverDescriptors.size());
            serverDescriptor = serverDescriptors.get(0);
            assertEquals(1, serverDescriptor.serverId);
            assertEquals(server1.endpoint(), serverDescriptor.endpoint);
            assertEquals(0, serverDescriptor.partitions.size());

            // Check the partition assignments
            clusterState = cluster.clusterState.get();
            partitionAssignment = cluster.partitionAssignment();
            assertEquals(2, clusterState.version);
            assertEquals(1, partitionAssignment.cversion);
            assertEquals(2, partitionAssignment.numPartitions);
            assertEquals(1, partitionAssignment.numEndpoints);
            assertEquals(1, partitionAssignment.serverIds().size());
            assertEquals(1, partitionAssignment.serverIds().iterator().next().intValue());
            partitions = partitionAssignment.partitionsFor(1);
            partitions.sort(Comparator.comparingInt(p -> p.partitionId));
            assertEquals(2, partitions.size());
            assertEquals(new PartitionInfo(0, 1), partitions.get(0));
            assertEquals(new PartitionInfo(1, 1), partitions.get(1));

            // Add and manage Server 2
            MockManagedServer server2 = new MockManagedServer("host2", 9002);
            clusterStateChangeFuture = cluster.clusterState.watch();
            cluster.manage(server2);
            clusterStateChangeFuture.get();

            // Check the server descriptors
            serverDescriptors = new ArrayList<>(cluster.serverDescriptors());
            serverDescriptors.sort(Comparator.comparing(sd -> sd.serverId));
            assertEquals(2, serverDescriptors.size());
            serverDescriptor = serverDescriptors.get(0);
            assertEquals(1, serverDescriptor.serverId);
            assertEquals(server1.endpoint(), serverDescriptor.endpoint);
            assertEquals(0, serverDescriptor.partitions.size());
            serverDescriptor = serverDescriptors.get(1);
            assertEquals(2, serverDescriptor.serverId);
            assertEquals(server2.endpoint(), serverDescriptor.endpoint);
            assertEquals(0, serverDescriptor.partitions.size());

            // Check the partition assignments
            clusterState = cluster.clusterState.get();
            partitionAssignment = cluster.partitionAssignment();
            assertEquals(3, clusterState.version);
            assertEquals(2, partitionAssignment.cversion);
            assertEquals(2, partitionAssignment.numPartitions);
            assertEquals(2, partitionAssignment.numEndpoints);
            assertEquals(2, partitionAssignment.serverIds().size());
            partitions = partitionAssignment.partitionsFor(1);
            partitions.sort(Comparator.comparingInt(p -> p.partitionId));
            assertEquals(1, partitions.size());
            partitions = partitionAssignment.partitionsFor(2);
            partitions.sort(Comparator.comparingInt(p -> p.partitionId));
            assertEquals(1, partitions.size());

        } finally {
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.clear();
        }
    }

    @Test
    public void testWithZookeeperConnectionLoss() throws Exception {
        MockManagedServer server1 = new MockManagedServer("host1", 9001);
        MockManagedServer server2 = new MockManagedServer("host2", 9002);
        MockManagedClient client1 = new MockManagedClient();
        MockManagedClient client2 = new MockManagedClient();
        StateChangeFuture<ClusterManagerImpl.Cluster> clusterStateChangeFuture;
        Map<Integer, Endpoint> expectedPartitions = new HashMap<>();

        ZooKeeperServerRunner zooKeeperServerRunner = new ZooKeeperServerRunner(0);
        try {
            String connectString = zooKeeperServerRunner.start();
            ZooKeeperClient zkClient = new ZooKeeperClientImpl(connectString, 30000);

            // Cluster setup
            final int numPartitions = 10;
            ZNode clusterZNode = new ZNode("/cluster");
            zkClient.create(clusterZNode, CreateMode.PERSISTENT);
            zkClient.setData(clusterZNode, new ClusterParams("test cluster", numPartitions), new ClusterParamsSerializer());
            ClusterManagerImpl.createZNodes(zkClient, root);

            // ClusterManager
            ClusterManagerImpl cluster = new ClusterManagerImpl(zkClient, root, new DynamicPartitionAssignmentPolicy());

            clusterStateChangeFuture = cluster.clusterState.watch();
            cluster.manage(server1);
            clusterStateChangeFuture.get();

            cluster.manage(client1);

            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.start();

            // Wait until server1 shows up
            clusterStateChangeFuture = cluster.clusterState.watch();
            while (clusterStateChangeFuture.currentState.partitionAssignment.serverIds().size() < 1) {
                clusterStateChangeFuture.get();
                clusterStateChangeFuture = cluster.clusterState.watch();
            }

            cluster.manage(server2);
            while (clusterStateChangeFuture.get().partitionAssignment.serverIds().size() < 2) {
                clusterStateChangeFuture = cluster.clusterState.watch();
            }

            cluster.manage(client2);

            // Check the partition assignment
            assertEquals(numPartitions / 2, server1.partitions.size());
            assertEquals(numPartitions / 2, server2.partitions.size());
            assertEquals(numPartitions, server1.partitions.size() + server2.partitions.size());

            computeExpectedPartitions(expectedPartitions, server1, server2);
            assertEquals(numPartitions, expectedPartitions.size());
            assertEquals(numPartitions, client1.partitions.size());
            assertEquals(numPartitions, client2.partitions.size());
            assertEquals(expectedPartitions, client1.partitions);
            assertEquals(expectedPartitions, client2.partitions);

        } finally {
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.clear();
        }
    }

    @Test
    public void testWithZooKeeperSessionExpiration() throws Exception {
        MockManagedServer server1 = new MockManagedServer("host1", 9001);
        MockManagedServer server2 = new MockManagedServer("host2", 9002);
        MockManagedClient client1 = new MockManagedClient();
        MockManagedClient client2 = new MockManagedClient();
        StateChangeFuture<ClusterManagerImpl.Cluster> clusterStateChangeFuture;
        Map<Integer, Endpoint> expectedPartitions = new HashMap<>();

        ZooKeeperServerRunner zooKeeperServerRunner = new ZooKeeperServerRunner(0);
        try {
            String connectString = zooKeeperServerRunner.start();
            ZooKeeperClient zkClient = new ZooKeeperClientImpl(connectString, 30000);

            // Cluster setup
            final int numPartitions = 10;
            ZNode clusterZNode = new ZNode("/cluster");
            zkClient.create(clusterZNode, CreateMode.PERSISTENT);
            zkClient.setData(clusterZNode, new ClusterParams("test cluster", numPartitions), new ClusterParamsSerializer());
            ClusterManagerImpl.createZNodes(zkClient, root);

            // ClusterManager
            ClusterManagerImpl cluster = new ClusterManagerImpl(zkClient, root, new DynamicPartitionAssignmentPolicy());

            clusterStateChangeFuture = cluster.clusterState.watch();
            cluster.manage(server1);
            clusterStateChangeFuture.get();

            cluster.manage(client1);

            ZKTestUtils.expire(zkClient.session());

            // Wait until server1 shows up
            clusterStateChangeFuture = cluster.clusterState.watch();
            while (clusterStateChangeFuture.currentState.partitionAssignment.serverIds().size() < 1) {
                clusterStateChangeFuture.get();
                clusterStateChangeFuture = cluster.clusterState.watch();
            }

            clusterStateChangeFuture = cluster.clusterState.watch();
            cluster.manage(server2);
            while (clusterStateChangeFuture.get().partitionAssignment.serverIds().size() < 2) {
                clusterStateChangeFuture = cluster.clusterState.watch();
            }

            cluster.manage(client2);

            // Check the partition assignment
            assertEquals(numPartitions / 2, server1.partitions.size());
            assertEquals(numPartitions / 2, server2.partitions.size());
            assertEquals(numPartitions, server1.partitions.size() + server2.partitions.size());

            computeExpectedPartitions(expectedPartitions, server1, server2);
            assertEquals(numPartitions, expectedPartitions.size());
            assertEquals(numPartitions, client1.partitions.size());
            assertEquals(numPartitions, client2.partitions.size());
            assertEquals(expectedPartitions, client1.partitions);
            assertEquals(expectedPartitions, client2.partitions);

        } finally {
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.clear();
        }
    }

    @Test
    public void testServerRecovery() throws Exception {
        MockManagedServer server = new MockManagedServer("host1", 9001);
        StateChangeFuture<ClusterManagerImpl.Cluster> clusterStateChangeFuture;
        Set<ServerDescriptor> serverDescriptors;
        ZNode step = new ZNode("/step");
        IntegerSerializer integerSerializer = new IntegerSerializer();

        ZooKeeperServerRunner zooKeeperServerRunner = new ZooKeeperServerRunner(0);
        try {
            String connectString = zooKeeperServerRunner.start();
            ZooKeeperClient zkClient = new ZooKeeperClientImpl(connectString, 30000);

            zkClient.create(step, 1, integerSerializer, CreateMode.PERSISTENT);

            // Cluster setup
            final int numPartitions = 10;
            ZNode clusterZNode = new ZNode("/cluster");
            ZNode serverDir = new ZNode(clusterZNode, "serverDescriptors");
            zkClient.create(clusterZNode, CreateMode.PERSISTENT);
            zkClient.setData(clusterZNode, new ClusterParams("test cluster", numPartitions), new ClusterParamsSerializer());
            ClusterManagerImpl.createZNodes(zkClient, root);

            // ClusterManager
            ClusterManagerImpl cluster = new ClusterManagerImpl(zkClient, root, new DynamicPartitionAssignmentPolicy());

            clusterStateChangeFuture = cluster.clusterState.watch();
            cluster.manage(server);
            clusterStateChangeFuture.get();

            // Wait until server shows up.
            clusterStateChangeFuture = cluster.clusterState.watch();
            while (clusterStateChangeFuture.currentState.serverMembership.size() < 1) {
                clusterStateChangeFuture.get();
                clusterStateChangeFuture = cluster.clusterState.watch();
            }

            // Check the server descriptors
            assertEquals(1, zkClient.getChildren(serverDir).size());
            serverDescriptors = cluster.serverDescriptors();
            assertEquals(1, serverDescriptors.size());
            for (ServerDescriptor serverDescriptor : serverDescriptors) {
                assertEquals(server.endpoint(), serverDescriptor.endpoint);
            }

            // Expire session.
            ZKTestUtils.expire(zkClient.session());

            // Wait for new session to become active.
            zkClient.setData(step, 2, integerSerializer);

            // Wait until server shows up.
            clusterStateChangeFuture = cluster.clusterState.watch();
            while (clusterStateChangeFuture.currentState.serverMembership.size() < 1) {
                clusterStateChangeFuture.get();
                clusterStateChangeFuture = cluster.clusterState.watch();
            }

            // Check the server descriptor
            assertEquals(1, zkClient.getChildren(serverDir).size());
            serverDescriptors = cluster.serverDescriptors();
            assertEquals(1, serverDescriptors.size());
            for (ServerDescriptor serverDescriptor : serverDescriptors) {
                assertEquals(server.endpoint(), serverDescriptor.endpoint);
            }

            // Force connection loss.
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.start();

            // Wait for the session to reconnect.
            zkClient.setData(step, 3, integerSerializer);

            // Wait until server shows up again.
            clusterStateChangeFuture = cluster.clusterState.watch();
            while (clusterStateChangeFuture.currentState.serverMembership.size() < 1) {
                clusterStateChangeFuture.get();
                clusterStateChangeFuture = cluster.clusterState.watch();
            }

            // Check the server descriptor
            assertEquals(1, zkClient.getChildren(serverDir).size());

            serverDescriptors = cluster.serverDescriptors();
            assertEquals(1, serverDescriptors.size());
            for (ServerDescriptor serverDescriptor : serverDescriptors) {
                assertEquals(server.endpoint(), serverDescriptor.endpoint);
            }

        } finally {
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.clear();
        }
    }

    private void computeExpectedPartitions(Map<Integer, Endpoint> expectedPartitions,
                                           MockManagedServer... servers) throws ClusterManagerException {
        expectedPartitions.clear();
        for (MockManagedServer server : servers) {
            for (Integer partitionId : server.partitions) {
                expectedPartitions.put(partitionId, server.endpoint());
            }
        }
    }

}
