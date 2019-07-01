package com.wepay.zktools.clustermgr;

import com.wepay.zktools.clustermgr.internal.ClusterManagerImpl;
import com.wepay.zktools.clustermgr.internal.StaticPartitionAssignmentPolicy;
import com.wepay.zktools.clustermgr.tools.CreateCluster;
import com.wepay.zktools.test.MockManagedClient;
import com.wepay.zktools.test.MockManagedServer;
import com.wepay.zktools.test.util.ZooKeeperServerRunner;
import com.wepay.zktools.util.Uninterruptibly;
import com.wepay.zktools.zookeeper.ZNode;
import com.wepay.zktools.zookeeper.ZooKeeperClient;
import com.wepay.zktools.zookeeper.internal.ZooKeeperClientImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * This test makes sure that there won't be a race condition when starting to manage multiple servers and
 * clients at the same time.
 */
public class ClusterManagerStressTest {

    private static final long TIMEOUT = 3000;
    private static final long SLEEP = 100;
    private static final int ZK_SESSION_TIMEOUT = 30000;

    private ZooKeeperServerRunner zooKeeperServerRunner = null;
    private ZooKeeperClient zooKeeperClient1 = null;
    private ZooKeeperClient zooKeeperClient2 = null;
    private ZooKeeperClient zooKeeperClient3 = null;
    private ZNode root = null;
    private ClusterManager clusterManager1 = null;
    private ClusterManager clusterManager2 = null;
    private ClusterManager clusterManager3 = null;

    @Before
    public void setup() throws Exception {
        // ZooKeeper setup
        zooKeeperServerRunner = new ZooKeeperServerRunner(0);
        zooKeeperServerRunner.start();

        zooKeeperClient1 = new ZooKeeperClientImpl(zooKeeperServerRunner.connectString(), ZK_SESSION_TIMEOUT);
        zooKeeperClient2 = new ZooKeeperClientImpl(zooKeeperServerRunner.connectString(), ZK_SESSION_TIMEOUT);
        zooKeeperClient3 = new ZooKeeperClientImpl(zooKeeperServerRunner.connectString(), ZK_SESSION_TIMEOUT);

        root = new ZNode("/test");

        // Create a cluster
        CreateCluster.create(root, "testCluster", 3, zooKeeperClient1, true, null);

        clusterManager1 = new ClusterManagerImpl(zooKeeperClient1, root, new StaticPartitionAssignmentPolicy());
        clusterManager2 = new ClusterManagerImpl(zooKeeperClient2, root, new StaticPartitionAssignmentPolicy());
        clusterManager3 = new ClusterManagerImpl(zooKeeperClient3, root, new StaticPartitionAssignmentPolicy());
    }

    @After
    public void teardown() throws Exception {
        clusterManager1.close();
        clusterManager2.close();
        clusterManager3.close();

        zooKeeperClient1.close();
        zooKeeperClient1 = null;

        zooKeeperClient2.close();
        zooKeeperClient2 = null;

        zooKeeperClient3.close();
        zooKeeperClient3 = null;

        zooKeeperServerRunner.stop();
        zooKeeperServerRunner.clear();
        zooKeeperServerRunner = null;
    }

    @Test
    public void test() throws Exception {
        // Setup the test cluster
        MockManagedClient client1 = new MockManagedClient();
        MockManagedClient client2 = new MockManagedClient();
        MockManagedClient client3 = new MockManagedClient();
        MockManagedServer server1 = new MockManagedServer("host1", 9001, Collections.singletonList(0));
        MockManagedServer server2 = new MockManagedServer("host1", 9002, Collections.singletonList(1));
        MockManagedServer server3 = new MockManagedServer("host1", 9003, Collections.singletonList(2));

        clusterManager1.manage(server1);
        clusterManager1.manage(client1);
        clusterManager2.manage(server2);
        clusterManager1.manage(client2);
        clusterManager3.manage(server3);
        clusterManager1.manage(client3);

        wait(client1, 3);
        wait(client2, 3);
        wait(client3, 3);

        assertEquals(server1.endpoint(), client1.partitions.get(0));
        assertEquals(server2.endpoint(), client1.partitions.get(1));
        assertEquals(server3.endpoint(), client1.partitions.get(2));

        assertEquals(new HashSet<>(server1.getPreferredPartitions()), server1.partitions);
        assertEquals(new HashSet<>(server2.getPreferredPartitions()), server2.partitions);
        assertEquals(new HashSet<>(server3.getPreferredPartitions()), server3.partitions);

        clusterManager1.unmanage(server1);

        wait(client1, 2);
        wait(client2, 2);
        wait(client3, 2);

        assertNull(client1.partitions.get(0));
        assertEquals(server2.endpoint(), client1.partitions.get(1));
        assertEquals(server3.endpoint(), client1.partitions.get(2));
    }

    private void wait(MockManagedClient client, int numAssignedPartitions) {
        long due = System.currentTimeMillis() + TIMEOUT;
        while (System.currentTimeMillis() < due) {
            if (client.partitions.size() == numAssignedPartitions) {
                break;

            } else {
                Uninterruptibly.sleep(SLEEP);
            }
        }
    }

}
