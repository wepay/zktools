package com.wepay.zktools.zookeeper;

import com.wepay.zktools.test.NodeDataWatch;
import com.wepay.zktools.test.ParentChildData;
import com.wepay.zktools.test.ParentChildDataWatch;
import com.wepay.zktools.test.ZKTestUtils;
import com.wepay.zktools.test.util.Utils;
import com.wepay.zktools.test.util.ZooKeeperServerRunner;
import com.wepay.zktools.util.State;
import com.wepay.zktools.util.StateChangeFuture;
import com.wepay.zktools.util.Uninterruptibly;
import com.wepay.zktools.zookeeper.internal.ZooKeeperClientImpl;
import com.wepay.zktools.zookeeper.serializer.IntegerSerializer;
import com.wepay.zktools.zookeeper.serializer.StringSerializer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ZooKeeperClientTest extends ZKTestUtils {

    @Test
    public void testConnectionLoss() throws Exception {
        ZooKeeperServerRunner zooKeeperServerRunner = new ZooKeeperServerRunner(0);
        try {
            String connectString = zooKeeperServerRunner.start();

            ZooKeeperClient client = new ZooKeeperClientImpl(connectString, 30000);

            ZooKeeperSession session = client.session();
            assertTrue(session.isConnected());

            State<Integer> disconnected = new State<>(0);
            State<Integer> connected = new State<>(0);

            client.onDisconnected(() ->
                disconnected.set(disconnected.get() + 1)
            );
            client.onConnected(s -> {
                connected.set(connected.get() + 1);
            });

            assertEquals(1, (int) connected.get());

            StateChangeFuture<Integer> disconnectedStateChangeFuture = disconnected.watch();
            assertEquals(0, (int) disconnectedStateChangeFuture.currentState);

            zooKeeperServerRunner.stop();

            try {
                session.exists(new ZNode("/dummy"));
                fail("still connected");
            } catch (KeeperException.ConnectionLossException ex) {
                // OK
            }

            assertEquals(1, (int) disconnectedStateChangeFuture.get(1000, TimeUnit.MILLISECONDS));

            StateChangeFuture<Integer> connectedStateChangeFuture = connected.watch();
            assertEquals(1, (int) connectedStateChangeFuture.currentState);

            Future<String> f = zooKeeperServerRunner.startAsync(1000L);
            assertFalse(f.isDone());

            try {
                // exists() awaits reconnect
                client.exists(new ZNode("/dummy"));

            } catch (KeeperException.ConnectionLossException ex) {
                fail("session not connected");
            } catch (KeeperException.SessionExpiredException ex) {
                fail("session expired");
            }

            assertTrue(f.isDone());
            assertEquals(2, (int) connectedStateChangeFuture.get(3000, TimeUnit.MILLISECONDS));
            assertEquals(1, (int) disconnected.get());

        } finally {
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.clear();
        }
    }

    @Test
    public void testConnectTimeoutWhenZooKeeperNotRunning() throws Exception {
        ZooKeeperServerRunner zooKeeperServerRunner = new ZooKeeperServerRunner(0);
        try {
            String connectString = zooKeeperServerRunner.connectString();

            new ZooKeeperClientImpl(connectString, 30000, 5000);
            fail("client unexpectedly created");
        } catch (ZooKeeperClientException ex) {
            // OK
        } finally {
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.clear();
        }
    }

    @Test
    public void testSessionExpiration() throws Exception {
        ZooKeeperServerRunner zooKeeperServerRunner = new ZooKeeperServerRunner(0);
        try {
            String connectString = zooKeeperServerRunner.start();

            ZooKeeperClient client = new ZooKeeperClientImpl(connectString, 30000);

            ZooKeeperSession session = client.session();
            assertTrue(session.isConnected());

            AtomicInteger expired = new AtomicInteger(0);
            client.onSessionExpired(expired::incrementAndGet);

            State<Integer> disconnected = new State<>(0);
            State<Integer> connected = new State<>(0);

            client.onDisconnected(() -> {
                disconnected.set(disconnected.get() + 1);
            });
            client.onConnected(s -> {
                connected.set(connected.get() + 1);
            });

            StateChangeFuture<Integer> connectedStateChangeFuture = connected.watch();
            assertEquals(1, (int) connectedStateChangeFuture.currentState);

            StateChangeFuture<Integer> disconnectedStateChangeFuture = disconnected.watch();
            assertEquals(0, (int) disconnectedStateChangeFuture.currentState);

            expired.set(0);

            // Kill the session and force the session expiration
            ZKTestUtils.expire(session);

            try {
                // exists() awaits a connection
                client.exists(new ZNode("/dummy"));

            } catch (KeeperException.ConnectionLossException ex) {
                fail("not connected");
            } catch (KeeperException.SessionExpiredException ex) {
                fail("session expired");
            }

            assertEquals(1, expired.get());

            assertEquals(1, (int) disconnectedStateChangeFuture.get(1000, TimeUnit.MILLISECONDS));
            assertEquals(2, (int) connectedStateChangeFuture.get(1000, TimeUnit.MILLISECONDS));

            client.close();

            assertEquals(1, expired.get());

        } finally {
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.clear();
        }
    }

    @Test
    public void testClosingHandlesForSessionEventHandlers() throws Exception {
        ZooKeeperServerRunner zooKeeperServerRunner = new ZooKeeperServerRunner(0);
        try {
            String connectString = zooKeeperServerRunner.start();

            ZooKeeperClient client = new ZooKeeperClientImpl(connectString, 30000);

            ZooKeeperSession session = client.session();
            assertTrue(session.isConnected());

            AtomicInteger expired = new AtomicInteger(0);
            AtomicInteger disconnected = new AtomicInteger(0);
            AtomicInteger connected = new AtomicInteger(0);

            WatcherHandle h1 = client.onSessionExpired(expired::incrementAndGet);
            WatcherHandle h2 = client.onDisconnected(disconnected::incrementAndGet);
            WatcherHandle h3 = client.onConnected(s -> connected.incrementAndGet());

            expired.set(0);
            disconnected.set(0);
            connected.set(0);

            assertFalse(h1.isClosed());
            assertFalse(h2.isClosed());
            assertFalse(h3.isClosed());

            h1.close();
            h2.close();
            h3.close();

            assertTrue(h1.isClosed());
            assertTrue(h2.isClosed());
            assertTrue(h3.isClosed());

            // Kill the session and force the session expiration
            ZKTestUtils.expire(session);

            try {
                // exists() awaits a connection
                client.exists(new ZNode("/dummy"));

            } catch (KeeperException.ConnectionLossException ex) {
                fail("not connected");
            } catch (KeeperException.SessionExpiredException ex) {
                fail("session expired");
            }

            assertEquals(0, expired.get());
            assertEquals(0, disconnected.get());
            assertEquals(0, connected.get());

            client.close();

            assertEquals(0, expired.get());
            assertEquals(0, disconnected.get());
            assertEquals(0, connected.get());

        } finally {
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.clear();
        }
    }

    @Test
    public void testZooKeeperSessionAPI() throws Exception {
        StringSerializer serializer = new StringSerializer();
        ZNode root = new ZNode("/test");

        ZooKeeperServerRunner zooKeeperServerRunner = new ZooKeeperServerRunner(0);
        try {
            String connectString = zooKeeperServerRunner.start();

            ZooKeeperClient client = new ZooKeeperClientImpl(connectString, 30000);

            ZNode a = new ZNode(root, "a");
            ZNode b = new ZNode(root, "b");
            ZNode c = new ZNode(root, "c");
            ZNode d = new ZNode(c, "d");
            ZNode e = new ZNode(root, "e");
            ZNode f = new ZNode(e, "f");
            ZNode g = new ZNode(f, "g");

            // Test znode existence

            assertNull(client.session().exists(root));
            assertNotNull(client.session().create(root, CreateMode.PERSISTENT));
            assertNotNull(client.session().exists(root));

            assertNull(client.session().exists(a));
            assertNull(client.session().exists(b));
            assertNull(client.session().exists(c));
            assertNull(client.session().exists(d));
            assertNull(client.session().exists(e));
            assertNull(client.session().exists(f));
            assertNull(client.session().exists(g));

            assertNotNull(client.session().create(a, CreateMode.PERSISTENT));
            assertNotNull(client.session().create(b, CreateMode.PERSISTENT));
            try {
                client.session().create(d, CreateMode.PERSISTENT);
                fail();
            } catch (KeeperException ex) {
                // ZNode "c" is not present
            }
            assertNotNull(client.session().create(c, "test data c", serializer, CreateMode.PERSISTENT));
            assertNotNull(client.session().create(d, "test data d", serializer, CreateMode.PERSISTENT));

            try {
                client.session().create(a, CreateMode.PERSISTENT);
                fail();
            } catch (KeeperException ex) {
                // ZNode exists already
            }

            assertNotNull(client.session().exists(a));
            assertNotNull(client.session().exists(b));
            assertNotNull(client.session().exists(c));
            assertNotNull(client.session().exists(d));

            client.session().delete(b);
            assertNull(client.session().exists(b));

            client.session().createPath(g);
            assertNotNull(client.session().exists(e));
            assertNotNull(client.session().exists(f));
            assertNotNull(client.session().exists(g));

            // Test data

            assertNull(client.session().getData(a, serializer).value);

            client.session().setData(a, "test data x", serializer);
            assertEquals("test data x", client.session().getData(a, serializer).value);

            client.session().setData(a, "test data a", serializer);
            assertEquals("test data a", client.session().getData(a, serializer).value);

            try {
                client.session().setData(b, "test data b", serializer);
                fail();
            } catch (KeeperException ex) {
                // ZNode no longer exists
            }
            try {
                client.session().getData(b, serializer);
                fail();
            } catch (KeeperException ex) {
                // ZNode no longer exists
            }

            assertEquals("test data c", client.session().getData(c, serializer).value);
            assertEquals("test data d", client.session().getData(d, serializer).value);

        } finally {
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.clear();
        }
    }

    @Test
    public void testWatchNode() throws Exception {
        ZooKeeperServerRunner zooKeeperServerRunner = new ZooKeeperServerRunner(0);
        try {
            String connectString = zooKeeperServerRunner.start();

            ZooKeeperClient reader = new ZooKeeperClientImpl(connectString, 30000);
            ZooKeeperClient writer = new ZooKeeperClientImpl(connectString, 30000);

            ZNode root = new ZNode("/test");
            writer.session().createPath(root);

            testWatchExistentNode(reader, writer);
            writer.session().deleteRecursively(root);

            testWatchNonExistentNode(reader, writer);
            writer.session().deleteRecursively(root);

        } finally {
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.clear();
        }
    }

    private void testWatchExistentNode(ZooKeeperClient reader, ZooKeeperClient writer) throws Exception {
        Stat target;
        NodeData<String> nodeData;
        NodeDataWatch<String> watch1 = new NodeDataWatch<>();
        NodeDataWatch<String> watch2 = new NodeDataWatch<>();

        ZNode root = new ZNode("/test");
        writer.session().createPath(root);

        // Create a znode
        ZNode a = new ZNode(root, "a");
        target = createNode(writer, a, null, null).stat;
        assertNotNull(target);

        // watch-1
        WatcherHandle h1 = reader.watch(a, watch1.handler, strSerializer);

        nodeData = watch1.await(target, TIMEOUT);
        assertEquals(null, nodeData.value);

        // Set data
        nodeData = updateNodeData(writer, a, "data-a", strSerializer, watch1);
        assertEquals("data-a", nodeData.value);

        // watch-2
        writer.watch(a, watch2.handler, strSerializer);

        nodeData = watch2.await(nodeData.stat, TIMEOUT);
        assertEquals("data-a", nodeData.value);

        // disable watch-1
        assertFalse(h1.isClosed());
        h1.close();
        assertTrue(h1.isClosed());
        watch1.resetUpdateCount();

        // Update data
        nodeData = updateNodeData(writer, a, "data-aa", strSerializer, watch2);
        assertEquals("data-aa", nodeData.value);

        // watch-1 shouldn't have been invoked
        assertEquals(0, watch1.getUpdateCount());

        // Recreate the znode
        nodeData = deleteNode(writer, a, watch2);
        assertTrue(nodeData.isEmpty());
        createNode(writer, a, "data-aaa", strSerializer, watch2);

        // Update data
        nodeData = updateNodeData(writer, a, "data-aaaa", strSerializer, watch2);
        assertEquals("data-aaaa", nodeData.value);
        nodeData = deleteNode(writer, a, watch2);
        assertTrue(nodeData.isEmpty());

        // watch-1 shouldn't have been invoked
        assertEquals(0, watch1.getUpdateCount());
    }

    private void testWatchNonExistentNode(ZooKeeperClient reader, ZooKeeperClient writer) throws Exception {
        Stat target;
        NodeData<String> nodeData;
        NodeDataWatch<String> watch1 = new NodeDataWatch<>();

        ZNode root = new ZNode("/test");
        writer.session().createPath(root);

        ZNode a = new ZNode(root, "a");

        // watch-1
        WatcherHandle h1 = reader.watch(a, watch1.handler, strSerializer);

        nodeData = watch1.await(null, TIMEOUT);
        assertTrue(nodeData.isEmpty());

        // Create the znode
        target = createNode(writer, a, null, null).stat;
        assertNotNull(target);

        nodeData = watch1.await(target, TIMEOUT);
        assertEquals(null, nodeData.value);

        // Set data
        nodeData = updateNodeData(writer, a, "data-a", strSerializer, watch1);
        assertEquals("data-a", nodeData.value);

        assertFalse(h1.isClosed());
        h1.close();
        assertTrue(h1.isClosed());
    }

    @Test
    public void testWatchChildren() throws Exception {
        ZooKeeperServerRunner zooKeeperServerRunner = new ZooKeeperServerRunner(0);
        try {
            String connectString = zooKeeperServerRunner.start();

            ZooKeeperClient client1 = new ZooKeeperClientImpl(connectString, 30000);
            ZooKeeperClient client2 = new ZooKeeperClientImpl(connectString, 30000);

            testWatchChildrenOfExistentNode(client1, client2);
            testWatchChildrenOfNonExistentNode(client1, client2);

        } finally {
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.clear();
        }
    }

    private void testWatchChildrenOfExistentNode(ZooKeeperClient reader, ZooKeeperClient writer) throws Exception {
        Stat target;
        ParentChildData<String, Integer> parentChildData;

        ParentChildDataWatch<String, Integer> watch1 = new ParentChildDataWatch<>();
        ParentChildDataWatch<String, Integer> watch2 = new ParentChildDataWatch<>();

        ZNode root = new ZNode("/test");
        writer.session().createPath(root);

        ZNode a = new ZNode(root, "a");
        ZNode b = new ZNode(a, "b");
        ZNode c = new ZNode(a, "c");

        // Create the parent
        target = createNode(writer, a, "data-a", strSerializer).stat;
        assertNotNull(target);

        // Place a watcher
        WatcherHandle h1 = reader.watch(a, watch1.handler, strSerializer, intSerializer);

        assertNull(writer.session().exists(b));
        assertNull(writer.session().exists(c));

        target = writer.exists(a);
        parentChildData = watch1.await(target, TIMEOUT);
        assertEquals("data-a", parentChildData.getParentData());
        assertEquals(Utils.set(), parentChildData.getChildren());

        // Create the child "b"
        parentChildData = createChild(writer, a, b, 123, intSerializer, watch1);
        assertEquals("data-a", parentChildData.getParentData());
        assertEquals(Utils.set(b), parentChildData.getChildren());
        assertEquals((Integer) 123, parentChildData.getChildData(b));

        // Create the child "c"
        parentChildData = createChild(writer, a, c, null, intSerializer, watch1);
        assertEquals("data-a", parentChildData.getParentData());
        assertEquals(Utils.set(b, c), parentChildData.getChildren());
        assertEquals((Integer) 123, parentChildData.getChildData(b));
        assertEquals(null, parentChildData.getChildData(c));

        // Update the parent value
        parentChildData = updateParentData(writer, a, "data-aa", strSerializer, watch1);
        assertEquals("data-aa", parentChildData.getParentData());
        assertEquals(Utils.set(b, c), parentChildData.getChildren());
        assertEquals((Integer) 123, parentChildData.getChildData(b));
        assertEquals(null, parentChildData.getChildData(c));

        // Update child data (node c)
        parentChildData = updateChildData(writer, a, c, 456, intSerializer, watch1);
        assertEquals("data-aa", parentChildData.getParentData());
        assertEquals(Utils.set(b, c), parentChildData.getChildren());
        assertEquals((Integer) 123, parentChildData.getChildData(b));
        assertEquals((Integer) 456, parentChildData.getChildData(c));

        // Delete child (node b)
        parentChildData = deleteChild(writer, a, b, watch1);
        assertEquals("data-aa", parentChildData.getParentData());
        assertEquals(Utils.set(c), parentChildData.getChildren());
        assertEquals((Integer) 456, parentChildData.getChildData(c));

        // Place a watcher
        WatcherHandle h2 = reader.watch(a, watch2.handler, strSerializer, intSerializer);

        // Close WatcherHandles to disable the watch
        assertFalse(h1.isClosed());
        h1.close();
        assertTrue(h1.isClosed());
        watch1.resetUpdateCount();

        parentChildData = updateChildData(writer, a, c, 789, intSerializer, watch2);
        assertEquals("data-aa", parentChildData.getParentData());
        assertEquals(Utils.set(c), parentChildData.getChildren());
        assertEquals((Integer) 789, parentChildData.getChildData(c));

        parentChildData = deleteChild(writer, a, c, watch2);
        assertEquals("data-aa", parentChildData.getParentData());
        assertEquals(Utils.set(), parentChildData.getChildren());

        parentChildData = updateParentData(writer, a, "data-aaa", strSerializer, watch2);
        assertEquals("data-aaa", parentChildData.getParentData());

        // Delete the parent
        parentChildData = deleteParent(writer, a, watch2);
        assertNull(parentChildData.parent.stat);

        assertFalse(h2.isClosed());
        h2.close();
        assertTrue(h2.isClosed());
        watch2.resetUpdateCount();

        assertEquals(0, watch1.getUpdateCount());
        assertEquals(0, watch2.getUpdateCount());
    }

    private void testWatchChildrenOfNonExistentNode(ZooKeeperClient reader, ZooKeeperClient writer) throws Exception {
        ParentChildData<String, Integer> parentChildData;

        ParentChildDataWatch<String, Integer> watch1 = new ParentChildDataWatch<>();

        ZNode root = new ZNode("/test");
        writer.session().createPath(root);

        ZNode a = new ZNode(root, "a");
        ZNode b = new ZNode(a, "b");
        ZNode c = new ZNode(a, "c");

        assertNull(writer.session().exists(a));
        assertNull(writer.session().exists(b));
        assertNull(writer.session().exists(c));

        // Place a watcher
        WatcherHandle h1 = reader.watch(a, watch1.handler, strSerializer, intSerializer);

        parentChildData = watch1.await(null, TIMEOUT);
        assertTrue(parentChildData.parent.isEmpty());

        // Create the parent
        parentChildData = createParent(writer, a, "data-a", strSerializer, watch1);
        assertEquals("data-a", parentChildData.getParentData());
        assertEquals(Utils.set(), parentChildData.getChildren());

        // Create the child "b"
        parentChildData = createChild(writer, a, b, 123, intSerializer, watch1);
        assertEquals("data-a", parentChildData.getParentData());
        assertEquals(Utils.set(b), parentChildData.getChildren());
        assertEquals((Integer) 123, parentChildData.getChildData(b));

        // Create the child "c"
        parentChildData = createChild(writer, a, c, null, intSerializer, watch1);
        assertEquals("data-a", parentChildData.getParentData());
        assertEquals(Utils.set(b, c), parentChildData.getChildren());
        assertEquals((Integer) 123, parentChildData.getChildData(b));
        assertEquals(null, parentChildData.getChildData(c));

        // Update the parent value
        parentChildData = updateParentData(writer, a, "data-aa", strSerializer, watch1);
        assertEquals("data-aa", parentChildData.getParentData());
        assertEquals(Utils.set(b, c), parentChildData.getChildren());
        assertEquals((Integer) 123, parentChildData.getChildData(b));
        assertEquals(null, parentChildData.getChildData(c));

        // Update child data (node c)
        parentChildData = updateChildData(writer, a, c, 456, intSerializer, watch1);
        assertEquals("data-aa", parentChildData.getParentData());
        assertEquals(Utils.set(b, c), parentChildData.getChildren());
        assertEquals((Integer) 123, parentChildData.getChildData(b));
        assertEquals((Integer) 456, parentChildData.getChildData(c));

        // Delete child (node b)
        parentChildData = deleteChild(writer, a, b, watch1);
        assertEquals("data-aa", parentChildData.getParentData());
        assertEquals(Utils.set(c), parentChildData.getChildren());
        assertEquals((Integer) 456, parentChildData.getChildData(c));

        // Delete child (node c)
        parentChildData = deleteChild(writer, a, c, watch1);
        assertEquals("data-aa", parentChildData.getParentData());
        assertEquals(Utils.set(), parentChildData.getChildren());

        // Delete the parent
        parentChildData = deleteParent(writer, a, watch1);
        assertNull(parentChildData.parent.stat);

        // Close WatcherHandles to disable the watcher
        assertFalse(h1.isClosed());
        h1.close();
        assertTrue(h1.isClosed());
        watch1.resetUpdateCount();

        assertEquals(0, watch1.getUpdateCount());
    }

    @Test
    public void testWatchWithConnectionLoss() throws Exception {
        ParentChildData<String, Integer> parentChildData;
        NodeData<Integer> nodeData;

        ParentChildDataWatch<String, Integer> watch1 = new ParentChildDataWatch<>();
        NodeDataWatch<Integer> watch2 = new NodeDataWatch<>();

        int expectedUpdates1 = 0;
        int expectedUpdates2 = 0;

        ZNode root = new ZNode("/test");
        ZNode a = new ZNode(root, "a");
        ZNode b = new ZNode(a, "b");

        ZooKeeperServerRunner zooKeeperServerRunner = new ZooKeeperServerRunner(0);
        try {
            String connectString = zooKeeperServerRunner.start();

            ZooKeeperClient reader = new ZooKeeperClientImpl(connectString, 30000);
            ZooKeeperClient writer = new ZooKeeperClientImpl(connectString, 30000);

            writer.session().createPath(root);

            // Place a watcher
            reader.watch(a, watch1.handler, strSerializer, intSerializer);
            reader.watch(b, watch2.handler, intSerializer);

            parentChildData = watch1.await(null, TIMEOUT);
            assertTrue(parentChildData.parent.isEmpty());

            nodeData = watch2.await(null, TIMEOUT);
            assertTrue(nodeData.isEmpty());

            // initial watch
            expectedUpdates1++;
            expectedUpdates2++;
            assertEquals(expectedUpdates1, watch1.getUpdateCount());
            assertEquals(expectedUpdates2, watch2.getUpdateCount());

            // Create the parent
            parentChildData = createParent(writer, a, "data-a", strSerializer, watch1);
            assertEquals("data-a", parentChildData.getParentData());
            assertEquals(Utils.set(), parentChildData.getChildren());

            // A parent created
            expectedUpdates1++;
            assertEquals(expectedUpdates1, watch1.getUpdateCount());
            assertEquals(expectedUpdates2, watch2.getUpdateCount());

            // Create the child "b"
            parentChildData = createChild(writer, a, b, 123, intSerializer, watch1);
            assertEquals("data-a", parentChildData.getParentData());
            assertEquals(Utils.set(b), parentChildData.getChildren());
            assertEquals((Integer) 123, parentChildData.getChildData(b));

            nodeData = watch2.await(parentChildData.children.get(b).stat, TIMEOUT);
            assertEquals((Integer) 123, nodeData.value);

            // A child created
            expectedUpdates1++;
            expectedUpdates2++;
            assertEquals(expectedUpdates1, watch1.getUpdateCount());
            assertEquals(expectedUpdates2, watch2.getUpdateCount());

            // Restart ZooKeeperServer
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.start();

            Stat parentTarget = writer.exists(a);
            assertNotNull(parentTarget);
            Stat childTarget = writer.exists(b);
            assertNotNull(childTarget);
            parentChildData = watch1.await(parentTarget, b, childTarget, TIMEOUT);
            assertEquals("data-a", parentChildData.getParentData());
            assertEquals(Utils.set(b), parentChildData.getChildren());
            assertEquals((Integer) 123, parentChildData.getChildData(b));

            nodeData = watch2.await(parentChildData.children.get(b).stat, TIMEOUT);
            assertEquals((Integer) 123, nodeData.value);

            // SyncConnected
            expectedUpdates1++;
            expectedUpdates2++;
            assertEquals(expectedUpdates1, watch1.getUpdateCount());
            assertEquals(expectedUpdates2, watch2.getUpdateCount());

            // Update parent data
            parentChildData = updateParentData(writer, a, "data-aa", strSerializer, watch1);
            assertEquals("data-aa", parentChildData.getParentData());
            assertEquals(Utils.set(b), parentChildData.getChildren());
            assertEquals((Integer) 123, parentChildData.getChildData(b));

            // A parent data updated
            expectedUpdates1++;
            assertEquals(expectedUpdates1, watch1.getUpdateCount());
            assertEquals(expectedUpdates2, watch2.getUpdateCount());

            // Update child data
            parentChildData = updateChildData(writer, a, b, 456, intSerializer, watch1);
            assertEquals("data-aa", parentChildData.getParentData());
            assertEquals(Utils.set(b), parentChildData.getChildren());
            assertEquals((Integer) 456, parentChildData.getChildData(b));

            nodeData = watch2.await(parentChildData.children.get(b).stat, TIMEOUT);
            assertEquals((Integer) 456, nodeData.value);

            // A child data updated
            expectedUpdates1++;
            expectedUpdates2++;
            assertEquals(expectedUpdates1, watch1.getUpdateCount());
            assertEquals(expectedUpdates2, watch2.getUpdateCount());

        } finally {
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.clear();
        }
    }

    @Test
    public void testWatchWithSessionExpiration() throws Exception {
        ParentChildData<String, Integer> parentChildData;
        NodeData<Integer> nodeData;

        ParentChildDataWatch<String, Integer> watch1 = new ParentChildDataWatch<>();
        NodeDataWatch<Integer> watch2 = new NodeDataWatch<>();

        int expectedUpdates1 = 0;
        int expectedUpdates2 = 0;

        ZNode root = new ZNode("/test");
        ZNode a = new ZNode(root, "a");
        ZNode b = new ZNode(a, "b");

        ZooKeeperServerRunner zooKeeperServerRunner = new ZooKeeperServerRunner(0);
        try {
            String connectString = zooKeeperServerRunner.start();

            ZooKeeperClient reader = new ZooKeeperClientImpl(connectString, 30000);
            ZooKeeperClient writer = new ZooKeeperClientImpl(connectString, 30000);

            writer.session().createPath(root);

            // Place a watcher
            reader.watch(a, watch1.handler, strSerializer, intSerializer);
            reader.watch(b, watch2.handler, intSerializer);

            parentChildData = watch1.await(null, TIMEOUT);
            assertTrue(parentChildData.parent.isEmpty());

            nodeData = watch2.await(null, TIMEOUT);
            assertTrue(nodeData.isEmpty());

            // initial watch
            expectedUpdates1++;
            expectedUpdates2++;
            assertEquals(expectedUpdates1, watch1.getUpdateCount());
            assertEquals(expectedUpdates2, watch2.getUpdateCount());

            // Create the parent
            parentChildData = createParent(writer, a, "data-a", strSerializer, watch1);
            assertEquals("data-a", parentChildData.getParentData());
            assertEquals(Utils.set(), parentChildData.getChildren());

            // A parent created
            expectedUpdates1++;
            assertEquals(expectedUpdates1, watch1.getUpdateCount());
            assertEquals(expectedUpdates2, watch2.getUpdateCount());

            // Create the child "b"
            parentChildData = createChild(writer, a, b, 123, intSerializer, watch1);
            assertEquals("data-a", parentChildData.getParentData());
            assertEquals(Utils.set(b), parentChildData.getChildren());
            assertEquals((Integer) 123, parentChildData.getChildData(b));

            nodeData = watch2.await(parentChildData.children.get(b).stat, TIMEOUT);
            assertEquals((Integer) 123, nodeData.value);

            // A child created
            expectedUpdates1++;
            expectedUpdates2++;
            assertEquals(expectedUpdates1, watch1.getUpdateCount());
            assertEquals(expectedUpdates2, watch2.getUpdateCount());

            // Expire the session
            ZKTestUtils.expire(reader.session());

            Stat parentTarget = writer.exists(a);
            assertNotNull(parentTarget);
            Stat childTarget = writer.exists(b);
            assertNotNull(childTarget);
            parentChildData = watch1.await(parentTarget, b, childTarget, TIMEOUT);
            assertEquals("data-a", parentChildData.getParentData());
            assertEquals(Utils.set(b), parentChildData.getChildren());
            assertEquals((Integer) 123, parentChildData.getChildData(b));

            nodeData = watch2.await(parentChildData.children.get(b).stat, TIMEOUT);
            assertEquals((Integer) 123, nodeData.value);

            // Expired (new watches installed)
            expectedUpdates1++;
            expectedUpdates2++;

            // Update parent data
            parentChildData = updateParentData(writer, a, "data-aa", strSerializer, watch1);
            assertEquals("data-aa", parentChildData.getParentData());
            assertEquals(Utils.set(b), parentChildData.getChildren());
            assertEquals((Integer) 123, parentChildData.getChildData(b));

            // A parent data updated
            expectedUpdates1++;
            assertEquals(expectedUpdates1, watch1.getUpdateCount());
            assertEquals(expectedUpdates2, watch2.getUpdateCount());

            // Update child data
            parentChildData = updateChildData(writer, a, b, 456, intSerializer, watch1);
            assertEquals("data-aa", parentChildData.getParentData());
            assertEquals(Utils.set(b), parentChildData.getChildren());
            assertEquals((Integer) 456, parentChildData.getChildData(b));

            nodeData = watch2.await(parentChildData.children.get(b).stat, TIMEOUT);
            assertEquals((Integer) 456, nodeData.value);

            // A child data updated
            expectedUpdates1++;
            expectedUpdates2++;
            assertEquals(expectedUpdates1, watch1.getUpdateCount());
            assertEquals(expectedUpdates2, watch2.getUpdateCount());

        } finally {
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.clear();
        }
    }

    @Test
    public void testClose() throws Exception {
        ZNode znode = new ZNode("/test");
        IntegerSerializer serializer = new IntegerSerializer();

        ZooKeeperServerRunner zooKeeperServerRunner = new ZooKeeperServerRunner(0);
        try {
            String connectString = zooKeeperServerRunner.start();

            AtomicInteger disconnected = new AtomicInteger(0);
            AtomicInteger currentValue = new AtomicInteger(0);

            ZooKeeperClient client = new ZooKeeperClientImpl(connectString, 30000);

            client.onDisconnected(disconnected::incrementAndGet);

            client.create(znode, 1, serializer, CreateMode.PERSISTENT);

            assertEquals(1, (int) client.getData(znode, serializer).value);

            client.watch(znode, n -> currentValue.set(n.value), serializer);

            assertEquals(1, currentValue.get());

            disconnected.set(0);
            client.close();

            // close() should trigger the onDisconnected handler
            assertEquals(1, disconnected.get());

            client = new ZooKeeperClientImpl(connectString, 30000);

            // Update the value
            client.setData(znode, 2, serializer);

            client.close();

            // Watcher should not be triggered
            assertEquals(1, currentValue.get());

        } finally {
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.clear();
        }
    }

    @Test
    public void testRemoveOnDelete() throws Exception {
        ZNode znode = new ZNode("/test");
        IntegerSerializer serializer = new IntegerSerializer();

        ZooKeeperServerRunner zooKeeperServerRunner = new ZooKeeperServerRunner(0);
        try {
            String connectString = zooKeeperServerRunner.start();

            ZooKeeperClient client = new ZooKeeperClientImpl(connectString, 30000);

            client.create(znode, 1, serializer, CreateMode.PERSISTENT);

            State<Integer> value1 = new State<>(0);
            State<Integer> value2 = new State<>(0);

            client.watch(znode, n -> value1.set(n.value), serializer, false);
            client.watch(znode, n -> value2.set(n.value), serializer, true);

            client.setData(znode, 1, serializer);
            Uninterruptibly.sleep(20);
            value1.await(1);
            assertEquals(1, (int) value2.get());

            client.setData(znode, 2, serializer);
            Uninterruptibly.sleep(20);
            value1.await(2);
            assertEquals(2, (int) value2.get());

            client.delete(znode);
            Uninterruptibly.sleep(20);
            value1.await(null);
            assertNull(value2.get());

            // The second watcher should be removed.

            client.create(znode, 3, serializer, CreateMode.PERSISTENT);
            Uninterruptibly.sleep(20);
            value1.await(3);
            // value2 should be still null
            assertNull(value2.get());

            client.setData(znode, 4, serializer);
            Uninterruptibly.sleep(20);
            value1.await(4);
            // value2 should be still null
            assertNull(value2.get());

            client.close();

        } finally {
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.clear();
        }
    }

    @Test
    public void testMutex() throws Exception {
        ZNode lockNode = new ZNode("/mutexLock");

        ZooKeeperServerRunner zooKeeperServerRunner = new ZooKeeperServerRunner(0);
        try {
            String connectString = zooKeeperServerRunner.start();

            ZooKeeperClient client1 = new ZooKeeperClientImpl(connectString, 30000);
            ZooKeeperClient client2 = new ZooKeeperClientImpl(connectString, 30000);

            Random rand = new Random();
            final int[] counter = new int[1];
            final int[] updater = new int[60];
            final CountDownLatch latch = new CountDownLatch(1);

            Thread thread1 = new Thread(() -> {
                Uninterruptibly.run(latch::await);
                for (int i = 0; i < 30; i++) {
                    try {
                        client1.mutex(lockNode, session -> {
                            int c = counter[0];
                            updater[c] = 1;
                            Uninterruptibly.sleep(5);
                            counter[0] = c + 1;
                        });
                    } catch (Exception ex) {
                        break;
                    }
                    Uninterruptibly.sleep(rand.nextInt(1));
                }
            });

            Thread thread2 = new Thread(() -> {
                Uninterruptibly.run(latch::await);
                for (int i = 0; i < 30; i++) {
                    try {
                        client2.mutex(lockNode, session -> {
                            int c = counter[0];
                            updater[c] = 2;
                            Uninterruptibly.sleep(5);
                            counter[0] = c + 1;
                        });
                    } catch (Exception ex) {
                        break;
                    }
                    Uninterruptibly.sleep(rand.nextInt(1));
                }
            });

            thread1.start();
            thread2.start();

            Uninterruptibly.sleep(10);
            latch.countDown();

            thread1.join();
            thread2.join();

            assertEquals(60, counter[0]);

        } finally {
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.clear();
        }
    }

    @Test
    public void testMutexSessionExpiration() throws Exception {
        ZNode lockNode = new ZNode("/mutexLock");

        ZooKeeperServerRunner zooKeeperServerRunner = new ZooKeeperServerRunner(0);
        try {
            String connectString = zooKeeperServerRunner.start();

            ZooKeeperClient client = new ZooKeeperClientImpl(connectString, 30000);

            try {
                client.mutex(lockNode, session -> {
                    // Kill the session and force the session expiration
                    ZKTestUtils.expire(session);
                });
            } catch (Exception ex) {
                fail();
            }

            assertNull(client.exists(lockNode));

        } finally {
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.clear();
        }
    }

    @Test
    public void testMutexWithException() throws Exception {
        ZNode lockNode = new ZNode("/mutexLock");

        ZooKeeperServerRunner zooKeeperServerRunner = new ZooKeeperServerRunner(0);
        try {
            String connectString = zooKeeperServerRunner.start();

            ZooKeeperClient client = new ZooKeeperClientImpl(connectString, 30000);

            try {
                client.mutex(lockNode, session -> {
                    throw new DummyException();
                });
            } catch (DummyException ex) {
                // OK
            } catch (Exception ex) {
                fail();
            }

            assertNull(client.exists(lockNode));

        } finally {
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.clear();
        }
    }

    private static class DummyException extends Exception {
    }

}
