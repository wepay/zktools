package com.wepay.zktools.taskmgr;

import com.wepay.zktools.taskmgr.internal.TaskManagerImpl;
import com.wepay.zktools.test.ZKTestUtils;
import com.wepay.zktools.test.util.ZooKeeperServerRunner;
import com.wepay.zktools.util.State;
import com.wepay.zktools.util.StateChangeFuture;
import com.wepay.zktools.util.Uninterruptibly;
import com.wepay.zktools.zookeeper.ZNode;
import com.wepay.zktools.zookeeper.ZooKeeperClient;
import com.wepay.zktools.zookeeper.internal.ZooKeeperClientImpl;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TaskManagerTest extends ZKTestUtils {

    private final ZNode root = new ZNode("/cluster");
    private final Random random = new Random();

    @Test
    public void testTaskAssignment() throws Exception {
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);
        ZooKeeperServerRunner zooKeeperServerRunner = new ZooKeeperServerRunner(0);
        try {
            String connectString = zooKeeperServerRunner.start();
            ZooKeeperClient zkClient = new ZooKeeperClientImpl(connectString, 30000);

            zkClient.create(root, CreateMode.PERSISTENT);

            State<ActiveTaskInstance> active = new State<>(null);

            TaskManager[] taskmgrs = setupTaskManagers(zkClient, active, executor);

            MockTask task0 = (MockTask) ((TaskManagerImpl) taskmgrs[0]).getTask("test");
            MockTask task1 = (MockTask) ((TaskManagerImpl) taskmgrs[1]).getTask("test");

            await(task0, active);
            // Now task0 is active

            int execCnt0 = task0.count.get();
            int execCnt1 = task1.count.get();

            Uninterruptibly.sleep(100);

            // The same task instance (task0) should be running
            assertEquals(task0, active.get().task);
            // The execution count of task0 should be increasing.
            assertTrue(execCnt0 < task0.count.get());
            // The execution count of task1 should not be changed.
            assertTrue(execCnt1 == task1.count.get());

            task0.errorCount.set(0);
            task1.errorCount.set(0);

            // Gracefully shutdown the first task manager
            taskmgrs[0].close();

            await(task1, active);
            // Now task1 is active

            execCnt0 = task0.count.get();
            execCnt1 = task1.count.get();

            Uninterruptibly.sleep(100);

            // task1 should be running
            assertEquals(task1, active.get().task);
            // The execution count of task0 should not be changed.
            assertTrue(execCnt0 == task0.count.get());
            // The execution count of task1 should be increasing.
            assertTrue(execCnt1 < task1.count.get());

            assertEquals(0, task0.errorCount.get());
            assertEquals(0, task1.errorCount.get());

        } finally {
            executor.shutdown();
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.clear();
        }
    }

    @Test
    public void testWithConnectionLoss() throws Exception {
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);
        ZooKeeperServerRunner zooKeeperServerRunner = new ZooKeeperServerRunner(0);
        try {
            String connectString = zooKeeperServerRunner.start();
            ZooKeeperClient zkClient = new ZooKeeperClientImpl(connectString, 30000);

            zkClient.create(root, CreateMode.PERSISTENT);

            State<ActiveTaskInstance> active = new State<>(null);

            TaskManager[] taskmgrs = setupTaskManagers(zkClient, active, executor);

            MockTask task0 = (MockTask) ((TaskManagerImpl) taskmgrs[0]).getTask("test");
            MockTask task1 = (MockTask) ((TaskManagerImpl) taskmgrs[1]).getTask("test");

            await(task0, active);
            // Now task0 is active

            int execCnt0 = task0.count.get();
            int execCnt1 = task1.count.get();

            Uninterruptibly.sleep(100);

            // The same task instance (task0) should be running
            assertEquals(task0, active.get().task);
            // THe execution count of task0 should be increasing.
            assertTrue(execCnt0 < task0.count.get());
            // The execution count of task1 should not be changed.
            assertTrue(execCnt1 == task1.count.get());

            // Induce a connection loss
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.start();

            try {
                // exists() awaits reconnect
                zkClient.exists(root);

            } catch (KeeperException.ConnectionLossException ex) {
                fail("session not connected");
            } catch (KeeperException.SessionExpiredException ex) {
                fail("session expired");
            }

            Uninterruptibly.sleep(100);

            await(task0, active);
            // Now task0 is active

            execCnt0 = task0.count.get();
            execCnt1 = task1.count.get();

            Uninterruptibly.sleep(100);

            // The same task instance (task0) should be running
            assertEquals(task0, active.get().task);
            // THe execution count of task0 should be increasing.
            assertTrue(execCnt0 < task0.count.get());
            // The execution count of task1 should not be changed.
            assertTrue(execCnt1 == task1.count.get());

        } finally {
            executor.shutdown();
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.clear();
        }
    }

    @Test
    public void testWithZooKeeperSessionExpiration() throws Exception {
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);
        ZooKeeperServerRunner zooKeeperServerRunner = new ZooKeeperServerRunner(0);
        try {
            String connectString = zooKeeperServerRunner.start();
            ZooKeeperClient zkClient = new ZooKeeperClientImpl(connectString, 30000);

            zkClient.create(root, CreateMode.PERSISTENT);

            State<ActiveTaskInstance> active = new State<>(null);

            TaskManager[] taskmgrs = setupTaskManagers(zkClient, active, executor);

            MockTask task0 = (MockTask) ((TaskManagerImpl) taskmgrs[0]).getTask("test");
            MockTask task1 = (MockTask) ((TaskManagerImpl) taskmgrs[1]).getTask("test");

            await(task0, active);
            // Now task0 is active

            int execCnt0 = task0.count.get();
            int execCnt1 = task1.count.get();

            Uninterruptibly.sleep(100);

            // The same task instance (task0) should be running
            assertEquals(task0, active.get().task);
            // THe execution count of task0 should be increasing.
            assertTrue(execCnt0 < task0.count.get());
            // The execution count of task1 should not be changed.
            assertTrue(execCnt1 == task1.count.get());

            // Cause a session expiration
            ZKTestUtils.expire(zkClient.session());

            await(task0, active);
            // Now task0 is active

            execCnt0 = task0.count.get();
            execCnt1 = task1.count.get();

            Uninterruptibly.sleep(100);

            // The same task instance (task0) should be running
            assertEquals(task0, active.get().task);
            // THe execution count of task0 should be increasing.
            assertTrue(execCnt0 < task0.count.get());
            // The execution count of task1 should not be changed.
            assertTrue(execCnt1 == task1.count.get());

        } finally {
            executor.shutdown();
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.clear();
        }
    }

    private TaskManager[] setupTaskManagers(
        ZooKeeperClient zkClient,
        State<ActiveTaskInstance> active,
        ScheduledExecutorService executorService) throws Exception {

        TaskManager[] taskmgrs = new TaskManager[2];

        taskmgrs[0] = TaskManager.create(zkClient, root, executorService);
        taskmgrs[1] = TaskManager.create(zkClient, root, executorService);

        MockTask task0 = new MockTask(active);
        MockTask task1 = new MockTask(active);

        task0.setOther(task1);
        task1.setOther(task0);

        taskmgrs[0].add("test", task0, 0);
        String key1 = ((TaskManagerImpl) taskmgrs[0]).getTaskRegistrationKey("test");
        taskmgrs[1].add("test", task1, 0);
        String key2 = ((TaskManagerImpl) taskmgrs[1]).getTaskRegistrationKey("test");
        while (key1.equals(key2)) {
            taskmgrs[1].remove("test");
            taskmgrs[1].add("test", task1, 0);
            key2 = ((TaskManagerImpl) taskmgrs[1]).getTaskRegistrationKey("test");
        }

        if (key1.compareTo(key2) > 0) {
            // Swap task managers
            TaskManager tmp = taskmgrs[0];
            taskmgrs[0] = taskmgrs[1];
            taskmgrs[1] = tmp;
        }

        return taskmgrs;
    }

    private void await(MockTask task, State<ActiveTaskInstance> active) throws Exception {
        active.set(null);
        StateChangeFuture<ActiveTaskInstance> f = active.watch();
        while (f.currentState == null || f.currentState.task != task) {
            f.get();
            f = active.watch();
        }
    }

    private static class ActiveTaskInstance {
        public final MockTask task;

        ActiveTaskInstance(MockTask task) {
            this.task = task;
        }

        ActiveTaskInstance next() {
            return new ActiveTaskInstance(task);
        }
    }

    private class MockTask implements Task {

        private final State<ActiveTaskInstance> active;
        private final AtomicInteger count = new AtomicInteger(0);
        private final AtomicInteger errorCount = new AtomicInteger(0);

        private ActiveTaskInstance thisInstance;
        private MockTask otherTask;
        private int otherCountSnapshot;


        MockTask(State<ActiveTaskInstance> active) {
            this.active = active;
            this.thisInstance = new ActiveTaskInstance(this);
        }

        public void setOther(MockTask other) {
            this.otherTask = other;
        }

        @Override
        public void execute(TaskContext context) {
            if (active.get() != thisInstance) {
                // Reset other's count snapshot
                thisInstance = thisInstance.next();
                active.set(thisInstance);
                otherCountSnapshot = otherTask.count();
            }

            count.incrementAndGet();

            Uninterruptibly.sleep(random.nextInt(30));

            if (otherCountSnapshot != otherTask.count()) {
                // If other task instance is also executing, this is a bug.
                errorCount.incrementAndGet();
            }
        }

        @Override
        public long nextStartTime() {
            return System.currentTimeMillis() + 1;
        }

        public int count() {
            return count.get();
        }
    }

}
