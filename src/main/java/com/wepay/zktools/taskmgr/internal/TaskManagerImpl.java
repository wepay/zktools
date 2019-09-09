package com.wepay.zktools.taskmgr.internal;

import com.wepay.zktools.taskmgr.Task;
import com.wepay.zktools.taskmgr.TaskContext;
import com.wepay.zktools.taskmgr.TaskManager;
import com.wepay.zktools.taskmgr.TaskManagerException;
import com.wepay.zktools.util.Logging;
import com.wepay.zktools.util.State;
import com.wepay.zktools.util.StateChangeFuture;
import com.wepay.zktools.zookeeper.NodeData;
import com.wepay.zktools.zookeeper.WatcherHandle;
import com.wepay.zktools.zookeeper.ZNode;
import com.wepay.zktools.zookeeper.ZooKeeperClient;
import com.wepay.zktools.zookeeper.ZooKeeperSession;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * An implementation of {@link TaskManager}
 */
public class TaskManagerImpl implements TaskManager {

    private static final Logger logger = Logging.getLogger(TaskManagerImpl.class);
    private static final Random random = new Random();

    private final ZooKeeperClient zkClient;
    private final ZNode root;
    private final ScheduledExecutorService executorService;

    private final HashMap<String, TaskRunnable> taskRunnables = new HashMap<>();
    private final WatcherHandle onDisconnectedHandle;
    private final WatcherHandle onConnectFailedHandle;

    private boolean closed = false;

    public TaskManagerImpl(ZooKeeperClient zkClient, ZNode root, ScheduledExecutorService executorService) {
        this.zkClient = zkClient;
        this.root = root;
        this.executorService = executorService;
        this.onDisconnectedHandle = zkClient.onDisconnected(this::disableAllTasks);
        this.onConnectFailedHandle = zkClient.onConnectFailed(this::reportException);
    }

    @Override
    public void close() {
        synchronized (taskRunnables) {
            if (!closed) {
                closed = true;
                onDisconnectedHandle.close();
                onConnectFailedHandle.close();

                ArrayList<CompletableFuture<Void>> closeFutures = new ArrayList<>(taskRunnables.size());
                for (TaskRunnable taskRunnable : taskRunnables.values()) {
                    closeFutures.add(taskRunnable.close());
                }

                taskRunnables.clear();

                for (CompletableFuture<Void> f : closeFutures) {
                    try {
                        f.get();
                    } catch (Exception ex) {
                        // Ignore
                    }
                }
            }
        }
    }

    @Override
    public void add(String name, Task task, long initialDelay) throws TaskManagerException {
        ZNode znode = createTaskZNode(name);

        synchronized (taskRunnables) {
            if (closed)
                throw new TaskManagerException("already closed");

            if (taskRunnables.containsKey(name))
                throw new TaskManagerException("task with the same name already exists: name=" + name);

            TaskRunnable taskRunnable = new TaskRunnable(name, task, initialDelay, znode);
            taskRunnables.put(name, taskRunnable);
        }
    }

    @Override
    public void remove(String name) throws TaskManagerException {
        synchronized (taskRunnables) {
            if (closed)
                throw new TaskManagerException("already closed");

            TaskRunnable removedTask = taskRunnables.remove(name);
            if (removedTask != null) {
                removedTask.enable(false);
            }
        }
    }

    @Override
    public boolean contains(String name) {
        synchronized (taskRunnables) {
            return taskRunnables.containsKey(name);
        }
    }

    private ZNode createTaskZNode(String name) throws TaskManagerException {
        ZNode znode = new ZNode(root, name);
        try {
            zkClient.create(znode, CreateMode.PERSISTENT);
            return znode;

        } catch (KeeperException.NodeExistsException ex) {
            // Ignore
            return znode;

        } catch (Exception ex) {
            String msg = "failed to create task znode : task=" + name;
            logger.error(msg, ex);
            throw new TaskManagerException(msg, ex);
        }
    }

    private void disableAllTasks() {
        // On connection loss, we disable all tasks.
        synchronized (taskRunnables) {
            for (TaskRunnable taskRunnable : taskRunnables.values()) {
                taskRunnable.enable(false);
            }
        }
    }

    private void reportException(Throwable t) {
        logger.error("Task manager is unable to connect to zookeeper servers. Retrying... ", t);
    }

    private class TaskRunnable implements Runnable {

        private final String name;
        private final Task task;
        private final long delayEndTime;
        private final ZNode znode;
        private final String key;
        private final TaskContext context = new TaskContext() {
            @Override
            public boolean isEnabled() {
                return enabled;
            }
        };
        private final State<Thread> threadRef = new State<>(null);
        private final WatcherHandle onConnectedHandle;
        private final WatcherHandle onNodeOrChildrenChangedHandle;

        private boolean closed = false;
        private boolean scheduled = false;
        private long nextStartTime = 0L;
        private volatile boolean enabled = false;
        private ZNode registrationZNode = null;
        private boolean isFirstRun = true;

        TaskRunnable(String name, Task task, long initialDelay, ZNode znode) {
            this.name = name;
            this.task = task;
            this.delayEndTime = System.currentTimeMillis() + initialDelay;
            this.znode = znode;
            this.key = String.format("%08x", random.nextInt());
            this.onConnectedHandle = zkClient.onConnected(this::registerTask);
            this.onNodeOrChildrenChangedHandle = zkClient.watch(znode, this::updateTask, null, null);
        }

        public CompletableFuture<Void> close() {
            synchronized (this) {
                closed = true;
                enable(false);

                onConnectedHandle.close();
                onNodeOrChildrenChangedHandle.close();
            }

            // If there is a thread executing the task, wait for completion
            StateChangeFuture<Thread> threadWatch = threadRef.watch();
            if (threadWatch.currentState == null) {
                deregisterTask();
                return CompletableFuture.completedFuture(null);
            } else {
                return threadWatch.thenRun(this::deregisterTask);
            }
        }

        /**
         * Marks the task enabled (true) or disabled (false)
         * @param enabled true to enable, false to disable
         */
        private void enable(boolean enabled) {
            synchronized (this) {
                if (this.enabled != enabled) {
                    this.enabled = enabled;

                    if (enabled) {
                        // Enabling. Schedule the task if not scheduled yet.
                        logger.info("Enabling a task: " + name);
                        scheduleTaskIfEnabled();
                    } else {
                        // Disabling. Interrupt the executing thread if any.
                        logger.info("Disabling a task: " + name);
                        Thread executingThread = threadRef.get();
                        if (executingThread != null) {
                            executingThread.interrupt();
                        }
                    }
                }
            }
        }

        public void run() {
            Thread currentThread = Thread.currentThread();

            synchronized (this) {
                // We start running the task. The state is no longer "scheduled".
                scheduled = false;

                Thread otherThread = threadRef.get();
                if (otherThread != null) {
                    String msg = "the task is being executed by " + otherThread.toString();
                    logger.error(msg);
                    throw new IllegalStateException(msg);
                }

                threadRef.set(currentThread);
            }

            try {
                // Make sure we don't execute the task prematurely
                synchronized (this) {
                    long now = System.currentTimeMillis();
                    while (now < nextStartTime) {
                        wait(nextStartTime - now);
                        now = System.currentTimeMillis();
                    }
                }

                if (enabled)
                    task.execute(context);

            } catch (Throwable ex) {
                logger.error("failed to execute the task: name=" + name, ex);

            } finally {
                synchronized (this) {
                    // Clear the interrupt flag if it is set.
                    if (currentThread.isInterrupted())
                        Thread.interrupted();

                    threadRef.set(null);
                }
                scheduleTaskIfEnabled();
            }
        }

        private void registerTask(ZooKeeperSession s) {
            synchronized (this) {
                try {
                    if (!closed) {
                        // Make sure we registered this task with ZooKeeper
                        if (registrationZNode == null || s.exists(registrationZNode) == null) {
                            // We create a new name using EPHEMERAL_SEQUENTIAL.
                            registrationZNode = s.create(new ZNode(znode, key), CreateMode.EPHEMERAL_SEQUENTIAL);
                        }
                        scheduleTaskIfEnabled();
                    }
                } catch (Exception ex) {
                    String msg = "failed to create registration znode: " + registrationZNode;
                    logger.error(msg, ex);
                }
            }
        }

        private void deregisterTask() {
            synchronized (this) {
                if (registrationZNode != null) {
                    try {
                        zkClient.delete(registrationZNode);
                    } catch (KeeperException.NoNodeException ex) {
                        // Ignore
                    } catch (Exception ex) {
                        logger.error("exception when closing a task", ex);
                    }
                }
            }
        }

        private void updateTask(NodeData<Object> nodeData, Map<ZNode, NodeData<Object>> childData) {
            synchronized (this) {
                if (!closed) {
                    if (registrationZNode != null) {
                        ZNode leader = null;
                        for (ZNode child : childData.keySet()) {
                            if (leader == null || leader.compareTo(child) > 0) {
                                leader = child;
                            }
                        }
                        enable(leader != null && leader.equals(registrationZNode));
                    } else {
                        enable(false);
                    }
                }
            }
        }

        private void scheduleTaskIfEnabled() {
            synchronized (this) {
                if (enabled && !scheduled) {
                    nextStartTime = task.nextStartTime();
                    long delay = nextStartTime - System.currentTimeMillis();

                    // If it's the first run, using "initialDelay"
                    if (isFirstRun) {
                        delay = delayEndTime - System.currentTimeMillis();
                        nextStartTime = delayEndTime;
                        isFirstRun = false;
                    }

                    executorService.schedule(this, delay, TimeUnit.MILLISECONDS);
                    scheduled = true;
                }
            }
        }
    }

    public String getTaskRegistrationKey(String name) {
        return getTaskRunnable(name).key;
    }

    public Task getTask(String name) {
        return getTaskRunnable(name).task;
    }

    private TaskRunnable getTaskRunnable(String name) {
        synchronized (taskRunnables) {
            TaskRunnable runnable = taskRunnables.get(name);
            if (runnable == null) {
                throw new IllegalArgumentException("no such task: name=" + name);
            } else {
                return runnable;
            }
        }
    }
}
