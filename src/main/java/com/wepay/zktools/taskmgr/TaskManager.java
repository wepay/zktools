package com.wepay.zktools.taskmgr;

import com.wepay.zktools.taskmgr.internal.TaskManagerImpl;
import com.wepay.zktools.zookeeper.ZNode;
import com.wepay.zktools.zookeeper.ZooKeeperClient;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Task Manager distributes tasks to participating machines. A task is identified by the name.
 * It is designed to reduce the chance that more than one machines execute the same task at the same time.
 * There is no strong guarantee. Coordinating execution through TaskManager merely reduces duplicate work and contention.
 * Each task must be written in a way that updates are idempotent in order to avoid data corruption.
 */
public interface TaskManager {

    static TaskManager create(ZooKeeperClient zkClient, ZNode root, ScheduledExecutorService executorService) {
        return new TaskManagerImpl(zkClient, root, executorService);
    }

    /**
     * Closes the task manager
     */
    void close();

    /**
     * Adds a task
     * @param name the name of the task
     * @param task the task
     * @param initialDelay the time to delay first execution
     * @throws TaskManagerException
     */
    void add(String name, Task task, long initialDelay) throws TaskManagerException;

    /**
     * Removes a task
     * @param name the name of the task
     * @throws TaskManagerException
     */
    void remove(String name) throws TaskManagerException;

    /**
     * Tests if the task exists
     */
    boolean contains(String name);

}
