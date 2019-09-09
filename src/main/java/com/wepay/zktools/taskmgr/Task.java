package com.wepay.zktools.taskmgr;

/**
 * This is an interface that a task should implement
 */
public interface Task {

    /**
     * A method executed as the task body
     *
     * @param context the context
     */
    void execute(TaskContext context);

    /**
     * Returns the next start time of the task execution in milliseconds.
     * @return the difference, measured in milliseconds, between the next start time and midnight, January 1, 1970 UTC.
     */
    long nextStartTime();

}
