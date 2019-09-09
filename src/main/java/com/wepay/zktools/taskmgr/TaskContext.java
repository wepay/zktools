package com.wepay.zktools.taskmgr;

/**
 * Task Context
 */
public interface TaskContext {

    /**
     * Returns {@code true} if the task is enabled.
     * @return {@code true} if the task is enabled
     */
    boolean isEnabled();

}
