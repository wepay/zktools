package com.wepay.zktools.taskmgr;

public class TaskManagerException extends Exception {

    public TaskManagerException(String msg) {
        super(msg);
    }

    public TaskManagerException(String msg, Throwable cause) {
        super(msg, cause);
    }

}
