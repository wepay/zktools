package com.wepay.zktools.clustermgr;

public class ClusterManagerException extends Exception {

    public ClusterManagerException(String msg) {
        super(msg);
    }

    public ClusterManagerException(String msg, Throwable ex) {
        super(msg, ex);
    }

}
