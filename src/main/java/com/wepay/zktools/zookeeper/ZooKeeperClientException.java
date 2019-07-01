package com.wepay.zktools.zookeeper;

public class ZooKeeperClientException extends Exception {

    public ZooKeeperClientException(String msg) {
        super(msg);
    }

    public ZooKeeperClientException(String msg, Throwable cause) {
        super(msg, cause);
    }

}
