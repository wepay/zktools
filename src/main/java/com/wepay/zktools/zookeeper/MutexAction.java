package com.wepay.zktools.zookeeper;


public interface MutexAction<E extends Exception> {

    void apply(ZooKeeperSession zkSession) throws E;

}
