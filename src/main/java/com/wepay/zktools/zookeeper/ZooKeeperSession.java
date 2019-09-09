package com.wepay.zktools.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.Set;

/**
 * ZooKeeper session
 */
public interface ZooKeeperSession {

    boolean isConnected();

    ZNode createPath(ZNode znode) throws KeeperException;

    ZNode createPath(ZNode znode, List<ACL> acl) throws KeeperException;

    ZNode create(ZNode znode, CreateMode createMode) throws KeeperException;

    ZNode create(ZNode znode, List<ACL> acl, CreateMode createMode) throws KeeperException;

    <T> ZNode create(ZNode znode, T data, Serializer<T> serializer, CreateMode createMode) throws KeeperException;

    <T> ZNode create(ZNode znode, T data, Serializer<T> serializer, List<ACL> acl, CreateMode createMode) throws KeeperException;

    Stat exists(ZNode znode) throws KeeperException;

    Stat exists(ZNode znode, Watcher watcher) throws KeeperException;

    Set<ZNode> getChildren(ZNode znode) throws KeeperException;

    Set<ZNode> getChildren(ZNode znode, Watcher watcher) throws KeeperException;

    <T> NodeData<T> getData(ZNode znode, Serializer<T> serializer) throws KeeperException;

    <T> NodeData<T> getData(ZNode znode, Serializer<T> serializer, Watcher watcher) throws KeeperException;

    <T> void setData(ZNode znode, T data, Serializer<T> serializer) throws KeeperException;

    <T> void setData(ZNode znode, T data, Serializer<T> serializer, int version) throws KeeperException;

    void delete(ZNode znode) throws KeeperException;

    void delete(ZNode znode, int version) throws KeeperException;

    void deleteRecursively(ZNode znode) throws KeeperException;

    NodeACL getACL(ZNode znode) throws KeeperException;

    Stat setACL(ZNode znode, List<ACL> acl, int version) throws KeeperException;

    long getSessionId();

    byte[] getSessionPasswd();

    String getConnectString();

}
