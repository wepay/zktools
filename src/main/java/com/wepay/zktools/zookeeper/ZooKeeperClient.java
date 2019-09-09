package com.wepay.zktools.zookeeper;

import com.wepay.zktools.zookeeper.internal.ZooKeeperClientImpl;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.io.Closeable;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

/**
 * ZooKeeper client wrapper
 */
public interface ZooKeeperClient extends Closeable {

    static ZooKeeperClient create(String connectString, int sessionTimeout) throws ZooKeeperClientException {
        return new ZooKeeperClientImpl(connectString, sessionTimeout);
    }

    /**
     * Adds auth info to the zookeeper session
     * @param scheme
     * @param auth
     */
    void addAuthInfo(String scheme, byte[] auth);

    /**
     * Sets a handler that is invoked when a session is connected.
     * @param handler
     * @return a watcher handle
     */
    WatcherHandle onConnected(Handlers.OnConnected handler);

    /**
     * Sets a handler that is invoked when a session is disconnected.
     * @param handler
     * @return a watcher handle
     */
    WatcherHandle onDisconnected(Handlers.OnDisconnected handler);

    /**
     * Sets a handler that is invoked when a zookeeper server is unreachable.
     * @param handler
     * @return a watcher handle
     */
    WatcherHandle onConnectFailed(Handlers.OnConnectFailed handler);

    /**
     * Sets a handler that is invoked when a session is expired.
     * @param handler
     * @return a watcher handle
     */
    WatcherHandle onSessionExpired(Handlers.OnSessionExpired handler);

    /**
     * Returns the current session {@link ZooKeeperSession}.
     * @return the current session {@link ZooKeeperSession}
     * @throws ZooKeeperClientException
     */
    ZooKeeperSession session() throws ZooKeeperClientException;

    /**
     * Sets a handler that is invoked when the specified znode is changed.
     * @param znode
     * @param onNodeChanged
     * @param nodeSerializer
     * @param <T> Class of znode data
     * @return a watcher handle
     */
    <T> WatcherHandle watch(ZNode znode, Handlers.OnNodeChanged<T> onNodeChanged, Serializer<T> nodeSerializer);

    /**
     * Sets a handler that is invoked when the specified znode is changed.
     * @param znode
     * @param onNodeChanged
     * @param nodeSerializer
     * @param removeOnDelete
     * @param <T> Class of znode data
     * @return a watcher handle
     */
    <T> WatcherHandle watch(ZNode znode, Handlers.OnNodeChanged<T> onNodeChanged, Serializer<T> nodeSerializer, boolean removeOnDelete);

    /**
     * Sets a handler that is invoked when the specified znode is changed or its child is created/changed/deleted.
     * @param znode
     * @param onNodeOrChildChanged
     * @param nodeSerializer
     * @param childSerializer
     * @param <P> Class of znode data
     * @param <C> Class of child znode data
     * @return a watcher handle
     */
    <P, C> WatcherHandle watch(ZNode znode,
                               Handlers.OnNodeOrChildChanged<P, C> onNodeOrChildChanged,
                               Serializer<P> nodeSerializer,
                               Serializer<C> childSerializer);
    /**
     * Sets a handler that is invoked when the specified znode is changed or its child is created/changed/deleted.
     * @param znode
     * @param onNodeOrChildChanged
     * @param nodeSerializer
     * @param childSerializer
     * @param removeOnDelete
     * @param <P> Class of znode data
     * @param <C> Class of child znode data
     * @return a watcher handle
     */
    <P, C> WatcherHandle watch(ZNode znode,
                               Handlers.OnNodeOrChildChanged<P, C> onNodeOrChildChanged,
                               Serializer<P> nodeSerializer,
                               Serializer<C> childSerializer,
                               boolean removeOnDelete);

    /**
     * Closes this client.
     */
    void close();

    ZNode createPath(ZNode znode) throws KeeperException, ZooKeeperClientException;

    ZNode createPath(ZNode znode, List<ACL> acl) throws KeeperException, ZooKeeperClientException;

    ZNode create(ZNode znode, CreateMode createMode) throws KeeperException, ZooKeeperClientException;

    ZNode create(ZNode znode, List<ACL> acl, CreateMode createMode) throws KeeperException, ZooKeeperClientException;

    <T> ZNode create(ZNode znode, T data, Serializer<T> serializer, CreateMode createMode) throws KeeperException, ZooKeeperClientException;

    <T> ZNode create(ZNode znode, T data, Serializer<T> serializer, List<ACL> acl, CreateMode createMode) throws KeeperException, ZooKeeperClientException;

    Stat exists(ZNode znode) throws KeeperException, ZooKeeperClientException;

    Set<ZNode> getChildren(ZNode znode) throws KeeperException, ZooKeeperClientException;

    <T> NodeData<T> getData(ZNode znode, Serializer<T> serializer) throws KeeperException, ZooKeeperClientException;

    <T> void setData(ZNode znode, T data, Serializer<T> serializer) throws KeeperException, ZooKeeperClientException;

    <T> void setData(ZNode znode, T data, Serializer<T> serializer, int version) throws KeeperException, ZooKeeperClientException;

    void delete(ZNode znode) throws KeeperException, ZooKeeperClientException;

    void delete(ZNode znode, int version) throws KeeperException, ZooKeeperClientException;

    void deleteRecursively(ZNode znode) throws KeeperException, ZooKeeperClientException;

    NodeACL getACL(ZNode znode) throws KeeperException, ZooKeeperClientException;

    Stat setACL(ZNode znode, List<ACL> acl, int version) throws KeeperException, ZooKeeperClientException;

    String getConnectString();

    int getSessionTimeout();

    /**
     * Executes an action mutually exclusive way. Locking (or synchronization) is done on the ephemeral node
     * designated by {@code lockNode}. {@code action} is an instance of {@link Consumer} that takes an instance of
     * {@link ZooKeeperSession}. All access to ZooKeeper from the {@code action} should use the {@code ZooKeeperSession} instance passed in.
     * @param lockNode znode to synchronize on
     * @param action a block of code executed mutually exclusive way.
     * @param <E> Class of the action specific exception
     * @throws E action specific exception
     * @throws KeeperException
     * @throws ZooKeeperClientException
     */
    <E extends Exception> void mutex(ZNode lockNode, MutexAction<E> action) throws E, KeeperException, ZooKeeperClientException;

}
