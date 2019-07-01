package com.wepay.zktools.zookeeper.internal;

import com.wepay.zktools.util.Logging;
import com.wepay.zktools.util.Uninterruptibly;
import com.wepay.zktools.zookeeper.NodeACL;
import com.wepay.zktools.zookeeper.NodeData;
import com.wepay.zktools.zookeeper.Serializer;
import com.wepay.zktools.zookeeper.ZNode;
import com.wepay.zktools.zookeeper.ZooKeeperSession;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * An implementation of {@link ZooKeeperSession}
 */
public final class ZooKeeperSessionImpl implements ZooKeeperSession {

    private static final Logger logger = Logging.getLogger(ZooKeeperSessionImpl.class);

    private class SessionWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            if (logger.isDebugEnabled()) {
                logger.debug("SessionWatcher: {}", event);
            }

            switch (event.getState()) {
                case Disconnected: {
                    zkClient.disconnected();
                    break;
                }
                case SyncConnected: {
                    // Invoke onConnected handlers by completing the future
                    boolean isNewConnection = sessionFuture.complete(ZooKeeperSessionImpl.this);

                    // If not a new connection, we must have recovered from a connection loss
                    if (!isNewConnection)
                        zkClient.recoverFromConnectionLoss();

                    break;
                }
                case AuthFailed: {
                    logger.warn("authorization failed", new KeeperException.AuthFailedException());
                    break;
                }
                case Expired: {
                    KeeperException.SessionExpiredException sessionExpiredException = new KeeperException.SessionExpiredException();
                    sessionFuture.completeExceptionally(sessionExpiredException);
                    logger.warn("session expired", sessionExpiredException);
                    zkClient.sessionExpired();
                    zkClient.refreshSessionAsync();
                    break;
                }
                default:
                    logger.warn("unhandled session event: {}", event);
            }
        }
    }

    final ZooKeeper zk;
    private final ZooKeeperClientImpl zkClient;
    private final CompletableFuture<ZooKeeperSessionImpl> sessionFuture;

    public static CompletableFuture<ZooKeeperSessionImpl> createAsync(ZooKeeperClientImpl zkClient) {
        try {
            ZooKeeperSessionImpl session = new ZooKeeperSessionImpl(zkClient);
            return session.sessionFuture;

        } catch (IOException ex) {
            CompletableFuture<ZooKeeperSessionImpl> doomedFuture = new CompletableFuture<>();
            doomedFuture.completeExceptionally(ex);
            return doomedFuture;
        }
    }

    private ZooKeeperSessionImpl(ZooKeeperClientImpl zkClient) throws IOException {
        this.zkClient = zkClient;
        this.sessionFuture = new CompletableFuture<>();
        this.zk = new ZooKeeper(zkClient.getConnectString(), zkClient.getSessionTimeout(), new SessionWatcher());
    }

    @Override
    public boolean isConnected() {
        ZooKeeper.States state = zk.getState();
        return state == ZooKeeper.States.CONNECTED;
    }

    @Override
    public ZNode createPath(ZNode znode) throws KeeperException {
        return createPath(znode, ZooDefs.Ids.OPEN_ACL_UNSAFE);
    }

    @Override
    public ZNode createPath(ZNode znode, List<ACL> acl) throws KeeperException {
        for (ZNode ancestor : znode.ancestors()) {
            if (logger.isDebugEnabled())
                logger.debug("Creating znode: {}", ancestor.path);

            try {
                create(ancestor, null, null, acl, CreateMode.PERSISTENT);

            } catch (KeeperException.NodeExistsException ex) {
                // Ignore existing nodes
            }
        }
        try {
            if (logger.isDebugEnabled())
                logger.debug("Creating znode: {}", znode.path);

            create(znode, null, null, acl, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException ex) {
            // ignore existing nodes
        }
        return znode;
    }

    @Override
    public ZNode create(ZNode znode, CreateMode createMode) throws KeeperException {
        return create(znode, null, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode);
    }

    @Override
    public ZNode create(ZNode znode, List<ACL> acl, CreateMode createMode) throws KeeperException {
        return create(znode, null, null, acl, createMode);
    }

    @Override
    public <T> ZNode create(ZNode znode, T data, Serializer<T> serializer, CreateMode createMode) throws KeeperException {
        return create(znode, data, serializer, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode);
    }

    @Override
    public <T> ZNode create(ZNode znode, T data, Serializer<T> serializer, List<ACL> acl, CreateMode createMode) throws KeeperException {
        try {
            byte[] rawData = data != null ? serializer.serialize(data) : null;

            while (true) {
                try {
                    return new ZNode(zk.create(znode.path, rawData, acl, createMode));

                } catch (InterruptedException ex) {
                    Thread.interrupted();
                }
            }
        } catch (IllegalArgumentException ex) {
            throw new IllegalStateException(ex);
        }
    }

    @Override
    public Stat exists(ZNode znode) throws KeeperException {
        return exists(znode, null);
    }

    @Override
    public Stat exists(ZNode znode, Watcher watcher) throws KeeperException {
        while (true) {
            try {
                return zk.exists(znode.path, watcher);

            } catch (InterruptedException ex) {
                Thread.interrupted();
            }
        }
    }

    @Override
    public Set<ZNode> getChildren(ZNode znode) throws KeeperException {
        return getChildren(znode, null);
    }

    @Override
    public Set<ZNode> getChildren(ZNode znode, Watcher watcher) throws KeeperException {
        while (true) {
            try {
                List<String> childNames = zk.getChildren(znode.path, watcher);
                HashSet<ZNode> childZNodes = new HashSet<>();
                for (String childName : childNames) {
                    try {
                        childZNodes.add(new ZNode(znode, childName));
                    } catch (IllegalArgumentException ex) {
                        // This should never happen
                        logger.error("parsing error: child name=" + childName, ex);
                    }
                }
                return childZNodes;

            } catch (InterruptedException ex) {
                Thread.interrupted();
            }
        }
    }

    @Override
    public <T> NodeData<T> getData(ZNode znode, Serializer<T> serializer) throws KeeperException {
        return getData(znode, serializer, null);
    }

    @Override
    public <T> NodeData<T> getData(ZNode znode, Serializer<T> serializer, Watcher watcher) throws KeeperException {
        Stat stat = new Stat();

        while (true) {
            try {
                byte[] rawData = zk.getData(znode.path, watcher, stat);
                T value = (rawData != null && serializer != null) ? serializer.deserialize(rawData) : null;

                return new NodeData<>(value, stat);

            } catch (InterruptedException ex) {
                Thread.interrupted();
            }
        }
    }

    public <T> void setData(ZNode znode, T data, Serializer<T> serializer) throws KeeperException {
        setData(znode, data, serializer, -1);
    }

    @Override
    public <T> void setData(ZNode znode, T data, Serializer<T> serializer, int version) throws KeeperException {
        byte[] rawData = data != null ? serializer.serialize(data) : null;

        while (true) {
            try {
                zk.setData(znode.path, rawData, version);
                return;

            } catch (InterruptedException ex) {
                Thread.interrupted();
            }
        }
    }

    @Override
    public void delete(ZNode znode) throws KeeperException {
        delete(znode, -1);
    }

    @Override
    public void delete(ZNode znode, int version) throws KeeperException {
        while (true) {
            try {
                zk.delete(znode.path, version);
                return;

            } catch (InterruptedException ex) {
                Thread.interrupted();
            }
        }
    }

    @Override
    public void deleteRecursively(ZNode znode) throws KeeperException {
        Set<ZNode> children = getChildren(znode);
        for (ZNode child : children) {
            deleteRecursively(child);
        }
        delete(znode);
    }

    @Override
    public NodeACL getACL(ZNode znode) throws KeeperException {
        Stat stat = new Stat();

        while (true) {
            try {
                List<ACL> acl = zk.getACL(znode.path, stat);
                return new NodeACL(acl, stat);

            } catch (InterruptedException ex) {
                Thread.interrupted();
            }
        }
    }

    @Override
    public Stat setACL(ZNode znode, List<ACL> acl, int version) throws KeeperException {
        while (true) {
            try {
                return zk.setACL(znode.path, acl, version);

            } catch (InterruptedException ex) {
                Thread.interrupted();
            }
        }
    }

    @Override
    public long getSessionId() {
        return zk.getSessionId();
    }

    @Override
    public byte[] getSessionPasswd() {
        return zk.getSessionPasswd();
    }

    @Override
    public String getConnectString() {
        return zkClient.getConnectString();
    }

    public void close() {
        Uninterruptibly.run(zk::close);
     }

    void install(NodeWatcherInstaller installer) {
        installer.install(this);
    }

}
