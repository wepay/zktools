package com.wepay.zktools.zookeeper.internal;

import com.wepay.zktools.util.Logging;
import com.wepay.zktools.util.Uninterruptibly;
import com.wepay.zktools.zookeeper.Handlers;
import com.wepay.zktools.zookeeper.MutexAction;
import com.wepay.zktools.zookeeper.NodeACL;
import com.wepay.zktools.zookeeper.NodeData;
import com.wepay.zktools.zookeeper.Serializer;
import com.wepay.zktools.zookeeper.WatcherHandle;
import com.wepay.zktools.zookeeper.ZNode;
import com.wepay.zktools.zookeeper.ZooKeeperClient;
import com.wepay.zktools.zookeeper.ZooKeeperClientException;
import com.wepay.zktools.zookeeper.ZooKeeperSession;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An implementation of {@link ZooKeeperClient}
 */
public class ZooKeeperClientImpl implements ZooKeeperClient {

    private static final Logger logger = Logging.getLogger(ZooKeeperClientImpl.class);

    private final String connectString;
    private final int sessionTimeout;
    private final SessionRef sessionRef;
    private final LinkedList<AuthInfo> authInfoList = new LinkedList<>();

    private final ConnectionWatcherManager<ZooKeeperSession> onConnectedWatcherManager;
    private final ConnectionWatcherManager<Void> onDisconnectedWatcherManager;
    private final ConnectionWatcherManager<Throwable> onConnectFailedWatcherManager;
    private final ConnectionWatcherManager<Void> onSessionExpiredWatcherManager;
    private final AtomicReference<Throwable> connectErrorRef = new AtomicReference<>(null);

    private final NodeWatcherManager nodeWatcherManager = new NodeWatcherManager();

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    private volatile boolean running = true;

    public ZooKeeperClientImpl(String connectString, int sessionTimeout) throws ZooKeeperClientException {
        this(connectString, sessionTimeout, null);
    }

    public ZooKeeperClientImpl(String connectString, int sessionTimeout, Integer connectTimeout) throws ZooKeeperClientException {
        this.connectString = connectString;
        this.sessionTimeout = sessionTimeout;
        this.onConnectedWatcherManager = new ConnectionWatcherManager<>();
        this.onDisconnectedWatcherManager = new ConnectionWatcherManager<>();
        this.onConnectFailedWatcherManager = new ConnectionWatcherManager<>();
        this.onSessionExpiredWatcherManager = new ConnectionWatcherManager<>();

        // Create a session
        try {
            CompletableFuture<ZooKeeperSessionImpl> future = ZooKeeperSessionImpl.createAsync(this);
            if (connectTimeout == null) {
                sessionRef = new SessionRef(future.join());
            } else {
                sessionRef = new SessionRef(future.get(connectTimeout, TimeUnit.SECONDS));
            }
        } catch (CompletionException | InterruptedException | ExecutionException | TimeoutException ex) {
            connectErrorRef.set(ex.getCause());
            throw new ZooKeeperClientException("connection failure", ex.getCause());
        }
    }

    void refreshSessionAsync() {
        executor.submit(() -> {
            long retryInterval = 1000; // 1 second

            while (running) {
                if (refreshSession())
                    return;

                Uninterruptibly.sleep(retryInterval);

                if (retryInterval < 8000) // limit
                    retryInterval = retryInterval * 2;
            }
        });
    }

    private boolean refreshSession() {
        try {
            CompletableFuture<ZooKeeperSessionImpl> future = ZooKeeperSessionImpl.createAsync(this);

            ZooKeeperSessionImpl newSession = future.join();
            for (AuthInfo authInfo : authInfoList) {
                newSession.zk.addAuthInfo(authInfo.scheme, authInfo.auth);
            }
            sessionRef.set(newSession);

            // Enable watchers
            nodeWatcherManager.installWatchers(newSession);

            return true;

        } catch (CompletionException ex) {
            Throwable t = ex.getCause();
            onConnectFailedWatcherManager.apply(t);
            connectErrorRef.set(t);
        } finally {
            // Wake up threads waiting for a connection
            sessionRef.wakeUpWaitingThread();
        }
        return false;
    }

    @Override
    public void addAuthInfo(String scheme, byte[] auth) {
        synchronized (sessionRef) {
            authInfoList.add(new AuthInfo(scheme, auth));
            sessionRef.get().zk.addAuthInfo(scheme, auth);
        }
    }

    @Override
    public WatcherHandle onConnected(Handlers.OnConnected handler) {
        ConnectionWatcher<ZooKeeperSession> watcher = new ConnectionWatcher<>(handler);
        ConnectionWatcherHandle handle = onConnectedWatcherManager.addConnectionWatcher(watcher);

        ZooKeeperSessionImpl session = sessionRef.get();

        // If already connected, execute now
        tryExecuteOnConnectedWatcher(session, watcher);

        return handle;
    }

    private void tryExecuteOnConnectedWatcher(ZooKeeperSessionImpl session, ConnectionWatcher<ZooKeeperSession> watcher) {
        if (session.zk.getState() == ZooKeeper.States.CONNECTED) {
            try {
                watcher.accept(session);

            } catch (Throwable ex) {
                logger.error("Exception during execution of an onConnected handler", ex);
            }
        } else {
            // Not connected. Execute the handler later
            onConnectedWatcherManager.addPendingConnectionWatcher(watcher);
        }
    }

    @Override
    public WatcherHandle onDisconnected(Handlers.OnDisconnected handler) {
        ConnectionWatcher<Void> watcher = new ConnectionWatcher<>(x -> handler.run());
        ConnectionWatcherHandle handle = onDisconnectedWatcherManager.addConnectionWatcher(watcher);

        return handle;
    }

    @Override
    public WatcherHandle onConnectFailed(Handlers.OnConnectFailed handler) {
        ConnectionWatcher<Throwable> watcher = new ConnectionWatcher<>(handler);
        ConnectionWatcherHandle handle = onConnectFailedWatcherManager.addConnectionWatcher(watcher);

        // If already failed, execute now
        tryExecuteOnConnectFailedWatcher(handler);

        return handle;
    }

    @Override
    public WatcherHandle onSessionExpired(Handlers.OnSessionExpired handler) {
        ConnectionWatcher<Void> watcher = new ConnectionWatcher<>(x -> handler.run());
        ConnectionWatcherHandle handle = onSessionExpiredWatcherManager.addConnectionWatcher(watcher);

        return handle;
    }

    private void tryExecuteOnConnectFailedWatcher(Handlers.OnConnectFailed handler) {
        try {
            Throwable error = connectErrorRef.get();
            if (error != null)
                handler.accept(error);
        } catch (Throwable ex) {
            // Ignore
        }
    }

    @Override
    public ZooKeeperSession session() throws ZooKeeperClientException {
        return sessionRef.getActiveSession();
    }

    @Override
    public <T> WatcherHandle watch(ZNode znode,
                                   Handlers.OnNodeChanged<T> onNodeChanged,
                                   Serializer<T> nodeSerializer) {
        return watch(znode, onNodeChanged, nodeSerializer, false);
    }

    @Override
    public <T> WatcherHandle watch(ZNode znode,
                                   Handlers.OnNodeChanged<T> onNodeChanged,
                                   Serializer<T> nodeSerializer,
                                   boolean removeOnDelete) {
        return install(new NodeWatcherInstaller<>(znode, onNodeChanged, nodeSerializer, removeOnDelete));
    }

    @Override
    public <P, C> WatcherHandle watch(ZNode znode,
                                      Handlers.OnNodeOrChildChanged<P, C> onChildrenChanged,
                                      Serializer<P> nodeSerializer,
                                      Serializer<C> childSerializer) {
        return watch(znode, onChildrenChanged, nodeSerializer, childSerializer, false);
    }

    @Override
    public <P, C> WatcherHandle watch(ZNode znode,
                                      Handlers.OnNodeOrChildChanged<P, C> onChildrenChanged,
                                      Serializer<P> nodeSerializer,
                                      Serializer<C> childSerializer,
                                      boolean removeOnDelete) {
        return install(new NodeWatcherInstaller<>(znode, onChildrenChanged, nodeSerializer, childSerializer, removeOnDelete));
    }

    private <P, C> WatcherHandle install(NodeWatcherInstaller<P, C> installer) {
        NodeWatcherHandle handle = nodeWatcherManager.registerWatcherInstaller(installer);
        sessionRef.get().install(installer);
        return handle;
    }

    @Override
    public void close() {
        running = false;
        sessionRef.wakeUpWaitingThread();
        sessionRef.get().close();
        executor.shutdown();
        disconnected();
    }

    @Override
    public ZNode createPath(ZNode znode) throws KeeperException, ZooKeeperClientException {
        while (true) {
            try {
                return session().createPath(znode);
            } catch (KeeperException ex) {
                check(ex);
            }
        }
    }

    @Override
    public ZNode createPath(ZNode znode, List<ACL> acl) throws KeeperException, ZooKeeperClientException {
        while (true) {
            try {
                return session().createPath(znode, acl);
            } catch (KeeperException ex) {
                check(ex);
            }
        }
    }

    @Override
    public ZNode create(ZNode znode, CreateMode createMode) throws KeeperException, ZooKeeperClientException {
        while (true) {
            try {
                return session().create(znode, createMode);
            } catch (KeeperException ex) {
                check(ex);
            }
        }
    }

    @Override
    public ZNode create(ZNode znode, List<ACL> acl, CreateMode createMode) throws KeeperException, ZooKeeperClientException {
        while (true) {
            try {
                return session().create(znode, acl, createMode);
            } catch (KeeperException ex) {
                check(ex);
            }
        }
    }

    @Override
    public <T> ZNode create(ZNode znode, T data, Serializer<T> serializer, CreateMode createMode) throws KeeperException, ZooKeeperClientException {
        while (true) {
            try {
                return session().create(znode, data, serializer, createMode);
            } catch (KeeperException ex) {
                check(ex);
            }
        }
    }

    @Override
    public <T> ZNode create(ZNode znode, T data, Serializer<T> serializer, List<ACL> acl, CreateMode createMode) throws KeeperException, ZooKeeperClientException {
        while (true) {
            try {
                return session().create(znode, data, serializer, acl, createMode);
            } catch (KeeperException ex) {
                check(ex);
            }
        }
    }

    @Override
    public Stat exists(ZNode znode) throws KeeperException, ZooKeeperClientException {
        while (true) {
            try {
                return session().exists(znode);
            } catch (KeeperException ex) {
                check(ex);
            }
        }
    }

    @Override
    public Set<ZNode> getChildren(ZNode znode) throws KeeperException, ZooKeeperClientException {
        while (true) {
            try {
                return session().getChildren(znode);
            } catch (KeeperException ex) {
                check(ex);
            }
        }
    }

    @Override
    public <T> NodeData<T> getData(ZNode znode, Serializer<T> serializer) throws KeeperException, ZooKeeperClientException {
        while (true) {
            try {
                return session().getData(znode, serializer);
            } catch (KeeperException ex) {
                check(ex);
            }
        }
    }

    @Override
    public <T> void setData(ZNode znode, T data, Serializer<T> serializer) throws KeeperException, ZooKeeperClientException {
        while (true) {
            try {
                session().setData(znode, data, serializer);
                return;
            } catch (KeeperException ex) {
                check(ex);
            }
        }
    }

    @Override
    public <T> void setData(ZNode znode, T data, Serializer<T> serializer, int version) throws KeeperException, ZooKeeperClientException {
        while (true) {
            try {
                session().setData(znode, data, serializer, version);
                return;
            } catch (KeeperException ex) {
                check(ex);
            }
        }
    }

    @Override
    public void delete(ZNode znode) throws KeeperException, ZooKeeperClientException {
        while (true) {
            try {
                session().delete(znode);
                return;
            } catch (KeeperException ex) {
                check(ex);
            }
        }
    }

    @Override
    public void delete(ZNode znode, int version) throws KeeperException, ZooKeeperClientException {
        while (true) {
            try {
                session().delete(znode, version);
                return;
            } catch (KeeperException ex) {
                check(ex);
            }
        }
    }

    @Override
    public void deleteRecursively(ZNode znode) throws KeeperException, ZooKeeperClientException {
        while (true) {
            try {
                session().deleteRecursively(znode);
                return;
            } catch (KeeperException ex) {
                check(ex);
            }
        }
    }

    @Override
    public NodeACL getACL(ZNode znode) throws KeeperException, ZooKeeperClientException {
        while (true) {
            try {
                return session().getACL(znode);
            } catch (KeeperException ex) {
                check(ex);
            }
        }
    }

    @Override
    public Stat setACL(ZNode znode, List<ACL> acl, int version) throws KeeperException, ZooKeeperClientException {
        while (true) {
            try {
                return session().setACL(znode, acl, version);
            } catch (KeeperException ex) {
                check(ex);
            }
        }
    }

    @Override
    public String getConnectString() {
        return connectString;
    }

    @Override
    public int getSessionTimeout() {
        return sessionTimeout;
    }

    @Override
    public <E extends Exception> void mutex(ZNode lockNode, MutexAction<E> action) throws E, KeeperException, ZooKeeperClientException {
        ZooKeeperSession zkSession = lock(lockNode);

        try {
            action.apply(zkSession);

        } finally {
            unlock(lockNode, zkSession);
        }
    }

    private ZooKeeperSession lock(ZNode lockNode) throws KeeperException, ZooKeeperClientException {
        while (true) {
            ZooKeeperSession zkSession = session();

            try {
                zkSession.create(lockNode, CreateMode.EPHEMERAL);
                return zkSession;

            } catch (KeeperException.NodeExistsException ex) {
                // The lock is held by other process
            }

            // Watch the lock node and wait. This thread will be unblocked by any type of event.
            final CountDownLatch latch = new CountDownLatch(1);
            if (zkSession.exists(lockNode, event -> latch.countDown()) != null) {
                Uninterruptibly.run(latch::await);
            }
        }
    }

    private void unlock(ZNode lockNode, ZooKeeperSession zkSession) {
        while (true) {
            try {
                zkSession.delete(lockNode);
                return;

            } catch (KeeperException keeperException) {
                switch (keeperException.code()) {
                    case NONODE:
                    case SESSIONEXPIRED:
                        return;
                    default:
                }
            }
        }
    }

    private void check(KeeperException keeperException) throws KeeperException {
        switch (keeperException.code()) {
            case CONNECTIONLOSS:
            case SESSIONEXPIRED: {
                break;
            }
            default:
                throw keeperException;
        }
    }

    void disconnected() {
        onDisconnectedWatcherManager.apply(null);
    }

    void sessionExpired() {
        onSessionExpiredWatcherManager.apply(null);
    }

    void recoverFromConnectionLoss() {
        // Wake up threads waiting for a connection immediately. Don't await handler installation to prevent deadlocks.
        sessionRef.wakeUpWaitingThread();

        ZooKeeperSessionImpl session = sessionRef.get();

        // Execute pending onConnected handlers if any
        List<ConnectionWatcher<ZooKeeperSession>> pending = onConnectedWatcherManager.getPendingWatchers();
        pending.forEach(handler -> tryExecuteOnConnectedWatcher(session, handler));

        onConnectedWatcherManager.apply(session);
    }

    private class SessionRef {

        private volatile ZooKeeperSessionImpl session;

        SessionRef(ZooKeeperSessionImpl session) {
            if (session == null)
                throw new NullPointerException("session cannot be null");

            this.session = session;
        }

        @SuppressFBWarnings("NN_NAKED_NOTIFY")
        void wakeUpWaitingThread() {
            synchronized (this) {
                this.notifyAll();
            }
        }

        ZooKeeperSessionImpl get() {
            return session;
        }

        void set(ZooKeeperSessionImpl ses) {
            synchronized (this) {
                this.notifyAll();
                ZooKeeperSessionImpl oldSession = session;
                session = ses;

                onConnectedWatcherManager.apply(session);

                // Make sure that the old session is closed
                if (oldSession != null) {
                    try {
                        oldSession.close();
                    } catch (Throwable ex) {
                        // Swallow any runtime exception
                    }
                }
            }
        }

        ZooKeeperSessionImpl getActiveSession() throws ZooKeeperClientException {
            ZooKeeperSessionImpl ses = session;
            if (ses.isConnected())
                return ses;

            synchronized (this) {
                while (running) {
                    try {
                        if (session.isConnected())
                            return session;

                        this.wait();

                    } catch (InterruptedException ex) {
                        Thread.interrupted();
                    }
                }
                throw new ZooKeeperClientException("client is closed");
            }
        }

    }

    private static class AuthInfo {

        final String scheme;
        final byte[] auth;

        AuthInfo(String scheme, byte[] auth) {
            this.scheme = scheme;
            this.auth = auth;
        }

    }
}