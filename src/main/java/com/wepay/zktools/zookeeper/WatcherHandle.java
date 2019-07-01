package com.wepay.zktools.zookeeper;

/**
 * A handle for a watcher. A watcher is created by either
 * {@link ZooKeeperClient#onConnected(Handlers.OnConnected)},
 * {@link ZooKeeperClient#onConnectFailed(Handlers.OnConnectFailed)},
 * {@link ZooKeeperClient#onDisconnected(Handlers.OnDisconnected)},
 * {@link ZooKeeperClient#watch(ZNode, Handlers.OnNodeChanged, Serializer)},
 * {@link ZooKeeperClient#watch(ZNode, Handlers.OnNodeChanged, Serializer, boolean)},
 * {@link ZooKeeperClient#watch(ZNode, Handlers.OnNodeOrChildChanged, Serializer, Serializer)}, or
 * {@link ZooKeeperClient#watch(ZNode, Handlers.OnNodeOrChildChanged, Serializer, Serializer, boolean)}.
 * <p>
 * Closing the handle will disable the watcher associated with the handle.
 * </p>
 *
 */
public interface WatcherHandle {

    /**
     * Closes the watcher handle. This effectively removes the associated watcher.
     * However, the actual removal of watcher at ZooKeeper level is deferred until some session event triggers it.
     */
    void close();

    /**
     * Returns {@code true} if the associated watcher is disabled
     */
    boolean isClosed();
}
