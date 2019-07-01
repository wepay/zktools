package com.wepay.zktools.zookeeper.internal;

import com.wepay.zktools.zookeeper.WatcherHandle;

/**
 * A handle for a connection watcher
 */
public class ConnectionWatcherHandle<T> implements WatcherHandle {

    private final ConnectionWatcher<T> watcher;
    private final ConnectionWatcherManager<T> manager;

    public ConnectionWatcherHandle(ConnectionWatcher<T> watcher, ConnectionWatcherManager<T> manager) {
        this.watcher = watcher;
        this.manager = manager;
    }

    public void close() {
        manager.removeConnectionWatcher(watcher);
    }

    public boolean isClosed() {
        return watcher.isClosed();
    }
}
