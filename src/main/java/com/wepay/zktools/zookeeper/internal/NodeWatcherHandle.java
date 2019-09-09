package com.wepay.zktools.zookeeper.internal;

import com.wepay.zktools.zookeeper.WatcherHandle;

/**
 * A handle for a node watcher
 */
public class NodeWatcherHandle implements WatcherHandle {

    private final NodeWatcherInstaller installer;
    private final NodeWatcherManager manager;

    public NodeWatcherHandle(NodeWatcherInstaller installer, NodeWatcherManager manager) {
        this.installer = installer;
        this.manager = manager;
    }

    public void close() {
        manager.deregisterWatcherInstaller(installer);
    }

    public boolean isClosed() {
        return installer.isClosed();
    }
}
