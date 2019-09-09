package com.wepay.zktools.zookeeper.internal;

import com.wepay.zktools.zookeeper.ZooKeeperSession;

import java.util.ArrayList;

/**
 * A class that installs node watchers through node watcher installers
 */
public class NodeWatcherManager {

    private final ArrayList<NodeWatcherInstaller> watcherInstallerList = new ArrayList<>();

    public NodeWatcherHandle registerWatcherInstaller(NodeWatcherInstaller installer) {
        synchronized (watcherInstallerList) {
            watcherInstallerList.add(installer);
        }
        return new NodeWatcherHandle(installer, this);
    }

    public void installWatchers(ZooKeeperSession session) {
        synchronized (watcherInstallerList) {
            watcherInstallerList.forEach(installer -> installer.install(session));
        }
    }

    public void deregisterWatcherInstaller(NodeWatcherInstaller installer) {
        installer.close();
        synchronized (watcherInstallerList) {
            watcherInstallerList.remove(installer);
        }
    }

}
