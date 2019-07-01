package com.wepay.zktools.zookeeper.internal;

import com.wepay.zktools.util.Logging;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * A class that manages connection watchers
 */
public class ConnectionWatcherManager<T> {

    private static final Logger logger = Logging.getLogger(ConnectionWatcherManager.class);

    private final ArrayList<ConnectionWatcher<T>> watchers = new ArrayList<>();
    private final ArrayList<ConnectionWatcher<T>> pending = new ArrayList<>();

    public ConnectionWatcherHandle<T> addConnectionWatcher(ConnectionWatcher<T> watcher) {
        synchronized (watchers) {
            watchers.add(watcher);
            return new ConnectionWatcherHandle<>(watcher, this);
        }
    }

    public void removeConnectionWatcher(ConnectionWatcher<T> watcher) {
        synchronized (watchers) {
            watcher.close();
            watchers.remove(watcher);
        }
    }

    public void addPendingConnectionWatcher(ConnectionWatcher<T> watcher) {
        synchronized (pending) {
            pending.add(watcher);
        }
    }

    public void apply(T value) {
        synchronized (watchers) {
            for (ConnectionWatcher<T> watcher : watchers) {
                try {
                    watcher.accept(value);
                } catch (Throwable handlerException) {
                    logger.error("exception in connection watcher", handlerException);
                }
            }
        }
    }

    public List<ConnectionWatcher<T>> getPendingWatchers() {
        List<ConnectionWatcher<T>> temp = new ArrayList<>();
        synchronized (pending) {
            temp.addAll(pending);
            pending.clear();
        }
        return temp;
    }

}
