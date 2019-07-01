package com.wepay.zktools.zookeeper.internal;

import java.util.function.Consumer;

/**
 * A wrapper for a connection watcher
 */
public class ConnectionWatcher<T> implements Consumer<T> {

    public final Consumer<T> handler;

    private volatile boolean closed = false;

    public ConnectionWatcher(Consumer<T> handler) {
        this.handler = handler;
    }

    /**
     * Closes the connection watcher. This disabled the watcher.
     */
    public void close() {
        closed = true;
    }

    public boolean isClosed() {
        return closed;
    }

    @Override
    public void accept(T value) {
        if (!closed) {
            handler.accept(value);
        }
    }
}
