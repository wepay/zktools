package com.wepay.zktools.zookeeper;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Definitions of various user supplied handlers
 */
public class Handlers {

    /**
     * OnConnected handler, a handler that is called when a ZooKeeper connection is established
     */
    public interface OnConnected extends Consumer<ZooKeeperSession> {
    }

    /**
     * OnConnectFailed handler, a handler that is called then an attempt to connect to ZooKeeper server failed
     */
    public interface OnConnectFailed extends Consumer<Throwable> {
    }

    /**
     * OnDisconnected handler, a handler that is called when ZooKeeper connection is lost
     */
    public interface OnDisconnected extends Runnable {
    }

    /**
     * OnNodeChanged handler, a handler that is called when znode is changed
     */
    public interface OnNodeChanged<T> extends Consumer<NodeData<T>> {
    }

    /**
     * OnNodeOrChildChanged handler, a handler that is called when a parent node or any of its children (including creation) is changed
     */
    public interface OnNodeOrChildChanged<P, C> extends BiConsumer<NodeData<P>, Map<ZNode, NodeData<C>>> {
    }

    /**
     * OnSessionExpired handler, a handler that is called when ZooKeeper connection is lost
     */
    public interface OnSessionExpired extends Runnable {
    }

}
