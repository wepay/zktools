package com.wepay.zktools.zookeeper.internal;

import com.wepay.zktools.util.Logging;
import com.wepay.zktools.zookeeper.Handlers;
import com.wepay.zktools.zookeeper.NodeData;
import com.wepay.zktools.zookeeper.Serializer;
import com.wepay.zktools.zookeeper.ZNode;
import com.wepay.zktools.zookeeper.ZooKeeperSession;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A watcher for a node
 */
public class NodeWatcher<P, C> implements Watcher {

    private static final Logger logger = Logging.getLogger(NodeWatcher.class);

    private final ZooKeeperSession session;
    private final ZNode znode;
    private final Handlers.OnNodeChanged<P> onNodeChanged;
    private final Serializer<P> nodeSerializer;
    private final Handlers.OnNodeOrChildChanged<P, C> onNodeOrChildChanged;
    private final Serializer<C> childSerializer;
    private final boolean removeOnDelete;
    private final NodeWatcherInstaller.WatcherState watcherState;

    private volatile NodeData<P> nodeData;
    private final ConcurrentHashMap<ZNode, NodeData<C>> childData;


    /**
     * A watcher for a child node
     */
    private class ChildWatcher implements Watcher {

        private final ZNode child;

        ChildWatcher(ZNode child) {
            this.child = child;
        }

        public void process(WatchedEvent event) {
            if (logger.isDebugEnabled()) {
                logger.debug("ChildWatcher: {}", event);
            }

            synchronized (watcherState) {
                // Process the event only when this node watcher is enabled.
                if (watcherState.isEnabled()) {
                    try {
                        switch (event.getType()) {
                            case NodeDataChanged:
                            case NodeCreated:
                            case NodeChildrenChanged: {
                                updateChild(child, this);
                                invokeHandler();
                                break;
                            }
                            case NodeDeleted: {
                                // Remove this child from the child data map
                                childData.remove(child);
                                break;
                            }
                            case None: {
                                // Ignore session events
                                break;
                            }
                            default: {
                                logger.error("unhandled event: " + event);
                            }
                        }
                    } catch (KeeperException ex) {
                        logger.error("failed to process event: " + event, ex);
                    }
                }
            }
        }
    }

    public NodeWatcher(ZooKeeperSession session,
                       ZNode znode,
                       Handlers.OnNodeChanged<P> onNodeChanged,
                       Handlers.OnNodeOrChildChanged<P, C> onNodeOrChildChanged,
                       Serializer<P> nodeDataDeserializer,
                       Serializer<C> childDataDeserializer,
                       boolean removeOnDelete,
                       NodeWatcherInstaller.WatcherState watcherState) {
        this.session = session;
        this.znode = znode;
        this.onNodeChanged = onNodeChanged;
        this.nodeSerializer = nodeDataDeserializer;
        this.onNodeOrChildChanged = onNodeOrChildChanged;
        this.childSerializer = childDataDeserializer;
        this.childData = new ConcurrentHashMap<>();
        this.removeOnDelete = removeOnDelete;
        this.watcherState = watcherState;
    }

    @Override
    public void process(WatchedEvent event) {
        if (logger.isDebugEnabled()) {
            logger.debug("NodeWatcher: {}", event);
        }

        synchronized (watcherState) {
            // Process the event only when this znode watcher is enabled.
            if (watcherState.isEnabled()) {
                try {
                    switch (event.getType()) {
                        case NodeDataChanged:
                        case NodeCreated:
                        case NodeChildrenChanged: {
                            updateNode();
                            updateChildren();
                            invokeHandler();
                            break;
                        }
                        case NodeDeleted: {
                            if (!removeOnDelete) {
                                initialize();
                            } else {
                                clear();
                                watcherState.disable();
                            }
                            invokeHandler();
                            break;
                        }
                        case None: {
                            // If SyncConnected, invoke the handler. Otherwise, ignore session events.
                            if (event.getState() == Event.KeeperState.SyncConnected) {
                                invokeHandler();
                            }
                            break;
                        }
                        default: {
                            logger.error("unhandled event: " + event);
                        }
                    }
                } catch (KeeperException ex) {
                    logger.error("failed to process event: " + event, ex);
                }
            }
        }
    }

    void initialize() {
        try {
            clear();

            if (removeOnDelete) {
                // Get the data. We set a watcher here. If the znode does not exist, we fail
                try {
                    nodeData = getData(znode, this, nodeSerializer);
                    updateChildren();
                } catch (KeeperException.NoNodeException ex) {
                    watcherState.disable();
                }
            } else {
                // Check the existence and set the watcher
                if (exists(znode, this) != null) {
                    // The znode exists
                    // Get the data (We do not set a watcher here. It was set by the exists call already)
                    nodeData = getData(znode, null, nodeSerializer);
                    updateChildren();
                }
            }
        } catch (KeeperException ex) {
            logger.error("failed to initialize node watch: path=" + znode.path, ex);
        }
    }

    private void clear() {
        // Set the empty node data.
        nodeData = NodeData.empty();
        // Clear the child data map.
        childData.clear();
    }

    void invokeHandler() {
        try {
            if (onNodeOrChildChanged != null) {
                // Make a shallow copy of the child data map and give it to the handler
                onNodeOrChildChanged.accept(nodeData, new HashMap<>(childData));
            } else {
                onNodeChanged.accept(nodeData);
            }
        } catch (Throwable ex) {
            logger.error("failed to invoke handler: path=" + znode.path, ex);
        }
    }

    private void updateNode() throws KeeperException {
        nodeData = getData(znode, this, nodeSerializer);
    }

    private void updateChildren() throws KeeperException {
        if (onNodeOrChildChanged != null) {
            Set<ZNode> children = session.getChildren(znode, this);
            for (ZNode child : childData.keySet()) {
                if (!children.contains(child)) {
                    childData.remove(child);
                }
            }
            for (ZNode child : children) {
                if (!childData.containsKey(child))
                    updateChild(child, new ChildWatcher(child));
            }
        }
    }

    private void updateChild(ZNode child, ChildWatcher watcher) throws KeeperException {
        try {
            childData.put(child, getData(child, watcher, childSerializer));
        } catch (KeeperException.NoNodeException ex) {
            // The child node has disappeared. Make sure to clean up the child data.
            childData.remove(child);
        }
    }

    private Stat exists(ZNode znode, Watcher watcher) throws KeeperException {
        return session.exists(znode, watcher);
    }

    private <T> NodeData<T> getData(ZNode znode, Watcher watcher, Serializer<T> serializer) throws KeeperException {
        return session.getData(znode, serializer, watcher);
    }

}
