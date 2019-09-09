package com.wepay.zktools.zookeeper.internal;

import com.wepay.zktools.zookeeper.Handlers;
import com.wepay.zktools.zookeeper.Serializer;
import com.wepay.zktools.zookeeper.ZNode;
import com.wepay.zktools.zookeeper.ZooKeeperSession;

/**
 * A class that installs node watchers
 */
public class NodeWatcherInstaller<P, C> {

    public interface WatcherState {
        boolean isEnabled();
        void disable();
    }

    private final ZNode znode;
    private final Handlers.OnNodeChanged<P> onNodeChanged;
    private final Serializer<P> nodeSerializer;
    private final Handlers.OnNodeOrChildChanged<P, C> onNodeOrChildChanged;
    private final Serializer<C> childSerializer;
    private final boolean removeOnDelete;
    private final WatcherState watcherState;

    private volatile boolean closed = false;


    public NodeWatcherInstaller(ZNode znode,
                                Handlers.OnNodeChanged<P> onNodeChanged,
                                Serializer<P> nodeSerializer,
                                boolean removeOnDelete) {
        this(znode, onNodeChanged, null, nodeSerializer, null, removeOnDelete);
    }

    public NodeWatcherInstaller(ZNode znode,
                                Handlers.OnNodeOrChildChanged<P, C> onNodeOrChildChanged,
                                Serializer<P> nodeSerializer,
                                Serializer<C> childSerializer,
                                boolean removeOnDelete) {
        this(znode, null, onNodeOrChildChanged, nodeSerializer, childSerializer, removeOnDelete);
    }

    private NodeWatcherInstaller(ZNode znode,
                                 Handlers.OnNodeChanged<P> onNodeChanged,
                                 Handlers.OnNodeOrChildChanged<P, C> onNodeOrChildChanged,
                                 Serializer<P> nodeDataDeserializer,
                                 Serializer<C> childDataDeserializer,
                                 boolean removeOnDelete) {
        this.znode = znode;
        this.onNodeChanged = onNodeChanged;
        this.nodeSerializer = nodeDataDeserializer;
        this.onNodeOrChildChanged = onNodeOrChildChanged;
        this.childSerializer = childDataDeserializer;
        this.removeOnDelete = removeOnDelete;

        this.watcherState = new WatcherState() {
            @Override
            public boolean isEnabled() {
                return !closed;
            }
            @Override
            public void disable() {
                closed = true;
            }
        };
    }

    public void install(ZooKeeperSession session) {
        NodeWatcher<P, C> watcher =
            new NodeWatcher<>(session, znode, onNodeChanged, onNodeOrChildChanged, nodeSerializer, childSerializer, removeOnDelete, watcherState);

        synchronized (watcherState) {
            if (watcherState.isEnabled()) {
                watcher.initialize();
                watcher.invokeHandler();
            }
        }
    }

    public void close() {
        closed = true;
    }

    public boolean isClosed() {
        return closed;
    }
}
