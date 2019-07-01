package com.wepay.zktools.zookeeper;

import org.apache.zookeeper.data.Stat;

/**
 * This class represents the state of a znode
 */
public class NodeData<T> {

    /**
     * Deserialized value of znode.
     */
    public final T value;

    /**
     * {@link org.apache.zookeeper.data.Stat} of znode. It is set to null when the node does not exist.
     */
    public final Stat stat;

    public NodeData(T value, Stat stat) {
        this.value = value;
        this.stat = stat;
    }

    public final boolean isEmpty() {
        return stat == null;
    }

    public static <U> NodeData<U> empty() {
        return new NodeData<>(null, null);
    }
}
