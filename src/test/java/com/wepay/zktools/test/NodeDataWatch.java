package com.wepay.zktools.test;

import com.wepay.zktools.zookeeper.Handlers;
import com.wepay.zktools.zookeeper.NodeData;
import com.wepay.zktools.zookeeper.ZNode;
import org.apache.zookeeper.data.Stat;

public class NodeDataWatch<T> extends DataWatch<NodeData<T>> {

    public final Handlers.OnNodeChanged<T> handler = this::update;

    protected boolean checkStats(Stat target, NodeData<T> item) {
        return checkStats(target, item.stat);
    }

    protected boolean checkStats(Stat parentTarget, ZNode child, Stat childTarget, NodeData<T> item) {
        throw new UnsupportedOperationException();
    }

}
