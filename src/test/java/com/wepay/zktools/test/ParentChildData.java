package com.wepay.zktools.test;

import com.wepay.zktools.zookeeper.NodeData;
import com.wepay.zktools.zookeeper.ZNode;

import java.util.Map;
import java.util.Set;

public class ParentChildData<P, C> {

    public final NodeData<P> parent;
    public final Map<ZNode, NodeData<C>> children;

    public ParentChildData(NodeData<P> parent, Map<ZNode, NodeData<C>> children) {
        this.parent = parent;
        this.children = children;
    }

    public P getParentData() {
        return parent.value;
    }

    public Set<ZNode> getChildren() {
        return children.keySet();
    }

    public C getChildData(ZNode child) {
        return children.get(child).value;
    }
}
