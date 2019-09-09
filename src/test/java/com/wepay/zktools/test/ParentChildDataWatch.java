package com.wepay.zktools.test;

import com.wepay.zktools.zookeeper.Handlers;
import com.wepay.zktools.zookeeper.ZNode;
import org.apache.zookeeper.data.Stat;

public class ParentChildDataWatch<P, C> extends DataWatch<ParentChildData<P, C>> {

    public final Handlers.OnNodeOrChildChanged<P, C> handler =
        (parent, children) -> update(new ParentChildData<>(parent, children));

    protected boolean checkStats(Stat target, ParentChildData<P, C> item) {
        return checkStats(target, item.parent.stat);
    }

    protected boolean checkStats(Stat parentTarget, ZNode child, Stat childTarget, ParentChildData<P, C> item) {
        Stat parentStat = item.parent.stat;
        if (checkStats(parentTarget, parentStat)) {
            return checkStats(childTarget, item.children.get(child).stat);
        } else {
            return false;
        }
    }

}
