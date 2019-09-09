package com.wepay.zktools.zookeeper;

import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.List;

/**
 * This class represents ACL of a znode
 */
public class NodeACL {

    /**
     * ACL of znode
     */
    public final List<ACL> acl;

    /**
     * {@link org.apache.zookeeper.data.Stat} of znode
     */
    public final Stat stat;

    public NodeACL(List<ACL> acl, Stat stat) {
        this.acl = acl;
        this.stat = stat;
    }
}
