package com.wepay.zktools.clustermgr.internal;

import com.wepay.zktools.zookeeper.ZNode;

public class ManagedServerInfo {

    public final Integer serverId;
    public final ZNode znode;

    public ManagedServerInfo(Integer serverId, ZNode znode) {
        this.serverId = serverId;
        this.znode = znode;
    }

}
