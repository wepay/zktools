package com.wepay.zktools.clustermgr.internal;

/**
 * Cluster parameters
 */
public class ClusterParams {

    public final String name;
    public final int numPartitions;

    public ClusterParams(String name, int numPartitions) {
        this.name = name;
        this.numPartitions = numPartitions;
    }

}
