package com.wepay.zktools.clustermgr.tools;

import com.wepay.zktools.clustermgr.ClusterManager;
import com.wepay.zktools.clustermgr.PartitionInfo;
import com.wepay.zktools.clustermgr.internal.ClusterManagerImpl;
import com.wepay.zktools.clustermgr.internal.DynamicPartitionAssignmentPolicy;
import com.wepay.zktools.clustermgr.internal.PartitionAssignment;
import com.wepay.zktools.clustermgr.internal.ServerDescriptor;
import com.wepay.zktools.zookeeper.ZNode;
import com.wepay.zktools.zookeeper.ZooKeeperClient;
import com.wepay.zktools.zookeeper.internal.ZooKeeperClientImpl;

import java.util.List;
import java.util.Set;
import java.util.StringJoiner;

/**
 * A tool for listing ZooKeeper partition data.
 * <pre>
 *     Usage: ListCluster -z zookeeperConnectString -r clusterRootPath
 * </pre>
 */
public class ListCluster {

    public static void main(String[] args) throws Exception {
        String zkConnectString = null;
        String clusterRootPath = null;
        int i = 0;
        while (i < args.length) {
            switch (args[i]) {
                case "-z": {
                    i++;
                    if (i < args.length) {
                        zkConnectString = args[i++];
                    } else {
                        usage();
                    }
                    break;
                }
                case "-r": {
                    i++;
                    if (i < args.length) {
                        clusterRootPath = args[i++];
                    } else {
                        usage();
                    }
                    break;
                }
                default: {
                    usage();
                }
            }
        }

        if (zkConnectString == null || clusterRootPath == null)
            usage();

        ZooKeeperClient zkClient = new ZooKeeperClientImpl(zkConnectString, 30000);

        list(new ZNode(clusterRootPath), zkClient);
    }

    public static void list(ZNode root, ZooKeeperClient zkClient) throws Exception {
        ClusterManager clusterManager = new ClusterManagerImpl(zkClient, root, new DynamicPartitionAssignmentPolicy());
        Set<ServerDescriptor> serverDescriptors = clusterManager.serverDescriptors();
        PartitionAssignment partitionAssignment = clusterManager.partitionAssignment();
        String clusterName = clusterManager.clusterName();
        int numPartitions = clusterManager.numPartitions();

        System.out.println("cluster root [" + root + "]:");

        System.out.println("  name=" + clusterName);
        System.out.println("  numPartitions=" + numPartitions);

        System.out.println(String.format("cluster root [%s] has server descriptors:", root));

        for (ServerDescriptor sd : serverDescriptors) {
            StringJoiner partitionJoiner = new StringJoiner(",");
            sd.partitions.forEach(p -> partitionJoiner.add(p.toString()));
            String partitionString = (sd.partitions.size() == 0) ? "*" : partitionJoiner.toString();
            System.out.println(String.format("  server=%d, endpoint=%s, preferred partitions=[%s]", sd.serverId, sd.endpoint, partitionString));
        }

        System.out.println(String.format("cluster root [%s] has partition assignment metadata:", root));
        System.out.println(String.format("  cversion=%d, endpoints=%d, partitions=%d",
                partitionAssignment.cversion, partitionAssignment.numEndpoints, partitionAssignment.numPartitions));

        System.out.println(String.format("cluster root [%s] has partition assignments:", root));
        for (int serverId : partitionAssignment.serverIds()) {
            List<PartitionInfo> partitionInfoList = partitionAssignment.partitionsFor(serverId);
            for (PartitionInfo partitionInfo : partitionInfoList) {
                System.out.println(String.format("  server=%d, partition=%d, generation=%d",
                        serverId, partitionInfo.partitionId, partitionInfo.generation));
            }
        }
    }

    private static void usage() {
        System.out.println("Usage: ListCluster -z <zookeeperConnectString> -r <clusterRootPath>");
        System.exit(1);
    }

}
