package com.wepay.zktools.clustermgr.tools;

import com.wepay.zktools.clustermgr.ClusterManagerException;
import com.wepay.zktools.clustermgr.internal.ClusterManagerImpl;
import com.wepay.zktools.clustermgr.internal.ClusterParams;
import com.wepay.zktools.clustermgr.internal.ClusterParamsSerializer;
import com.wepay.zktools.zookeeper.NodeData;
import com.wepay.zktools.zookeeper.ZNode;
import com.wepay.zktools.zookeeper.ZooKeeperClient;
import com.wepay.zktools.zookeeper.internal.ZooKeeperClientImpl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/**
 * A tool for setting up ZooKeeper nodes for a ledger cluster.
 * <pre>
 *     Usage: CreateCluster -z zookeeperConnectString -n clusterName -p numberOfPartitions -r clusterRootPath
 * </pre>
 */
public class CreateCluster {

    public static void main(String[] args) throws Exception {
        String zkConnectString = null;
        String clusterRootPath = null;
        String name = null;
        int numPartitions = 0;
        int i = 0;

        while (i < args.length) {
            switch (args[i]) {
                case "-z": {
                    i++;
                    if (i < args.length) {
                        zkConnectString = args[i];
                    } else {
                        usage();
                    }
                    break;
                }
                case "-r": {
                    i++;
                    if (i < args.length) {
                        clusterRootPath = args[i];
                    } else {
                        usage();
                    }
                    break;
                }
                case "-n": {
                    i++;
                    if (i < args.length) {
                        name = args[i];
                    } else {
                        usage();
                    }
                    break;
                }
                case "-p": {
                    i++;
                    if (i < args.length) {
                        numPartitions = Integer.parseInt(args[i]);
                    } else {
                        usage();
                    }
                    break;
                }
                default: {
                    usage();
                }

            }
            i++; // move to the next arg
        }

        if (zkConnectString == null || name == null || clusterRootPath == null)
            usage();

        try (ZooKeeperClient zkClient = new ZooKeeperClientImpl(zkConnectString, 30000);
             BufferedReader reader = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {

            create(new ZNode(clusterRootPath), name, numPartitions, zkClient, false, reader);

        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }
    }

    public static void create(ZNode root, String name, int numPartitions, ZooKeeperClient zkClient, boolean force, BufferedReader reader) throws Exception {
        int version;

        if (!force && zkClient.exists(root) != null) {
            if (reader != null) {
                System.out.println("cluster root [" + root + "] already exists");
                while (true) {
                    System.out.println("OK? [Y/N]");
                    String line = prompt(reader);
                    if (line.equalsIgnoreCase("Y")) {
                        break;
                    } else if (line.equalsIgnoreCase("N")) {
                        throw new ClusterManagerException("failed to create cluster: cluster root [" + root + "] already exists");
                    }
                }
            } else {
                throw new ClusterManagerException("failed to create cluster: cluster root [" + root + "] already exists");
            }
        }
        // Create the cluster znode
        zkClient.createPath(root);

        ClusterParamsSerializer serializer = new ClusterParamsSerializer();
        NodeData<ClusterParams> nodeData = zkClient.getData(root, serializer);
        if (!force && nodeData.value != null) {
            if (reader != null) {
                System.out.println("cluster root [" + root + "] has cluster parameters:");
                System.out.println("  name=" + nodeData.value.name);
                System.out.println("  numPartitions=" + nodeData.value.numPartitions);
                while (true) {
                    System.out.println("Do you want to overwrite? [Y/N]");
                    System.out.println("Overwriting cluster parameters may make existing ledger data inaccessible.");
                    String line = prompt(reader);
                    if (line.equalsIgnoreCase("Y")) {
                        break;
                    } else if (line.equalsIgnoreCase("N")) {
                        throw new ClusterManagerException("failed to create cluster: cluster root [" + root + "] has cluster parameters");
                    }
                }
            } else {
                throw new ClusterManagerException("failed to create cluster: cluster root [" + root + "] has cluster parameters");
            }
        }
        version = nodeData.stat.getVersion();

        try {
            zkClient.setData(root, new ClusterParams(name, numPartitions), new ClusterParamsSerializer(), version);
        } catch (Exception ex) {
            throw new ClusterManagerException("failed to create cluster: failed to set cluster parameters");
        }

        // ClusterManager
        ClusterManagerImpl.createZNodes(zkClient, root);
    }

    private static String prompt(BufferedReader reader) throws IOException {
        System.out.print("> ");
        return reader.readLine();
    }

    private static void usage() {
        System.out.println("Usage: CreateCluster -z <zookeeperConnectString> -p <numberOfPartitions> -r <clusterRootPath>");
        System.exit(1);
    }

}
