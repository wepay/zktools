package com.wepay.zktools.test;

import com.wepay.zktools.zookeeper.NodeData;
import com.wepay.zktools.zookeeper.Serializer;
import com.wepay.zktools.zookeeper.ZNode;
import com.wepay.zktools.zookeeper.ZooKeeperClient;
import com.wepay.zktools.zookeeper.ZooKeeperClientException;
import com.wepay.zktools.zookeeper.ZooKeeperSession;
import com.wepay.zktools.zookeeper.serializer.IntegerSerializer;
import com.wepay.zktools.zookeeper.serializer.StringSerializer;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.CompletableFuture;

@SuppressFBWarnings("NP_NONNULL_PARAM_VIOLATION") // https://github.com/findbugsproject/findbugs/issues/79
public class ZKTestUtils {

    public static final long TIMEOUT = 50000;
    public final StringSerializer strSerializer = new StringSerializer();
    public final IntegerSerializer intSerializer = new IntegerSerializer();

    public static <T> NodeData<T> createNode(
        ZooKeeperClient client,
        ZNode znode,
        T value,
        Serializer<T> serializer
    ) throws KeeperException, ZooKeeperClientException {
        client.create(znode, value, serializer, CreateMode.PERSISTENT);
        return client.getData(znode, serializer);
    }

    public static <T> NodeData<T> createNode(
        ZooKeeperClient client,
        ZNode znode,
        T value,
        Serializer<T> serializer,
        DataWatch<NodeData<T>> watch
    ) throws KeeperException, ZooKeeperClientException {
        client.create(znode, value, serializer, CreateMode.PERSISTENT);
        Stat target = client.exists(znode);
        return watch.await(target, TIMEOUT);
    }

    public static <T> NodeData<T> deleteNode(
        ZooKeeperClient client,
        ZNode znode,
        DataWatch<NodeData<T>> watch
    ) throws KeeperException, ZooKeeperClientException {
        client.delete(znode);
        return watch.await(null, TIMEOUT);
    }

    public static <T> NodeData<T> updateNodeData(
        ZooKeeperClient client,
        ZNode znode,
        T value,
        Serializer<T> serializer,
        DataWatch<NodeData<T>> watch
    ) throws KeeperException, ZooKeeperClientException {
        client.setData(znode, value, serializer);
        Stat target = client.exists(znode);
        return watch.await(target, TIMEOUT);
    }

    public static <P, C> ParentChildData<P, C> createParent(
        ZooKeeperClient client,
        ZNode parent,
        P value,
        Serializer<P> serializer,
        ParentChildDataWatch<P, C> watch
    ) throws KeeperException, ZooKeeperClientException {
        client.create(parent, value, serializer, CreateMode.PERSISTENT);
        Stat target = client.exists(parent);
        return watch.await(target, TIMEOUT);
    }

    public static <P, C> ParentChildData<P, C> createChild(
        ZooKeeperClient client,
        ZNode parent,
        ZNode child,
        C value,
        Serializer<C> serializer,
        ParentChildDataWatch<P, C> watch
    ) throws KeeperException, ZooKeeperClientException {
        client.create(child, value, serializer, CreateMode.PERSISTENT);
        Stat target = client.exists(parent);
        return watch.await(target, TIMEOUT);
    }

    public static <P, C> ParentChildData<P, C> updateParentData(
        ZooKeeperClient client,
        ZNode parent,
        P value,
        Serializer<P> serializer,
        ParentChildDataWatch<P, C> watch
    ) throws KeeperException, ZooKeeperClientException {
        client.setData(parent, value, serializer);
        Stat target = client.exists(parent);
        return watch.await(target, TIMEOUT);
    }

    public static <P, C> ParentChildData<P, C> updateChildData(
        ZooKeeperClient client,
        ZNode parent,
        ZNode child,
        C value,
        Serializer<C> serializer,
        ParentChildDataWatch<P, C> watch
    ) throws KeeperException, ZooKeeperClientException {
        client.setData(child, value, serializer);
        Stat parentTarget = client.exists(parent);
        Stat childTarget = client.exists(child);
        return watch.await(parentTarget, child, childTarget, TIMEOUT);
    }

    public static <P, C> ParentChildData<P, C> deleteParent(
        ZooKeeperClient client,
        ZNode parent,
        ParentChildDataWatch<P, C> watch
    ) throws KeeperException, ZooKeeperClientException {
        client.delete(parent);
        return watch.await(null, TIMEOUT);
    }

    public static <P, C> ParentChildData<P, C> deleteChild(
        ZooKeeperClient client,
        ZNode parent,
        ZNode child,
        ParentChildDataWatch<P, C> watch
    ) throws KeeperException, ZooKeeperClientException {
        client.delete(child);
        Stat target = client.exists(parent);
        return watch.await(target, TIMEOUT);
    }

    @SuppressFBWarnings("REC_CATCH_EXCEPTION")
    public static boolean expire(ZooKeeperSession session) {
        CompletableFuture<?> future = new CompletableFuture<>();
        try {
            ZooKeeper zk = new ZooKeeper(
                session.getConnectString(),
                30000,
                new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                        switch (event.getState()) {
                            case SyncConnected: {
                                future.complete(null);
                                break;
                            }
                            case AuthFailed: {
                                future.completeExceptionally(new KeeperException.AuthFailedException());
                                break;
                            }
                            case Expired: {
                                future.completeExceptionally(new KeeperException.SessionExpiredException());
                                break;
                            }
                            default:
                                // TODO: logging
                        }
                    }
                },
                session.getSessionId(),
                session.getSessionPasswd()
            );

            future.get();
            zk.close();
            return true;

        } catch (Exception ex) {
            return false;
        }

    }

}
