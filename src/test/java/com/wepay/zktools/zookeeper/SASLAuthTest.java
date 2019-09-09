package com.wepay.zktools.zookeeper;

import com.wepay.zktools.test.util.ZooKeeperServerRunner;
import com.wepay.zktools.zookeeper.internal.ZooKeeperClientImpl;
import com.wepay.zktools.zookeeper.serializer.ByteArraySerializer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SASLAuthTest extends SASLTestBase {

    @Test
    public void test() throws Exception {
        ZNode znodeWithACL = new ZNode("/nodeWithACL");
        ZNode znodeWithACLChild = new ZNode(znodeWithACL, "child");
        ZNode znodeWithNoACL = new ZNode("/nodeWithNoACL");
        ZNode znodeWithNoACLChild = new ZNode(znodeWithNoACL, "child");

        ByteArraySerializer serializer = new ByteArraySerializer();

        File confDir = Files.createTempDirectory("zktools-jaas-").toFile();
        try {
            File jaasConfFile = configJaas(confDir);
            appendClientJaasConfig(jaasConfFile);

            ZooKeeperServerRunner zooKeeperServerRunner = new ZooKeeperServerRunner(0);
            try {
                String connectString = zooKeeperServerRunner.start();

                ZooKeeperClient client1 = new ZooKeeperClientImpl(connectString, 30000);

                client1.create(znodeWithACL, ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
                client1.create(znodeWithACLChild, ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);

                client1.create(znodeWithNoACL, CreateMode.PERSISTENT);
                client1.create(znodeWithNoACLChild, CreateMode.PERSISTENT);

                NodeACL nodeACL = client1.getACL(znodeWithACL);
                assertEquals(1, nodeACL.acl.size());
                assertEquals(ZooDefs.Perms.ALL, nodeACL.acl.get(0).getPerms());
                assertEquals("sasl", nodeACL.acl.get(0).getId().getScheme());
                assertEquals("user1", nodeACL.acl.get(0).getId().getId());

                NodeACL nodeNoACL = client1.getACL(znodeWithNoACL);
                assertEquals(1, nodeNoACL.acl.size());
                assertEquals(ZooDefs.Perms.ALL, nodeNoACL.acl.get(0).getPerms());
                assertEquals("world", nodeNoACL.acl.get(0).getId().getScheme());
                assertEquals("anyone", nodeNoACL.acl.get(0).getId().getId());

                assertNotNull(client1.getData(znodeWithACL, serializer));
                assertNotNull(client1.getData(znodeWithNoACL, serializer));

                client1.delete(znodeWithACLChild);
                client1.delete(znodeWithNoACLChild);

            } catch (Exception ex) {
                ex.printStackTrace();
                throw ex;
            } finally {
                zooKeeperServerRunner.stop();
                zooKeeperServerRunner.clear();
            }
        } finally {
            clearSystemProperties();
        }
    }

}
