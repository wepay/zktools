package com.wepay.zktools.zookeeper;

import com.wepay.zktools.test.ZKTestUtils;
import com.wepay.zktools.test.util.ZooKeeperServerRunner;
import com.wepay.zktools.zookeeper.internal.ZooKeeperClientImpl;
import com.wepay.zktools.zookeeper.serializer.ByteArraySerializer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class DigestAuthTest extends ZKTestUtils {

    @Test
    public void test() throws Exception {
        ZNode znodeWithACL = new ZNode("/nodeWithACL");
        ZNode znodeWithNoACL = new ZNode("/nodeWithNoACL");
        ByteArraySerializer serializer = new ByteArraySerializer();

        ZooKeeperServerRunner zooKeeperServerRunner = new ZooKeeperServerRunner(0);
        try {
            String connectString = zooKeeperServerRunner.start();

            String credential1 = "user1:passwd1";
            String credential2 = "user2:passwd2";

            ZooKeeperClient client1 = new ZooKeeperClientImpl(connectString, 30000);
            client1.addAuthInfo("digest", credential1.getBytes("UTF-8"));

            ZooKeeperClient client2 = new ZooKeeperClientImpl(connectString, 30000);
            client2.addAuthInfo("digest", credential2.getBytes("UTF-8"));

            client1.create(znodeWithACL, ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
            client1.create(znodeWithNoACL, CreateMode.PERSISTENT);

            NodeACL nodeACL = client1.getACL(znodeWithACL);
            assertEquals(1, nodeACL.acl.size());
            assertEquals(ZooDefs.Perms.ALL, nodeACL.acl.get(0).getPerms());
            assertEquals("digest", nodeACL.acl.get(0).getId().getScheme());
            assertEquals(DigestAuthenticationProvider.generateDigest(credential1), nodeACL.acl.get(0).getId().getId());

            NodeACL nodeNoACL = client1.getACL(znodeWithNoACL);
            assertEquals(1, nodeNoACL.acl.size());
            assertEquals(ZooDefs.Perms.ALL, nodeNoACL.acl.get(0).getPerms());
            assertEquals("world", nodeNoACL.acl.get(0).getId().getScheme());
            assertEquals("anyone", nodeNoACL.acl.get(0).getId().getId());

            assertNotNull(client1.getData(znodeWithACL, serializer));
            assertNotNull(client1.getData(znodeWithNoACL, serializer));

            try {
                client2.getData(znodeWithACL, serializer);
                fail();
            } catch (KeeperException.NoAuthException ex) {
                // Ignore
            }
            assertNotNull(client2.getData(znodeWithNoACL, serializer));

            // Expire the session
            ZKTestUtils.expire(client1.session());

            try {
                client2.getData(znodeWithACL, serializer);
                fail();
            } catch (KeeperException.NoAuthException ex) {
                // Ignore
            }

            client1.addAuthInfo("digest", credential2.getBytes("UTF-8"));
            client1.setACL(znodeWithACL, ZooDefs.Ids.CREATOR_ALL_ACL, nodeACL.stat.getAversion());

            assertNotNull(client1.getData(znodeWithACL, serializer));
            assertNotNull(client2.getData(znodeWithACL, serializer));

        } finally {
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.clear();
        }
    }

}
