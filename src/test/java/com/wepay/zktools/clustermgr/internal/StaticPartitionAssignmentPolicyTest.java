package com.wepay.zktools.clustermgr.internal;

import com.wepay.zktools.clustermgr.Endpoint;
import com.wepay.zktools.clustermgr.PartitionInfo;
import com.wepay.zktools.test.util.Utils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class StaticPartitionAssignmentPolicyTest {

    private static final int SERVER0 = 0;
    private static final int SERVER1 = 1;
    private static final int SERVER2 = 2;
    private static final int NUM_PARTITIONS = 5;

    @Test
    public void test() {
        int cversion = 0;
        StaticPartitionAssignmentPolicy policy = new StaticPartitionAssignmentPolicy();

        PartitionAssignment assignment = new PartitionAssignment(0, NUM_PARTITIONS, Collections.emptyMap());

        Map<Integer, ServerDescriptor> servers = new HashMap<>();

        assignment = policy.update(++cversion, assignment, NUM_PARTITIONS, servers);
        assertEquals(0, assignment.numEndpoints);
        assertEquals(Utils.set(), assignment.serverIds());

        // add server0 (serves 0, 1, 2), missing 3, 4
        servers.put(SERVER0, new ServerDescriptor(SERVER0, new Endpoint("host0", 6000), Arrays.asList(0, 1, 2)));
        assignment = policy.update(++cversion, assignment, NUM_PARTITIONS, servers);
        assertEquals(1, assignment.numEndpoints);
        assertEquals(Utils.set(0), assignment.serverIds());
        assertEquals(partitionInfoList(0, 1, 2), assignment.partitionsFor(0));

        // add server1 (serves 0, 1), still missing 3, 4
        servers.put(SERVER1, new ServerDescriptor(SERVER1, new Endpoint("host1", 6000), Arrays.asList(0, 1)));
        assignment = policy.update(++cversion, assignment, NUM_PARTITIONS, servers);
        assertEquals(2, assignment.numEndpoints);
        assertEquals(Utils.set(SERVER0, SERVER1), assignment.serverIds());
        assertEquals(partitionInfoList(0, 1, 2), assignment.partitionsFor(SERVER0));
        assertEquals(partitionInfoList(0, 1), assignment.partitionsFor(SERVER1));

        // add server2 (serves 3, 4), no missing partition
        servers.put(SERVER2, new ServerDescriptor(SERVER2, new Endpoint("host2", 6000), Arrays.asList(3, 4)));
        assignment = policy.update(++cversion, assignment, NUM_PARTITIONS, servers);
        assertEquals(3, assignment.numEndpoints);
        assertEquals(Utils.set(SERVER0, SERVER1, SERVER2), assignment.serverIds());
        assertEquals(partitionInfoList(0, 1, 2), assignment.partitionsFor(SERVER0));
        assertEquals(partitionInfoList(0, 1), assignment.partitionsFor(SERVER1));
        assertEquals(partitionInfoList(3, 4), assignment.partitionsFor(SERVER2));

        // Completely change servers' preferred partitions
        servers.put(SERVER0, new ServerDescriptor(SERVER0, new Endpoint("host0", 6000), Arrays.asList(0, 1)));
        servers.put(SERVER1, new ServerDescriptor(SERVER1, new Endpoint("host1", 6000), Arrays.asList(1, 2)));
        servers.put(SERVER2, new ServerDescriptor(SERVER2, new Endpoint("host2", 6000), Arrays.asList(0, 3)));
        assignment = policy.update(++cversion, assignment, NUM_PARTITIONS, servers);
        assertEquals(3, assignment.numEndpoints);
        assertEquals(Utils.set(SERVER0, SERVER1, SERVER2), assignment.serverIds());
        assertEquals(partitionInfoList(0, 1), assignment.partitionsFor(SERVER0));
        assertEquals(partitionInfoList(1, 2), assignment.partitionsFor(SERVER1));
        assertEquals(partitionInfoList(0, 3), assignment.partitionsFor(SERVER2));

        // Remove server0
        servers.remove(SERVER0);
        assignment = policy.update(++cversion, assignment, NUM_PARTITIONS, servers);
        assertEquals(2, assignment.numEndpoints);
        assertEquals(Utils.set(SERVER1, SERVER2), assignment.serverIds());
        assertEquals(partitionInfoList(1, 2), assignment.partitionsFor(SERVER1));
        assertEquals(partitionInfoList(0, 3), assignment.partitionsFor(SERVER2));

        // Remove server1
        servers.remove(SERVER1);
        assignment = policy.update(++cversion, assignment, NUM_PARTITIONS, servers);
        assertEquals(1, assignment.numEndpoints);
        assertEquals(Utils.set(SERVER2), assignment.serverIds());
        assertEquals(partitionInfoList(0, 3), assignment.partitionsFor(SERVER2));

        // Remove server2
        servers.remove(SERVER2);
        assignment = policy.update(++cversion, assignment, NUM_PARTITIONS, servers);
        assertEquals(0, assignment.numEndpoints);
        assertEquals(Utils.set(), assignment.serverIds());
    }

    private List<PartitionInfo> partitionInfoList(int... ids) {
        List<PartitionInfo> list = new ArrayList<>();
        for (int id : ids) {
            // generation is fixed to zero
            list.add(new PartitionInfo(id, 0));
        }
        return list;
    }
}
