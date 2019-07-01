package com.wepay.zktools.clustermgr.internal;

import com.wepay.zktools.clustermgr.PartitionInfo;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class PartitionAssignmentTest {

    private static final int SERVER0 = 0;
    private static final int SERVER1 = 1;
    private static final int SERVER2 = 2;
    private static final int NUM_PARTITIONS = 3;

    @Test
    public void testPartitionsFor() {
        Map<Integer, List<PartitionInfo>> map = new HashMap<>();
        map.put(SERVER0, partitionInfoList(0, 1));
        map.put(SERVER1, partitionInfoList(1, 2));

        PartitionAssignment partitionAssignment = new PartitionAssignment(1, NUM_PARTITIONS, map);

        assertEquals(partitionInfoList(0, 1), partitionAssignment.partitionsFor(SERVER0));
        assertEquals(partitionInfoList(1, 2), partitionAssignment.partitionsFor(SERVER1));
        assertEquals(partitionInfoList(), partitionAssignment.partitionsFor(SERVER2));
    }

    @Test
    public void testIsComplete() {
        Map<Integer, List<PartitionInfo>> assignment;

        assignment = Collections.emptyMap();
        assertFalse(PartitionAssignment.isComplete(NUM_PARTITIONS, assignment));

        assignment = Collections.singletonMap(SERVER0, partitionInfoList(0, 1));
        assertFalse(PartitionAssignment.isComplete(NUM_PARTITIONS, assignment));

        assignment = Collections.singletonMap(SERVER0, partitionInfoList(0, 1, 2));
        assertTrue(PartitionAssignment.isComplete(NUM_PARTITIONS, assignment));

        assignment = new HashMap<>();
        assignment.put(SERVER0, partitionInfoList(0, 1));
        assignment.put(SERVER1, partitionInfoList(0, 1));
        assertFalse(PartitionAssignment.isComplete(NUM_PARTITIONS, assignment));

        assignment = new HashMap<>();
        assignment.put(SERVER0, partitionInfoList(0, 1));
        assignment.put(SERVER1, partitionInfoList(1, 2));
        assertTrue(PartitionAssignment.isComplete(NUM_PARTITIONS, assignment));

        assignment = new HashMap<>();
        assignment.put(SERVER0, partitionInfoList(0, 1));
        assignment.put(SERVER1, partitionInfoList(1, 2));
        assignment.put(SERVER2, partitionInfoList(0, 1));
        assertTrue(PartitionAssignment.isComplete(NUM_PARTITIONS, assignment));

        try {
            // partition id out of range
            assignment = Collections.singletonMap(SERVER0, partitionInfoList(0, 1, 3));
            PartitionAssignment.isComplete(NUM_PARTITIONS, assignment);
            fail();
        } catch (IllegalArgumentException ex) {
            // OK
        }
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
