package com.wepay.zktools.clustermgr.internal;

import com.wepay.zktools.clustermgr.PartitionInfo;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class PartitionAssignmentSerializerTest {

    private static final int SERVER0 = 0;
    private static final int SERVER1 = 1;
    private static final int SERVER2 = 2;
    private static final int NUM_PARTITIONS = 3;

    @Test
    public void test() {
        PartitionAssignment expected;
        PartitionAssignment actual;
        Map<Integer, List<PartitionInfo>> map;

        map = new HashMap<>();
        expected = new PartitionAssignment(1, NUM_PARTITIONS, map);
        actual = serializeThenDeserialize(expected);
        assertEquals(expected, actual);

        map.put(SERVER0, partitionInfoList(0, 1));
        expected = new PartitionAssignment(1, NUM_PARTITIONS, map);
        actual = serializeThenDeserialize(expected);
        assertEquals(expected, actual);

        map.put(SERVER1, partitionInfoList(1, 2));
        expected = new PartitionAssignment(1, NUM_PARTITIONS, map);
        actual = serializeThenDeserialize(expected);
        assertEquals(expected, actual);

        map.put(SERVER2, partitionInfoList(0, 1));
        expected = new PartitionAssignment(1, NUM_PARTITIONS, map);
        actual = serializeThenDeserialize(expected);
        assertEquals(expected, actual);
    }

    private List<PartitionInfo> partitionInfoList(int... ids) {
        List<PartitionInfo> list = new ArrayList<>();
        for (int id : ids) {
            // generation is fixed to zero
            list.add(new PartitionInfo(id, 0));
        }
        return list;
    }

    private PartitionAssignment serializeThenDeserialize(PartitionAssignment assignment) {
        PartitionAssignmentSerializer serializer = new PartitionAssignmentSerializer();

        return serializer.deserialize(serializer.serialize(assignment));
    }

}
