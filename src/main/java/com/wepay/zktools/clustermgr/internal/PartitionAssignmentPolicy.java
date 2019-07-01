package com.wepay.zktools.clustermgr.internal;

import java.util.Map;

public interface PartitionAssignmentPolicy {

    PartitionAssignment update(int cversion,
                               PartitionAssignment oldAssignment,
                               int numPartitions,
                               Map<Integer, ServerDescriptor> serverDescriptors);

}
