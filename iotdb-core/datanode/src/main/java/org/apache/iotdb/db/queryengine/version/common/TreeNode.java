package org.apache.iotdb.db.queryengine.version.common;

import java.util.List;

public abstract class TreeNode extends WritableNode {
    // srcId, targetId ref. MergeTreeService.getMergeTree(), sys will create writable node for buffer output of a subquery
    Integer srcId;
    Integer targetId;

    // when is writable, it is an executable, active node to compute
    boolean isWritable;

    // next nodes for page readers.
    List<Integer> next;

    // sub-plan
    PlanNode plan;

    // merge priority
    long version;
}
