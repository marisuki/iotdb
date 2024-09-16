package org.apache.iotdb.db.queryengine.version.common;

import java.util.List;

public class PlanNode {
  // contains plans attached to a writable service.
  VersionOperator operator;
  List<PlanNode> children;
  long treeId;
}
