package org.apache.iotdb.db.queryengine.version.common;

import org.apache.iotdb.db.utils.SetThreadName;

public class Task implements Runnable {
  TreeNode toProcess;
  TreeNode writableNodeRef;
  SetThreadName threadName;

  @Override
  public void run() {}
}
