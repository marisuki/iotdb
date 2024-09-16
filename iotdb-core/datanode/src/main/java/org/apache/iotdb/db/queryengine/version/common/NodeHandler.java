package org.apache.iotdb.db.queryengine.version.common;

public class NodeHandler {
  private final ContextVersionMergerTree tree;
  private final PredicateHolder predicateHolder;

  public NodeHandler(ContextVersionMergerTree tree, PredicateHolder predicateHolder) {
    this.tree = tree;
    this.predicateHolder = predicateHolder;
  }

  public ContextVersionMergerTree getTree() {
    return tree;
  }

  public PredicateHolder getPredicateHolder() {
    return predicateHolder;
  }
}
