package org.apache.iotdb.db.queryengine.version.common;

import org.apache.iotdb.commons.path.IFullPath;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.tsfile.read.reader.IPageReader;

import java.util.List;
import java.util.Map;

public class ContextVersionMergerTree {

    // path is either a device.sensor path or a hashcode assigned by SeriesScanService to cache an intermediate page
    IFullPath srcPath;
    long treeId;
    // all tree nodes, including active ones and folded ones (not cached to read and merge)
    List<TreeNode> nodeList;

    public ContextVersionMergerTree(IFullPath srcPath, long treeId) {}

    public IPageReader getPageReader() {
        return null;
    }
}
