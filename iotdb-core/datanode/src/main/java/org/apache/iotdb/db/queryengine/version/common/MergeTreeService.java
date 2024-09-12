package org.apache.iotdb.db.queryengine.version.common;

import org.apache.tsfile.read.reader.IPageReader;

import java.util.List;

public class MergeTreeService {
    private static final MergeTreeService singleton = new MergeTreeService();
    List<ContextVersionMergerTree> trees;
    SeriesScanService seriesScanService = SeriesScanService.getInstance();

    private MergeTreeService() {}
    public static MergeTreeService getInstance() {
        return singleton;
    }

    public ContextVersionMergerTree getMergedTree(Integer treeId) { return trees.get(treeId); }

    public IPageReader getPageReader(Integer treeId) {
        return trees.get(treeId).getPageReader();
    }

}
