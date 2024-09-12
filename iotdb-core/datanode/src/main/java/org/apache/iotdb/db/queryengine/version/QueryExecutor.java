package org.apache.iotdb.db.queryengine.version;

import org.apache.iotdb.db.queryengine.version.common.MergeTreeService;
import org.apache.iotdb.db.queryengine.version.common.SeriesScanService;

public class QueryExecutor {
    MergeTreeService mergeTreeService;
    SeriesScanService seriesScanService;

    public QueryExecutor() {
        mergeTreeService = MergeTreeService.getInstance();
        seriesScanService = SeriesScanService.getInstance();
    }

    public void query(String sql) {}
}
