package org.apache.iotdb.db.queryengine.generalgebra;

public class QueryExecutor {
  MergeTreeService mergeTreeService;
  SeriesScanService seriesScanService;

  public QueryExecutor() {
    mergeTreeService = MergeTreeService.getInstance();
    seriesScanService = SeriesScanService.getInstance();
  }

  public void query(String sql) {}
}
