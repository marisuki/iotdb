package org.apache.iotdb.warehouse.storage;

import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.warehouse.common.Dataset;

import java.util.List;

public abstract class TsfileConnector {
    private List<Dataset> data;
    private List<String> seriesName;

    public void add_series_stream(Path path) {

    }

    public abstract Dataset getData(String seriesName);

}
