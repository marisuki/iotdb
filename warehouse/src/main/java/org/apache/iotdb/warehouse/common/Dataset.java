package org.apache.iotdb.warehouse.common;

import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.Vector;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.utils.Pair;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class Dataset {
    private TSDataType[] schema;
    private String name;
    public WarehouseCache buffer;

    public List<Integer> version_pos = new ArrayList<>();
    public List<Integer> filter_pos = new ArrayList<>();
    //private Stream<Long> timeDim;
    //private Stream<Pair<Long, List<Double>>> values;
    //private Stream<Long> versions;

    public Dataset(String name) {
        this.name = name;
    }
    public Dataset(TSDataType[] dataTypes) {
        this.schema = dataTypes;
    }


    public void setSchema(TSDataType[] schema) {
        this.schema = schema;
    }

    public TSDataType[] getSchema() {
        return schema;
    }


    public void setBuffer(WarehouseCache buffer) {
        this.buffer = buffer;
    }

    public WarehouseCache getBuffer() {
        return buffer;
    }

    public String getName() {
        return name;
    }

    public Stream<RowRecord> getNextBlockValues() throws IOException {
        buffer.loadSeriesBlk(name, schema);
        return buffer.cache.get(name);
    }

    public Boolean hasNext() {
        return buffer.fin.get(name);
    }
}
