package org.apache.iotdb.db.queryengine.generalgebra.common;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.filter.basic.Filter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ColumnIterator implements Iterator {

    private TSDataType dataType;
    private boolean[] valid;
    private int position;
    private int start, end;

    @Override
    public List<TSDataType> getDataType() {
        return Collections.singletonList(dataType);
    }

    @Override
    public List<TimeValuePair> nextPair() {
        return List.of();
    }

    @Override
    public boolean hasNext() {
        return position < end;
    }

    @Override
    public void remove() {

    }

    @Override
    public void insert() {

    }

    @Override
    public void modify() {

    }

    @Override
    public void skip(int numAfter) {

    }

    @Override
    public void prev(int numBefore) {

    }

    @Override
    public TsBlock nextBlock() {
        return null;
    }

    @Override
    public void filter(Filter filter) {

    }
}
