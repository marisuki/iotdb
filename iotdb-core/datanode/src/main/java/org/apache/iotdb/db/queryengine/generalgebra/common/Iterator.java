package org.apache.iotdb.db.queryengine.generalgebra.common;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.filter.basic.Filter;

import java.nio.ByteBuffer;
import java.util.List;

public interface Iterator {
    List<TSDataType> getDataType();
    List<TimeValuePair> nextPair();
    boolean hasNext();
    void remove();
    void append();
    void modify();
    void skip(int numAfter);
    void prev(int numBefore);
    TsBlock nextBlock();
    void filter(Filter filter);
}
