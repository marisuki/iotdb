package org.apache.iotdb.db.queryengine.common;

import org.apache.tsfile.read.reader.IPointReader;

import java.util.Comparator;
import java.util.List;

public interface IVersionPointReader extends IPointReader {

    List<Integer> getVersion(int id);

    boolean isOnlyPageReader();

    void setVersion(List<Integer> version);

    boolean versionCompare(Comparator<List<Integer>> comparator, List<Integer> otherVersion);

}
