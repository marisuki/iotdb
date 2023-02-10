package org.apache.iotdb.db.mpp.common;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class L3CacheAdoptHelper {
  public static long L3CACHE_SIZE = 12 * (1L << 30) * 8; // in bits
  private long allocated_bits = 0;

  public static long numOfValInL3(TSDataType dataType) {
    return solve(dataType, L3CACHE_SIZE);
  }

  private static long solve(TSDataType dataType, long allocation) {
    switch (dataType) {
      case INT32:
      case FLOAT:
        return allocation / 32;
      case DOUBLE:
      case INT64:
        return allocation / 64;
      case TEXT:
        return allocation / 16;
      case BOOLEAN:
        return allocation;
      default:
        return -1;
    }
  }

  public void allocate(long bits) {
    allocated_bits += bits;
  }

  public long restNumOfVal(TSDataType dataType) {
    return solve(dataType, L3CACHE_SIZE - allocated_bits);
  }
}
