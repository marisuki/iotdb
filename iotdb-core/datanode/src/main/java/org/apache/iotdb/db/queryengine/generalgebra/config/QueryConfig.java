package org.apache.iotdb.db.queryengine.generalgebra.config;

public enum QueryConfig {
    QUERY_MEM_CAPACITY(1024*32),
    QUERY_MEM_MINIMUM_ALLOCATABLE(32),
    QUERY_MAX_CORES(16),
    QUERY_WAIT_INTERVALS(5);

    int value;
    QueryConfig(int value) {
        this.value = value;
    }

    public int value() {
        return value;
    }
}
