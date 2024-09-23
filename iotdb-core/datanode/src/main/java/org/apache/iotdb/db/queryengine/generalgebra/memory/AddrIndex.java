package org.apache.iotdb.db.queryengine.generalgebra.memory;

import org.apache.iotdb.db.queryengine.generalgebra.config.QueryConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class AddrIndex {
    // binary tree for continuous available buffer ranges.
    List<RangeNode> ranges;
    private static final int ALLOCATE_UNIT = QueryConfig.QUERY_MEM_CAPACITY.value()/QueryConfig.QUERY_MEM_MINIMUM_ALLOCATABLE.value();
    int valid;
    private static int MASK = (ALLOCATE_UNIT-1) | ALLOCATE_UNIT;
    private static final int REVERT_MASK = ~MASK ;
    private ReentrantLock lock = new ReentrantLock();

    public AddrIndex() {
        ranges = new ArrayList<RangeNode>();
        ranges.add(new RangeNode(ALLOCATE_UNIT));
        valid = ALLOCATE_UNIT;
    }

    public static int addr_start(int addr) {
        return addr & REVERT_MASK;
    }

    public static int addr_end(int addr) {
        return addr
    }

    public void insert(int addr) {
        try {
            if(lock.tryLock(QueryConfig.QUERY_WAIT_INTERVALS.value(), TimeUnit.MICROSECONDS)) {
                mark_valid(addr);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void mark_valid(int addr) {

    }

    public boolean[] query(int size) {

    }

    private void mark_invalid(int size) {

    }

    private class RangeNode {
        public int addr = 0;

        RangeNode() {}

        RangeNode(int x) {
            addr = x;
        }
    }
}
