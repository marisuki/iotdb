package org.apache.iotdb.db.queryengine.generalgebra.memory;

import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.path.IFullPath;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.db.queryengine.generalgebra.common.Iterator;
import org.apache.iotdb.db.queryengine.generalgebra.config.QueryConfig;
import org.apache.iotdb.db.queryengine.generalgebra.exception.WaitTimeoutException;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.filter.basic.Filter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.iotdb.commons.service.ServiceType.QUERY_MEMORY_MANAGER;

public class MemoryManager implements IService {
    private static final MemoryManager instance = new MemoryManager();

    // data buffer: value and timestamps
    // the times of reorganizing buffer and timestamps should be optimized and minimized
    private List<List<Long>> timestamps = new ArrayList<>();
    private List<Column> buffer = new ArrayList<>();
    // some values may be filtered out by predicates but didn't trigger data movement.
//    private boolean activate[] = new boolean[CAPACITY];
    private ReentrantLock lockAllocate = new ReentrantLock();

    // get token (hashcode) to iterate columns, rows, intermediate results, algebra are translated into iterators
    private ConcurrentHashMap<IFullPath, Iterator> paths = new ConcurrentHashMap<>();
    private static final long MIN_INTERVAL_SECONDS =
            PipeConfig.getInstance().getPipeSubtaskExecutorCronHeartbeatEventIntervalSeconds();

    private MemoryManager() {}

    public void delete(IFullPath path) {
        // recycle necessary buffers.
        Iterator it = paths.get(path);
        try {
            if(lockAllocate.tryLock(MIN_INTERVAL_SECONDS, TimeUnit.SECONDS)) {
                timestamps.get(it.pos()) = null;
                paths.remove(path);
            }
            lockAllocate.unlock();
        } catch (Exception e) {
            throw new WaitTimeoutException("Timeout waiting for lock allocation.");
        }
    }

    Iterator allocate(int numBytes) {
        ColumnIterator iterator = null;
        try {
            if(lockAllocate.tryLock(MIN_INTERVAL_SECONDS, TimeUnit.SECONDS)) {

            }
            lockAllocate.unlock();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return iterator;
    }

    public static MemoryManager getInstance() {
        return instance;
    }

    public int getOccupiedSpace() {
        return buffer.capacity() + activate.length/8 + timestamps.length*4;
    }

    public void clean() throws IOException {
        // to close the memory manager
        timestamps = null;
        buffer.clear(); buffer = null;
        activate = null;
    }

    @Override
    public void start() throws StartupException {
        // nothing to initiate
    }

    @Override
    public void stop() {
        // nothing.
    }

    @Override
    public ServiceType getID() {
        return QUERY_MEMORY_MANAGER;
    }

    private class DataPair {
        List
    }

    private class ColumnIterator implements Iterator {

        private TSDataType dataType;
        private boolean[] valid;
        private int position;
        private int start, end;
        private int location; // location of values in timestamps and buffer

        public ColumnIterator(TSDataType dataType, int size, int hashedLocation) {
            this.dataType = dataType;
            valid = new boolean[size];
            Arrays.fill(valid, false);
            position = 0;
            start = 0;
            end = size;
            location = hashedLocation;
        }

        @Override
        public List<TSDataType> getDataType() {
            return Collections.singletonList(dataType);
        }

        @Override
        public List<TimeValuePair> nextPair() {
            long timestamp = timestamps.get(location).get(position);
            return Collections.singletonList(
                    new TimeValuePair(timestamp, buffer.get(location).getTsPrimitiveType(position)));
        }

        @Override
        public boolean hasNext() {
            return position < end;
        }

        @Override
        public void remove() {
            valid[position] = false;
        }

        @Override
        public void append() {
            
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

}
