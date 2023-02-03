package org.apache.iotdb.tsfile.encoding.encoder;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class RQEncoder extends Encoder{

    public static final int ENCODING_THREAD_QUEUE_SIZE = 100;
    private BlockingQueue<Thread> que;
    private TSDataType dataType;


    public RQEncoder(TSDataType tsDataType) {
        super(TSEncoding.RangeQUAL);
        que = new ArrayBlockingQueue<>(ENCODING_THREAD_QUEUE_SIZE);
        dataType = tsDataType;

    }



    @Override
    public void flush(ByteArrayOutputStream out) throws IOException {

    }
}
