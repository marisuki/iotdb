package org.apache.iotdb.tsfile.encoding.decoder;

import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.io.IOException;
import java.nio.ByteBuffer;

public class LongRQDecoder extends Decoder{



    public LongRQDecoder() {
        super(TSEncoding.RangeQUAL);
    }

    @Override
    public boolean hasNext(ByteBuffer buffer) throws IOException {
        return false;
    }

    @Override
    public void reset() {

    }
}
