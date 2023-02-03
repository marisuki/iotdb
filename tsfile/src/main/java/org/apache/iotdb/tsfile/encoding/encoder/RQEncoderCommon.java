package org.apache.iotdb.tsfile.encoding.encoder;

import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public abstract class RQEncoderCommon extends Encoder{
    public RQEncoderCommon() {
        super(TSEncoding.RangeQUAL);
    }

    public abstract void setOutputStream(OutputStream os);

    public abstract void append(long convertedValue);

    public abstract void append(int convertedValue);

    @Override
    public void flush(ByteArrayOutputStream out) throws IOException {

    }
}
