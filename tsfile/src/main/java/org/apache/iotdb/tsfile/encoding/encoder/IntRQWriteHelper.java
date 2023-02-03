package org.apache.iotdb.tsfile.encoding.encoder;

import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class IntRQWriteHelper extends Encoder {
    public static final int DEFAULT_ALLOCATION = 4096;
    private int numOfFlushed = 0;

    public int[] buffer; // 4 * allocated
    private OutputStream os;

    private int curr = 0;
    private int bit_bias;
    private int allocated_size;
    private int actual_allocation;
    private int lb;


    public final static int[] MASK_ARRAY;
    public final static int[] SET_BIT;

    static {
        MASK_ARRAY = new int[32];
        SET_BIT = new int[32];

        int value = 0;
        int mask = 1;
        for(int i=0;i< MASK_ARRAY.length;i++) {
            value = value | mask;
            MASK_ARRAY[i] = value;
            mask = mask << 1;
        }
        for(int i=0;i<SET_BIT.length ;i++) {
            SET_BIT[i] = (1 << i);
        }
    }

    private void clearAndFlush() throws IOException {
        for(int i=0;i<curr;i++) {
            os.write(buffer[i]);
        }
        numOfFlushed += curr;
        buffer = new int[actual_allocation];
        curr = 0;
    }

    public void flushAll() throws IOException {
        for(int i=0;i<=curr;i++) {
            os.write(buffer[i]);
        }
        numOfFlushed += curr;
        buffer = new int[actual_allocation];
        curr = 0;
    }

    public void checkAndFlush() throws IOException {
        if (curr >= allocated_size) {
            clearAndFlush();
        }
        if(bit_bias == 0) writeLB();
    }

    private void writeLB() {
        buffer[curr++] = lb;
        reset();
    }

    private void reset() {
        lb = 0;
        bit_bias = Integer.SIZE;
    }

    public void writeBit() throws IOException {
        lb |= SET_BIT[bit_bias-1];
        bit_bias--;
        if(bit_bias == 0) writeLB();
        checkAndFlush();
    }

    public void skipBit() throws IOException {
        bit_bias--;
        if(bit_bias == 0) writeLB();
        checkAndFlush();
    }

    public void writeBits(int input, int bits) throws IOException {
        input = input & (MASK_ARRAY[bits-1]);
        if(bits > bit_bias) {
            lb |= (input) >>> (bit_bias - bits);
            bits -= bit_bias;
            writeLB();
            lb |= input << (Integer.SIZE - bits);
            bit_bias -= bits;
        } else {
            lb |= (input << (bit_bias - bits));
            bit_bias -= bits;
        }
        checkAndFlush();
    }

    public int upper32(long value) {
        long mask = (1L << 32) - 1;
        return (int) ((value & (mask << 32)) >>> 32);
    }

    public int lower32(long value) {
        long mask = (1L << 32) - 1;
        return (int) (value & mask);
    }

    public IntRQWriteHelper(int allocation_size) {
        super(TSEncoding.RangeQUAL);
        init(allocation_size);
    }

    public IntRQWriteHelper() {
        super(TSEncoding.RangeQUAL);
        init(DEFAULT_ALLOCATION);
    }

    public void setOutputStream(OutputStream os) {
        this.os = os;
    }

    public long used_num_of_bits() {
        return (long) (1 + curr + numOfFlushed) *Integer.SIZE - bit_bias;
    }

    private void init(int allocation_size) {
        buffer = new int[allocation_size + 64];
        allocated_size = allocation_size;
        actual_allocation = allocation_size + 64;
        bit_bias = Integer.SIZE;
        curr = 0;
    }

    @Override
    public void flush(ByteArrayOutputStream out) throws IOException {
        flushAll();
    }

    public static void main(String[] args) throws IOException {
        IntRQWriteHelper rq = new IntRQWriteHelper();
        rq.writeBits(Integer.valueOf("32", 10), 31);
        rq.writeBits(Integer.valueOf("1", 2), 1);
        rq.writeBits(Integer.valueOf("803", 10), 32);
        System.out.println(rq.buffer[0]);
        System.out.println(rq.lb);
    }
}
