package org.apache.iotdb.tsfile.encoding.encoder;

import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

public class LongRQEncoder extends Encoder {

    public static final int BUFFERED_BLOCK_SIZE = 4096;
    public int bucket_sz;
    private int allocated_buffer_sz;

    private int NUM_BITS_RLE_MAX; // log2(allocated_buffer_sz) 4096 -> 11'b;
    private int MASK_SELECT_RLE_DUP;
    private int numOfFlushed = 0;

    private LIPair[] data;
    //private long[] diff;
    private int curr = 0;
    private List<List<Integer>> index_buffer;
    private int domain_sz;

    private IntRQWriteHelper writeHelper;
    private OutputStream os;

    private int status;
    private boolean useFastDomainFilter;
    private int usedFastFilterBits;

    private OutputStream summaryOS; //temporary usage to test summary/


    public LongRQEncoder() {
        super(TSEncoding.RangeQUAL);
        data = new LIPair[BUFFERED_BLOCK_SIZE + 64];
        allocated_buffer_sz = BUFFERED_BLOCK_SIZE;
        bucket_sz = 64;
        this.writeHelper = new IntRQWriteHelper();
        init_const();
    }

    public LongRQEncoder(int custom_buffer_sz) {
        super(TSEncoding.RangeQUAL);
        data = new LIPair[custom_buffer_sz + 64];
        allocated_buffer_sz = custom_buffer_sz;
        bucket_sz = 64;
        this.writeHelper = new IntRQWriteHelper();
        init_const();
    }

    public LongRQEncoder(int custom_buffer_sz, int bucket_size, IntRQWriteHelper writeHelper) {
        super(TSEncoding.RangeQUAL);
        data = new LIPair[custom_buffer_sz + 64];
        allocated_buffer_sz = custom_buffer_sz;
        bucket_sz = bucket_size;
        this.writeHelper = writeHelper;
        init_const();
    }

    public void setUseFastDomainFilter(boolean useFastDomainFilter) {
        this.useFastDomainFilter = useFastDomainFilter;
    }

    public void setSummaryOS(OutputStream summaryOS) {
        this.summaryOS = summaryOS;
    }

    public void setUsedFastFilterBits(int usedFastFilterBits) {
        this.usedFastFilterBits = usedFastFilterBits;
    }

    private int num_of_bits_log2(int value) {
        int ans = 0;
        while(value != 0) {value = value >>> 1; ans += 1; }
        return ans;
    }

    private void init_const() {
        //NUM_BITS_RLE_MAX = 0;
        int allocated = allocated_buffer_sz - 1;
        NUM_BITS_RLE_MAX = num_of_bits_log2(allocated);
        //while(allocated != 0) {allocated = allocated >>> 1; NUM_BITS_RLE_MAX += 1; }
        assert NUM_BITS_RLE_MAX < 32; // otherwise we need 274GB RAM for buffer.

        MASK_SELECT_RLE_DUP = (1<<NUM_BITS_RLE_MAX) -1;
        index_buffer = new ArrayList<>();

        status = 1; // listen and encode:1, already pushed to writeHelper: -1

        useFastDomainFilter = false;
        usedFastFilterBits = 0;
    }

    public void setOutputStream(OutputStream os) {
        this.os = os;
        writeHelper.setOutputStream(os);
    }

    public void append(long convertedValue) throws IOException {
        // no output while encoding, use flush to write out
        data[curr] = new LIPair(convertedValue, curr);
        curr ++;
        if(curr >= allocated_buffer_sz) encodeBuffered();
    }

    public double compressRatio() {
        return (64*numOfFlushed) *1.0/extractBitUsage();
    }

    public int getNumOfFlushedDataBits() {
        return numOfFlushed*64;
    }

    public void addAll(Stream<Long> input) {

    }

    private void collectRedundant() {
        long curr = data[0].val;
        int cursor = 0;
        List<Integer> tmp_col = new ArrayList<>();
        tmp_col.add(0);
        for(int i=1;i<this.curr;i++) {
            if(curr == data[i].val) {
                tmp_col.add(i);
            } else {
                data[++cursor] = data[i];
                index_buffer.add(tmp_col);
                tmp_col = new ArrayList<>();
                curr = data[i].val;
            }
        }
        domain_sz = cursor;
    }

    private void encodeSortedBuffer(boolean alreadySorted) throws IOException {
        // discover and collect identify values and indexes.
        collectRedundant();

        long[] diff = new long[domain_sz];
        for(int i=1;i<diff.length;i++) diff[i-1] = data[i].val - data[i-1].val;
        Pair<List<Integer>, Integer> rle = checkRLEApplicable(diff);
        //if(useFastDomainFilter) writePivotAndQuantile();
        //for(int i=0;i<domain_sz;i++) System.out.println(data[i].toString());
        //for(int i=0;i<rle.right;i++) {
        //    System.out.println(diff[i]);
        // }

        // encode values
        int accum = 64;


        // value enc len: 19b  to skip.
        for(int i = 0;i<=rle.right;i++) {
            int dup = rle.left.get(i); // 000000...0-101010...0011(12b < 4096)
            if(dup == 1) accum += 66;
            else accum += 77;
        }
        writeHelper.writeBits(accum, 19);

        // first data point.
        writeHelper.writeBits(writeHelper.upper32(data[0].val), Integer.SIZE);
        writeHelper.writeBits(writeHelper.lower32(data[0].val), Integer.SIZE);

        // value encoding:
        // (1) diff+rle (if rle > 1): '0' + 64b diff + 12b dup (77b)
        // (2) diff (only if rle==1): '10' + 64b diff (66b)
        for(int i = 0;i<=rle.right;i++) {
            long difference = diff[i]; // 64b
            int dup = rle.left.get(i); // 000000...0-101010...0011(12b < 4096)
            if (dup == 1) {
                writeHelper.writeBit();
                writeHelper.skipBit();
                writeHelper.writeBits(writeHelper.upper32(difference), Integer.SIZE);
                writeHelper.writeBits(writeHelper.lower32(difference), Integer.SIZE);
            } else {
                writeHelper.skipBit();
                writeHelper.writeBits(writeHelper.upper32(difference), Integer.SIZE);
                writeHelper.writeBits(writeHelper.lower32(difference), Integer.SIZE);
                writeHelper.writeBits(dup, NUM_BITS_RLE_MAX);
            }
        }

        if(alreadySorted) {
            // '11' + '11-1' for prime encoding only/stop encoder and decoder on a block.
            // index encoding is not needed.
            writeHelper.writeBit();
            writeHelper.writeBit();
            writeHelper.writeBit();
            writeHelper.writeBit();
            writeHelper.writeBit();
            return;
        }


        // control: connect the value encoding part,
        // '110' encoding indexes.
        writeHelper.writeBit();
        writeHelper.writeBit();
        writeHelper.skipBit();

        // encode indexes
        // use 3 strategies:
        // '0'  (1) val: cost=12*N
        // '10' (2) [sz] | one-hot + rle: < 4096
        // '11' (3) diff + rle: 12*12*rle
        int sum = 0;
        boolean[] usedBits = new boolean[allocated_buffer_sz+64];
        for(int i=0;i<this.index_buffer.size();i++) {
            List<Integer> tmp = index_buffer.get(i);
            if(tmp.size()*NUM_BITS_RLE_MAX < (1<<NUM_BITS_RLE_MAX)-sum) {
                int[] dif = new int[tmp.size()];
                int[] dup_dif = new int[tmp.size()];
                int curr_dif = 1, dup_cnt = 1;
                for(int k=1;k<tmp.size();k++) {
                    dif[k] = tmp.get(k) - tmp.get(k-1);
                }
                for(int k=2;k<tmp.size();k++) {
                    if(dif[k] == dif[k-1]) {
                        dup_cnt ++;
                    } else {
                        dup_dif[curr_dif++] = dup_cnt;
                        dup_cnt = 1;
                        dif[curr_dif] = dif[k];
                    }
                }
                if(tmp.size()>2) dup_dif[curr_dif] = dup_cnt;
                if(curr_dif*(2*NUM_BITS_RLE_MAX) >= NUM_BITS_RLE_MAX*tmp.size()) {
                    // use val encoding.
                    // control code=0
                    writeHelper.skipBit();
                    for (Integer integer : tmp) {
                        writeHelper.writeBits(integer, NUM_BITS_RLE_MAX);
                        usedBits[integer] = true;
                    }
                } else {
                    // use diff rle.
                    // control code=11
                    writeHelper.writeBit();
                    writeHelper.writeBit();
                    writeHelper.skipBit();
                    writeHelper.writeBits(tmp.get(0), NUM_BITS_RLE_MAX);
                    for(int k=0;k<=curr_dif;k++) {
                        writeHelper.writeBits(dif[k], NUM_BITS_RLE_MAX);
                        writeHelper.writeBits(dup_dif[k], NUM_BITS_RLE_MAX);
                    }
                    for(Integer x: tmp) usedBits[x] = true;
                }
            } else {
                writeHelper.writeBit();
                writeHelper.skipBit();
                // use one-hot
                // control code=10
                writeHelper.writeBits(allocated_buffer_sz-sum, NUM_BITS_RLE_MAX);
                boolean[] rest = new boolean[allocated_buffer_sz+64];
                for (Integer integer : tmp) {
                    rest[integer] = true;
                }
                for(int k=0;k<allocated_buffer_sz;k++) {
                    if(!usedBits[k]) {
                        if(rest[k]) writeHelper.writeBit();
                        else writeHelper.skipBit();
                        usedBits[k] = rest[k];
                    }
                }
            }
            sum += tmp.size();
        }
        status = -1;
    }

    private void encodeEqValuedBuffer() throws IOException {
        writeHelper.writeBits(77, 19);
        writeHelper.writeBits(writeHelper.upper32(data[0].val), Integer.SIZE);
        writeHelper.writeBits(writeHelper.lower32(data[0].val), Integer.SIZE);
        writeHelper.writeBits(writeHelper.upper32(0L), Integer.SIZE);
        writeHelper.writeBits(writeHelper.lower32(0L), Integer.SIZE);
        writeHelper.writeBits(curr-1, NUM_BITS_RLE_MAX);
        writeHelper.writeBit();
        writeHelper.writeBit();
        writeHelper.writeBit();
    }

    private void writePivotAndQuantile() {
        // ask for sorted data.

    }


    public void encodeBuffered() throws IOException {
        int data_already_sorted = 1, eq=0;
        for(int i=1;i<curr;i++) {
            if(data[i].val > data[i-1].val) data_already_sorted += 1;
            else if (data[i].val == data[i-1].val) eq += 1;
            else {
                data_already_sorted -= 1;
            }
        }
        if(eq + 1 == curr) {
            if(useFastDomainFilter) writePivotAndQuantile();
            encodeEqValuedBuffer();
        } else if (data_already_sorted + eq == curr || data_already_sorted - eq == -curr) {
            if(useFastDomainFilter) writePivotAndQuantile();
            encodeSortedBuffer(true);
        } else {
            Arrays.sort(data, 0, curr);
            if(useFastDomainFilter) writePivotAndQuantile();
            encodeSortedBuffer(false);
        }
        flushAll();
    }

    private void reset() {
        data = new LIPair[allocated_buffer_sz + 64];
        index_buffer = new ArrayList<>();
        curr = 0;
    }

    public void flushAll() throws IOException {
        writeHelper.setOutputStream(os);
        writeHelper.flushAll();
        numOfFlushed += curr;
        reset();
    }

    @Override
    public void flush(ByteArrayOutputStream out) throws IOException {
        //setOutputStream(out);
        flushAll();
    }

    private Pair<List<Integer>, Integer> checkRLEApplicable(long[] diff) {
        List<Integer> ans = new ArrayList<>();
        int cursor = 0;
        int dup = 1;

        for(int i=1;i< diff.length;i++) {
            if(diff[i] == diff[cursor]) {
                dup ++;
            } else {
                ans.add(dup);
                diff[++cursor] = diff[i];

                dup = 1;
            }
        }
        ans.add(dup);
        return new Pair<>(ans, cursor);
    }

    public long extractBitUsage() {
        return writeHelper.used_num_of_bits();
    }

    private static class LIPair implements Comparable<LIPair> {
        long val;
        int id;

        public LIPair(long value, int index) {
            val = value;
            id = index;
        }

        @Override
        public int compareTo(LIPair o) {
            return Long.compare(val, o.val);
        }

        @Override
        public String toString() {
            return "Val: "+ val + "; Index: " + id;
        }
    }

    public static List<List<Long>> generate_dataset
            (String fileName, boolean skipFirstLine, String regex, boolean firstColTimeStamp, boolean needQualify) throws FileNotFoundException {
        Scanner sc = new Scanner(new FileReader(fileName));
        int cont = 0;
        long QUALIFY = 1000000;
        List<List<Long>> dataset = new ArrayList<>();
        int col_sz = 0;
        while (sc.hasNext()) {
            String line = sc.nextLine();
            String[] all = line.split(regex);
            //System.out.println(all[0]);
            if (cont == 0 && skipFirstLine) {
                cont++; continue;
            }
            List<Long> line_data = new ArrayList<>();
            for (int i = 0; i < all.length; i++) {
                if(i==0 && firstColTimeStamp) {
                    long t = java.sql.Timestamp.valueOf(all[i]).getTime();
                    line_data.add(t);
                } else {
                    if(needQualify) {
                        line_data.add((long) (QUALIFY * Double.parseDouble(all[i])));
                    } else {
                        line_data.add(Long.parseLong(all[i]));
                    }
                }
                //dataset[cont][i] = (long) (QUALIFY*Double.parseDouble(all[i]));
            }
            dataset.add(line_data);
            cont++;
        }
        return dataset;
    }


    public static void main(String[] args) throws IOException {
        //String path = "C:\\Users\\RKMar\\Desktop\\projects\\compress\\data\\shore_public.dat";
        String path = "C:\\Users\\RKMar\\Desktop\\projects\\dataset\\Ship.Compress\\w.csv";
        List<List<Long>> dataset =
                generate_dataset(path, true, ",", false, false);
        run(dataset);
    }

    public static void run(List<List<Long>> dataset) throws IOException {
        long total_usage = 0;
        long cost = 0;
        double[] ratio = new double[dataset.get(0).size()];
        System.out.println("Dataset info (row x col): " + dataset.size() + " x " + dataset.get(0).size());
        for(int col=0;col<dataset.get(0).size();col++) {
            System.out.println("Processing:" + col + "/" + dataset.get(0).size());
            LongRQEncoder longRQEncoder = new LongRQEncoder(LongRQEncoder.BUFFERED_BLOCK_SIZE*512);
            OutputStream os = Files.newOutputStream(Paths.get("C:\\Users\\RKMar\\Desktop\\projects\\compress\\result\\RQ_result_" + col +".txt"));
            longRQEncoder.setOutputStream(os);
            long time = System.currentTimeMillis();
            for(int i=0;i< dataset.size();i++) {
                longRQEncoder.append(dataset.get(i).get(col));
                //longRQEncoder.append(dataset[i][col]);
            }
            longRQEncoder.encodeBuffered();
            cost += (System.currentTimeMillis()-time);
            os.close();
            total_usage += longRQEncoder.extractBitUsage();
            ratio[col] = longRQEncoder.compressRatio();
        }
        System.out.println("Total used bits:");
        System.out.println(total_usage);

        System.out.println("Averaged Compression Ratio:");
        System.out.println((64L * dataset.size() * dataset.get(0).size()) * 1.0/ total_usage);

        System.out.println("Time cost:");
        System.out.println(cost);

        System.out.println("Detailed Compression Ratio:");
        for(int i=0;i<ratio.length;i++) {
            System.out.print(i);
            System.out.print("\t");
        }
        System.out.println();
        for(int i=0;i<ratio.length;i++) {
            System.out.print(ratio[i]);
            System.out.print("\t");
        }
        System.out.println();
    }
}
