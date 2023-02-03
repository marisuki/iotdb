package org.apache.iotdb.tsfile.encoding.encoder;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Scanner;

public class CompareRLE {
    public static final int BUFFERED_BLOCK_SIZE = 4096;
    int allocated_buffer_sz;
    private int numOfFlushed= 0;

    private int NUM_BITS_RLE_MAX; // log2(allocated_buffer_sz) 4096 -> 11'b;
    private int MASK_SELECT_RLE_DUP;

    private long[] data;
    private int data_curr = 0;

    private IntRQWriteHelper writeHelper;
    private OutputStream os;

    public CompareRLE() {
        data = new long[BUFFERED_BLOCK_SIZE + 64];
        writeHelper = new IntRQWriteHelper();
        allocated_buffer_sz = BUFFERED_BLOCK_SIZE;
    }

    public void setWriteHelper(IntRQWriteHelper writeHelper) {
        this.writeHelper = writeHelper;
    }

    public void setOs(OutputStream os) {
        this.os = os;
        writeHelper.setOutputStream(os);
    }

    public void append(long value) throws IOException {
        data[data_curr++] = value;
        if(data_curr >= allocated_buffer_sz) encodeAll();
    }

    public long extractBitUsage() {
        return writeHelper.used_num_of_bits();
    }

    public void reset() {
        numOfFlushed += data_curr;
        data = new long[BUFFERED_BLOCK_SIZE + 64];
        data_curr = 0;
    }

    public void encodeAll() throws IOException {
        long[] diff = new long[BUFFERED_BLOCK_SIZE + 64];
        for(int i=1;i<=data_curr;i++) {
            diff[i] = data[i] - data[i-1];
        }
        writeHelper.writeBits(writeHelper.upper32(data[0]), 32);
        writeHelper.writeBits(writeHelper.lower32(data[0]), 32);
        int curr = 1, dup_item = 1;
        int[] dup = new int[BUFFERED_BLOCK_SIZE + 64];
        for(int i=1;i<=data_curr;i++) {
            if(diff[curr] == diff[i]) {
                dup_item += 1;
            } else {
                dup[curr] = dup_item;
                curr ++;
                diff[curr] = diff[i];
                dup_item = 1;
            }
        }
        dup[curr] = dup_item;
        for(int i=1;i<=curr;i++) {
            writeHelper.writeBits(writeHelper.upper32(diff[i]), 32);
            writeHelper.writeBits(writeHelper.lower32(diff[i]), 32);
            writeHelper.writeBits(dup[i], 32);
        }
        reset();
    }

    public double compressRatio() {
        return 64*(data_curr + numOfFlushed)*1.0/ extractBitUsage();
    }

    public static void run() throws IOException {
        Scanner sc = new Scanner(new FileReader("C:\\Users\\RKMar\\Desktop\\projects\\compress\\data\\shore_public.dat"));
        int cont = 0;
        long QUALIFY = 1000000;
        long cost = 0;
        long[][] dataset = new long[520000][75];
        int col_sz = 0;
        while(sc.hasNext()) {
            String line = sc.nextLine();
            String[] all = line.split("\\s");
            //System.out.println(all[0]);
            if(cont == 0) col_sz = all.length;
            for(int i=0;i<all.length;i++) {
                dataset[cont][i] = (long) (QUALIFY*Double.parseDouble(all[i]));
            }
            cont ++;
        }
        long total_usage = 0;
        double[] ratio = new double[dataset[0].length];
        for(int col=0;col<col_sz;col++) {
            System.out.println(col);
            CompareRLE longRQEncoder = new CompareRLE();
            OutputStream os = Files.newOutputStream(Paths.get("C:\\Users\\RKMar\\Desktop\\projects\\compress\\result\\RLE_result.txt"));
            longRQEncoder.setOs(os);
            long time = System.currentTimeMillis();
            for(int i=0;i< dataset.length;i++) {
                longRQEncoder.append(dataset[i][col]);
            }
            longRQEncoder.encodeAll();
            cost += (System.currentTimeMillis()-time);
            os.close();
            total_usage += longRQEncoder.extractBitUsage();
            ratio[col] = longRQEncoder.compressRatio();
        }
        System.out.println("Total used bits:");
        System.out.println(total_usage);

        System.out.println("Averaged Compression Ratio:");
        System.out.println((64L * cont * col_sz) * 1.0/ total_usage);

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

    public static void main(String[] args) throws IOException {
        run();
        /*
        CompareRLE encoder = new CompareRLE();
        OutputStream os = Files.newOutputStream(Paths.get("C:\\Users\\RKMar\\Desktop\\projects\\compress\\result\\RLE_result.txt"));
        encoder.setOs(os);
        Scanner sc = new Scanner(new FileReader("C:\\Users\\RKMar\\Desktop\\projects\\compress\\data\\shore_public.dat"));
        int cont = 0;
        long QUALIFY = 1000000;
        long cost = 0;
        while(sc.hasNext()) {
            String line = sc.nextLine();
            String[] all = line.split("\\s");
            //System.out.println(all[0]);
            long time = System.currentTimeMillis();
            encoder.append((long) (QUALIFY*Double.parseDouble(all[1])));
            cost += (System.currentTimeMillis()-time);
            cont ++;
            //if(cont == 4096) break;
        }
        os.close();
        System.out.println(encoder.extractBitUsage());
        System.out.println(encoder.compressRatio());
        System.out.println(cost);
        */
    }
}
