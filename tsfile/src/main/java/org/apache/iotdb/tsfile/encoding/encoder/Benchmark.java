package org.apache.iotdb.tsfile.encoding.encoder;

import org.apache.iotdb.tsfile.exception.encoding.TsFileEncodingException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Benchmark {

    public static long inMemUsage;
    public static long startMem;
    public static final long BYTE2BIT = 8L;
    public static final long MEGABYTE2BYTE = 1024L*1024L;
    public static BufferedWriter fos;

    public static List<List<Long>> generate_dataset
            (String fileName, boolean skipFirstLine, String regex, boolean firstColTimeStamp, boolean needQualify, boolean skipFirstCol) throws FileNotFoundException {
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
                col_sz = all.length;
                cont++; continue;
            }
            if(col_sz == 0) col_sz = all.length;
            List<Long> line_data = new ArrayList<>();
            for (int i = 0; i < col_sz; i++) {
                if(i==0 && skipFirstCol) continue;
                if(i==0 && firstColTimeStamp) {
                    long t = 0;
                    try {
                        t = java.sql.Timestamp.valueOf(all[i]).getTime();
                    } catch (Exception e) {
                        System.out.println(all[i]);
                    }
                    line_data.add(t);
                } else {
                    String ext;
                    if(i>= all.length || all[i].equals("")) ext = "-1.0";
                    else ext = all[i];
                    if(needQualify) {
                        line_data.add((long) (QUALIFY * Double.parseDouble(ext)));
                    } else {
                        line_data.add(Long.parseLong(ext));
                    }
                }
                //dataset[cont][i] = (long) (QUALIFY*Double.parseDouble(all[i]));
            }
            dataset.add(line_data);
            cont++;
        }
        return dataset;
    }


    public static void floatEnc(List<List<Long>> data, TSEncoding encodingType) throws IOException {
        Encoder encoder = null;


        if (encodingType == TSEncoding.RLE) {
            encoder = new LongRleEncoder();
        } else if (encodingType == TSEncoding.TS_2DIFF) {
            encoder = new DeltaBinaryEncoder.LongDeltaEncoder();
        } else if(encodingType == TSEncoding.GORILLA) {
            encoder = new LongGorillaEncoder();
        } else if (encodingType == TSEncoding.DICTIONARY) {
            encoder = new DictionaryEncoder();
        } else if (encodingType == TSEncoding.ZIGZAG) {
            encoder = new LongZigzagEncoder();
        } else if (encodingType == TSEncoding.PLAIN) {
            //encoder = null;
            encoder = new PlainEncoder(TSDataType.INT64, 10);
        }

        if(encoder!= null) {
            runEnc(data, encoder);
        } else {
            runRQEnc(data);
        }
    }

    public static void runEnc(List<List<Long>> dataset, Encoder encoder) throws IOException {
        long total_usage = 0;
        long cost = 0;
        double[] ratio = new double[dataset.get(0).size()];

        System.out.println("Dataset info (row x col): " + dataset.size() + " x " + dataset.get(0).size());
        for(int col=0;col<dataset.get(0).size();col++) {
            System.out.println("Processing:" + col + "/" + dataset.get(0).size());
            //LongRQEncoder longRQEncoder = new LongRQEncoder(LongRQEncoder.BUFFERED_BLOCK_SIZE*512);
            //OutputStream os = Files.newOutputStream(Paths.get("C:\\Users\\RKMar\\Desktop\\projects\\compress\\result\\RQ_result_" + col +".txt"));
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            long time = System.currentTimeMillis();
            for(int i=0;i< dataset.size();i++) {
                encoder.encode(dataset.get(i).get(col), os);
                //longRQEncoder.append(dataset.get(i).get(col));
                //longRQEncoder.append(dataset[i][col]);
            }
            inMemUsage = Math.max(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory(), inMemUsage);
            encoder.flush(os);
            cost += (System.currentTimeMillis()-time);
            total_usage += ((long) os.size())*BYTE2BIT;
            ratio[col] = (64L * dataset.size())*1.0/((long) os.size()*BYTE2BIT);
            os.close();
        }
        System.out.println("Total used bits:");
        System.out.println(total_usage);
        fos.write(total_usage + " ");

        System.out.println("Averaged Compression Ratio:");
        double avg_cr = (64L * dataset.size() * dataset.get(0).size()) * 1.0/ total_usage;
        System.out.println(avg_cr);
        fos.write(avg_cr + " ");

        System.out.println("Time cost:");
        System.out.println(cost);
        fos.write(cost + " ");

        System.out.println("Memory cost (MB):");
        double mem = (inMemUsage)*1.0/MEGABYTE2BYTE - startMem*1.0/MEGABYTE2BYTE;
        System.out.println(mem);
        fos.write(mem + " ");

        System.out.println("Throughput (MB/s):");
        double through = (8L* dataset.size()*dataset.get(0).size())*1000.0/(MEGABYTE2BYTE*cost);
        System.out.println(through);
        fos.write(through + " ");

        System.out.println("Detailed Compression Ratio:");
        for(int i=0;i<ratio.length;i++) {
            System.out.print(i);
            System.out.print("\t");
        }
        System.out.println();
        for(int i=0;i<ratio.length;i++) {
            System.out.print(ratio[i]);
            System.out.print("\t");
            fos.write(ratio[i] + " ");
        }
        System.out.println();
        fos.write("\n");
    }

    public static List<List<Long>> getData(String key) throws FileNotFoundException {
        String prefix = "C:\\Users\\RKMar\\Desktop\\projects\\dataset\\";
        String[] post =
                {"Open.gas\\gas.csv", "Open.Bitcoin\\bitcoin.csv",
                 "Open.RSSI\\rssi.csv", "Ship\\iot.ship.csv", "Climate\\climate",
                 "Ship.Compress\\w.csv", "gen.noise\\noise4107.csv", "gen.sin\\sin4107.csv"};
        List<List<Long>> dataset = null;
        System.gc();

        switch (key) {
            // open
            case "Gas": dataset = generate_dataset(prefix + post[0], true, ",", true, true, false); break;
            case "Bitcoin": dataset = generate_dataset(prefix + post[1], true, ",", true, true, false); break;
            case "RSSI": dataset = generate_dataset(prefix + post[2], true, ",", true, true, false); break;
            // private
            case "Ship": dataset = generate_dataset(prefix + post[3], true, ",", true, true, false); break;
            case "Climate":
                dataset = new ArrayList<>();
                for(int i=1;i<=10;i++) {
                    dataset.addAll(generate_dataset(prefix + post[4] + i + ".csv", true, ",", true, true, true));
                }
                break;
                // generated
            case "Ship.Compress":
                dataset = generate_dataset(prefix + post[5], true, ",", false, false, false); break;
            case "Noise":
                dataset = generate_dataset(prefix + post[6], true, ",", true, true, false);
                break;
            case "Sin":
                dataset = generate_dataset(prefix + post[7], true, ",", true, true, false);
                break;
        }
        return dataset;
    }

    public static void launch(TSEncoding encodingType, String notes) throws IOException {
        String[] key = {"Gas", "Bitcoin", "RSSI", "Ship", "Climate", "Noise", "Sin", "Ship.Compress"};
        //String[] key = {"Noise", "Sin", "Ship.Compress"}; //, "Ship.Compress"
        //String[] key = {"Ship.Compress"};
        String path = "C:\\Users\\RKMar\\Desktop\\projects\\compress\\stat\\stat"
                + encodingType + notes + ".txt";
        //fos = Files.newOutputStream(Paths.get());
        fos = new BufferedWriter(new FileWriter(path));
        for(String k: key) {
            List<List<Long>> dataset = getData(k);
            Runtime rt = Runtime.getRuntime();
            System.gc();
            startMem = rt.totalMemory() - rt.freeMemory();
            floatEnc(dataset, encodingType);
        }
        fos.close();
    }


    public static void main(String[] args) throws IOException {
        //String path = "C:\\Users\\RKMar\\Desktop\\projects\\compress\\data\\shore_public.dat";

        //String path = "C:\\Users\\RKMar\\Desktop\\projects\\dataset\\Ship.Compress\\w.csv";
        //List<List<Long>> dataset =
        //        generate_dataset(path, true, ",", false, false);
        //runRQEnc(dataset);
        launch(TSEncoding.RangeQUAL, "1024");
    }

    public static void runRQEnc(List<List<Long>> dataset) throws IOException {
        long total_usage = 0;
        long cost = 0;
        double[] ratio = new double[dataset.get(0).size()];
        System.out.println("Dataset info (row x col): " + dataset.size() + " x " + dataset.get(0).size());
        for(int col=0;col<dataset.get(0).size();col++) {
            System.out.println("Processing:" + col + "/" + dataset.get(0).size());
            LongRQEncoder longRQEncoder = new LongRQEncoder(LongRQEncoder.BUFFERED_BLOCK_SIZE*1024);
            //OutputStream os = Files.newOutputStream(Paths.get("C:\\Users\\RKMar\\Desktop\\projects\\compress\\result\\RQ_result_" + col +".txt"));
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            longRQEncoder.setOutputStream(os);
            long time = System.currentTimeMillis();
            for(int i=0;i< dataset.size();i++) {
                longRQEncoder.append(dataset.get(i).get(col));
                //longRQEncoder.append(dataset[i][col]);
            }
            longRQEncoder.encodeBuffered();
            inMemUsage = Math.max(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory(), inMemUsage);
            cost += (System.currentTimeMillis()-time);
            os.close();
            //total_usage += ((long) os.size())*BYTE2BIT;
            //ratio[col] = ((long) os.size())*BYTE2BIT*1.0/(64L * dataset.size());
            total_usage += longRQEncoder.extractBitUsage();
            ratio[col] = longRQEncoder.compressRatio();
        }
        System.out.println("Total used bits:");
        System.out.println(total_usage);
        fos.write(total_usage + " ");

        System.out.println("Averaged Compression Ratio:");
        double avg_cr = (64L * dataset.size() * dataset.get(0).size()) * 1.0/ total_usage;
        System.out.println(avg_cr);
        fos.write(avg_cr + " ");

        System.out.println("Time cost:");
        System.out.println(cost);
        fos.write(cost + " ");

        System.out.println("Memory cost (MB):");
        double mem = (inMemUsage)*1.0/MEGABYTE2BYTE - startMem*1.0/MEGABYTE2BYTE;
        System.out.println(mem);
        fos.write(mem + " ");

        System.out.println("Throughput (MB/s):");
        double through = (8L* dataset.size()*dataset.get(0).size())*1000.0/(MEGABYTE2BYTE*cost);
        System.out.println(through);
        fos.write(through + " ");

        System.out.println("Detailed Compression Ratio:");
        for(int i=0;i<ratio.length;i++) {
            System.out.print(i);
            System.out.print("\t");
        }
        System.out.println();
        for(int i=0;i<ratio.length;i++) {
            System.out.print(ratio[i]);
            System.out.print("\t");
            fos.write(ratio[i] + " ");
        }
        System.out.println();
        fos.write("\n");
    }
}
