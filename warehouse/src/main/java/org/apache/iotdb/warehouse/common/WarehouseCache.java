package org.apache.iotdb.warehouse.common;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.*;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

public class WarehouseCache {
    Map<String, Integer> partitions = new HashMap<>();
    Map<String, Stream<RowRecord>> cache = new HashMap<>();
    Map<String, Boolean> fin = new HashMap<>();

    int numTuples = 1000000;
    int numTupleBlk = 100000;

    public WarehouseCache() {}

    public void importFromCsv(String file) throws IOException {
        // partition
        String filePath = "./" + file+".csv";
        BufferedReader br = new BufferedReader(new FileReader(filePath));
        int fit = 0;
        BufferedWriter bw = new BufferedWriter(new FileWriter("./"+file + "/" + fit + ".csv"));
        //BufferedWriter helper = new BufferedWriter(new FileWriter("./"+file + "/stat.csv"));
        Scanner sc = new Scanner(br);
        int cnt = 1;
        //double sum = 0.;
        while(sc.hasNext()) {
            String l = sc.nextLine();
            String[] line = l.split(",");
            if(line[0].equals("Time")) continue;
            else bw.write(l);
            if(cnt % numTupleBlk == 0) {
                cnt = 1;
                bw.close();
                fit ++;
                bw = new BufferedWriter(new FileWriter("./"+ file + "/" + fit + ".csv"));
            }
            else {
                cnt += 1;
            }
        }
        if(cnt != numTupleBlk) bw.close();
    }

    public void loadSeriesBlk(String seriesName, TSDataType[] types) throws IOException {
        File path = new File("./"+ seriesName +"/");
        File[] fs = path.listFiles();
        assert fs != null;
        if(!partitions.containsKey(seriesName)) {
            partitions.put(seriesName, -1);
            fin.put(seriesName, false);
        }
        if(fs.length > partitions.get(seriesName)) {
            int next = partitions.get(seriesName) + 1;
            partitions.replace(seriesName, next);
            String filePath = "./" + seriesName +"/" + next +".csv";
            BufferedReader br = new BufferedReader(new FileReader(filePath));
            Stream<RowRecord> data = br.lines().map(new Function<String, RowRecord>() {
                @Override
                public RowRecord apply(String s) {
                    String[] res = s.split(",");
                    List<Field> comp = new ArrayList<>();
                    for(int pos=0; pos < types.length; pos ++) {
                        Field field = new Field(types[pos]);
                        if(types[pos].equals(TSDataType.DOUBLE)) {
                            field.setDoubleV(Double.parseDouble(res[pos+1]));
                        } else field.setLongV(Long.parseLong(res[pos+1]));
                        comp.add(field);
                    }
                    return new RowRecord(Long.parseLong(res[0]), comp);
                }
            });
            if(cache.containsKey(seriesName)) {
                cache.replace(seriesName, data);
            } else cache.put(seriesName,data);
            br.close();
        }
        if(fs.length == partitions.get(seriesName)) {
            fin.replace(seriesName, true);
        }
    }

    public void createBuffer(String node_id, Stream<RowRecord> data) {
        cache.put(node_id,data);
        fin.put(node_id, true);
    }

    public void releaseBuffer(String name) {
        cache.remove(name);
        fin.remove(name);
        partitions.remove(name);
    }

    public Stream<RowRecord> getDataByName(String name) {
        if(cache.containsKey(name)) return cache.get(name);
        return null;
    }
}
