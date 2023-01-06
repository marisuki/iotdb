package org.apache.iotdb.warehouse.server.query.simd;

import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.warehouse.common.Dataset;

import java.util.*;
import java.util.function.*;
import java.util.function.BinaryOperator;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
public class HashReduce extends UnaryOperator {

    public HashReduce() {}

    @Override
    public Dataset transform(Dataset data, int refer_id) {
        Dataset ans = new Dataset("" + refer_id);
        ans.setSchema(data.getSchema());
        ans.filter_pos = data.filter_pos;
        ans.version_pos = data.version_pos;
        Stream<RowRecord> d = data.getBuffer().getDataByName(data.getName());
        Map<Long, List<RowRecord>> res = d.collect(
                Collectors.groupingBy(
                        entry -> ((RowRecord) entry).getTimestamp(),
                        Collectors.toList()
                )
        );
        List<RowRecord> tmp = new ArrayList<>();
        for(Long k: res.keySet()) {
            List<RowRecord> rc = res.get(k);
            rc.sort(new Comparator<RowRecord>() {
                @Override
                public int compare(RowRecord o1, RowRecord o2) {
                    int d1 = o1.getFields().get(data.version_pos).getIntV();
                    int d2 = o2.getFields().get(data.version_pos).getIntV();
                    if (d1 < d2) return -1;
                    else return 1;
                }
            });
            boolean flag = false;
            for (int posx : data.filter_pos) {

            }
        }
        return ans;
    }
    @Override
    public String getOperatorName() {
        return null;
    }
}
